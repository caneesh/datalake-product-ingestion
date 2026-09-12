package com.hcsc.datalake.product.bluepcs.core

import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}

class HdfsFilePoller(
  fs: FileSystem,
  pollingConfig: HdfsPollingConfig,
  processor: BluepcssPMMPlusMessageProcessor
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val running = new AtomicBoolean(false)

  @volatile private var scheduler: ScheduledExecutorService = _

  private val claimingPool: ExecutorService = Executors.newFixedThreadPool(
    pollingConfig.parallelFileClaims,
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, "hdfs-file-claimer")
        t.setDaemon(true)
        t
      }
    }
  )

  private val lifecycleManager = new FileLifecycleManager(fs, pollingConfig)

  private val uniqueSuffixPattern = "\\.[a-f0-9]{8}$".r

  def start(): Unit = {
    if (running.compareAndSet(false, true)) {
      try {
        logger.info("Starting HDFS file poller with config: {}", pollingConfig)
        ensureBaseDirectories()

        if (pollingConfig.recoverStrandedOnStartup) {
          recoverStrandedFiles()
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
          override def newThread(r: Runnable): Thread = {
            val t = new Thread(r, "hdfs-file-poller")
            t.setDaemon(true)
            t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
              override def uncaughtException(thread: Thread, ex: Throwable): Unit = {
                logger.error(s"CRITICAL: Uncaught exception in thread [${thread.getName}]. " +
                  s"HDFS file poller may have stopped. Manual restart may be required.", ex)
              }
            })
            t
          }
        })

        scheduler.scheduleWithFixedDelay(
          new Runnable {
            override def run(): Unit = safePoll()
          },
          0L,
          pollingConfig.pollIntervalMs,
          TimeUnit.MILLISECONDS
        )
      } catch {
        case ex: Exception =>
          running.set(false)
          logger.error("Failed to start HDFS file poller", ex)
      }
    }
  }

  def stop(): Unit = {
    if (running.compareAndSet(true, false)) {
      logger.info("Stopping HDFS file poller...")
      if (scheduler != null) {
        scheduler.shutdown()
        try {
          if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
            scheduler.shutdownNow()
          }
        } catch {
          case _: InterruptedException =>
            scheduler.shutdownNow()
        }
      }
      claimingPool.shutdown()
      logger.info("HDFS file poller stopped")
    }
  }

  private def safePoll(): Unit = {
    try {
      pollOnce()
    } catch {
      case ex: Exception =>
        logger.error("Unexpected error during HDFS polling cycle", ex)
    }
  }

  private[core] def pollOnce(): Unit = {
    tryArchiveCompletedFiles()

    val candidates = try {
      listEligibleFiles()
    } catch {
      case ex: Exception =>
        logger.error("Failed to list eligible files from incoming directory. Will retry next poll cycle.", ex)
        return
    }

    if (candidates.isEmpty) {
      logger.debug("No eligible files found in incoming directory")
      return
    }

    logger.info("Found [{}] eligible files in incoming directory", candidates.size)

    val filesToProcess = candidates.take(pollingConfig.maxFilesPerPoll)

    if (pollingConfig.batchProcessing) {
      processBatch(filesToProcess)
    } else {
      processSequentially(filesToProcess)
    }
  }

  /**
   * BATCH PROCESSING MODE (Recommended - Most Efficient)
   *
   * 1. Claim multiple files in parallel using thread pool
   * 2. Read all claimed files in parallel using thread pool
   * 3. Process entire batch as single Spark job (like Kafka mode)
   * 4. Archive successful files, move failed to error dir
   */
  private def processBatch(candidates: Seq[FileStatus]): Unit = {
    val batchStartTime = System.currentTimeMillis()
    logger.info("Starting batch processing of {} files", candidates.size)

    // Step 1: Claim files in parallel
    val claimedFiles = claimFilesInParallel(candidates)

    if (claimedFiles.isEmpty) {
      logger.warn("No files could be claimed for processing")
      return
    }

    logger.info("Successfully claimed {} files for batch processing", claimedFiles.size)

    // Step 2: Read all files in parallel using thread pool
    val readFutures = claimedFiles.map { case (processingPath, claimedAt) =>
      claimingPool.submit(new Callable[Either[Path, (Path, String, Long)]] {
        override def call(): Either[Path, (Path, String, Long)] = {
          try {
            val content = readWholeFile(processingPath)
            Right((processingPath, content, claimedAt))
          } catch {
            case ex: Exception =>
              logger.error(s"Failed to read file [$processingPath]. Will move to error directory.", ex)
              Left(processingPath)
          }
        }
      })
    }

    // Collect results from parallel reads
    val readResults = readFutures.map { future =>
      try {
        future.get(120, TimeUnit.SECONDS)
      } catch {
        case ex: Exception =>
          logger.error("Timeout or error waiting for file read", ex)
          Left(new Path("/unknown"))
      }
    }

    val batchPayload = readResults.collect { case Right(payload) => payload }
    val failedReads = readResults.collect { case Left(path) => path }.filter(_.getName != "unknown")

    // Move failed reads to error directory
    failedReads.foreach { path =>
      moveToErrorDir(path, "Read failed or timed out")
    }

    if (batchPayload.isEmpty) {
      logger.warn("All files failed to read. Batch processing skipped.")
      return
    }

    logger.info(s"Read ${batchPayload.size} files successfully in parallel, ${failedReads.size} failed")

    // Step 3: Process batch through Spark
    val successfulFiles = ArrayBuffer[Path]()
    val processingFailures = ArrayBuffer[(Path, String)]()

    try {
      // Build batch message with all file contents
      val batchMessages = batchPayload.map { case (path, content, claimedAt) =>
        HdfsInputMessage(
          messageId = generateMessageId(path),
          payload = content,
          sourceFile = path.toString,
          claimedAt = claimedAt
        )
      }.toSeq

      // Process entire batch as one Spark job
      processor.processBatch(batchMessages)

      // All succeeded
      successfulFiles ++= batchPayload.map(_._1)

    } catch {
      case ex: Exception =>
        logger.error("Batch processing failed. Will process files individually for granular error handling.", ex)

        // Fallback: process remaining files individually to isolate failures
        batchPayload.foreach { case (path, content, claimedAt) =>
          try {
            val message = HdfsInputMessage(
              messageId = generateMessageId(path),
              payload = content,
              sourceFile = path.toString,
              claimedAt = claimedAt
            )
            processor.process(message)
            successfulFiles += path
          } catch {
            case fileEx: Exception =>
              logger.error(s"Individual file processing failed for [$path]", fileEx)
              processingFailures += ((path, fileEx.getMessage))
          }
        }
    }

    // Step 4: Archive successful files, move failures to error
    successfulFiles.foreach { path =>
      try {
        lifecycleManager.archiveFile(path, pollingConfig.archiveDir)
        logger.debug("Archived successfully processed file: {}", path)
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to archive file [$path]. File remains in processing dir.", ex)
      }
    }

    processingFailures.foreach { case (path, errorMsg) =>
      moveToErrorDir(path, s"Processing failed: $errorMsg")
    }

    val elapsed = System.currentTimeMillis() - batchStartTime
    logger.info(
      "Batch processing completed: {} files processed, {} succeeded, {} failed, elapsed={}ms",
      batchPayload.size: Integer,
      successfulFiles.size: Integer,
      processingFailures.size: Integer,
      elapsed: java.lang.Long
    )
  }

  /**
   * Claim multiple files in parallel using thread pool.
   * Returns list of (processingPath, claimedAt) for successfully claimed files.
   */
  private def claimFilesInParallel(candidates: Seq[FileStatus]): Seq[(Path, Long)] = {
    val futures = candidates.map { status =>
      claimingPool.submit(new Callable[(Option[Path], Long)] {
        override def call(): (Option[Path], Long) = {
          val claimedAt = System.currentTimeMillis()
          val result = claimFileForProcessing(status.getPath)
          (result, claimedAt)
        }
      })
    }

    futures.flatMap { future =>
      try {
        val (optPath, claimedAt) = future.get(60, TimeUnit.SECONDS)
        optPath.map(p => (p, claimedAt))
      } catch {
        case ex: Exception =>
          logger.warn("Failed to claim file within timeout", ex)
          None
      }
    }
  }

  /**
   * SEQUENTIAL PROCESSING MODE (Fallback - Original behavior)
   */
  private def processSequentially(candidates: Seq[FileStatus]): Unit = {
    candidates.foreach { status =>
      try {
        processSingleFile(status)
      } catch {
        case ex: Exception =>
          logger.error(s"Unexpected error processing file [${status.getPath}]. Continuing with next file.", ex)
      }
    }
  }

  private def processSingleFile(status: FileStatus): Unit = {
    val incomingFile = status.getPath
    val claimedAt = System.currentTimeMillis()

    logger.info("Attempting to claim file [{}]", incomingFile)

    claimFileForProcessing(incomingFile) match {
      case None =>
        logger.warn("Could not claim file [{}]. It may be processed by another runner.", incomingFile)

      case Some(processingFile) =>
        val startTs = System.currentTimeMillis()
        logger.info("Claimed file [{}] as [{}]", incomingFile: Any, processingFile: Any)

        try {
          val payload = readWholeFile(processingFile)
          val message = HdfsInputMessage(
            messageId = generateMessageId(processingFile),
            payload = payload,
            sourceFile = processingFile.toString,
            claimedAt = claimedAt
          )

          logger.info(
            "Processing file [{}], messageId=[{}], payloadLength=[{}]",
            processingFile,
            message.messageId,
            payload.length: java.lang.Integer
          )

          lifecycleManager.writeMarker(processingFile, FileLifecycleManager.STARTED_MARKER,
            s"started=${System.currentTimeMillis()}\nmessageId=${message.messageId}")

          processor.process(message)

          lifecycleManager.writeMarker(processingFile, FileLifecycleManager.COMPLETED_MARKER,
            s"completed=${System.currentTimeMillis()}\nmessageId=${message.messageId}")

          try {
            lifecycleManager.archiveFile(processingFile, pollingConfig.archiveDir)
            logger.info("Archived file [{}] after successful processing", processingFile)
          } catch {
            case archiveEx: Exception =>
              logger.error(s"Failed to archive file [$processingFile]. Processing succeeded but file remains.", archiveEx)
          }

          val elapsed = System.currentTimeMillis() - startTs
          logger.info(s"Successfully processed file [$processingFile] in ${elapsed}ms")

        } catch {
          case ex: Exception =>
            logger.error(s"Failed to process file [$processingFile]", ex)
            moveToErrorDir(processingFile, ex.getMessage)
        }
    }
  }

  private def claimFileForProcessing(incomingFile: Path): Option[Path] = {
    try {
      val fileName = incomingFile.getName
      val processingFile = new Path(pollingConfig.processingDir, fileName)

      if (fs.rename(incomingFile, processingFile)) {
        Some(processingFile)
      } else {
        None
      }
    } catch {
      case ex: Exception =>
        logger.warn(s"Exception while claiming file [$incomingFile]", ex)
        None
    }
  }

  private def readWholeFile(path: Path): String = {
    val in = fs.open(path)
    try {
      org.apache.commons.io.IOUtils.toString(in, "UTF-8")
    } finally {
      in.close()
    }
  }

  private def generateMessageId(path: Path): String = {
    val fileName = path.getName
    val baseName = uniqueSuffixPattern.replaceFirstIn(fileName, "")
    s"hdfs-${baseName}-${System.currentTimeMillis()}"
  }

  private def moveToErrorDir(path: Path, reason: String): Unit = {
    try {
      val errorFile = new Path(pollingConfig.errorDir, path.getName)
      fs.rename(path, errorFile)

      val reasonFile = new Path(pollingConfig.errorDir, path.getName + ".error")
      val out = fs.create(reasonFile, true)
      try {
        out.writeBytes(s"error_time=${System.currentTimeMillis()}\nreason=$reason\n")
      } finally {
        out.close()
      }

      logger.info("Moved failed file [{}] to error directory", path)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to move file [$path] to error directory", ex)
    }
  }

  private def ensureBaseDirectories(): Unit = {
    Seq(
      pollingConfig.incomingDir,
      pollingConfig.processingDir,
      pollingConfig.archiveDir,
      pollingConfig.errorDir
    ).foreach { dir =>
      val path = new Path(dir)
      if (!fs.exists(path)) {
        fs.mkdirs(path)
        logger.info("Created directory: {}", dir)
      }
    }
  }

  private def recoverStrandedFiles(): Unit = {
    try {
      val processingPath = new Path(pollingConfig.processingDir)
      if (fs.exists(processingPath)) {
        val stranded = fs.listStatus(processingPath).filter { status =>
          !status.isDirectory && status.getPath.getName.endsWith(pollingConfig.fileExtension)
        }

        if (stranded.nonEmpty) {
          logger.warn("Found {} stranded files in processing directory. Moving back to incoming.", stranded.length)
          stranded.foreach { status =>
            val incomingPath = new Path(pollingConfig.incomingDir, status.getPath.getName)
            try {
              fs.rename(status.getPath, incomingPath)
              logger.info(s"Recovered stranded file: ${status.getPath} -> $incomingPath")
            } catch {
              case ex: Exception =>
                logger.error(s"Failed to recover stranded file [${status.getPath}]", ex)
            }
          }
        }
      }
    } catch {
      case ex: Exception =>
        logger.error("Error during stranded file recovery", ex)
    }
  }

  private def tryArchiveCompletedFiles(): Unit = {
    try {
      val processingPath = new Path(pollingConfig.processingDir)
      if (fs.exists(processingPath)) {
        val completedMarkers = fs.listStatus(processingPath).filter { status =>
          status.getPath.getName.endsWith(FileLifecycleManager.COMPLETED_MARKER)
        }

        completedMarkers.foreach { marker =>
          val baseName = marker.getPath.getName.stripSuffix(FileLifecycleManager.COMPLETED_MARKER)
          val dataFile = new Path(pollingConfig.processingDir, baseName)

          if (fs.exists(dataFile)) {
            try {
              lifecycleManager.archiveFile(dataFile, pollingConfig.archiveDir)
              fs.delete(marker.getPath, false)
              logger.info("Archived completed file: {}", dataFile)
            } catch {
              case ex: Exception =>
                logger.error(s"Failed to archive completed file [$dataFile]", ex)
            }
          } else {
            fs.delete(marker.getPath, false)
          }
        }
      }
    } catch {
      case ex: Exception =>
        logger.warn("Error during completed file archival check", ex)
    }
  }

  private def listEligibleFiles(): Seq[FileStatus] = {
    val incomingPath = new Path(pollingConfig.incomingDir)
    if (!fs.exists(incomingPath)) {
      return Seq.empty
    }

    val now = System.currentTimeMillis()

    fs.listStatus(incomingPath)
      .filter { status =>
        !status.isDirectory &&
        status.getPath.getName.endsWith(pollingConfig.fileExtension) &&
        (now - status.getModificationTime) >= pollingConfig.fileStabilityMs &&
        status.getLen <= pollingConfig.maxFileSizeBytes &&
        status.getLen > 0
      }
      .sortBy(_.getModificationTime)
      .toSeq
  }
}

case class HdfsInputMessage(
  messageId: String,
  payload: String,
  sourceFile: String,
  claimedAt: Long
)

// FileLifecycleManager is defined in FileLifecycleManager.scala
