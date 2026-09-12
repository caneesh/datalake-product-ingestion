package com.hcsc.datalake.product.bluepcs.core

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.slf4j.{LoggerFactory, MDC}
import com.hcsc.datalake.product.application.common.AppTrait

/**
 * Message processor that bridges HDFS file input to the existing
 * BluepcssPMMPlusProcessor pipeline.
 *
 * Supports both single-file and batch processing modes.
 */
class BluepcssPMMPlusMessageProcessor(
  common_conf: Config,
  bluepcs_conf: Config,
  env: String
) extends AppTrait {

  // Uses logger from AppTrait

  /**
   * Process a single message (file content).
   * Used in sequential processing mode.
   */
  def process(message: HdfsInputMessage): Unit = {
    val startTime = System.currentTimeMillis()

    try {
      MDC.put("hdfsCycleId", message.messageId)
      MDC.put("hdfsFileName", message.sourceFile)
      MDC.put("hdfsFileSizeBytes", message.payload.length.toString)

      logger.info(s"@@@ Processing HDFS message: messageId=${message.messageId}, sourceFile=${message.sourceFile}")

      val jsonRDD: RDD[(Int, Int, String)] = spark.sparkContext.parallelize(
        Seq((0, 0, message.payload))
      )

      val emptyOffsetDF = createEmptyOffsetDF()

      BluepcssPMMPlusProcessor.processJSONMessages(
        common_conf,
        bluepcs_conf,
        jsonRDD,
        emptyOffsetDF,
        env,
        sourceType = BluepcssPMMPlusProcessor.SOURCE_HDFS
      )

      val elapsed = System.currentTimeMillis() - startTime
      logger.info(s"@@@ HDFS message processing completed: messageId=${message.messageId}, elapsed=${elapsed}ms")

    } finally {
      MDC.remove("hdfsCycleId")
      MDC.remove("hdfsFileName")
      MDC.remove("hdfsFileSizeBytes")
    }
  }

  /**
   * Process a batch of messages as a single Spark job.
   * Most efficient mode - reduces Spark job overhead.
   */
  def processBatch(messages: Seq[HdfsInputMessage]): Unit = {
    if (messages.isEmpty) {
      logger.warn("@@@ processBatch called with empty message list")
      return
    }

    val batchId = s"batch-${System.currentTimeMillis()}"
    val startTime = System.currentTimeMillis()
    val totalBytes = messages.map(_.payload.length).sum

    try {
      MDC.put("hdfsCycleId", batchId)
      MDC.put("hdfsFileName", s"batch_${messages.size}_files")
      MDC.put("hdfsFileSizeBytes", totalBytes.toString)

      logger.info(s"@@@ Processing HDFS batch: batchId=$batchId, fileCount=${messages.size}, totalBytes=$totalBytes")

      // Create RDD from all messages in batch
      // Each message gets a unique partition/offset for tracking
      val batchData: Seq[(Int, Int, String)] = messages.zipWithIndex.map { case (msg, idx) =>
        (idx, idx, msg.payload)
      }

      val jsonRDD: RDD[(Int, Int, String)] = spark.sparkContext.parallelize(batchData,
        math.min(messages.size, spark.sparkContext.defaultParallelism)
      )

      val emptyOffsetDF = createEmptyOffsetDF()

      logger.info(s"@@@ Batch RDD created with ${jsonRDD.count()} records")

      BluepcssPMMPlusProcessor.processJSONMessages(
        common_conf,
        bluepcs_conf,
        jsonRDD,
        emptyOffsetDF,
        env,
        sourceType = BluepcssPMMPlusProcessor.SOURCE_HDFS
      )

      val elapsed = System.currentTimeMillis() - startTime
      logger.info(
        "@@@ HDFS batch processing completed: batchId={}, fileCount={}, totalBytes={}, elapsed={}ms",
        batchId,
        messages.size: Integer,
        totalBytes: Integer,
        elapsed: java.lang.Long
      )

    } finally {
      MDC.remove("hdfsCycleId")
      MDC.remove("hdfsFileName")
      MDC.remove("hdfsFileSizeBytes")
    }
  }

  /**
   * Create an empty offset DataFrame for HDFS mode.
   * In HDFS mode, we don't use HBase offsets - file lifecycle handles idempotency.
   */
  private def createEmptyOffsetDF(): DataFrame = {
    import spark.implicits._
    // Column order must match: (Partition, Offset, RowTag) - same as HBase offset table schema
    Seq.empty[(Int, Int, String)].toDF("Partition", "Offset", "RowTag")
  }
}
