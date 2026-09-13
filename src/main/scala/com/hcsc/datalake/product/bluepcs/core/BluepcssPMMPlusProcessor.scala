package com.hcsc.datalake.product.bluepcs.core

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import org.slf4j.MDC
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions.col
import com.typesafe.config.Config
import com.hcsc.datalake.product.application.common.AppTrait
import com.hcsc.datalake.product.bluepcs.common.BluepcssTrait
import com.hcsc.datalake.product.bluepcs.hbase.HbaseOffsetManagement

object BluepcssPMMPlusProcessor extends AppTrait with BluepcssTrait {
  import spark.sqlContext.implicits._

  // Source type constants
  val SOURCE_KAFKA = "kafka"
  val SOURCE_HDFS = "hdfs"

  // Thread pool for parallel tag processing
  private lazy val tagProcessingPool = Executors.newFixedThreadPool(
    Runtime.getRuntime.availableProcessors().min(8)
  )
  private implicit lazy val tagProcessingEC: ExecutionContext =
    ExecutionContext.fromExecutor(tagProcessingPool)

  def processJSONMessages(
    common_conf: Config,
    bluepcs_conf: Config,
    inputRDD: RDD[(Int, Int, String)],        // Renamed: generic input RDD (from Kafka or HDFS)
    offsetDetailsDF: DataFrame,                // Renamed: offset details (empty for HDFS)
    env: String,
    sourceType: String = SOURCE_KAFKA  // Default to kafka for backward compatibility
  ): Unit = {

    val debugMode = spark.conf.get("spark.bluepcs.debug.mode", "false").toBoolean
    val isHdfsSource = sourceType == SOURCE_HDFS
    val sparkProcessingStartMs = System.nanoTime()
    val hdfsCycleId = Option(MDC.get("hdfsCycleId")).getOrElse("na")
    val hdfsFileName = Option(MDC.get("hdfsFileName")).getOrElse("na")
    val hdfsFileSizeBytes = Option(MDC.get("hdfsFileSizeBytes")).getOrElse("na")

    logger.info(s"@@@ processJSONMessages started (sourceType=$sourceType)")

    // DEBUG: Check input RDD
    val inputCount = inputRDD.count()
    logger.info(s"@@@ DEBUG: inputRDD.count() = $inputCount")
    if (inputCount == 0) {
      logger.error("@@@ ERROR: inputRDD is EMPTY - no data to process!")
      return
    }

    try {
      // Log full JSON messages only while debugging.
      if (debugMode) {
        inputRDD.take(10).foreach { case (partition, offset, json) =>
          val sourceLabel = if (isHdfsSource) "HDFS" else "Kafka"
          logger.info(s"@@@ $sourceLabel Record [Partition: $partition, Offset: $offset]: $json")
        }
      }

      inputRDD.persist()

      // Create DataFrame with all 3 parts
      val inputDF = inputRDD.toDF("Partition", "Offset", "JSONData")
      logger.info(s"@@@ DEBUG: inputDF.count() = ${inputDF.count()}")

      // Save raw JSON to HDFS - skip in debug mode or if source is already HDFS (avoid duplicate HDFS write)
      val jsonHdfspath = bluepcs_conf.getString("hdfs_raw_incr_data_path").replace("$hdfs_env_nm", env)
      if (isHdfsSource) {
        logger.info(s"@@@ Skipping raw JSON HDFS write - source is already HDFS")
      } else if (debugMode) {
        logger.info(s"@@@ DEBUG MODE: skipping raw JSON HDFS write to $jsonHdfspath")
      } else {
        logger.info("@@@ Writing jsonData to disk")
        val jsonHDFSDF = inputRDD.map(_._3).toDF("json")
        logger.info("@@@ jsonHdfspath: " + jsonHdfspath)
        jsonHDFSDF.coalesce(1).write.format("text").mode(SaveMode.Append).save(jsonHdfspath)
      }

      // Pull list of tags for RAW_CURPIT
      val rawcur_jsonTagList = bluepcs_conf.getString("raw_cur_hive_table_list")
      logger.info("@@@ RAW_CURPIT tags from config: " + rawcur_jsonTagList)
      val rawcur_jsonTags = rawcur_jsonTagList.split(",")

      val rawcurOffsetDF = if (isHdfsSource) offsetDetailsDF else offsetDetailsDF.filter(col("RowTag").startsWith("RAW_CURPIT"))
      val rawcurStartNs = if (isHdfsSource) System.nanoTime() else 0L
      prepareJSONData(common_conf, bluepcs_conf, inputDF, rawcurOffsetDF, rawcur_jsonTags, "RAW_CURPIT", env, sourceType)
      if (isHdfsSource) {
        logger.info(
          "PERF_HDFS sparkLayerProcessing cycleId={} fileName={} fileSizeBytes={} layer={} elapsedMs={}",
          hdfsCycleId,
          hdfsFileName,
          hdfsFileSizeBytes,
          "RAW_CURPIT",
          elapsedMsSince(rawcurStartNs): java.lang.Long
        )
      }

      // Pull list of tags for GOLD_PIT
      val gold_jsonTagList = bluepcs_conf.getString("gold_hive_table_list")
      logger.info("@@@ GOLD_PIT tags from config: " + gold_jsonTagList)
      val gold_jsonTags = gold_jsonTagList.split(",")

      val goldOffsetDF = offsetDetailsDF.filter(col("RowTag").startsWith("GOLD_PIT"))
      val goldPitStartNs = if (isHdfsSource) System.nanoTime() else 0L
      prepareJSONData(common_conf, bluepcs_conf, inputDF, goldOffsetDF, gold_jsonTags, "GOLD_PIT", env, sourceType)
      if (isHdfsSource) {
        logger.info(
          "PERF_HDFS sparkLayerProcessing cycleId={} fileName={} fileSizeBytes={} layer={} elapsedMs={}",
          hdfsCycleId,
          hdfsFileName,
          hdfsFileSizeBytes,
          "GOLD_PIT",
          elapsedMsSince(goldPitStartNs): java.lang.Long
        )
      }
    } finally {
      inputRDD.unpersist()
    }

    if (isHdfsSource) {
      logger.info(
        "PERF_HDFS sparkProcessing cycleId={} fileName={} fileSizeBytes={} elapsedMs={}",
        hdfsCycleId,
        hdfsFileName,
        hdfsFileSizeBytes,
        elapsedMsSince(sparkProcessingStartMs): java.lang.Long
      )
    }
  }

  def prepareJSONData(common_conf: Config, bluepcs_conf: Config, inputDF: DataFrame, offsetDF: DataFrame, jsonTags: Array[String], layer: String, env: String, sourceType: String = SOURCE_KAFKA): Unit = {

    val debugMode = spark.conf.get("spark.bluepcs.debug.mode", "false").toBoolean
    val isHdfsSource = sourceType == SOURCE_HDFS
    val parallelTagProcessing = spark.conf.get("spark.bluepcs.parallel.tags", "false").toBoolean

    logger.info(s"@@@ prepareJSONData: inputDF.count=${inputDF.count()}, offsetDF.isEmpty=${offsetDF.rdd.isEmpty()}")

    if (parallelTagProcessing && jsonTags.length > 1) {
      logger.info(s"@@@ Processing ${jsonTags.length} tags in PARALLEL for layer=$layer")
      processTagsInParallel(common_conf, bluepcs_conf, inputDF, offsetDF, jsonTags, layer, env, sourceType, debugMode, isHdfsSource)
    } else {
      logger.info(s"@@@ Processing ${jsonTags.length} tags SEQUENTIALLY for layer=$layer")
      jsonTags.foreach(tag => processTag(common_conf, bluepcs_conf, inputDF, offsetDF, tag, layer, env, sourceType, debugMode, isHdfsSource))
    }
  }

  private def processTagsInParallel(
    common_conf: Config,
    bluepcs_conf: Config,
    inputDF: DataFrame,
    offsetDF: DataFrame,
    jsonTags: Array[String],
    layer: String,
    env: String,
    sourceType: String,
    debugMode: Boolean,
    isHdfsSource: Boolean
  ): Unit = {
    val futures = jsonTags.map { tag =>
      Future {
        try {
          processTag(common_conf, bluepcs_conf, inputDF, offsetDF, tag, layer, env, sourceType, debugMode, isHdfsSource)
          (tag, None)
        } catch {
          case ex: Exception =>
            logger.error(s"@@@ PARALLEL: Failed to process tag=$tag in layer=$layer", ex)
            (tag, Some(ex))
        }
      }
    }

    val results = Await.result(Future.sequence(futures.toSeq), 60.minutes)
    val failures = results.collect { case (tag, Some(ex)) => (tag, ex) }

    if (failures.nonEmpty) {
      logger.error(s"@@@ PARALLEL: ${failures.size}/${jsonTags.length} tags failed for layer=$layer: ${failures.map(_._1).mkString(", ")}")
      throw new RuntimeException(s"Parallel tag processing failed for ${failures.size} tags: ${failures.map(_._1).mkString(", ")}")
    }

    logger.info(s"@@@ PARALLEL: All ${jsonTags.length} tags completed successfully for layer=$layer")
  }

  private def processTag(
    common_conf: Config,
    bluepcs_conf: Config,
    inputDF: DataFrame,
    offsetDF: DataFrame,
    tag: String,
    layer: String,
    env: String,
    sourceType: String,
    debugMode: Boolean,
    isHdfsSource: Boolean
  ): Unit = {
    val tagStartTime = System.currentTimeMillis()
    logger.info(s"@@@ Processing tag: $tag (inputDF.count=${inputDF.count()}, offsetDF.isEmpty=${offsetDF.rdd.isEmpty()})")

    val filteredDF = if (!offsetDF.rdd.isEmpty()) {
      logger.info("@@@ offsetDF is not empty, filtering by offset (Kafka mode)")
      inputDF.join(offsetDF, (inputDF.col("Partition") === offsetDF.col("Partition"))
        && (inputDF.col("Offset") > offsetDF.col("Offset")) && (offsetDF.col("RowTag") === tag), "inner")
        .select(inputDF("Partition"), inputDF("Offset"), inputDF("JSONData"))
    } else {
      logger.info("@@@ offsetDF is empty, using full input data (HDFS mode)")
      inputDF
    }

    logger.info(s"@@@ filteredDF.count = ${filteredDF.count()}")

    if (!filteredDF.rdd.isEmpty()) {
      val jsonRDD = filteredDF.rdd.map { case x: Row => x(2).asInstanceOf[String] }

      if (layer == "RAW_CURPIT") {
        logger.info("@@@ Calling runBluepcsJsonToRawCur")
        BluepcssPMMPlusLoader.runBluepcsJsonToRawCur(common_conf, bluepcs_conf, tag, jsonRDD, env)

        if (isHdfsSource) {
          logger.info(s"@@@ HDFS MODE: skipping updateKafkaOffsetDetails for tag=$tag, layer=$layer (HDFS uses file lifecycle for idempotency)")
        } else if (debugMode) {
          logger.info(s"@@@ DEBUG MODE: skipping updateKafkaOffsetDetails for tag=$tag, layer=$layer")
        } else {
          logger.info("@@@ Calling updateKafkaOffsetDetails")
          HbaseOffsetManagement.updateKafkaOffsetDetails(tag, layer, filteredDF)
          logger.info("@@@ Updated KafkaOffsetDetails successfully")
        }

      } else if (layer == "GOLD_PIT") {
        logger.info("@@@ Calling runBluepcsJsonToGold")
        BluepcssPMMPlusLoader.runBluepcsJsonToGold(common_conf, bluepcs_conf, tag, jsonRDD, env)

        if (isHdfsSource) {
          logger.info(s"@@@ HDFS MODE: skipping updateKafkaOffsetDetails for tag=$tag, layer=$layer (HDFS uses file lifecycle for idempotency)")
        } else if (debugMode) {
          logger.info(s"@@@ DEBUG MODE: skipping updateKafkaOffsetDetails for tag=$tag, layer=$layer")
        } else {
          logger.info("@@@ Kafka offset update for GOLD_PIT is currently disabled in this flow")
        }
      }

      val elapsed = System.currentTimeMillis() - tagStartTime
      logger.info(s"@@@ Tag $tag completed in ${elapsed}ms for layer=$layer")
    }
  }

  private def elapsedMsSince(startNs: Long): Long =
    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs)

}
