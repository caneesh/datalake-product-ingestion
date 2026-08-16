package com.hcsc.datalake.product.bluepcs.core

import java.util.concurrent.TimeUnit
import org.slf4j.MDC

object BluepcssPMMPlusProcessor extends AppTrait with BluepcssTrait {
  import spark.sqlContext.implicits._

  // Source type constants
  val SOURCE_KAFKA = "kafka"
  val SOURCE_HDFS = "hdfs"

  def processJSONMessages(
    common_conf: Config,
    bluepcs_conf: Config,
    kafkaRDD: RDD[(Int, Int, String)],
    kafkaOffsetDetailsDF: DataFrame,
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

    try {
      // Log full JSON messages only while debugging.
      if (debugMode) {
        kafkaRDD.take(10).foreach { case (partition, offset, json) =>
          val sourceLabel = if (isHdfsSource) "HDFS" else "Kafka"
          logger.info(s"@@@ $sourceLabel Record [Partition: $partition, Offset: $offset]: $json")
        }
      }

      kafkaRDD.persist()

      // Create DataFrame with all 3 parts
      val kafkaDF = kafkaRDD.toDF("Partition", "Offset", "JSONData")

      // Save raw JSON to HDFS - skip in debug mode or if source is already HDFS (avoid duplicate HDFS write)
      val jsonHdfspath = bluepcs_conf.getString("hdfs_raw_incr_data_path").replace("$hdfs_env_nm", env)
      if (isHdfsSource) {
        logger.info(s"@@@ Skipping raw JSON HDFS write - source is already HDFS")
      } else if (debugMode) {
        logger.info(s"@@@ DEBUG MODE: skipping raw JSON HDFS write to $jsonHdfspath")
      } else {
        logger.info("@@@ Writing jsonData to disk")
        val jsonHDFSDF = kafkaRDD.map(_._3).toDF("json")
        logger.info("@@@ jsonHdfspath: " + jsonHdfspath)
        jsonHDFSDF.coalesce(1).write.format("text").mode(SaveMode.Append).save(jsonHdfspath)
      }

      // Pull list of tags for RAW_CURPIT
      val rawcur_jsonTagList = bluepcs_conf.getString("raw_cur_hive_table_list")
      logger.info("@@@ RAW_CURPIT tags from config: " + rawcur_jsonTagList)
      val rawcur_jsonTags = rawcur_jsonTagList.split(",")

      val offsetDetailsDF = if (isHdfsSource) kafkaOffsetDetailsDF else kafkaOffsetDetailsDF.filter(col("RowTag").startsWith("RAW_CURPIT"))
      val rawcurStartNs = if (isHdfsSource) System.nanoTime() else 0L
      prepareJSONData(common_conf, bluepcs_conf, kafkaDF, offsetDetailsDF, rawcur_jsonTags, "RAW_CURPIT", env, sourceType)
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

      val JsonoffsetDetailsDF = kafkaOffsetDetailsDF.filter(col("RowTag").startsWith("GOLD_PIT"))
      val goldPitStartNs = if (isHdfsSource) System.nanoTime() else 0L
      prepareJSONData(common_conf, bluepcs_conf, kafkaDF, JsonoffsetDetailsDF, gold_jsonTags, "GOLD_PIT", env, sourceType)
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
      kafkaRDD.unpersist()
    }

    if (isHdfsSource) {
      logger.info(
        "PERF_HDFS sparkProcessing cycleId={} fileName={} fileSizeBytes={} elapsedMs={}",
        hdfsCycleId,
        hdfsFileName,
        hdfsFileSizeBytes,
        elapsedMsSince(sparkProcessingStartNs): java.lang.Long
      )
    }
  }

  def prepareJSONData(common_conf: Config, bluepcs_conf: Config, kafkaDF: DataFrame, OffsetDetailsDF: DataFrame, jsonTags: Array[String], layer: String, env: String, sourceType: String = SOURCE_KAFKA): Unit = {

    val debugMode = spark.conf.get("spark.bluepcs.debug.mode", "false").toBoolean
    val isHdfsSource = sourceType == SOURCE_HDFS

    jsonTags.foreach(tag => {
      logger.info("@@@ Processing tag: " + tag)

      val kafkaFilteredDF = if (!OffsetDetailsDF.rdd.isEmpty()) {
        logger.info("@@@ kafkaOffsetDetailsDF is not empty, filtering Kafka data")
        val kafkaFilteredDF = kafkaDF.join(OffsetDetailsDF, (kafkaDF.col("Partition") === OffsetDetailsDF.col("Partition"))
          && (kafkaDF.col("Offset") > OffsetDetailsDF.col("Offset")) && (OffsetDetailsDF.col("RowTag") === tag), "inner")
          .select(kafkaDF("Partition"), kafkaDF("Offset"), kafkaDF("JSONData"))
        kafkaFilteredDF
      } else {
        logger.info("@@@ kafkaOffsetDetailsDF is empty, select full Kafka data")
        kafkaDF
      }

      if (!kafkaFilteredDF.rdd.isEmpty()) {
        val jsonRDD = kafkaFilteredDF.rdd.map { case x: Row => x(2).asInstanceOf[String] }

        //Can try to make this dynamic in future
        if (layer == "RAW_CURPIT") {
          logger.info("@@@ Calling runBluepcsJsonToRawCur")
          BluepcssPMMPlusLoader.runBluepcsJsonToRawCur(common_conf, bluepcs_conf, tag, jsonRDD, env)

          if (isHdfsSource) {
            logger.info(s"@@@ HDFS MODE: skipping updateKafkaOffsetDetails for tag=$tag, layer=$layer (HDFS uses file lifecycle for idempotency)")
          } else if (debugMode) {
            logger.info(s"@@@ DEBUG MODE: skipping updateKafkaOffsetDetails for tag=$tag, layer=$layer")
          } else {
            logger.info("@@@ Calling updateKafkaOffsetDetails")
            HbaseOffsetManagement.updateKafkaOffsetDetails(tag, layer, kafkaFilteredDF)
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
      }
    })
  }

  private def elapsedMsSince(startNs: Long): Long =
    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs)

}
