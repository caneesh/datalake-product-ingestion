package com.hcsc.datalake.product.bluepcs.core

import scala.util.{Try, Success, Failure}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import com.typesafe.config.Config
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import com.fasterxml.jackson.databind.ObjectMapper
import com.hcsc.datalake.product.bluepcs.common.{AppTrait, HbaseOffsetManagement}
import com.hcsc.bluepcs.common.BridgeMessageResolver

object BluepcssPMMPlusConsumer extends AppTrait {
  import spark.sqlContext.implicits._

  // Default input mode - can be overridden via config or system property
  private val DEFAULT_INPUT_MODE = "kafka"

  def main(args: Array[String]): Unit = {

    Try {

      val debugMode = true
      val debugKafkaSampleSize = 10
      val skipHiveWrite = true

      var commonParamFilePath = ""
      var blupcsParamFilePath = ""
      var env = ""

      if (args.size == 3) {
        commonParamFilePath = args(0).toString
        blupcsParamFilePath = args(1).toString
        env = args(2)

        logger.info("@@@ Arguments for the job")
        logger.info("@@@ =====================================================")
        logger.info("@@@ Common Parameter file : " + commonParamFilePath)
        logger.info("@@@ Bluepcs Parameter file : " + blupcsParamFilePath)
        logger.info("@@@ Env : " + env)
        logger.info("@@@ =====================================================")

      } else {
        logger.error("@@@ Please provide Common Parameter file, Bluepcs Parameter file and Environment")
        System.exit(1)
      }

      //Logging Start Date and Time
      val dateFormat = new SimpleDateFormat("yyyyMMdd");
      val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
      val currentDate = dateFormat.format(Calendar.getInstance().getTime());
      val currentTime = timeFormat.format(Calendar.getInstance().getTime());

      logger.info("@@@ Current Date: " + currentDate)
      logger.info("@@@ Current Time: " + currentTime)
      logger.info("@@@ Step 1 : Sourcing Common param values")

      val common_conf = readConfigFile(commonParamFilePath)
      val bluepcs_conf = readConfigFile(blupcsParamFilePath)

      // Determine input mode: kafka (default) or hdfs
      // Can be set via: 1) bluepcs_conf, 2) system property -Dbluepcssinputmode
      val inputMode = sys.props.get("bluepcs.input.mode")
        .orElse(Try(bluepcs_conf.getString("input_mode")).toOption)
        .getOrElse(DEFAULT_INPUT_MODE)
        .trim.toLowerCase

      logger.info(s"@@@ Input mode: $inputMode")

      // Read debug flags from JVM system properties (-Dbluepcssdebugmode / -Dbluepcssskiphivewrite)
      // supplied via --conf "spark.driver.extraJavaOptions=..." on spark-submit.
      // Falls back to the hardcoded local defaults when the property is absent.
      val effectiveDebugMode     = sys.props.get("bluepcs.debug.mode").map(_.toBoolean).getOrElse(debugMode)
      // FIX: Use effectiveDebugMode (not debugMode) so -Dbluepcs.debug.mode=false also disables skipHiveWrite
      val effectiveSkipHiveWrite = sys.props.get("bluepcs.debug.skipHiveWrite").map(_.toBoolean).getOrElse(effectiveDebugMode && skipHiveWrite)

      // Publish resolved values via Spark conf so downstream components
      // (BluepcssPMMPlusProcessor, LoadHiveTable) can read them without needing
      // direct access to sys.props on executors.
      spark.conf.set("spark.bluepcs.debug.mode",          effectiveDebugMode.toString)
      spark.conf.set("spark.bluepcs.debug.skipHiveWrite", effectiveSkipHiveWrite.toString)

      // Parallel tag processing - enabled by default for HDFS mode for faster throughput
      val parallelTagProcessing = sys.props.get("bluepcs.parallel.tags")
        .orElse(Try(bluepcs_conf.getString("parallel_tag_processing")).toOption)
        .map(_.toBoolean)
        .getOrElse(inputMode == "hdfs")  // Default: enabled for HDFS, disabled for Kafka
      spark.conf.set("spark.bluepcs.parallel.tags", parallelTagProcessing.toString)

      logger.info(s"@@@ Debug controls: debugMode=$effectiveDebugMode, debugKafkaSampleSize=$debugKafkaSampleSize, skipHiveWrite=$effectiveSkipHiveWrite")
      logger.info(s"@@@ Performance controls: parallelTagProcessing=$parallelTagProcessing")

      // Route to appropriate input source based on mode
      inputMode match {
        case "hdfs" =>
          logger.info("@@@ Starting HDFS file polling mode")
          runHdfsMode(common_conf, bluepcs_conf, env)

        case "kafka" =>
          logger.info("@@@ Starting Kafka streaming mode")
          runKafkaMode(common_conf, bluepcs_conf, env, effectiveDebugMode, debugKafkaSampleSize)

        case other =>
          throw new IllegalArgumentException(s"Unsupported input_mode: $other. Valid values are 'kafka' or 'hdfs'")
      }

    } match {
      case Success(k) => logger.info("@@@ Execution is success" + k)
      case Failure(e) =>
        logger.error("@@@ Error in consumer " + e.toString, e)
        System.exit(1)

    }
  }

  /**
   * Run the application in HDFS file polling mode.
   * Polls a directory for JSON files and processes them through the existing pipeline.
   */
  private def runHdfsMode(common_conf: Config, bluepcs_conf: Config, env: String): Unit = {
    logger.info("@@@ Initializing HDFS polling mode")

    // Read HDFS polling configuration from bluepcs_conf
    // HDFS config: spark.conf overrides > param file > defaults
    // This allows passing via spark-submit --conf spark.bluepcs.hdfs.*=value
    def getHdfsConf[T](sparkKey: String, paramKey: String, default: T, parse: String => T): T = {
      Try(parse(spark.conf.get(sparkKey)))
        .orElse(Try(parse(bluepcs_conf.getString(paramKey))))
        .getOrElse(default)
    }

    val hdfsConfig = HdfsPollingConfig(
      incomingDir        = bluepcs_conf.getString("hdfs_incoming_dir").replace("$hdfs_env_nm", env),
      processingDir      = bluepcs_conf.getString("hdfs_processing_dir").replace("$hdfs_env_nm", env),
      archiveDir         = bluepcs_conf.getString("hdfs_archive_dir").replace("$hdfs_env_nm", env),
      errorDir           = bluepcs_conf.getString("hdfs_error_dir").replace("$hdfs_env_nm", env),
      pollIntervalMs     = getHdfsConf("spark.bluepcs.hdfs.pollIntervalMs", "hdfs_poll_interval_ms", 30000L, _.toLong),
      maxFilesPerPoll    = getHdfsConf("spark.bluepcs.hdfs.maxFilesPerPoll", "hdfs_max_files_per_poll", 10, _.toInt),
      fileExtension      = Try(bluepcs_conf.getString("hdfs_file_extension")).getOrElse(".json"),
      fileStabilityMs    = getHdfsConf("spark.bluepcs.hdfs.fileStabilityMs", "hdfs_file_stability_ms", 5000L, _.toLong),
      recoverStrandedOnStartup = Try(bluepcs_conf.getString("hdfs_recover_stranded").toBoolean).getOrElse(true),
      maxFileSizeBytes   = getHdfsConf("spark.bluepcs.hdfs.maxFileSizeBytes", "hdfs_max_file_size_bytes", 100L * 1024 * 1024, _.toLong),
      parallelFileClaims = getHdfsConf("spark.bluepcs.hdfs.parallelFileClaims", "hdfs_parallel_file_claims", 4, _.toInt),
      batchProcessing    = getHdfsConf("spark.bluepcs.hdfs.batchProcessing", "hdfs_batch_processing", true, _.toBoolean)
    )

    logger.info(s"@@@ HDFS Polling Config: incomingDir=${hdfsConfig.incomingDir}")
    logger.info(s"@@@ HDFS Polling Config: processingDir=${hdfsConfig.processingDir}")
    logger.info(s"@@@ HDFS Polling Config: archiveDir=${hdfsConfig.archiveDir}")
    logger.info(s"@@@ HDFS Polling Config: errorDir=${hdfsConfig.errorDir}")
    logger.info(s"@@@ HDFS Polling Config: pollIntervalMs=${hdfsConfig.pollIntervalMs}")
    logger.info(s"@@@ HDFS Polling Config: maxFilesPerPoll=${hdfsConfig.maxFilesPerPoll}")
    logger.info(s"@@@ HDFS Polling Config: fileExtension=${hdfsConfig.fileExtension}")
    logger.info(s"@@@ HDFS Polling Config: fileStabilityMs=${hdfsConfig.fileStabilityMs}")
    logger.info(s"@@@ HDFS Polling Config: recoverStrandedOnStartup=${hdfsConfig.recoverStrandedOnStartup}")
    logger.info(s"@@@ HDFS Polling Config: maxFileSizeBytes=${hdfsConfig.maxFileSizeBytes}")
    logger.info(s"@@@ HDFS Polling Config: parallelFileClaims=${hdfsConfig.parallelFileClaims}")
    logger.info(s"@@@ HDFS Polling Config: batchProcessing=${hdfsConfig.batchProcessing}")

    // Create the message processor that bridges to existing BluepcssPMMPlusProcessor
    val processor = new BluepcssPMMPlusMessageProcessor(common_conf, bluepcs_conf, env)

    // Get HDFS filesystem
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Create and start the HDFS file poller
    val poller = new HdfsFilePoller(fs, hdfsConfig, processor)

    logger.info("@@@ Starting HDFS file poller")
    poller.start()

    // Keep the main thread alive using a latch that can be released by shutdown hook
    val shutdownLatch = new java.util.concurrent.CountDownLatch(1)

    // Update shutdown hook to release the latch
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        logger.info("@@@ Shutdown hook triggered for HDFS mode - releasing latch")
        shutdownLatch.countDown()
      }
    })

    logger.info("@@@ Main thread waiting on shutdown latch")
    shutdownLatch.await() // Blocks until shutdown hook releases it

    logger.info("@@@ Shutdown latch released, stopping poller gracefully")
    poller.stop()
    logger.info("@@@ HDFS polling mode terminated gracefully")
  }

  /**
   * Run the application in Kafka streaming mode (original behavior).
   */
  private def runKafkaMode(
                            common_conf: Config,
                            bluepcs_conf: Config,
                            env: String,
                            effectiveDebugMode: Boolean,
                            debugKafkaSampleSize: Int
                          ): Unit = {
    logger.info("@@@ Initializing Kafka streaming mode")

    val kafkaTopicReadDuration = bluepcs_conf.getString("kafka_topic_read_duration").toLong
    val kafkaTopicName = bluepcs_conf.getString("kafka_topic_name")
    val offsetTableName = bluepcs_conf.getString("hbase_offset_table_name")
    logger.info("@@@ kafkaTopicReadDuration" + kafkaTopicReadDuration)
    logger.info("@@@ kafkaTopicName" + kafkaTopicName)
    logger.info("@@@ offsetTableName" + offsetTableName)

    val kafkaParams = prepareKafkaParams(common_conf, bluepcs_conf, env)
    logger.info("@@@ Prepared Kafka Params. Values" + kafkaParams)

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(kafkaTopicReadDuration))
    logger.info("@@@ spark streaming context created")

    val kafkaOffsetDetailsDF = HbaseOffsetManagement.getKafkaOffsetDetails()
    logger.info("@@@ Offset details fetched from Hbase")

    kafkaParams.foreach { case (k, v) => logger.info(s"KAFKA PARAM $k = $v") }
    val stream = if (kafkaOffsetDetailsDF.rdd.isEmpty()) {
      logger.info("@@@ kafkaOffsetDetailsDF is initial")
      logger.info("@@@ Subscribing to topic " + kafkaTopicName)
      KafkaUtils.createDirectStream[String, String](
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(kafkaTopicName), kafkaParams))
    } else {

      val fromOffsets = HbaseOffsetManagement.getFromOffsetDetails(bluepcs_conf, kafkaOffsetDetailsDF)
      logger.info("@@@Printing contents of fromOffsets:" + fromOffsets)

      logger.info("@@@ Assigning offsets for topic " + kafkaTopicName)
      KafkaUtils.createDirectStream[String, String](
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))
    }

    stream.foreachRDD({ rdd =>
      if (!rdd.isEmpty()) {
        val kafkaRDD = rdd.map(x => (x.partition.toInt, x.offset.toInt, x.value.toString))

        logger.info("@@@ Processing kafkaRDD")

        val inputRDD =
          if (effectiveDebugMode) {
            val sample = kafkaRDD.take(debugKafkaSampleSize)
            logger.info(s"@@@ DEBUG MODE: sampled ${sample.length} Kafka messages only")
            sample.foreach(x => logger.info(s"@@@ DEBUG SAMPLE partition=${x._1}, offset=${x._2}"))
            spark.sparkContext.parallelize(sample)
          } else {
            kafkaRDD
          }

        // Normalize both formats (legacy Talend inline vs bridge claim-check) to
        // the same wrapper shape; (partition, offset, json) tuple layout unchanged,
        // so the processor and HBase offset management need no modifications.
        val resolvedRDD =
          inputRDD.mapPartitions { it =>
            // one FileSystem + ObjectMapper per partition, not per record
            val fs = FileSystem.get(new Configuration())
            val mapper = new ObjectMapper()
            it.flatMap {
              case (partition, offset, raw) =>
                BridgeMessageResolver
                  .resolveOrSkip(raw, fs, mapper)
                  .map(resolved => (partition, offset, resolved.json))
            }
          }

        logger.info("@@@ Messages normalized (claim-check resolved from HDFS where present)")

        BluepcssPMMPlusProcessor.processJSONMessages(
          common_conf, bluepcs_conf, resolvedRDD, kafkaOffsetDetailsDF, env)

      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def prepareKafkaParams(common_conf: Config,
                         bluepcs_conf: Config,
                         env: String): Map[String, Object] = {

    import org.apache.kafka.common.serialization.StringDeserializer
    val principal = bluepcs_conf.getString("kafka_principal")

    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> common_conf.getString(env + "_bootstrap_servers"),
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[StringDeserializer].getName,
      "group.id" -> bluepcs_conf.getString("kafka_group_id"),
      "security.protocol" -> "SASL_SSL",
      "sasl.mechanism" -> "GSSAPI",
      "sasl.kerberos.service.name" -> "cp-kafka",

      "ssl.truststore.location" -> "kafka-truststore.jks",
      "ssl.truststore.password" -> "confluenttruststorepass",
      "ssl.truststore.type" -> "JKS",

      "sasl.jaas.config" ->
        s"""com.sun.security.auth.module.Krb5LoginModule required
           useKeyTab=true
           storeKey=true
           useTicketCache=false
           doNotPrompt=true
           keyTab="kafka_broker.keytab"
           principal="${principal}";
        """,

      "auto.offset.reset" ->
        common_conf.getString("auto_offset_reset"),
      // createDirectStream manages offsets manually via HBase.
      // Auto-commit MUST be false or broker offsets will diverge from HBase on restart.
      "enable.auto.commit" -> "false"
    )
    kafkaParams
  }

}
