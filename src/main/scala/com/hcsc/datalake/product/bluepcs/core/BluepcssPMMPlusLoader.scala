package com.hcsc.datalake.product.bluepcs.core

import ...

object BluepcssPMMPlusLoader extends AppTrait with BluepcssTrait {

  case class SqlStage(sqlFile: String, viewName: String)
  case class StagedSqlFlow(stages: Seq[SqlStage], finalSqlFile: String)

  private val stagedSqlFlows: Map[String, StagedSqlFlow] = StagedSqlFlowRegistry.stagedSqlFlows

  // ================================================================
  // Schema Cleanup Utilities
  // ================================================================

  /** Recursively strip `_extract_` prefix from a DataType (struct fields, array elements, map values). */
  private def stripExtractFromType(dt: DataType): DataType = dt match {
    case st: StructType =>
      StructType(st.fields.map { f =>
        val cleanName = if (f.name.startsWith("_extract_")) f.name.stripPrefix("_extract_") else f.name
        f.copy(name = cleanName, dataType = stripExtractFromType(f.dataType))
      })
    case at: ArrayType =>
      at.copy(elementType = stripExtractFromType(at.elementType))
    case mt: MapType =>
      mt.copy(keyType = stripExtractFromType(mt.keyType), valueType = stripExtractFromType(mt.valueType))
    case other => other
  }

  /** Strip `_extract_` prefix from all column names - including nested struct fields, arrays, maps. */
  private def stripExtractPrefix(df: DataFrame): DataFrame = {
    val cleanSchema = stripExtractFromType(df.schema).asInstanceOf[StructType]
    df.sparkSession.createDataFrame(df.rdd, cleanSchema)
  }

  // ================================================================

  private def runStagedSqlFlow(
    sourceView: String,
    flow: StagedSqlFlow,
    hdfsFilepath: String,
    stageLabel: String
  ): DataFrame = {
    logger.info(s"@@@ [$stageLabel] Using source view: $sourceView")

    flow.stages.foreach { stage =>
      val branchSql = readSqlFile(stage.sqlFile, hdfsFilepath)
      logger.info(s"@@@ [$stageLabel] Executing ${stage.sqlFile} -> ${stage.viewName}")

      val branchDF = spark.sql(branchSql)
      branchDF.cache()
      branchDF.count()
      branchDF.createOrReplaceTempView(stage.viewName)
    }

    val finalSql = readSqlFile(flow.finalSqlFile, hdfsFilepath)
    logger.info(s"@@@ [$stageLabel] Executing final assembly SQL -> ${flow.finalSqlFile}")
    spark.sql(finalSql)
  }

  private def executeSqlTransform(
    jsonTag: String,
    jsonDF: DataFrame,
    hdfsFilepath: String
  ): DataFrame = {
    val tagKey = jsonTag.toLowerCase

    stagedSqlFlows.get(tagKey) match {
      case Some(flow) =>
        jsonDF.createOrReplaceTempView(jsonTag)
        runStagedSqlFlow(
          sourceView = jsonTag,
          flow = flow,
          hdfsFilepath = hdfsFilepath,
          stageLabel = s"$jsonTag staged"
        )

      case None =>
        val sqlQuery = readSqlFile(jsonTag + "_incr_gold_sql", hdfsFilepath)
        runSql(jsonTag, sqlQuery, jsonDF)
    }
  }

  // ================================================================

  private def applyTimestampTransform(df: DataFrame, jsonTag: String): DataFrame = {
    if (df.columns.contains("EVNT_TS")) {
      df.withColumn("EVNT_TS", to_timestamp(col("EVNT_TS"), "yyyy-MM-dd HH:mm:ss.SSSX"))
    } else {
      logger.warn(s"@@@ Column EVNT_TS not found in $jsonTag - skipping timestamp conversion")
      df
    }
  }

  // ================================================================
  // Trigger File Creation
  // ================================================================

  private def createTriggerFile(
    triggerPath: String,
    triggerPattern: String,
    jsonTag: String
  ): Unit = {
    val timeFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val currentTime = timeFormat.format(Calendar.getInstance().getTime())
    val filePath = s"$triggerPath/$triggerPattern$jsonTag.d$currentTime.trg"
    val content = jsonTag + "Table is Loaded"
    logger.info(s"@@@ creating trigger file for : $jsonTag at $filePath")
    hdfsWriteNewFile(filePath, content.getBytes)
  }

  // ================================================================

  private def processRawFlatten(
    jsonTag: String,
    jsonDF: DataFrame,
    hdfsFilepath: String
  ): DataFrame = {
    val sqlQuery = readSqlFile(jsonTag + "_incr_raw_spark_sql", hdfsFilepath)
    spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "false")

    val jsonFlattenDFRaw = runSql(jsonTag, sqlQuery, jsonDF)
    val jsonFlattenDFNoExtract = stripExtractPrefix(jsonFlattenDFRaw)
    val jsonFlattenDF = applyTimestampTransform(jsonFlattenDFNoExtract, jsonTag + " raw flatten")

    logger.info("jsonFlattenDF.printSchema()")
    jsonFlattenDF.printSchema()
    jsonFlattenDF
  }

  private def processCurated(
    jsonTag: String,
    jsonFinalDF: DataFrame,
    hdfsFilepath: String,
    hiveCuratedDb: String,
    hiveCuratedHdfsPath: String,
    hiveCuratedWriteMode: String
  ): Unit = {
    val standardSqlQuery = readSqlFile(jsonTag + "_curate_sql", hdfsFilepath)
    val standardTmpDF = runSql("curate_" + jsonTag, standardSqlQuery, jsonFinalDF).dropDuplicates()

    val trimmedCols = standardTmpDF.columns.map { colName =>
      upper(trim(col(colName))).as(colName)
    }
    logger.info("Trimmed columns" + trimmedCols.mkString(","))
    val curatedDF = standardTmpDF.select(trimmedCols: _*).dropDuplicates()

    logger.info("@@@ Loading data to the curated DB table : " + jsonTag)
    loadJsonToHive(jsonTag, hiveCuratedDb, hiveCuratedHdfsPath, hiveCuratedWriteMode, curatedDF)
  }

  private def processRawAudit(
    jsonFinalDF: DataFrame,
    jsonNullDF: DataFrame,
    audit: String,
    hdfsFilepath: String,
    jsonTag: String,
    rawAuditTable: String,
    hiveWorkDb: String,
    hiveRawHdfsPath: String,
    hiveRawWriteMode: String
  ): Unit = {
    logger.info("@@@ Preparing finalAuditDf")
    val rawFinalAuditDf = auditInfo(jsonFinalDF, audit, hdfsFilepath, jsonTag)
      .withColumn("data_status", lit("YES"))
      .withColumn("table_name", lit(jsonTag))
    logger.info("@@@ Loading valid data to the raw audit DB table : " + rawAuditTable)
    loadJsonToHive(rawAuditTable, hiveWorkDb, hiveRawHdfsPath, hiveRawWriteMode, rawFinalAuditDf)

    val rawNullAuditDf = auditInfo(jsonNullDF, audit, hdfsFilepath, jsonTag)
      .withColumn("data_status", lit("NO"))
      .withColumn("table_name", lit(jsonTag))
    logger.info("@@@ Loading null data to the raw audit DB table : " + rawAuditTable)
    loadJsonToHive(rawAuditTable, hiveWorkDb, hiveRawHdfsPath, hiveRawWriteMode, rawNullAuditDf)
  }

  // ================================================================
  // Gold Layer Processing
  // ================================================================

  private def processGoldWithoutPit(
    jsonTag: String,
    jsonFinalDF: DataFrame,
    bluepcs_conf: Config,
    hiveGoldDb: String,
    hiveGoldHdfsPath: String,
    hiveGoldWriteMode: String
  ): Unit = {
    val tableDF = spark.read.table(s"$hiveGoldDb.$jsonTag")

    val stringIdColumn = bluepcs_conf.getString(s"${jsonTag}_id")
    logger.info("@@@ MD5 column : " + stringIdColumn)

    val joinDF = jsonFinalDF.join(
      tableDF,
      jsonFinalDF(stringIdColumn) === tableDF(stringIdColumn),
      "leftanti"
    )

    val columnsDF = jsonFinalDF.columns.mkString(",")
    val finalSql = s"select $columnsDF from jsonDF"

    val dfRaw = runSql("jsonDF", finalSql, joinDF)
    val finalDF = applyTimestampTransform(dfRaw, jsonTag).dropDuplicates()

    loadJsonToHive(jsonTag, hiveGoldDb, hiveGoldHdfsPath, hiveGoldWriteMode, finalDF)
  }

  private def processGoldWithPit(
    jsonTag: String,
    jsonFinalDF: DataFrame,
    jsonNullDF: DataFrame,
    hiveGoldHistDb: String,
    hiveGoldHdfsPath: String,
    hiveGoldWriteMode: String,
    hiveWorkDb: String,
    hiveAuditTableName: String,
    hdfsFilepath: String,
    hdfsAfterIncrLoadTriggerPath: String,
    hdfsAfterIncrLoadTriggerPattern: String
  ): Unit = {
    val noDupJsonDF = applyTimestampTransform(jsonFinalDF, jsonTag + " gold").dropDuplicates()

    logger.info("noDupJsonDF.printSchema()")
    noDupJsonDF.printSchema()

    loadJsonToHive(jsonTag, hiveGoldHistDb, hiveGoldHdfsPath, hiveGoldWriteMode, noDupJsonDF)

    createTriggerFile(hdfsAfterIncrLoadTriggerPath, hdfsAfterIncrLoadTriggerPattern, jsonTag)

    processGoldAudit(
      noDupJsonDF, jsonNullDF, hiveAuditTableName, hdfsFilepath, jsonTag,
      hiveWorkDb, hiveGoldHdfsPath, hiveGoldWriteMode
    )
  }

  private def processGoldAudit(
    noDupJsonDF: DataFrame,
    jsonNullDF: DataFrame,
    hiveAuditTableName: String,
    hdfsFilepath: String,
    jsonTag: String,
    hiveWorkDb: String,
    hiveGoldHdfsPath: String,
    hiveGoldWriteMode: String
  ): Unit = {
    val goldFinalAuditDF = auditGoldInfo(noDupJsonDF, hiveAuditTableName, hdfsFilepath, jsonTag)
    logger.info("@@@ Loading valid data to the gold audit DB table for " + jsonTag)
    loadJsonToHive(hiveAuditTableName, hiveWorkDb, hiveGoldHdfsPath, hiveGoldWriteMode, goldFinalAuditDF)

    val goldNullAuditDF = auditGoldInfo(jsonNullDF, hiveAuditTableName, hdfsFilepath, jsonTag)
    logger.info("@@@ Loading invalid data to the gold audit DB table for " + jsonTag)
    loadJsonToHive(hiveAuditTableName, hiveWorkDb, hiveGoldHdfsPath, hiveGoldWriteMode, goldNullAuditDF)
  }

  // ================================================================
  // Main Entry Points
  // ================================================================

  def runBluepcsJsonToRawCur(
    common_conf: Config,
    bluepcs_conf: Config,
    jsonTag: String,
    jsonRDD: RDD[String],
    env: String
  ): Unit = {
    logger.info("@@@ Json to Raw Load for " + jsonTag + " tag started")
    logger.info("@@@ Step 1 : Sourcing Common and Table param values")

    // Extract configuration
    val hdfsFilepath = bluepcs_conf.getString("hdfs_raw_incr_bluepcs_param_path").replace("$hdfs_env_nm", env)
    val hiveRawDb = bluepcs_conf.getString("hive_bluepcs_raw_db")
    val hiveCuratedDb = bluepcs_conf.getString("hive_bluepcs_curated_db")
    val hiveWorkDb = bluepcs_conf.getString("hive_bluepcs_work_db")
    val hiveRawHdfsPath = bluepcs_conf.getString("hdfs_raw_bluepcs_hive_path").replace("$hdfs_env_nm", env)
    val hiveCuratedHdfsPath = bluepcs_conf.getString("hdfs_curated_bluepcs_hive_path").replace("$hdfs_env_nm", env)
    val repartition = bluepcs_conf.getInt("spark_repartition")
    val rawAuditTable = bluepcs_conf.getString("hive_bluepcs_raw_audit_tbl_nm")
    val hiveRawWriteMode = bluepcs_conf.getString("hive_bluepcs_raw_write_mode")
    val hiveCuratedWriteMode = bluepcs_conf.getString("hive_bluepcs_curated_db_write_mode")
    val etlFieldsStr = bluepcs_conf.getString("bluepcs_raw_etl_fields")
    val hdfsAfterIncrLoadTriggerPath = bluepcs_conf.getString("hdfs_after_incr_load_trigger_path").replace("$hdfs_env_nm", env)
    val hdfsAfterIncrLoadTriggerPattern = bluepcs_conf.getString("hdfs_after_incr_load_trigger_pattern")
    val audit = bluepcs_conf.getString("hive_bluepcs_audit_tbl_nm")

    ConfigLogger.logRawCurConfig(
      hdfsFilepath, hiveRawDb, hiveCuratedDb, hiveWorkDb, hiveRawHdfsPath,
      hiveCuratedHdfsPath, repartition, rawAuditTable, hiveRawWriteMode,
      hiveCuratedWriteMode, etlFieldsStr, hdfsAfterIncrLoadTriggerPath,
      hdfsAfterIncrLoadTriggerPattern, audit
    )

    // Read and clean JSON
    val jsonSchema = readJsonSchema(jsonTag + "_incr", hdfsFilepath)
    val jsonDFRaw = jsonFileReader(jsonSchema, jsonRDD)
    val jsonDF = stripExtractPrefix(jsonDFRaw)

    if (!jsonDF.rdd.isEmpty()) {
      jsonDF.persist()

      // Process raw flatten
      val jsonFlattenDF = processRawFlatten(jsonTag, jsonDF, hdfsFilepath)
      jsonFlattenDF.persist()

      // Filter null records
      val allFields = jsonFlattenDF.columns
      val etlFields = etlFieldsStr.split(",")
      val busFields = allFields.diff(etlFields)
      val (jsonFinalDF, jsonNullDF) = removeNullRecord(busFields, jsonFlattenDF)

      logger.info("jsonFinalDF.printSchema()")
      jsonFinalDF.printSchema()

      // Load to raw layer
      loadJsonToHive(jsonTag, hiveRawDb, hiveRawHdfsPath, hiveRawWriteMode, jsonFinalDF)

      // Process curated layer
      processCurated(jsonTag, jsonFinalDF, hdfsFilepath, hiveCuratedDb, hiveCuratedHdfsPath, hiveCuratedWriteMode)

      // Process audit
      processRawAudit(
        jsonFinalDF, jsonNullDF, audit, hdfsFilepath, jsonTag,
        rawAuditTable, hiveWorkDb, hiveRawHdfsPath, hiveRawWriteMode
      )

      // Create trigger file
      createTriggerFile(hdfsAfterIncrLoadTriggerPath, hdfsAfterIncrLoadTriggerPattern, jsonTag)

      // Cleanup
      DataFrameResourceManager.unpersistIfCached(jsonFlattenDF)
      DataFrameResourceManager.unpersistIfCached(jsonDF)
    }
  }

  def runBluepcsJsonToGold(
    common_conf: Config,
    bluepcs_conf: Config,
    jsonTag: String,
    jsonRDD: RDD[String],
    env: String
  ): Unit = {
    logger.info("@@@ Json to Gold Load for " + jsonTag + " tag started")
    logger.info("@@@ Step 1 : Sourcing Common and Table param values")

    // Extract configuration
    val hdfsFilepath = bluepcs_conf.getString("hdfs_gold_incr_bluepcs_param_path").replace("$hdfs_env_nm", env)
    val hiveGoldDb = bluepcs_conf.getString("hive_bluepcs_gold_db")
    val hiveGoldHistDb = bluepcs_conf.getString("hive_bluepcs_gold_pit_db")
    val hiveGoldWriteMode = bluepcs_conf.getString("hive_bluepcs_gold_write_mode")
    val hiveWorkDb = bluepcs_conf.getString("hive_bluepcs_work_db")
    val hiveAuditTableName = bluepcs_conf.getString("hive_bluepcs_gold_audit_tbl_nm")
    val hiveGoldHdfsPath = bluepcs_conf.getString("hdfs_gold_bluepcs_hive_path").replace("$hdfs_env_nm", env)
    val hiveGoldHistHdfsPath = bluepcs_conf.getString("hdfs_gold_pit_bluepcs_hive_path").replace("$hdfs_env_nm", env)
    val tableListForGoldWitoutPit = bluepcs_conf.getString("gold_without_pit_tablelist")
    val etlFieldsStr = bluepcs_conf.getString("bluepcs_gold_etl_fields")
    val hdfsAfterIncrLoadTriggerPath = bluepcs_conf.getString("hdfs_after_incr_load_trigger_path").replace("$hdfs_env_nm", env)
    val hdfsAfterIncrLoadTriggerPattern = bluepcs_conf.getString("hdfs_after_gold_incr_load_trigger_pattern")

    ConfigLogger.logGoldConfig(
      hdfsFilepath, hiveGoldDb, hiveGoldHistDb, hiveGoldWriteMode, hiveWorkDb,
      hiveAuditTableName, hiveGoldHdfsPath, hiveGoldHistHdfsPath,
      tableListForGoldWitoutPit, etlFieldsStr
    )

    // Read and clean JSON
    val jsonSchema = readJsonSchema(jsonTag + "_incr_gold", hdfsFilepath)
    val jsonDFRaw = jsonFileReader(jsonSchema, jsonRDD)
    val jsonDF = stripExtractPrefix(jsonDFRaw)
    jsonDF.persist()

    spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "false")

    // Execute SQL transform (staged or single)
    val jsonFlattenDFRaw = executeSqlTransform(jsonTag, jsonDF, hdfsFilepath)
    val jsonFlattenDFNoExtract = stripExtractPrefix(jsonFlattenDFRaw)

    logger.info("jsonFlattenDF.printSchema()")
    jsonFlattenDFNoExtract.printSchema()

    // Filter null records
    val allFields = jsonFlattenDFNoExtract.columns
    val etlFields = etlFieldsStr.split(",")
    val busFields = allFields.diff(etlFields)
    val (jsonFinalDF, jsonNullDF) = removeNullRecord(busFields, jsonFlattenDFNoExtract)

    // Process based on PIT requirement
    if (tableListForGoldWitoutPit.contains(jsonTag)) {
      processGoldWithoutPit(jsonTag, jsonFinalDF, bluepcs_conf, hiveGoldDb, hiveGoldHdfsPath, hiveGoldWriteMode)
    } else {
      processGoldWithPit(
        jsonTag, jsonFinalDF, jsonNullDF, hiveGoldHistDb, hiveGoldHdfsPath,
        hiveGoldWriteMode, hiveWorkDb, hiveAuditTableName, hdfsFilepath,
        hdfsAfterIncrLoadTriggerPath, hdfsAfterIncrLoadTriggerPattern
      )
    }

    // Cleanup
    DataFrameResourceManager.unpersistIfCached(jsonDF)
  }

}
