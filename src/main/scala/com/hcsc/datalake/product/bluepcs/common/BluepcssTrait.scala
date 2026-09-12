package com.hcsc.datalake.product.bluepcs.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import scala.io.Source

trait BluepcssTrait extends AppTrait {

  def readSqlFile(sqlFileName: String, hdfsFilepath: String): String = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val sqlPath = new Path(s"$hdfsFilepath/$sqlFileName.sql")

    if (fs.exists(sqlPath)) {
      val inputStream = fs.open(sqlPath)
      try {
        Source.fromInputStream(inputStream).mkString
      } finally {
        inputStream.close()
      }
    } else {
      throw new RuntimeException(s"SQL file not found: $sqlPath")
    }
  }

  def readJsonSchema(schemaName: String, hdfsFilepath: String): StructType = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val schemaPath = new Path(s"$hdfsFilepath/$schemaName.json")

    if (fs.exists(schemaPath)) {
      val inputStream = fs.open(schemaPath)
      try {
        val schemaJson = Source.fromInputStream(inputStream).mkString
        org.apache.spark.sql.types.DataType.fromJson(schemaJson).asInstanceOf[StructType]
      } finally {
        inputStream.close()
      }
    } else {
      throw new RuntimeException(s"Schema file not found: $schemaPath")
    }
  }

  def jsonFileReader(schema: StructType, jsonRDD: RDD[String]): DataFrame = {
    spark.read.schema(schema).json(jsonRDD)
  }

  def runSql(viewName: String, sqlQuery: String, df: DataFrame): DataFrame = {
    df.createOrReplaceTempView(viewName)
    spark.sql(sqlQuery)
  }

  def loadJsonToHive(
    tableName: String,
    hiveDb: String,
    hdfsPath: String,
    writeMode: String,
    df: DataFrame
  ): Unit = {
    val skipHiveWrite = spark.conf.get("spark.bluepcs.debug.skipHiveWrite", "false").toBoolean

    if (skipHiveWrite) {
      logger.info(s"@@@ DEBUG: Skipping Hive write for $hiveDb.$tableName")
      return
    }

    val fullTableName = s"$hiveDb.$tableName"
    logger.info(s"@@@ LoadHiveTable$$: @@@ loadJsonToHive started for: $fullTableName")

    val saveModeStr = writeMode.toLowerCase match {
      case "append" => org.apache.spark.sql.SaveMode.Append
      case "overwrite" => org.apache.spark.sql.SaveMode.Overwrite
      case _ => org.apache.spark.sql.SaveMode.Append
    }

    df.write
      .mode(saveModeStr)
      .format("parquet")
      .option("path", s"$hdfsPath/$tableName")
      .saveAsTable(fullTableName)

    logger.info(s"@@@ LoadHiveTable$$: @@@ loadJsonToHive completed successfully: $fullTableName")
  }

  def removeNullRecord(busFields: Array[String], df: DataFrame): (DataFrame, DataFrame) = {
    val nonNullCondition = busFields.map(f => col(f).isNotNull).reduce(_ && _)
    val validDF = df.filter(nonNullCondition)
    val nullDF = df.filter(!nonNullCondition)
    (validDF, nullDF)
  }

  def auditInfo(df: DataFrame, auditTableName: String, hdfsFilepath: String, jsonTag: String): DataFrame = {
    df.withColumn("audit_timestamp", current_timestamp())
      .withColumn("record_count", lit(df.count()))
  }

  def auditGoldInfo(df: DataFrame, auditTableName: String, hdfsFilepath: String, jsonTag: String): DataFrame = {
    df.withColumn("audit_timestamp", current_timestamp())
      .withColumn("record_count", lit(df.count()))
      .withColumn("layer", lit("GOLD"))
  }

  def hdfsWriteNewFile(filePath: String, content: Array[Byte]): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val path = new Path(filePath)
    val outputStream = fs.create(path, true)
    try {
      outputStream.write(content)
    } finally {
      outputStream.close()
    }
  }
}

object BluepcssTrait extends BluepcssTrait
