package com.hcsc.datalake.product.bluepcs.common

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.InputStreamReader

trait AppTrait {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  lazy val spark: SparkSession = SparkSession.builder()
    .enableHiveSupport()
    .getOrCreate()

  def readConfigFile(path: String): Config = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val hdfsPath = new Path(path)

    if (fs.exists(hdfsPath)) {
      val inputStream = fs.open(hdfsPath)
      try {
        ConfigFactory.parseReader(new InputStreamReader(inputStream))
      } finally {
        inputStream.close()
      }
    } else {
      ConfigFactory.parseFile(new java.io.File(path))
    }
  }
}

object AppTrait extends AppTrait
