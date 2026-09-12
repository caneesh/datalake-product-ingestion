package com.hcsc.datalake.product.bluepcs.common

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object DataFrameResourceManager {

  private val logger = LoggerFactory.getLogger(getClass)

  def unpersistIfCached(df: DataFrame): Unit = {
    try {
      if (df.storageLevel.useMemory || df.storageLevel.useDisk) {
        df.unpersist()
        logger.debug("@@@ Unpersisted DataFrame")
      }
    } catch {
      case e: Exception =>
        logger.warn(s"@@@ Failed to unpersist DataFrame: ${e.getMessage}")
    }
  }
}
