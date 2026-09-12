package com.hcsc.datalake.product.bluepcs.common

import org.slf4j.LoggerFactory

object ConfigLogger {

  private val logger = LoggerFactory.getLogger(getClass)

  def logRawCurConfig(
    hdfsFilepath: String,
    hiveRawDb: String,
    hiveCuratedDb: String,
    hiveWorkDb: String,
    hiveRawHdfsPath: String,
    hiveCuratedHdfsPath: String,
    repartition: Int,
    rawAuditTable: String,
    hiveRawWriteMode: String,
    hiveCuratedWriteMode: String,
    etlFieldsStr: String,
    hdfsAfterIncrLoadTriggerPath: String,
    hdfsAfterIncrLoadTriggerPattern: String,
    audit: String
  ): Unit = {
    logger.info("@@@ RAW_CURPIT Configuration:")
    logger.info(s"@@@   hdfsFilepath: $hdfsFilepath")
    logger.info(s"@@@   hiveRawDb: $hiveRawDb")
    logger.info(s"@@@   hiveCuratedDb: $hiveCuratedDb")
    logger.info(s"@@@   hiveWorkDb: $hiveWorkDb")
    logger.info(s"@@@   hiveRawHdfsPath: $hiveRawHdfsPath")
    logger.info(s"@@@   hiveCuratedHdfsPath: $hiveCuratedHdfsPath")
    logger.info(s"@@@   repartition: $repartition")
    logger.info(s"@@@   rawAuditTable: $rawAuditTable")
    logger.info(s"@@@   hiveRawWriteMode: $hiveRawWriteMode")
    logger.info(s"@@@   hiveCuratedWriteMode: $hiveCuratedWriteMode")
  }

  def logGoldConfig(
    hdfsFilepath: String,
    hiveGoldDb: String,
    hiveGoldHistDb: String,
    hiveGoldWriteMode: String,
    hiveWorkDb: String,
    hiveAuditTableName: String,
    hiveGoldHdfsPath: String,
    hiveGoldHistHdfsPath: String,
    tableListForGoldWitoutPit: String,
    etlFieldsStr: String
  ): Unit = {
    logger.info("@@@ GOLD_PIT Configuration:")
    logger.info(s"@@@   hdfsFilepath: $hdfsFilepath")
    logger.info(s"@@@   hiveGoldDb: $hiveGoldDb")
    logger.info(s"@@@   hiveGoldHistDb: $hiveGoldHistDb")
    logger.info(s"@@@   hiveGoldWriteMode: $hiveGoldWriteMode")
    logger.info(s"@@@   hiveWorkDb: $hiveWorkDb")
    logger.info(s"@@@   hiveAuditTableName: $hiveAuditTableName")
    logger.info(s"@@@   hiveGoldHdfsPath: $hiveGoldHdfsPath")
    logger.info(s"@@@   hiveGoldHistHdfsPath: $hiveGoldHistHdfsPath")
  }
}
