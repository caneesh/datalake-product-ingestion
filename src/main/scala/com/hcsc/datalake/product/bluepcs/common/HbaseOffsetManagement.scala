package com.hcsc.datalake.product.bluepcs.common

import org.apache.spark.sql.DataFrame
import org.apache.kafka.common.TopicPartition
import com.typesafe.config.Config

object HbaseOffsetManagement extends AppTrait {

  def getKafkaOffsetDetails(): DataFrame = {
    import spark.implicits._
    // Return empty DataFrame with expected schema
    // In production, this would read from HBase
    Seq.empty[(Int, Int, String)].toDF("Partition", "Offset", "RowTag")
  }

  def getFromOffsetDetails(bluepcs_conf: Config, offsetDF: DataFrame): Map[TopicPartition, Long] = {
    import spark.implicits._

    if (offsetDF.rdd.isEmpty()) {
      Map.empty[TopicPartition, Long]
    } else {
      val topicName = bluepcs_conf.getString("kafka_topic_name")
      offsetDF.collect().map { row =>
        val partition = row.getAs[Int]("Partition")
        val offset = row.getAs[Int]("Offset").toLong
        new TopicPartition(topicName, partition) -> offset
      }.toMap
    }
  }

  def updateKafkaOffsetDetails(tag: String, layer: String, kafkaDF: DataFrame): Unit = {
    // In production, this would write to HBase
    logger.info(s"@@@ Updating Kafka offset details for tag=$tag, layer=$layer")

    val maxOffsets = kafkaDF.groupBy("Partition").agg(
      org.apache.spark.sql.functions.max("Offset").as("MaxOffset")
    ).collect()

    maxOffsets.foreach { row =>
      val partition = row.getAs[Any]("Partition")
      val maxOffset = row.getAs[Any]("MaxOffset")
      logger.info(s"@@@ Partition $partition: MaxOffset = $maxOffset")
    }
  }
}
