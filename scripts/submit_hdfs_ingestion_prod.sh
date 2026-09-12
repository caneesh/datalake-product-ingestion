#!/bin/bash
# Spark submit script for HDFS ingestion mode with performance optimizations
# Usage: ./submit_hdfs_ingestion_prod.sh

/usr/odp/current/spark3-client/bin/spark-submit \
  --files '/etc/spark3/conf/hive-site.xml','/etc/security/keytabs/a6946038-Prod.keytab#kafka_broker.keytab','/datalakedata/prod/gold/integration/conf/product/confluent_kafka/kafka.truststore-prod_2025.jks#kafka-truststore.jks','/usr/odp/current/spark3-client/conf/hbase-site.xml','/datalakedata/prod/gold/integration/params/product/common/product_gold_common_param_odp.prm','/datalakedata/prod/gold/integration/params/product/bluepcs/product_bluepcs_common_param_odp.prm' \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.sql.optimizer.nestedSchemaPruning.enabled=false \
  --conf spark.sql.optimizer.nestedSchemaPruning.maxFields=0 \
  --conf spark.sql.codegen.wholeStage=false \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.yarn.tokens.hbase.enabled=true \
  --conf spark.default.parallelism=100 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.bluepcs.hdfs.maxFilesPerPoll=100 \
  --conf spark.bluepcs.hdfs.parallelFileClaims=8 \
  --conf spark.bluepcs.hdfs.batchProcessing=true \
  --conf spark.bluepcs.parallel.tags=true \
  --master yarn --deploy-mode cluster \
  --driver-memory 20g --num-executors 6 --executor-cores 4 --executor-memory 28g \
  --queue HIRES \
  --name product_gold_bluepcs_ingestion_hdfs \
  --class com.hcsc.datalake.product.bluepcs.core.BluepcssPMMPlusConsumer \
  '/datalakedata/prod/gold/integration/src/scripts/product/bluepcs/datalake-product-ingestion-1.0.0-SNAPSHOT-jar-with-dependencies.jar' \
  'product_gold_common_param_odp.prm' 'product_bluepcs_common_param_odp.prm' 'prod'
