# Kafka Mode Troubleshooting Guide

## Common Issues and Fixes

### Issue 1: Only 10-12 messages processed instead of full dataset

**Symptom:** Logs show "DEBUG MODE: skipping updateKafkaOffsetDetails" for all tables.

**Cause:** Debug mode is enabled, which limits message processing to `debugKafkaSampleSize` (default: 10).

**Fix:** Add `-Dbluepcs.debug.mode=false` to spark-submit:

```bash
--conf "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=DEBUG,console -Dbluepcs.debug.mode=false"
```

### Issue 2: Application reads from wrong offset position

**Symptom:** Application starts from beginning or latest instead of stored offset.

**Cause:** 
- HBase offset table is empty (if truncated)
- `auto.offset.reset` setting controls fallback behavior

**Fix:** Set appropriate offset reset policy:

```bash
# Start from beginning when no offset found:
--conf spark.kafka.consumer.auto.offset.reset=earliest

# Start from latest when no offset found:
--conf spark.kafka.consumer.auto.offset.reset=latest
```

### Issue 3: Some tables not loading to gold_product

**Symptom:** Certain tables are missing from gold layer.

**Cause:** Table not listed in `gold_hive_table_list` config.

**Fix:** Update config file to include all required tables:

```properties
# Include all tables you want processed:
gold_hive_table_list="svc,bnft,covrg,pkg,pkg_covrg,ntwk,pln,pln_covrg,pln_bnft_tier,pln_mapg,pln_pkg,prod,covrg_ridr"
```

## Sample spark-submit Command

```bash
/usr/odp/current/spark3-client/bin/spark-submit \
  --files /etc/spark3/conf/hive-site.xml,... \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=DEBUG,console -Dbluepcs.debug.mode=false" \
  --conf spark.sql.optimizer.nestedSchemaPruning.enabled=false \
  --conf spark.kafka.consumer.auto.offset.reset=earliest \
  --deploy-mode cluster \
  --driver-memory 10g \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 22g \
  --queue SparkQue \
  --name product_gold_bluepcs_ingestion_streaming \
  --class com.hcsc.datalake.product.bluepcs.core.BluepcssPMMPlusConsumer \
  datalake-product-ingestion-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  product_gold_common_param_odp.prm \
  product_bluepcs_common_param_odp.prm \
  test
```

## Key Configuration Properties

| Property | Description | Default |
|----------|-------------|---------|
| `bluepcs.debug.mode` | Enable/disable debug sampling | `true` (in code) |
| `debugKafkaSampleSize` | Messages to sample in debug mode | `10` |
| `gold_hive_table_list` | Tables to process for GOLD layer | varies |
| `raw_cur_hive_table_list` | Tables to process for RAW layer | varies |
| `auto_offset_reset` | Kafka offset reset policy | `earliest` |

## HBase Offset Management

Offsets are stored in HBase table: `product_bluepcs_stream_kafka_offsets`

To check offsets:
```bash
hbase shell
scan 'product_bluepcs_stream_kafka_offsets'
```

To truncate (start fresh):
```bash
hbase shell
truncate 'product_bluepcs_stream_kafka_offsets'
```
