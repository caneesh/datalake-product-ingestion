# Bluepcs Ingestion Troubleshooting Guide

## Table of Contents
- [Kafka Mode Issues](#kafka-mode-issues)
- [HDFS Mode Issues](#hdfs-mode-issues)

---

# Kafka Mode Issues

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

---

# HDFS Mode Issues

## Issue 1: HDFS file processing is very slow

**Symptom:** Files take a long time to process, one file at a time.

**Cause:** Default sequential processing mode processes files one at a time on the driver.

**Fix:** Enable batch processing mode in config:

```properties
hdfs_batch_processing=true
hdfs_parallel_file_claims=4
hdfs_max_files_per_poll=20
```

Or in HdfsPollingConfig:
```scala
HdfsPollingConfig(
  ...
  batchProcessing = true,      // Process multiple files in single Spark job
  parallelFileClaims = 4,      // Parallel threads for file claiming
  maxFilesPerPoll = 20         // Files per batch
)
```

**Performance comparison:**
| Mode | 100 files | Notes |
|------|-----------|-------|
| Sequential | ~10 min | 1 Spark job per file |
| Batch | ~1 min | 1 Spark job per batch |

## Issue 2: Files stuck in processing directory

**Symptom:** Files remain in processing directory after restart.

**Cause:** Application crashed before archiving completed files.

**Fix:** Enable stranded file recovery:

```properties
hdfs_recover_stranded=true
```

This moves stranded files back to incoming on startup.

## Issue 3: Large files cause OOM

**Symptom:** OutOfMemoryError when processing large files.

**Cause:** File exceeds max file size or too many files in batch.

**Fix:** Adjust limits:

```properties
hdfs_max_file_size_bytes=104857600    # 100MB max per file
hdfs_max_files_per_poll=10            # Reduce batch size
```

## HDFS Mode Configuration Reference

| Property | Description | Default |
|----------|-------------|---------|
| `hdfs_incoming_dir` | Directory to poll for new files | required |
| `hdfs_processing_dir` | Working directory for files being processed | required |
| `hdfs_archive_dir` | Directory for successfully processed files | required |
| `hdfs_error_dir` | Directory for failed files | required |
| `hdfs_poll_interval_ms` | Polling interval in milliseconds | 30000 |
| `hdfs_max_files_per_poll` | Max files per poll cycle | 10 |
| `hdfs_file_extension` | File extension to look for | .json |
| `hdfs_file_stability_ms` | Wait time before processing new file | 5000 |
| `hdfs_recover_stranded` | Recover files stuck in processing | true |
| `hdfs_max_file_size_bytes` | Max file size in bytes | 104857600 |

## HDFS Directory Structure

```
/incoming/raw/product/bluepcs/
├── incoming/          <- New files land here
├── processing/        <- Files being processed
├── archive/           <- Successfully processed (by date)
│   ├── 20260816/
│   └── 20260817/
└── error/             <- Failed files with .error reason
```
