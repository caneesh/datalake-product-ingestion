package com.hcsc.datalake.product.bluepcs.core

case class HdfsPollingConfig(
  incomingDir: String,
  processingDir: String,
  archiveDir: String,
  errorDir: String,
  pollIntervalMs: Long = 30000L,
  maxFilesPerPoll: Int = 10,
  fileExtension: String = ".json",
  fileStabilityMs: Long = 5000L,
  recoverStrandedOnStartup: Boolean = true,
  maxFileSizeBytes: Long = 100L * 1024 * 1024,
  parallelFileClaims: Int = 4,        // Parallel threads for file claiming
  batchProcessing: Boolean = true      // Enable batch mode for efficiency
)
