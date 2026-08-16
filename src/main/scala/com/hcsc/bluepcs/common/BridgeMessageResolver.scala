package com.hcsc.bluepcs.common

import ...

object BridgeMessageResolver extends Serializable {

  /** Highest claim-check contract version this consumer understands. */
  val SupportedSchemaVersion = 1

  val ExpectedSource = "mq-kafka-bridge"

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  final case class ResolvedMessage(
                                    json: String,            // full wrapper document (legacy shape), ready for processor
                                    isClaimCheck: Boolean,   // true when fetched via hdfsPath
                                    eventId: Option[String]  // present only for bridge messages - usable for dedup
                                  )

  /**
   * Normalizes one Kafka record value; returns None for a claim-check duplicate
   * whose HDFS file no longer exists (already processed + archived).
   * Still throws on unknown schemaVersion or checksum mismatch - those are
   * contract violations that must stop the job, not be skipped.
   */
  def resolveOrSkip(raw: String, fs: FileSystem, mapper: ObjectMapper): Option[ResolvedMessage] =
    try {
      Some(resolve(raw, fs, mapper))
    } catch {
      case e: FileNotFoundException =>
        logger.warn(
          "@@@ Claim-check HDFS file missing - treating as already-processed duplicate and skipping: {}",
          e.getMessage)
        None
    }

  /** Strict variant: throws on any resolution problem, including a missing file. */
  def resolve(raw: String, fs: FileSystem, mapper: ObjectMapper): ResolvedMessage = {
    parseJsonOrNull(raw, mapper) match {
      case node if node != null && isBridgeMessage(node) =>
        val version = node.path("schemaVersion").asInt(1) // pre-marker builds -> treat as v1
        require(
          version <= SupportedSchemaVersion,
          s"Unsupported bridge schemaVersion $version " +
            s"(this consumer supports <= $SupportedSchemaVersion). " +
            s"eventId=${node.path("eventId").asText("?")} - update the consumer before processing."
        )
        resolveClaimCheck(node, fs)

      case _ =>
        // Legacy Talend inline message - OR anything that is not parseable JSON.
        // Both pass through untouched: the processor must see exactly what it saw
        // before this resolver existed, including malformed values it already has
        // its own handling for. The resolver must never fail a batch on input the
        // old pipeline tolerated.
        ResolvedMessage(raw, isClaimCheck = false, eventId = None)
    }
  }

  /** Null when the value is not valid JSON (or is null/empty) - caller passes through. */
  private def parseJsonOrNull(raw: String, mapper: ObjectMapper): JsonNode =
    try {
      if (raw == null || raw.trim.isEmpty) null else mapper.readTree(raw)
    } catch {
      case _: Exception =>
        logger.warn("@@@ Kafka value is not valid JSON - passing through to processor unchanged")
        null
    }

  /**
   * Primary discriminator: schemaVersion. Fallback: hdfsPath-without-RestAPIResponse
   * covers messages from bridge builds that predate the marker fields.
   */
  private def isBridgeMessage(node: JsonNode): Boolean =
    node.hasNonNull("schemaVersion") ||
      (node.hasNonNull("hdfsPath") && node.hasNonNull("eventId") && !node.has("RestAPIResponse"))

  private def resolveClaimCheck(node: JsonNode, fs: FileSystem): ResolvedMessage = {
    val hdfsPath = node.get("hdfsPath").asText()
    val eventId  = node.get("eventId").asText()

    val bytes = {
      val in = fs.open(new Path(hdfsPath)) // throws FileNotFoundException if archived
      try org.apache.commons.io.IOUtils.toByteArray(in)
      finally in.close()
    }

    // Verify integrity against the checksum the bridge computed at write time.
    val actual = sha256Hex(bytes)
    val expected = node.path("checksum").asText("")
    require(
      expected.isEmpty || actual == expected,
      s"Checksum mismatch for $hdfsPath (eventId=$eventId): expected $expected, got $actual"
    )

    // Unmissable per-message marker: grep consumer logs for "CLAIM-CHECK RESOLVED"
    // to find every message that took the new bridge path (legacy messages log nothing).
    logger.info(
      "@@@ CLAIM-CHECK RESOLVED: eventId={}, hdfsPath={}, bytes={}",
      eventId, hdfsPath, Integer.valueOf(bytes.length))

    ResolvedMessage(
      json = new String(bytes, StandardCharsets.UTF_8),
      isClaimCheck = true,
      eventId = Some(eventId)
    )
  }

  private def sha256Hex(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x".format(_)).mkString

}
