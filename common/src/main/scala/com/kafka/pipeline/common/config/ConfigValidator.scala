package com.kafka.pipeline.common.config

import com.kafka.pipeline.common.error.AppError
import com.kafka.pipeline.common.error.AppError.ConfigurationError

object ConfigValidator {

  private val ValidOffsetResets: Set[String] = Set("earliest", "latest", "none")
  private val ValidAcks: Set[String] = Set("0", "1", "all", "-1")

  def validate(config: AppConfig): Either[AppError, AppConfig] = {
    val errors = collectErrors(config)
    if (errors.isEmpty) Right(config)
    else Left(ConfigurationError(s"Configuration validation failed:\n${errors.mkString("\n")}"))
  }

  private[config] def collectErrors(config: AppConfig): List[String] = {
    List(
      validateNonEmpty(config.connection.bootstrapServers, "bootstrap-servers"),
      validateNonEmpty(config.producer.topic, "producer topic"),
      validateNonEmpty(config.consumer.topic, "consumer topic"),
      validateNonEmpty(config.consumer.groupId, "consumer group-id"),
      validateAcks(config.producer.acks),
      validateOffsetReset(config.consumer.autoOffsetReset),
      validatePositive(config.producer.retries, "producer retries"),
      validatePositive(config.producer.batchSize, "producer batch-size"),
      validatePositive(config.consumer.pollTimeoutMs.toInt, "consumer poll-timeout-ms"),
      validatePositive(config.consumer.sessionTimeoutMs, "consumer session-timeout-ms"),
      validatePositive(config.consumer.maxPollRecords, "consumer max-poll-records"),
      validateIdempotenceConsistency(config.producer)
    ).flatten
  }

  private def validateNonEmpty(value: String, name: String): Option[String] =
    if (value == null || value.trim.isEmpty) Some(s"  - '$name' must not be empty")
    else None

  private def validateAcks(acks: String): Option[String] =
    if (!ValidAcks.contains(acks)) Some(s"  - 'acks' must be one of ${ValidAcks.mkString(", ")}, got: $acks")
    else None

  private def validateOffsetReset(value: String): Option[String] =
    if (!ValidOffsetResets.contains(value))
      Some(s"  - 'auto-offset-reset' must be one of ${ValidOffsetResets.mkString(", ")}, got: $value")
    else None

  private def validatePositive(value: Int, name: String): Option[String] =
    if (value < 0) Some(s"  - '$name' must be non-negative, got: $value")
    else None

  private def validateIdempotenceConsistency(producer: ProducerConfig): Option[String] =
    if (producer.enableIdempotence && producer.acks != "all" && producer.acks != "-1")
      Some("  - When idempotence is enabled, 'acks' must be 'all' (or '-1')")
    else None
}
