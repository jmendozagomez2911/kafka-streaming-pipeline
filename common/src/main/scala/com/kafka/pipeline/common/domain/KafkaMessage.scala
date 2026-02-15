package com.kafka.pipeline.common.domain

final case class KafkaMessage(
  key: Option[String],
  value: String,
  headers: Map[String, String] = Map.empty
)

final case class ReceivedRecord(
  topic: String,
  partition: Int,
  offset: Long,
  key: Option[String],
  value: String,
  timestamp: Long
)
