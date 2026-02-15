package com.kafka.pipeline.common.config

object ConfigKeys {

  // Kafka connection
  val BootstrapServers: String = "kafka.bootstrap-servers"
  val SecurityProtocol: String = "kafka.security-protocol"
  val SaslMechanism: String = "kafka.sasl-mechanism"
  val SaslJaasConfig: String = "kafka.sasl-jaas-config"

  // Producer
  val ProducerTopic: String = "kafka.producer.topic"
  val ProducerKeySerializer: String = "kafka.producer.key-serializer"
  val ProducerValueSerializer: String = "kafka.producer.value-serializer"
  val ProducerAcks: String = "kafka.producer.acks"
  val ProducerRetries: String = "kafka.producer.retries"
  val ProducerLingerMs: String = "kafka.producer.linger-ms"
  val ProducerBatchSize: String = "kafka.producer.batch-size"
  val ProducerIdempotence: String = "kafka.producer.enable-idempotence"
  val ProducerMaxInFlightRequests: String = "kafka.producer.max-in-flight-requests"
  val ProducerRequestTimeoutMs: String = "kafka.producer.request-timeout-ms"
  val ProducerDeliveryTimeoutMs: String = "kafka.producer.delivery-timeout-ms"

  // Consumer
  val ConsumerTopic: String = "kafka.consumer.topic"
  val ConsumerGroupId: String = "kafka.consumer.group-id"
  val ConsumerKeyDeserializer: String = "kafka.consumer.key-deserializer"
  val ConsumerValueDeserializer: String = "kafka.consumer.value-deserializer"
  val ConsumerAutoOffsetReset: String = "kafka.consumer.auto-offset-reset"
  val ConsumerEnableAutoCommit: String = "kafka.consumer.enable-auto-commit"
  val ConsumerAutoCommitIntervalMs: String = "kafka.consumer.auto-commit-interval-ms"
  val ConsumerPollTimeoutMs: String = "kafka.consumer.poll-timeout-ms"
  val ConsumerSessionTimeoutMs: String = "kafka.consumer.session-timeout-ms"
  val ConsumerMaxPollRecords: String = "kafka.consumer.max-poll-records"
  val ConsumerPartitionAssignmentStrategy: String = "kafka.consumer.partition-assignment-strategy"

  // Application
  val AppName: String = "app.name"
}
