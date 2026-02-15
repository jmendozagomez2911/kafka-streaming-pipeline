package com.kafka.pipeline.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigKeysSpec extends AnyFlatSpec with Matchers {

  "ConfigKeys" should "have non-empty kafka connection keys" in {
    ConfigKeys.BootstrapServers should not be empty
    ConfigKeys.SecurityProtocol should not be empty
    ConfigKeys.SaslMechanism should not be empty
    ConfigKeys.SaslJaasConfig should not be empty
  }

  it should "have non-empty producer keys" in {
    ConfigKeys.ProducerTopic should not be empty
    ConfigKeys.ProducerKeySerializer should not be empty
    ConfigKeys.ProducerValueSerializer should not be empty
    ConfigKeys.ProducerAcks should not be empty
    ConfigKeys.ProducerRetries should not be empty
    ConfigKeys.ProducerLingerMs should not be empty
    ConfigKeys.ProducerBatchSize should not be empty
    ConfigKeys.ProducerIdempotence should not be empty
    ConfigKeys.ProducerMaxInFlightRequests should not be empty
    ConfigKeys.ProducerRequestTimeoutMs should not be empty
    ConfigKeys.ProducerDeliveryTimeoutMs should not be empty
  }

  it should "have non-empty consumer keys" in {
    ConfigKeys.ConsumerTopic should not be empty
    ConfigKeys.ConsumerGroupId should not be empty
    ConfigKeys.ConsumerKeyDeserializer should not be empty
    ConfigKeys.ConsumerValueDeserializer should not be empty
    ConfigKeys.ConsumerAutoOffsetReset should not be empty
    ConfigKeys.ConsumerEnableAutoCommit should not be empty
    ConfigKeys.ConsumerAutoCommitIntervalMs should not be empty
    ConfigKeys.ConsumerPollTimeoutMs should not be empty
    ConfigKeys.ConsumerSessionTimeoutMs should not be empty
    ConfigKeys.ConsumerMaxPollRecords should not be empty
    ConfigKeys.ConsumerPartitionAssignmentStrategy should not be empty
  }

  it should "have non-empty app keys" in {
    ConfigKeys.AppName should not be empty
  }

  it should "use correct config path prefixes" in {
    ConfigKeys.BootstrapServers should startWith("kafka.")
    ConfigKeys.ProducerTopic should startWith("kafka.producer.")
    ConfigKeys.ConsumerTopic should startWith("kafka.consumer.")
    ConfigKeys.AppName should startWith("app.")
  }
}
