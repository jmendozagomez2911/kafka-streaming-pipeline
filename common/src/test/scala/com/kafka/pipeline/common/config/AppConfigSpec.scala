package com.kafka.pipeline.common.config

import com.kafka.pipeline.common.error.AppError.ConfigurationError
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AppConfigSpec extends AnyFlatSpec with Matchers {

  "AppConfig.load" should "load default configuration from application.conf" in {
    val result = AppConfig.load()
    result shouldBe a[Right[_, _]]
    val config = result.right.get
    config.appName shouldBe "kafka-streaming-pipeline"
    config.connection.bootstrapServers shouldBe "localhost:9092"
    config.producer.topic shouldBe "pipeline-output"
    config.producer.acks shouldBe "all"
    config.producer.retries shouldBe 3
    config.producer.enableIdempotence shouldBe true
    config.consumer.topic shouldBe "pipeline-output"
    config.consumer.groupId shouldBe "pipeline-consumer-group"
    config.consumer.autoOffsetReset shouldBe "earliest"
  }

  it should "override values from CLI args" in {
    val args = Array(
      "--kafka.bootstrap-servers", "remote:9093",
      "--kafka.producer.topic", "cli-topic"
    )
    val result = AppConfig.load(args)
    result shouldBe a[Right[_, _]]
    val config = result.right.get
    config.connection.bootstrapServers shouldBe "remote:9093"
    config.producer.topic shouldBe "cli-topic"
  }

  it should "return Left for invalid configuration" in {
    val args = Array("--kafka.producer.acks", "invalid-acks-value")
    val result = AppConfig.load(args)
    result shouldBe a[Left[_, _]]
    val error = result.left.get
    error shouldBe a[ConfigurationError]
    error.message should include("acks")
  }

  it should "handle optional security config as None when not set" in {
    val result = AppConfig.load()
    result shouldBe a[Right[_, _]]
    val config = result.right.get
    config.connection.securityProtocol shouldBe None
    config.connection.saslMechanism shouldBe None
    config.connection.saslJaasConfig shouldBe None
  }

  it should "parse all producer config values correctly" in {
    val result = AppConfig.load()
    result shouldBe a[Right[_, _]]
    val p = result.right.get.producer
    p.keySerializer should include("StringSerializer")
    p.valueSerializer should include("StringSerializer")
    p.lingerMs shouldBe 20
    p.batchSize shouldBe 32768
    p.maxInFlightRequests shouldBe 5
    p.requestTimeoutMs shouldBe 30000
    p.deliveryTimeoutMs shouldBe 120000
  }

  it should "parse all consumer config values correctly" in {
    val result = AppConfig.load()
    result shouldBe a[Right[_, _]]
    val c = result.right.get.consumer
    c.keyDeserializer should include("StringDeserializer")
    c.valueDeserializer should include("StringDeserializer")
    c.enableAutoCommit shouldBe true
    c.autoCommitIntervalMs shouldBe 5000
    c.pollTimeoutMs shouldBe 1000L
    c.sessionTimeoutMs shouldBe 45000
    c.maxPollRecords shouldBe 500
    c.partitionAssignmentStrategy should include("CooperativeStickyAssignor")
  }

  it should "handle odd number of CLI args gracefully" in {
    val args = Array("--kafka.bootstrap-servers")
    val result = AppConfig.load(args)
    result shouldBe a[Right[_, _]]
  }
}
