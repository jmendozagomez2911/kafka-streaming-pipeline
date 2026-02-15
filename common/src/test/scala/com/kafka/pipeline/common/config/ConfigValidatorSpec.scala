package com.kafka.pipeline.common.config

import com.kafka.pipeline.common.error.AppError.ConfigurationError
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigValidatorSpec extends AnyFlatSpec with Matchers {

  private def validConfig: AppConfig = AppConfig(
    appName = "test-app",
    connection = KafkaConnectionConfig(
      bootstrapServers = "localhost:9092",
      securityProtocol = None,
      saslMechanism = None,
      saslJaasConfig = None
    ),
    producer = ProducerConfig(
      topic = "test-topic",
      keySerializer = "org.apache.kafka.common.serialization.StringSerializer",
      valueSerializer = "org.apache.kafka.common.serialization.StringSerializer",
      acks = "all",
      retries = 3,
      lingerMs = 20,
      batchSize = 32768,
      enableIdempotence = true,
      maxInFlightRequests = 5,
      requestTimeoutMs = 30000,
      deliveryTimeoutMs = 120000
    ),
    consumer = ConsumerConfig(
      topic = "test-topic",
      groupId = "test-group",
      keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
      valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
      autoOffsetReset = "earliest",
      enableAutoCommit = true,
      autoCommitIntervalMs = 5000,
      pollTimeoutMs = 1000L,
      sessionTimeoutMs = 45000,
      maxPollRecords = 500,
      partitionAssignmentStrategy = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
    )
  )

  "ConfigValidator" should "accept a valid configuration" in {
    ConfigValidator.validate(validConfig) shouldBe Right(validConfig)
  }

  it should "reject empty bootstrap servers" in {
    val cfg = validConfig.copy(connection = validConfig.connection.copy(bootstrapServers = ""))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("bootstrap-servers")
  }

  it should "reject null bootstrap servers" in {
    val cfg = validConfig.copy(connection = validConfig.connection.copy(bootstrapServers = null))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
  }

  it should "reject empty producer topic" in {
    val cfg = validConfig.copy(producer = validConfig.producer.copy(topic = ""))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("producer topic")
  }

  it should "reject empty consumer topic" in {
    val cfg = validConfig.copy(consumer = validConfig.consumer.copy(topic = ""))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("consumer topic")
  }

  it should "reject empty consumer group id" in {
    val cfg = validConfig.copy(consumer = validConfig.consumer.copy(groupId = ""))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("consumer group-id")
  }

  it should "reject invalid acks value" in {
    val cfg = validConfig.copy(producer = validConfig.producer.copy(acks = "two", enableIdempotence = false))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("acks")
  }

  it should "accept valid acks values" in {
    for (acks <- Seq("0", "1", "all", "-1")) {
      val idempotent = acks == "all" || acks == "-1"
      val cfg = validConfig.copy(producer = validConfig.producer.copy(acks = acks, enableIdempotence = idempotent))
      ConfigValidator.validate(cfg) shouldBe a[Right[_, _]]
    }
  }

  it should "reject invalid auto offset reset" in {
    val cfg = validConfig.copy(consumer = validConfig.consumer.copy(autoOffsetReset = "something"))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("auto-offset-reset")
  }

  it should "accept all valid auto offset reset values" in {
    for (reset <- Seq("earliest", "latest", "none")) {
      val cfg = validConfig.copy(consumer = validConfig.consumer.copy(autoOffsetReset = reset))
      ConfigValidator.validate(cfg) shouldBe a[Right[_, _]]
    }
  }

  it should "reject negative retries" in {
    val cfg = validConfig.copy(producer = validConfig.producer.copy(retries = -1))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("retries")
  }

  it should "reject negative batch size" in {
    val cfg = validConfig.copy(producer = validConfig.producer.copy(batchSize = -1))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("batch-size")
  }

  it should "reject negative poll timeout" in {
    val cfg = validConfig.copy(consumer = validConfig.consumer.copy(pollTimeoutMs = -1L))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("poll-timeout-ms")
  }

  it should "reject negative session timeout" in {
    val cfg = validConfig.copy(consumer = validConfig.consumer.copy(sessionTimeoutMs = -1))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("session-timeout-ms")
  }

  it should "reject negative max poll records" in {
    val cfg = validConfig.copy(consumer = validConfig.consumer.copy(maxPollRecords = -1))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("max-poll-records")
  }

  it should "reject idempotence enabled with acks != all" in {
    val cfg = validConfig.copy(producer = validConfig.producer.copy(acks = "1", enableIdempotence = true))
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    result.left.get.message should include("idempotence")
  }

  it should "allow idempotence enabled with acks = -1" in {
    val cfg = validConfig.copy(producer = validConfig.producer.copy(acks = "-1", enableIdempotence = true))
    ConfigValidator.validate(cfg) shouldBe a[Right[_, _]]
  }

  it should "collect multiple errors at once" in {
    val cfg = validConfig.copy(
      connection = validConfig.connection.copy(bootstrapServers = ""),
      producer = validConfig.producer.copy(topic = "", acks = "bad", enableIdempotence = false),
      consumer = validConfig.consumer.copy(groupId = "", autoOffsetReset = "bad")
    )
    val result = ConfigValidator.validate(cfg)
    result shouldBe a[Left[_, _]]
    val error = result.left.get.asInstanceOf[ConfigurationError]
    error.message should include("bootstrap-servers")
    error.message should include("producer topic")
    error.message should include("group-id")
    error.message should include("acks")
    error.message should include("auto-offset-reset")
  }

  "collectErrors" should "return empty list for valid config" in {
    ConfigValidator.collectErrors(validConfig) shouldBe empty
  }
}
