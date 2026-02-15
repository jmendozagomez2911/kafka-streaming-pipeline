package com.kafka.pipeline.kafka.adapter

import com.kafka.pipeline.common.config._
import com.kafka.pipeline.common.domain.KafkaMessage
import com.kafka.pipeline.common.error.AppError.ProducerError
import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConverters._

class KafkaProducerAdapterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var mockProducer: MockProducer[String, String] = _
  private var adapter: KafkaProducerAdapter = _

  private val testConfig = AppConfig(
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

  override def beforeEach(): Unit = {
    mockProducer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    adapter = new KafkaProducerAdapter(testConfig, _ => mockProducer)
  }

  "KafkaProducerAdapter.send" should "send a message synchronously" in {
    val msg = KafkaMessage(Some("key1"), "value1")
    val result = adapter.send(msg)
    result shouldBe Right(())
    mockProducer.history().size() shouldBe 1
    val sent = mockProducer.history().get(0)
    sent.key() shouldBe "key1"
    sent.value() shouldBe "value1"
  }

  it should "send a message with null key when key is None" in {
    val msg = KafkaMessage(None, "value-only")
    val result = adapter.send(msg)
    result shouldBe Right(())
    val sent = mockProducer.history().get(0)
    sent.key() shouldBe null
    sent.value() shouldBe "value-only"
  }

  it should "return Left on send failure" in {
    val failingMock = new MockProducer[String, String](false, new StringSerializer, new StringSerializer)
    val failAdapter = new KafkaProducerAdapter(testConfig, _ => failingMock)

    val msg = KafkaMessage(Some("key1"), "value1")

    val sendThread = new Thread(() => {
      Thread.sleep(50)
      failingMock.errorNext(new RuntimeException("broker down"))
    })
    sendThread.start()

    val result = failAdapter.send(msg)
    result shouldBe a[Left[_, _]]
    result.left.get shouldBe a[ProducerError]
    result.left.get.message should include("broker down")
    sendThread.join()
  }

  "KafkaProducerAdapter.sendAsync" should "invoke callback on success" in {
    var callbackResult: Either[Any, Unit] = null
    val msg = KafkaMessage(Some("key-async"), "value-async")

    adapter.sendAsync(msg) { result =>
      callbackResult = result
    }

    callbackResult shouldBe Right(())
    mockProducer.history().size() shouldBe 1
  }

  it should "invoke callback with error on failure" in {
    val failingMock = new MockProducer[String, String](false, new StringSerializer, new StringSerializer)
    val failAdapter = new KafkaProducerAdapter(testConfig, _ => failingMock)

    var callbackResult: Either[Any, Unit] = null
    val msg = KafkaMessage(Some("key-fail"), "value-fail")

    failAdapter.sendAsync(msg) { result =>
      callbackResult = result
    }

    failingMock.errorNext(new RuntimeException("async failure"))
    callbackResult shouldBe a[Left[_, _]]
  }

  "KafkaProducerAdapter.flush" should "succeed on a healthy producer" in {
    adapter.flush() shouldBe Right(())
  }

  "KafkaProducerAdapter.close" should "close without errors" in {
    noException should be thrownBy adapter.close()
  }

  "buildRecord" should "include headers when provided" in {
    val msg = KafkaMessage(Some("k"), "v", Map("h1" -> "v1", "h2" -> "v2"))
    val record = adapter.buildRecord("test-topic", msg)
    record.topic() shouldBe "test-topic"
    record.key() shouldBe "k"
    record.value() shouldBe "v"
    val headers = record.headers().asScala.map(h => h.key() -> new String(h.value(), "UTF-8")).toMap
    headers shouldBe Map("h1" -> "v1", "h2" -> "v2")
  }

  it should "create record with empty headers" in {
    val msg = KafkaMessage(Some("k"), "v")
    val record = adapter.buildRecord("my-topic", msg)
    record.headers().asScala shouldBe empty
  }

  "buildProperties" should "set all expected properties" in {
    val props = adapter.buildProperties(testConfig.connection, testConfig.producer)
    props.getProperty("bootstrap.servers") shouldBe "localhost:9092"
    props.getProperty("key.serializer") should include("StringSerializer")
    props.getProperty("acks") shouldBe "all"
    props.getProperty("retries") shouldBe "3"
    props.getProperty("enable.idempotence") shouldBe "true"
  }

  it should "set security properties when present" in {
    val secureConn = testConfig.connection.copy(
      securityProtocol = Some("SASL_SSL"),
      saslMechanism = Some("PLAIN"),
      saslJaasConfig = Some("org.apache.kafka.common.security.plain.PlainLoginModule required;")
    )
    val props = adapter.buildProperties(secureConn, testConfig.producer)
    props.getProperty("security.protocol") shouldBe "SASL_SSL"
    props.getProperty("sasl.mechanism") shouldBe "PLAIN"
    props.getProperty("sasl.jaas.config") should include("PlainLoginModule")
  }

  it should "not set security properties when absent" in {
    val props = adapter.buildProperties(testConfig.connection, testConfig.producer)
    props.getProperty("security.protocol") shouldBe null
    props.getProperty("sasl.mechanism") shouldBe null
    props.getProperty("sasl.jaas.config") shouldBe null
  }

  it should "set all producer-specific properties" in {
    val props = adapter.buildProperties(testConfig.connection, testConfig.producer)
    props.getProperty("value.serializer") should include("StringSerializer")
    props.getProperty("linger.ms") shouldBe "20"
    props.getProperty("batch.size") shouldBe "32768"
    props.getProperty("max.in.flight.requests.per.connection") shouldBe "5"
    props.getProperty("request.timeout.ms") shouldBe "30000"
    props.getProperty("delivery.timeout.ms") shouldBe "120000"
  }

  "KafkaProducerAdapter" should "handle close when flush throws" in {
    val failingMock = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val failAdapter = new KafkaProducerAdapter(testConfig, _ => failingMock)
    failingMock.close()
    noException should be thrownBy failAdapter.close()
  }

  it should "send multiple messages in sequence" in {
    val messages = (1 to 5).map(i => KafkaMessage(Some(s"key-$i"), s"value-$i"))
    messages.foreach { msg =>
      adapter.send(msg) shouldBe Right(())
    }
    mockProducer.history().size() shouldBe 5
  }

  it should "send async with error callback when producer fails" in {
    val failingMock = new MockProducer[String, String](false, new StringSerializer, new StringSerializer)
    val failAdapter = new KafkaProducerAdapter(testConfig, _ => failingMock)

    var results = List.empty[Either[Any, Unit]]
    val msg = KafkaMessage(Some("k"), "v")
    failAdapter.sendAsync(msg)(r => results = results :+ r)
    failAdapter.sendAsync(msg)(r => results = results :+ r)

    failingMock.errorNext(new RuntimeException("fail1"))
    failingMock.errorNext(new RuntimeException("fail2"))

    results.size shouldBe 2
    results.foreach(_ shouldBe a[Left[_, _]])
  }
}
