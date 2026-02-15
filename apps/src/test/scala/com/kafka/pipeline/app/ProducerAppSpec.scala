package com.kafka.pipeline.app

import com.kafka.pipeline.common.config._
import com.kafka.pipeline.common.domain.KafkaMessage
import com.kafka.pipeline.common.error.AppError
import com.kafka.pipeline.common.error.AppError.ProducerError
import com.kafka.pipeline.kafka.adapter.KafkaProducerAdapter
import com.kafka.pipeline.kafka.port.ProducerPort
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProducerAppSpec extends AnyFlatSpec with Matchers {

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

  "ProducerApp.run" should "send 10 messages via the producer" in {
    val mockProducer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val adapter = new KafkaProducerAdapter(testConfig, _ => mockProducer)

    noException should be thrownBy ProducerApp.run(adapter, "test-topic")
    mockProducer.history().size() shouldBe 10
  }

  it should "send messages with correct keys" in {
    val mockProducer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val adapter = new KafkaProducerAdapter(testConfig, _ => mockProducer)

    ProducerApp.run(adapter, "test-topic")

    val keys = new java.util.ArrayList[String]()
    val history = mockProducer.history()
    for (i <- 0 until history.size()) {
      keys.add(history.get(i).key())
    }
    import scala.collection.JavaConverters._
    keys.asScala should contain allOf("key-1", "key-5", "key-10")
  }

  it should "send messages to the correct topic" in {
    val mockProducer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val adapter = new KafkaProducerAdapter(testConfig, _ => mockProducer)

    ProducerApp.run(adapter, "my-custom-topic")

    import scala.collection.JavaConverters._
    val topics = mockProducer.history().asScala.map(_.topic()).toSet
    topics shouldBe Set("test-topic")
  }

  it should "produce messages with JSON values" in {
    val mockProducer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val adapter = new KafkaProducerAdapter(testConfig, _ => mockProducer)

    ProducerApp.run(adapter, "test-topic")

    import scala.collection.JavaConverters._
    val values = mockProducer.history().asScala.map(_.value())
    values.head should include("\"id\": 1")
    values.head should include("\"payload\": \"message-1\"")
    values.last should include("\"id\": 10")
  }

  "ProducerApp.handleResult" should "log success for Right" in {
    noException should be thrownBy ProducerApp.handleResult(1)(Right(()))
  }

  it should "log error for Left" in {
    val error = ProducerError("send failed")
    noException should be thrownBy ProducerApp.handleResult(1)(Left(error))
  }

  it should "include index in error handling" in {
    val error = ProducerError("broker down")
    noException should be thrownBy ProducerApp.handleResult(5)(Left(error))
  }

  it should "handle different indices" in {
    for (i <- 1 to 10) {
      noException should be thrownBy ProducerApp.handleResult(i)(Right(()))
    }
  }

  "ProducerApp.run with stub" should "work with a ProducerPort stub" in {
    var sentCount = 0
    var flushed = false
    val stubProducer = new ProducerPort {
      override def send(message: KafkaMessage): Either[AppError, Unit] = {
        sentCount += 1
        Right(())
      }
      override def sendAsync(message: KafkaMessage)(callback: Either[AppError, Unit] => Unit): Unit = {
        sentCount += 1
        callback(Right(()))
      }
      override def flush(): Either[AppError, Unit] = {
        flushed = true
        Right(())
      }
      override def close(): Unit = ()
    }

    ProducerApp.run(stubProducer, "test-topic")
    sentCount shouldBe 10
    flushed shouldBe true
  }

  it should "handle errors from stub producer" in {
    val stubProducer = new ProducerPort {
      override def send(message: KafkaMessage): Either[AppError, Unit] =
        Left(ProducerError("stub error"))
      override def sendAsync(message: KafkaMessage)(callback: Either[AppError, Unit] => Unit): Unit =
        callback(Left(ProducerError("stub async error")))
      override def flush(): Either[AppError, Unit] = Right(())
      override def close(): Unit = ()
    }

    noException should be thrownBy ProducerApp.run(stubProducer, "test-topic")
  }
}
