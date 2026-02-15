package com.kafka.pipeline.kafka.adapter

import com.kafka.pipeline.common.config._
import com.kafka.pipeline.common.domain.ReceivedRecord
import com.kafka.pipeline.kafka.handler.RecordHandler
import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class KafkaConsumerAdapterSpec extends AnyFlatSpec with Matchers {

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
      pollTimeoutMs = 100L,
      sessionTimeoutMs = 45000,
      maxPollRecords = 500,
      partitionAssignmentStrategy = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
    )
  )

  private def createMockConsumer(): MockConsumer[String, String] = {
    new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
  }

  private def scheduleMockData(
    mockConsumer: MockConsumer[String, String],
    topic: String,
    records: Seq[(String, String)]
  ): Unit = {
    val tp = new TopicPartition(topic, 0)
    mockConsumer.schedulePollTask(new Runnable {
      override def run(): Unit = {
        mockConsumer.rebalance(List(tp).asJava)
        mockConsumer.updateBeginningOffsets(Map(tp -> java.lang.Long.valueOf(0L)).asJava)
        records.zipWithIndex.foreach { case ((key, value), idx) =>
          mockConsumer.addRecord(new ConsumerRecord[String, String](topic, 0, idx.toLong, key, value))
        }
      }
    })
  }

  "KafkaConsumerAdapter" should "process records through the handler" in {
    val received = ListBuffer.empty[ReceivedRecord]
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = received.synchronized { received += record }
    }

    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    scheduleMockData(mockConsumer, "test-topic", Seq("k1" -> "v1", "k2" -> "v2"))

    val consumerThread = new Thread(() => adapter.start())
    consumerThread.start()

    Thread.sleep(800)
    adapter.shutdown()
    consumerThread.join(5000)

    received.synchronized {
      received.size shouldBe 2
      received(0).key shouldBe Some("k1")
      received(0).value shouldBe "v1"
      received(0).partition shouldBe 0
      received(0).offset shouldBe 0L
      received(1).key shouldBe Some("k2")
      received(1).value shouldBe "v2"
      received(1).offset shouldBe 1L
    }
  }

  it should "report isRunning correctly" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }

    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    adapter.isRunning shouldBe false

    val tp = new TopicPartition("test-topic", 0)
    mockConsumer.schedulePollTask(new Runnable {
      override def run(): Unit = {
        mockConsumer.rebalance(List(tp).asJava)
        mockConsumer.updateBeginningOffsets(Map(tp -> java.lang.Long.valueOf(0L)).asJava)
      }
    })

    val consumerThread = new Thread(() => adapter.start())
    consumerThread.start()

    Thread.sleep(500)
    adapter.isRunning shouldBe true
    adapter.shutdown()
    consumerThread.join(5000)

    adapter.isRunning shouldBe false
  }

  it should "handle records with null keys" in {
    val received = ListBuffer.empty[ReceivedRecord]
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = received.synchronized { received += record }
    }

    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val tp = new TopicPartition("test-topic", 0)
    mockConsumer.schedulePollTask(new Runnable {
      override def run(): Unit = {
        mockConsumer.rebalance(List(tp).asJava)
        mockConsumer.updateBeginningOffsets(Map(tp -> java.lang.Long.valueOf(0L)).asJava)
        mockConsumer.addRecord(new ConsumerRecord[String, String]("test-topic", 0, 0L, null, "value-no-key"))
      }
    })

    val consumerThread = new Thread(() => adapter.start())
    consumerThread.start()

    Thread.sleep(800)
    adapter.shutdown()
    consumerThread.join(5000)

    received.synchronized {
      received.size shouldBe 1
      received.head.key shouldBe None
      received.head.value shouldBe "value-no-key"
    }
  }

  it should "not start twice" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }

    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val tp = new TopicPartition("test-topic", 0)
    mockConsumer.schedulePollTask(new Runnable {
      override def run(): Unit = {
        mockConsumer.rebalance(List(tp).asJava)
        mockConsumer.updateBeginningOffsets(Map(tp -> java.lang.Long.valueOf(0L)).asJava)
      }
    })

    val thread1 = new Thread(() => adapter.start())
    thread1.start()

    Thread.sleep(500)
    adapter.isRunning shouldBe true

    adapter.shutdown()
    thread1.join(5000)
  }

  it should "call shutdown independently" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }
    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    adapter.isRunning shouldBe false
    noException should be thrownBy adapter.shutdown()
    adapter.isRunning shouldBe false
  }

  it should "handle wakeup exception during poll when already shutting down" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }

    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val tp = new TopicPartition("test-topic", 0)
    mockConsumer.schedulePollTask(new Runnable {
      override def run(): Unit = {
        mockConsumer.rebalance(List(tp).asJava)
        mockConsumer.updateBeginningOffsets(Map(tp -> java.lang.Long.valueOf(0L)).asJava)
      }
    })

    val consumerThread = new Thread(() => adapter.start())
    consumerThread.start()
    Thread.sleep(500)

    adapter.shutdown()
    consumerThread.join(5000)

    adapter.isRunning shouldBe false
  }

  it should "include partition assignment strategy in properties" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }
    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val props = adapter.buildProperties(testConfig.connection, testConfig.consumer)
    props.getProperty("partition.assignment.strategy") should include("CooperativeStickyAssignor")
  }

  it should "handle runtime exception in handler gracefully" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = throw new RuntimeException("handler boom")
    }

    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val tp = new TopicPartition("test-topic", 0)
    mockConsumer.schedulePollTask(new Runnable {
      override def run(): Unit = {
        mockConsumer.rebalance(List(tp).asJava)
        mockConsumer.updateBeginningOffsets(Map(tp -> java.lang.Long.valueOf(0L)).asJava)
        mockConsumer.addRecord(new ConsumerRecord[String, String]("test-topic", 0, 0L, "k1", "v1"))
      }
    })

    val consumerThread = new Thread(() => adapter.start())
    consumerThread.start()

    Thread.sleep(800)
    consumerThread.join(5000)

    adapter.isRunning shouldBe false
  }

  it should "handle consumer wakeup when running is still true" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }

    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val tp = new TopicPartition("test-topic", 0)
    mockConsumer.schedulePollTask(new Runnable {
      override def run(): Unit = {
        mockConsumer.rebalance(List(tp).asJava)
        mockConsumer.updateBeginningOffsets(Map(tp -> java.lang.Long.valueOf(0L)).asJava)
      }
    })
    mockConsumer.schedulePollTask(new Runnable {
      override def run(): Unit = {
        mockConsumer.wakeup()
      }
    })

    val consumerThread = new Thread(() => adapter.start())
    consumerThread.start()
    consumerThread.join(5000)

    adapter.isRunning shouldBe false
  }

  "closeConsumer" should "close the consumer and set running to false" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }
    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    noException should be thrownBy adapter.closeConsumer()
  }

  "processRecords" should "process empty records without error" in {
    val received = ListBuffer.empty[ReceivedRecord]
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = received.synchronized { received += record }
    }
    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val emptyRecords = new org.apache.kafka.clients.consumer.ConsumerRecords[String, String](
      new java.util.HashMap()
    )
    noException should be thrownBy adapter.processRecords(emptyRecords)
    received shouldBe empty
  }

  it should "process non-empty records and delegate to handler" in {
    val received = ListBuffer.empty[ReceivedRecord]
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = received.synchronized { received += record }
    }
    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val tp = new TopicPartition("test-topic", 0)
    val recordsList = new java.util.ArrayList[ConsumerRecord[String, String]]()
    recordsList.add(new ConsumerRecord[String, String]("test-topic", 0, 0L, "k1", "v1"))
    recordsList.add(new ConsumerRecord[String, String]("test-topic", 0, 1L, null, "v2"))
    val recordsMap = new java.util.HashMap[TopicPartition, java.util.List[ConsumerRecord[String, String]]]()
    recordsMap.put(tp, recordsList)
    val records = new org.apache.kafka.clients.consumer.ConsumerRecords[String, String](recordsMap)

    adapter.processRecords(records)

    received.synchronized {
      received.size shouldBe 2
      received(0).key shouldBe Some("k1")
      received(0).value shouldBe "v1"
      received(1).key shouldBe None
      received(1).value shouldBe "v2"
    }
  }

  "buildProperties" should "set all expected consumer properties" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }
    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val props = adapter.buildProperties(testConfig.connection, testConfig.consumer)
    props.getProperty("bootstrap.servers") shouldBe "localhost:9092"
    props.getProperty("group.id") shouldBe "test-group"
    props.getProperty("auto.offset.reset") shouldBe "earliest"
    props.getProperty("enable.auto.commit") shouldBe "true"
    props.getProperty("auto.commit.interval.ms") shouldBe "5000"
    props.getProperty("session.timeout.ms") shouldBe "45000"
    props.getProperty("max.poll.records") shouldBe "500"
    props.getProperty("key.deserializer") should include("StringDeserializer")
    props.getProperty("value.deserializer") should include("StringDeserializer")
  }

  it should "set security properties when present" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }
    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val secureConn = testConfig.connection.copy(
      securityProtocol = Some("SASL_SSL"),
      saslMechanism = Some("SCRAM-SHA-256"),
      saslJaasConfig = Some("some.jaas.config")
    )
    val props = adapter.buildProperties(secureConn, testConfig.consumer)
    props.getProperty("security.protocol") shouldBe "SASL_SSL"
    props.getProperty("sasl.mechanism") shouldBe "SCRAM-SHA-256"
    props.getProperty("sasl.jaas.config") shouldBe "some.jaas.config"
  }

  it should "not set security properties when absent" in {
    val handler = new RecordHandler {
      override def handle(record: ReceivedRecord): Unit = ()
    }
    val mockConsumer = createMockConsumer()
    val adapter = new KafkaConsumerAdapter(testConfig, handler, _ => mockConsumer)

    val props = adapter.buildProperties(testConfig.connection, testConfig.consumer)
    props.getProperty("security.protocol") shouldBe null
    props.getProperty("sasl.mechanism") shouldBe null
    props.getProperty("sasl.jaas.config") shouldBe null
  }
}
