package com.kafka.pipeline.kafka.adapter

import com.kafka.pipeline.common.config.{AppConfig, ConsumerConfig => AppConsumerConfig, KafkaConnectionConfig}
import com.kafka.pipeline.common.domain.ReceivedRecord
import com.kafka.pipeline.common.logging.Logging
import com.kafka.pipeline.kafka.handler.RecordHandler
import com.kafka.pipeline.kafka.port.ConsumerPort
import org.apache.kafka.clients.consumer.{Consumer => KafkaConsumerInterface, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException

import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._

class KafkaConsumerAdapter(
  config: AppConfig,
  handler: RecordHandler,
  consumerFactory: Properties => KafkaConsumerInterface[String, String]
) extends ConsumerPort with Logging {

  // $COVERAGE-OFF$ Auxiliary constructor requires live Kafka broker
  def this(config: AppConfig, handler: RecordHandler) =
    this(config, handler, props => new KafkaConsumer[String, String](props))
  // $COVERAGE-ON$

  private val running = new AtomicBoolean(false)
  private val consumer: KafkaConsumerInterface[String, String] = {
    val props = buildProperties(config.connection, config.consumer)
    log.info(s"Initialising Kafka consumer for topic=${config.consumer.topic} group-id=${config.consumer.groupId} bootstrap-servers=${config.connection.bootstrapServers}")
    consumerFactory(props)
  }

  override def start(): Unit = {
    if (!running.compareAndSet(false, true)) {
      log.warn("Consumer is already running")
      return
    }

    installShutdownHook()

    try {
      consumer.subscribe(List(config.consumer.topic).asJava)
      log.info(s"Consumer subscribed to topic=${config.consumer.topic}")
      pollLoop()
    } catch {
      case _: WakeupException =>
        if (running.get()) {
          log.error("Unexpected WakeupException while consumer is still marked as running")
        }
        log.info("Consumer wakeup received, shutting down gracefully")
      case ex: Exception =>
        log.error(s"Unexpected exception in consumer loop: ${ex.getMessage}")
    } finally {
      closeConsumer()
    }
  }

  private[adapter] def pollLoop(): Unit = {
    while (running.get()) {
      val records = consumer.poll(Duration.ofMillis(config.consumer.pollTimeoutMs))
      val count = records.count()
      if (count > 0) {
        log.info(s"Polled $count records from topic=${config.consumer.topic}")
      }
      processRecords(records)
    }
  }

  private[adapter] def processRecords(records: org.apache.kafka.clients.consumer.ConsumerRecords[String, String]): Unit = {
    for (record <- records.asScala) {
      val received = ReceivedRecord(
        topic = record.topic(),
        partition = record.partition(),
        offset = record.offset(),
        key = Option(record.key()),
        value = record.value(),
        timestamp = record.timestamp()
      )
      handler.handle(received)
    }
  }

  private[adapter] def closeConsumer(): Unit = {
    log.info("Closing Kafka consumer")
    consumer.close()
    running.set(false)
    log.info("Kafka consumer closed successfully")
  }

  // $COVERAGE-OFF$ JVM shutdown hooks cannot be unit tested
  private def installShutdownHook(): Unit = {
    val mainThread = Thread.currentThread()
    sys.addShutdownHook {
      log.info("Shutdown hook detected, calling consumer.wakeup()")
      shutdown()
      try { mainThread.join() }
      catch { case _: InterruptedException => () }
    }
  }
  // $COVERAGE-ON$

  override def shutdown(): Unit = {
    log.info("Shutdown requested for consumer")
    running.set(false)
    consumer.wakeup()
  }

  override def isRunning: Boolean = running.get()

  private[adapter] def buildProperties(conn: KafkaConnectionConfig, cons: AppConsumerConfig): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conn.bootstrapServers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, cons.keyDeserializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, cons.valueDeserializer)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, cons.groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, cons.autoOffsetReset)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, cons.enableAutoCommit.toString)
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, cons.autoCommitIntervalMs.toString)
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, cons.sessionTimeoutMs.toString)
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, cons.maxPollRecords.toString)
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, cons.partitionAssignmentStrategy)

    conn.securityProtocol.foreach(v => props.put("security.protocol", v))
    conn.saslMechanism.foreach(v => props.put("sasl.mechanism", v))
    conn.saslJaasConfig.foreach(v => props.put("sasl.jaas.config", v))

    props
  }
}
