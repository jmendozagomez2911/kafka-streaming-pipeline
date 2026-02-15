package com.kafka.pipeline.kafka.adapter

import com.kafka.pipeline.common.config.{AppConfig, KafkaConnectionConfig, ProducerConfig => AppProducerConfig}
import com.kafka.pipeline.common.domain.KafkaMessage
import com.kafka.pipeline.common.error.AppError
import com.kafka.pipeline.common.error.AppError.ProducerError
import com.kafka.pipeline.common.logging.Logging
import com.kafka.pipeline.kafka.port.ProducerPort
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.clients.producer.{Producer => KafkaProducerInterface}

import java.util.Properties
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class KafkaProducerAdapter(
  config: AppConfig,
  producerFactory: Properties => KafkaProducerInterface[String, String]
) extends ProducerPort with Logging {

  // $COVERAGE-OFF$ Auxiliary constructor requires live Kafka broker
  def this(config: AppConfig) = this(config, props => new KafkaProducer[String, String](props))
  // $COVERAGE-ON$

  private val producer: KafkaProducerInterface[String, String] = {
    val props = buildProperties(config.connection, config.producer)
    log.info(s"Initialising Kafka producer for topic=${config.producer.topic} bootstrap-servers=${config.connection.bootstrapServers}")
    producerFactory(props)
  }

  override def send(message: KafkaMessage): Either[AppError, Unit] = {
    Try {
      val record = buildRecord(config.producer.topic, message)
      producer.send(record).get()
      ()
    } match {
      case Success(_) => Right(())
      case Failure(ex) =>
        log.error(s"Synchronous send failed: ${ex.getMessage}")
        Left(ProducerError(s"Failed to send message: ${ex.getMessage}", Some(ex)))
    }
  }

  override def sendAsync(message: KafkaMessage)(callback: Either[AppError, Unit] => Unit): Unit = {
    val record = buildRecord(config.producer.topic, message)
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          log.error(s"Async send failed: ${exception.getMessage}")
          callback(Left(ProducerError(s"Async send failed: ${exception.getMessage}", Some(exception))))
        } else {
          log.debug(s"Message sent to topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()}")
          callback(Right(()))
        }
      }
    })
  }

  override def flush(): Either[AppError, Unit] = {
    Try(producer.flush()) match {
      case Success(_) =>
        log.info("Producer flushed successfully")
        Right(())
      case Failure(ex) =>
        log.error(s"Producer flush failed: ${ex.getMessage}")
        Left(ProducerError(s"Flush failed: ${ex.getMessage}", Some(ex)))
    }
  }

  override def close(): Unit = {
    log.info("Closing Kafka producer")
    Try {
      producer.flush()
      producer.close()
    } match {
      case Success(_) => log.info("Kafka producer closed successfully")
      case Failure(ex) => log.error(s"Error closing Kafka producer: ${ex.getMessage}")
    }
  }

  private[adapter] def buildRecord(topic: String, message: KafkaMessage): ProducerRecord[String, String] = {
    val record = new ProducerRecord[String, String](topic, message.key.orNull, message.value)
    message.headers.foreach { case (k, v) =>
      record.headers().add(new RecordHeader(k, v.getBytes("UTF-8")))
    }
    record
  }

  private[adapter] def buildProperties(conn: KafkaConnectionConfig, prod: AppProducerConfig): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conn.bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, prod.keySerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, prod.valueSerializer)
    props.put(ProducerConfig.ACKS_CONFIG, prod.acks)
    props.put(ProducerConfig.RETRIES_CONFIG, prod.retries.toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, prod.lingerMs.toString)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, prod.batchSize.toString)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, prod.enableIdempotence.toString)
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, prod.maxInFlightRequests.toString)
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, prod.requestTimeoutMs.toString)
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, prod.deliveryTimeoutMs.toString)

    conn.securityProtocol.foreach(v => props.put("security.protocol", v))
    conn.saslMechanism.foreach(v => props.put("sasl.mechanism", v))
    conn.saslJaasConfig.foreach(v => props.put("sasl.jaas.config", v))

    props
  }
}
