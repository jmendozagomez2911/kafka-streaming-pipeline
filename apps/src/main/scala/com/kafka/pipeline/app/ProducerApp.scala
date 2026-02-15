package com.kafka.pipeline.app

import com.kafka.pipeline.common.config.AppConfig
import com.kafka.pipeline.common.domain.KafkaMessage
import com.kafka.pipeline.common.error.AppError
import com.kafka.pipeline.common.logging.Logging
import com.kafka.pipeline.kafka.adapter.KafkaProducerAdapter
import com.kafka.pipeline.kafka.port.ProducerPort

object ProducerApp extends Logging {

  // $COVERAGE-OFF$ Entry point wires real dependencies and calls sys.exit
  def main(args: Array[String]): Unit = {
    log.info("Starting Producer application")

    AppConfig.load(args) match {
      case Left(error) =>
        log.error("Configuration error: {}", error.message)
        sys.exit(1)

      case Right(config) =>
        log.info(s"Configuration loaded successfully: app=${config.appName} topic=${config.producer.topic} bootstrap-servers=${config.connection.bootstrapServers}")

        val producer = new KafkaProducerAdapter(config)
        try {
          run(producer, config.producer.topic)
        } finally {
          producer.close()
          log.info("Producer application shut down")
        }
    }
  }
  // $COVERAGE-ON$

  private[app] def run(producer: ProducerPort, topic: String): Unit = {
    for (i <- 1 to 10) {
      val message = KafkaMessage(
        key = Some(s"key-$i"),
        value = s"""{"id": $i, "payload": "message-$i"}"""
      )
      producer.sendAsync(message)(handleResult(i))
    }
    producer.flush()
    log.info("All messages sent and flushed")
  }

  private[app] def handleResult(index: Int)(result: Either[AppError, Unit]): Unit = result match {
    case Right(_) =>
      log.info(s"Message $index sent successfully")
    case Left(error) =>
      log.error(s"Failed to send message $index: ${error.message}")
  }
}
