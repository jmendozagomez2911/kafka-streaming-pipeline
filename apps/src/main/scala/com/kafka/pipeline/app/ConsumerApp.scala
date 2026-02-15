package com.kafka.pipeline.app

import com.kafka.pipeline.common.config.AppConfig
import com.kafka.pipeline.common.domain.ReceivedRecord
import com.kafka.pipeline.common.logging.Logging
import com.kafka.pipeline.kafka.adapter.KafkaConsumerAdapter
import com.kafka.pipeline.kafka.handler.RecordHandler

object ConsumerApp extends Logging {

  // $COVERAGE-OFF$ Entry point wires real dependencies and calls sys.exit
  def main(args: Array[String]): Unit = {
    log.info("Starting Consumer application")

    AppConfig.load(args) match {
      case Left(error) =>
        log.error("Configuration error: {}", error.message)
        sys.exit(1)

      case Right(config) =>
        log.info(s"Configuration loaded successfully: app=${config.appName} topic=${config.consumer.topic} group-id=${config.consumer.groupId} bootstrap-servers=${config.connection.bootstrapServers}")

        val handler = new LoggingRecordHandler
        val consumer = new KafkaConsumerAdapter(config, handler)

        log.info("Starting consumer loop (Ctrl+C to stop)")
        consumer.start()
    }
  }
  // $COVERAGE-ON$
}

class LoggingRecordHandler extends RecordHandler with Logging {

  override def handle(record: ReceivedRecord): Unit = {
    log.info(s"Received record: topic=${record.topic} partition=${record.partition} offset=${record.offset} key=${record.key.getOrElse("null")} value=${record.value}")
  }
}
