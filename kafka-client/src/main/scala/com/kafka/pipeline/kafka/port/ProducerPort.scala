package com.kafka.pipeline.kafka.port

import com.kafka.pipeline.common.domain.KafkaMessage
import com.kafka.pipeline.common.error.AppError

trait ProducerPort {

  def send(message: KafkaMessage): Either[AppError, Unit]

  def sendAsync(message: KafkaMessage)(callback: Either[AppError, Unit] => Unit): Unit

  def flush(): Either[AppError, Unit]

  def close(): Unit
}
