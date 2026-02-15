package com.kafka.pipeline.common.error

sealed trait AppError {
  def message: String
}

object AppError {

  final case class ConfigurationError(message: String) extends AppError

  final case class ProducerError(message: String, cause: Option[Throwable] = None) extends AppError

  final case class ConsumerError(message: String, cause: Option[Throwable] = None) extends AppError

  final case class SerializationError(message: String, cause: Option[Throwable] = None) extends AppError

  final case class ShutdownError(message: String, cause: Option[Throwable] = None) extends AppError
}
