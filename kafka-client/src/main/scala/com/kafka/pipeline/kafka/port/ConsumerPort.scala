package com.kafka.pipeline.kafka.port

trait ConsumerPort {

  def start(): Unit

  def shutdown(): Unit

  def isRunning: Boolean
}
