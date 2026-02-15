package com.kafka.pipeline.kafka.handler

import com.kafka.pipeline.common.domain.ReceivedRecord

trait RecordHandler {

  def handle(record: ReceivedRecord): Unit
}
