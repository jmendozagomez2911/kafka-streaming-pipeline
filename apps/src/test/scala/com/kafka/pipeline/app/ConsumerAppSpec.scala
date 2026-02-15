package com.kafka.pipeline.app

import com.kafka.pipeline.common.domain.ReceivedRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConsumerAppSpec extends AnyFlatSpec with Matchers {

  "LoggingRecordHandler" should "handle a record without throwing" in {
    val handler = new LoggingRecordHandler

    val record = ReceivedRecord(
      topic = "test-topic",
      partition = 0,
      offset = 0L,
      key = Some("key1"),
      value = "value1",
      timestamp = System.currentTimeMillis()
    )

    noException should be thrownBy handler.handle(record)
  }

  it should "handle a record with None key" in {
    val handler = new LoggingRecordHandler

    val record = ReceivedRecord(
      topic = "test-topic",
      partition = 1,
      offset = 42L,
      key = None,
      value = "value-without-key",
      timestamp = System.currentTimeMillis()
    )

    noException should be thrownBy handler.handle(record)
  }

  it should "handle multiple records" in {
    val handler = new LoggingRecordHandler

    val records = (1 to 5).map { i =>
      ReceivedRecord(
        topic = "test-topic",
        partition = i % 3,
        offset = i.toLong,
        key = Some(s"key-$i"),
        value = s"value-$i",
        timestamp = System.currentTimeMillis()
      )
    }

    records.foreach(r => noException should be thrownBy handler.handle(r))
  }
}
