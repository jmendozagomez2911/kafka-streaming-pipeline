package com.kafka.pipeline.common.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KafkaMessageSpec extends AnyFlatSpec with Matchers {

  "KafkaMessage" should "create with key and value" in {
    val msg = KafkaMessage(Some("key1"), "value1")
    msg.key shouldBe Some("key1")
    msg.value shouldBe "value1"
    msg.headers shouldBe Map.empty
  }

  it should "create with None key" in {
    val msg = KafkaMessage(None, "value1")
    msg.key shouldBe None
  }

  it should "create with headers" in {
    val headers = Map("h1" -> "v1", "h2" -> "v2")
    val msg = KafkaMessage(Some("key1"), "value1", headers)
    msg.headers shouldBe headers
  }

  it should "default headers to empty map" in {
    val msg = KafkaMessage(Some("key1"), "value1")
    msg.headers shouldBe empty
  }

  "ReceivedRecord" should "carry all fields" in {
    val record = ReceivedRecord(
      topic = "test-topic",
      partition = 0,
      offset = 42L,
      key = Some("k"),
      value = "v",
      timestamp = 1234567890L
    )
    record.topic shouldBe "test-topic"
    record.partition shouldBe 0
    record.offset shouldBe 42L
    record.key shouldBe Some("k")
    record.value shouldBe "v"
    record.timestamp shouldBe 1234567890L
  }

  it should "handle None key" in {
    val record = ReceivedRecord("topic", 1, 0L, None, "val", 0L)
    record.key shouldBe None
  }
}
