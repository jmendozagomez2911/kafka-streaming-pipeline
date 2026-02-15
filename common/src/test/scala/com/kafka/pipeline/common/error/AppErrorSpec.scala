package com.kafka.pipeline.common.error

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AppErrorSpec extends AnyFlatSpec with Matchers {

  "ConfigurationError" should "carry a message" in {
    val error = AppError.ConfigurationError("missing key")
    error.message shouldBe "missing key"
    error shouldBe a[AppError]
  }

  "ProducerError" should "carry a message and optional cause" in {
    val cause = new RuntimeException("boom")
    val error = AppError.ProducerError("send failed", Some(cause))
    error.message shouldBe "send failed"
    error.cause shouldBe Some(cause)
  }

  it should "default cause to None" in {
    val error = AppError.ProducerError("send failed")
    error.cause shouldBe None
  }

  "ConsumerError" should "carry a message and optional cause" in {
    val cause = new RuntimeException("poll failed")
    val error = AppError.ConsumerError("consumer error", Some(cause))
    error.message shouldBe "consumer error"
    error.cause shouldBe Some(cause)
  }

  it should "default cause to None" in {
    val error = AppError.ConsumerError("consumer error")
    error.cause shouldBe None
  }

  "SerializationError" should "carry a message and optional cause" in {
    val cause = new RuntimeException("bad bytes")
    val error = AppError.SerializationError("deserialization failed", Some(cause))
    error.message shouldBe "deserialization failed"
    error.cause shouldBe Some(cause)
  }

  it should "default cause to None" in {
    val error = AppError.SerializationError("deserialization failed")
    error.cause shouldBe None
  }

  "ShutdownError" should "carry a message and optional cause" in {
    val cause = new RuntimeException("shutdown failed")
    val error = AppError.ShutdownError("shutdown error", Some(cause))
    error.message shouldBe "shutdown error"
    error.cause shouldBe Some(cause)
  }

  it should "default cause to None" in {
    val error = AppError.ShutdownError("shutdown error")
    error.cause shouldBe None
  }

  "All error types" should "be instances of AppError" in {
    val errors: Seq[AppError] = Seq(
      AppError.ConfigurationError("a"),
      AppError.ProducerError("b"),
      AppError.ConsumerError("c"),
      AppError.SerializationError("d"),
      AppError.ShutdownError("e")
    )
    errors.foreach(_ shouldBe a[AppError])
  }
}
