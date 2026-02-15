package com.kafka.pipeline.common.logging

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LoggingSpec extends AnyFlatSpec with Matchers {

  private class TestClass extends Logging {
    def loggerName: String = log.getName
    def doLog(): Unit = log.info("test message")
  }

  "Logging trait" should "provide a logger named after the class" in {
    val instance = new TestClass
    instance.loggerName should include("TestClass")
  }

  it should "allow logging without errors" in {
    val instance = new TestClass
    noException should be thrownBy instance.doLog()
  }

  it should "return the same logger on multiple accesses" in {
    val instance = new TestClass
    val logger1 = instance.loggerName
    val logger2 = instance.loggerName
    logger1 shouldBe logger2
  }
}
