package com.kafka.pipeline.common.logging

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  protected lazy val log: Logger = LoggerFactory.getLogger(getClass)
}
