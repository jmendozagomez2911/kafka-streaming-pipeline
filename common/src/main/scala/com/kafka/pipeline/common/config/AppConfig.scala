package com.kafka.pipeline.common.config

import com.kafka.pipeline.common.error.AppError
import com.kafka.pipeline.common.error.AppError.ConfigurationError
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Success, Try}

final case class KafkaConnectionConfig(
  bootstrapServers: String,
  securityProtocol: Option[String],
  saslMechanism: Option[String],
  saslJaasConfig: Option[String]
)

final case class ProducerConfig(
  topic: String,
  keySerializer: String,
  valueSerializer: String,
  acks: String,
  retries: Int,
  lingerMs: Int,
  batchSize: Int,
  enableIdempotence: Boolean,
  maxInFlightRequests: Int,
  requestTimeoutMs: Int,
  deliveryTimeoutMs: Int
)

final case class ConsumerConfig(
  topic: String,
  groupId: String,
  keyDeserializer: String,
  valueDeserializer: String,
  autoOffsetReset: String,
  enableAutoCommit: Boolean,
  autoCommitIntervalMs: Int,
  pollTimeoutMs: Long,
  sessionTimeoutMs: Int,
  maxPollRecords: Int,
  partitionAssignmentStrategy: String
)

final case class AppConfig(
  appName: String,
  connection: KafkaConnectionConfig,
  producer: ProducerConfig,
  consumer: ConsumerConfig
)

object AppConfig {

  def load(args: Array[String] = Array.empty): Either[AppError, AppConfig] = {
    Try {
      val cliConfig = parseCliArgs(args)
      val envConfig = ConfigFactory.systemEnvironment()
      val sysConfig = ConfigFactory.systemProperties()
      val fileConfig = ConfigFactory.load()

      val merged = cliConfig
        .withFallback(sysConfig)
        .withFallback(envConfig)
        .withFallback(fileConfig)
        .resolve()

      buildAppConfig(merged)
    } match {
      case Success(config) => ConfigValidator.validate(config)
      case Failure(ex) => Left(ConfigurationError(s"Failed to load configuration: ${ex.getMessage}"))
    }
  }

  private def parseCliArgs(args: Array[String]): Config = {
    val pairs = args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") =>
        key.stripPrefix("--") -> value
    }.toMap

    import scala.collection.JavaConverters._
    ConfigFactory.parseMap(pairs.asJava)
  }

  private def buildAppConfig(config: Config): AppConfig = {
    AppConfig(
      appName = config.getString(ConfigKeys.AppName),
      connection = KafkaConnectionConfig(
        bootstrapServers = config.getString(ConfigKeys.BootstrapServers),
        securityProtocol = getOptionalString(config, ConfigKeys.SecurityProtocol),
        saslMechanism = getOptionalString(config, ConfigKeys.SaslMechanism),
        saslJaasConfig = getOptionalString(config, ConfigKeys.SaslJaasConfig)
      ),
      producer = ProducerConfig(
        topic = config.getString(ConfigKeys.ProducerTopic),
        keySerializer = config.getString(ConfigKeys.ProducerKeySerializer),
        valueSerializer = config.getString(ConfigKeys.ProducerValueSerializer),
        acks = config.getString(ConfigKeys.ProducerAcks),
        retries = config.getInt(ConfigKeys.ProducerRetries),
        lingerMs = config.getInt(ConfigKeys.ProducerLingerMs),
        batchSize = config.getInt(ConfigKeys.ProducerBatchSize),
        enableIdempotence = config.getBoolean(ConfigKeys.ProducerIdempotence),
        maxInFlightRequests = config.getInt(ConfigKeys.ProducerMaxInFlightRequests),
        requestTimeoutMs = config.getInt(ConfigKeys.ProducerRequestTimeoutMs),
        deliveryTimeoutMs = config.getInt(ConfigKeys.ProducerDeliveryTimeoutMs)
      ),
      consumer = ConsumerConfig(
        topic = config.getString(ConfigKeys.ConsumerTopic),
        groupId = config.getString(ConfigKeys.ConsumerGroupId),
        keyDeserializer = config.getString(ConfigKeys.ConsumerKeyDeserializer),
        valueDeserializer = config.getString(ConfigKeys.ConsumerValueDeserializer),
        autoOffsetReset = config.getString(ConfigKeys.ConsumerAutoOffsetReset),
        enableAutoCommit = config.getBoolean(ConfigKeys.ConsumerEnableAutoCommit),
        autoCommitIntervalMs = config.getInt(ConfigKeys.ConsumerAutoCommitIntervalMs),
        pollTimeoutMs = config.getLong(ConfigKeys.ConsumerPollTimeoutMs),
        sessionTimeoutMs = config.getInt(ConfigKeys.ConsumerSessionTimeoutMs),
        maxPollRecords = config.getInt(ConfigKeys.ConsumerMaxPollRecords),
        partitionAssignmentStrategy = config.getString(ConfigKeys.ConsumerPartitionAssignmentStrategy)
      )
    )
  }

  private def getOptionalString(config: Config, path: String): Option[String] = {
    if (config.hasPath(path)) Some(config.getString(path)) else None
  }
}
