# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-02-15

### Added

- **Multi-module Maven project** with parent POM and three modules: `common`, `kafka-client`, `apps`.
- **`common` module**
  - `AppConfig` — Typesafe Config loader with CLI argument and environment variable overrides.
  - `ConfigValidator` — validates all required configuration fields with descriptive error messages.
  - `ConfigKeys` — centralised configuration key constants.
  - `KafkaMessage` / `ReceivedRecord` — domain model for produced and consumed records.
  - `AppError` ADT — sealed trait with `ConfigError`, `ProducerError`, `ConsumerError`, `SerializationError`, and `ShutdownError` subtypes.
  - `Logging` trait — SLF4J-based structured logging mixin.
  - `application.conf` — default configuration with all Kafka producer and consumer settings.
  - `logback.xml` — console and rolling-file appender configuration.
- **`kafka-client` module**
  - `ProducerPort` / `ConsumerPort` — hexagonal port traits for Kafka operations.
  - `KafkaProducerAdapter` — async send with callbacks, idempotent writes (`acks=all`, `enable.idempotence=true`), configurable batching (`linger.ms`, `batch.size`), flush, and graceful close.
  - `KafkaConsumerAdapter` — poll loop with `WakeupException`-based graceful shutdown, `sys.addShutdownHook` integration, cooperative sticky rebalancing (`CooperativeStickyAssignor`), and configurable poll parameters.
  - `RecordHandler` trait — pluggable record processing interface.
  - Factory-injection pattern (`Properties => Producer/Consumer`) for full testability with `MockProducer` / `MockConsumer`.
- **`apps` module**
  - `ProducerApp` — CLI entry point that loads config, sends 10 sample JSON messages asynchronously, flushes, and shuts down.
  - `ConsumerApp` — CLI entry point that loads config and runs an infinite consumer poll loop with `LoggingRecordHandler`.
- **Testing**
  - 94 unit tests across all modules using ScalaTest (FlatSpec + Matchers).
  - `MockProducer` and `MockConsumer` based tests for Kafka adapters.
  - `ProducerPort` stub tests for application-level logic.
  - `schedulePollTask`-based consumer tests for correct rebalance simulation.
- **Coverage enforcement**
  - Scoverage Maven Plugin configured with ≥ 90% minimum threshold per module.
  - Build fails automatically if any module drops below the threshold.
  - `$COVERAGE-OFF$` markers for untestable code (JVM shutdown hooks, auxiliary constructors requiring live brokers).
- **Build plugins**
  - `scala-maven-plugin` — Scala 2.12 compilation with Java 17 target.
  - `scalatest-maven-plugin` — test execution.
  - `scoverage-maven-plugin` — instrumentation, reporting, and coverage enforcement.
  - `maven-shade-plugin` — uber-JAR packaging for `apps` module.

### Security

- Optional SASL/SSL configuration support (`security.protocol`, `sasl.mechanism`, `sasl.jaas.config`) via externalized config.
