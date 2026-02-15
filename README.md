# Kafka Streaming Pipeline

A production-style Apache Kafka streaming pipeline built with **Scala 2.12**, **Java 17**, and **Maven**. The project follows hexagonal architecture principles with clean separation between ports (traits) and adapters, externalized configuration, structured logging, and comprehensive test coverage.

## Tech Stack

| Component       | Version / Tool                          |
|-----------------|-----------------------------------------|
| Language        | Scala 2.12.18                           |
| JDK             | Java 17                                 |
| Build           | Apache Maven 3.9+                       |
| Kafka Clients   | 3.7.1                                   |
| Spark (planned) | 3.5.4                                   |
| Configuration   | Typesafe Config 1.4.3                   |
| Logging         | SLF4J 2.0.12 + Logback 1.5.6           |
| Testing         | ScalaTest 3.2.18, Mockito-Scala 1.17.31|
| Coverage        | Scoverage Maven Plugin 2.0.2 (≥ 90%)   |

## Project Structure

```
kafka-streaming-pipeline/
├── pom.xml                          # Parent POM (dependency & plugin management)
├── common/                          # Shared kernel: config, domain, errors, logging
│   ├── src/main/scala/
│   │   └── com/kafka/pipeline/common/
│   │       ├── config/              # AppConfig, ConfigKeys, ConfigValidator
│   │       ├── domain/              # KafkaMessage, ReceivedRecord
│   │       ├── error/               # AppError ADT
│   │       └── logging/             # Logging trait (SLF4J)
│   └── src/main/resources/
│       ├── application.conf         # Typesafe Config defaults
│       └── logback.xml              # Logging configuration
├── kafka-client/                    # Kafka producer & consumer adapters
│   └── src/main/scala/
│       └── com/kafka/pipeline/kafka/
│           ├── port/                # ProducerPort, ConsumerPort (traits)
│           ├── adapter/             # KafkaProducerAdapter, KafkaConsumerAdapter
│           └── handler/             # RecordHandler trait
└── apps/                            # Runnable applications
    └── src/main/scala/
        └── com/kafka/pipeline/app/
            ├── ProducerApp.scala    # CLI producer entry point
            └── ConsumerApp.scala    # CLI consumer entry point
```

## Modules

### `common`
Shared domain model, configuration loading with validation, error ADT, and the `Logging` trait. Configuration is loaded from `application.conf` with support for environment variable and CLI overrides.

### `kafka-client`
Hexagonal-architecture Kafka adapters:
- **`ProducerPort`** / **`KafkaProducerAdapter`** — async send with callbacks, idempotent writes, batching, flush, and graceful close.
- **`ConsumerPort`** / **`KafkaConsumerAdapter`** — poll loop with `WakeupException`-based graceful shutdown, cooperative sticky rebalancing, and configurable poll parameters.

### `apps`
Entry-point applications that wire configuration and adapters:
- **`ProducerApp`** — sends 10 sample JSON messages and flushes.
- **`ConsumerApp`** — runs an infinite poll loop with a `LoggingRecordHandler`, stops on `Ctrl+C`.

## Prerequisites

- **JDK 17** (set `JAVA_HOME`)
- **Apache Maven 3.9+** on `PATH`
- A running Kafka cluster (or adjust `application.conf` / env vars)

## Build & Test

```bash
# Full build: compile, test, enforce ≥ 90% coverage, package JARs
mvn clean package

# Run tests only (no coverage enforcement)
mvn test

# Generate Scoverage HTML reports (target/<module>/scoverage-report/)
mvn scoverage:report
```

## Configuration

All runtime values are externalized in `common/src/main/resources/application.conf`.

Key configuration paths:

| Config Key                           | Default               | Description                    |
|--------------------------------------|-----------------------|--------------------------------|
| `app.name`                           | `kafka-pipeline`      | Application name               |
| `kafka.connection.bootstrap-servers` | `localhost:9092`      | Kafka broker addresses         |
| `kafka.producer.topic`               | `pipeline-output`     | Producer target topic          |
| `kafka.consumer.topic`               | `pipeline-input`      | Consumer source topic          |
| `kafka.consumer.group-id`            | `pipeline-consumers`  | Consumer group ID              |
| `kafka.consumer.auto-offset-reset`   | `earliest`            | Offset reset policy            |

Override any value via environment variables (e.g., `KAFKA_CONNECTION_BOOTSTRAP_SERVERS`) or JVM system properties (`-Dkafka.connection.bootstrap-servers=...`).

## Running the Applications

```bash
# Producer — sends sample messages
java -cp apps/target/apps-1.0-SNAPSHOT.jar com.kafka.pipeline.app.ProducerApp

# Consumer — polls and logs records (Ctrl+C to stop)
java -cp apps/target/apps-1.0-SNAPSHOT.jar com.kafka.pipeline.app.ConsumerApp
```

## Test Coverage

Coverage is enforced at **≥ 90%** per module via the Scoverage Maven Plugin. The build fails if any module drops below this threshold.

| Module        | Coverage |
|---------------|----------|
| `common`      | 95.15%   |
| `kafka-client`| 93.23%   |
| `apps`        | 100%     |

## Design Decisions

- **Hexagonal architecture** — ports (traits) decouple business logic from Kafka client internals, enabling full unit testing with `MockProducer` / `MockConsumer`.
- **Factory injection** — adapters accept a `Properties => Producer/Consumer` factory, allowing mock injection without heavy mocking frameworks.
- **`$COVERAGE-OFF$` markers** — JVM shutdown hooks and auxiliary constructors that require live brokers are excluded from coverage measurement.
- **Cooperative sticky rebalancing** — `CooperativeStickyAssignor` minimizes partition movement during consumer group scaling.
- **Idempotent producer** — `enable.idempotence=true` with `acks=all` for exactly-once semantics within a single producer session.

## License

This project is provided as-is for educational and demonstration purposes.
