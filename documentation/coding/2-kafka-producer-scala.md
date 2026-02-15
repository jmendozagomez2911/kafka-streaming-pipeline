## Producer: mental model
A Kafka producer workflow is:

1. Create **Properties** (connection + behaviour)
2. Create the **KafkaProducer**
3. Create **ProducerRecord**
4. `send()` (asynchronous)
5. `flush()` (force send buffered records; blocks)
6. `close()` (also flushes)

---

## Logging (SLF4J)
Instead of `println`, use structured logs:
- `val log = LoggerFactory.getLogger(...)`
- `log.info(...)`, `log.error(...)`

If logs don’t show up, re-check that:
- `slf4j-api` + `slf4j-simple` are added as **implementation**
- Gradle was refreshed

---

## Producer properties: connect to Kafka

### Local (simple)
```scala
properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
````

### Remote / secured cluster (example pattern)

When connecting to a secured cluster, you add security-related properties (values depend on your cluster setup):

* `security.protocol`
* `sasl.jaas.config`
* `sasl.mechanism`
* `bootstrap.servers` (remote address)

> IntelliJ may auto-escape quotes inside `sasl.jaas.config`. That’s expected.

---

## Producer properties: serialization (required)

Kafka brokers accept bytes. Your producer converts objects → bytes using serializers.

For String keys/values:

```scala
properties.setProperty("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
```

---

## Create the producer

```scala
val producer = new KafkaProducer[String, String](properties)
```

`[String, String]` means:

* key type: String
* value type: String
  …and must match the serializers you set.

---

## Create and send a record

```scala
val record =
  new ProducerRecord[String, String]("demo_scala", "hello world")

producer.send(record)
producer.flush()
producer.close()
```

### Important IntelliJ detail (parameter hints)

IntelliJ may show `topic:` and `value:` hints next to arguments.
Those hints are **not code**—they’re only UI annotations.

---

## Callbacks: learn partition/offset of each send

`send()` is async. If you want to know where the record landed, add a callback.

```scala
producer.send(record, (metadata, exception) => {
  if (exception == null) {
    log.info(
      "topic={} partition={} offset={} timestamp={}",
      metadata.topic(),
      metadata.partition(),
      metadata.offset(),
      metadata.timestamp()
    )
  } else {
    log.error("Error while producing", exception)
  }
})
```

This runs:

* on success (metadata filled)
* or on failure (exception filled)

---

## Sticky partitioning (default behaviour when key is null)

If you send messages without a key, you might expect Round Robin partitioning.
In practice, modern Kafka producers use a **sticky** strategy for performance:

* The producer batches records.
* It “sticks” to one partition for a batch.
* Then it switches partitions for the next batch.

Result: when sending many records quickly, you often see:

* many consecutive records go to the **same** partition

### Demo tricks (for learning only)

You can force more obvious switching by:

* sending batches separated by a small sleep
* temporarily reducing `batch.size` (not recommended in production)

You can also force Round Robin via:

```scala
properties.setProperty(
  "partitioner.class",
  "org.apache.kafka.clients.producer.RoundRobinPartitioner"
)
```

> Useful to demonstrate, usually not what you want in production.

---

## Keys: “same key → same partition”

If you provide a key, Kafka uses it (hashing) to choose a partition consistently.

Example:

```scala
val topic = "demo_scala"
val key   = s"id_$i"
val value = s"hello world $i"

val record =
  new ProducerRecord[String, String](topic, key, value)

producer.send(record, callback)
```

If you run this twice, you’ll observe:

* `id_4` always goes to the same partition across runs (as long as partition count stays the same)

---

## Verification: confirm produced messages

You can verify messages using:

* a UI (e.g., a Kafka UI / playground)
* or `kafka-console-consumer` reading from the topic (optionally from beginning)

The key idea: produce with Scala → consume with CLI/UI to confirm end-to-end.

