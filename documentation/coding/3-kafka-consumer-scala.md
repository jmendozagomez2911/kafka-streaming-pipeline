## Consumer: mental model
Consumers **pull** data from Kafka (they do not receive push events).

Core loop:
1. Create Properties (connection + deserialization + group)
2. Create KafkaConsumer
3. `subscribe(...)`
4. `poll(timeout)`
5. Process records
6. Repeat

---

## Consumer properties: deserialization
Producer serializes → consumer deserializes bytes → objects.

For String keys/values:
```scala
properties.setProperty("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer")
properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
````

---

## Consumer properties: group id

```scala
val groupId = "my-scala-application"
properties.setProperty("group.id", groupId)
```

A **consumer group** is how Kafka scales consumption:

* partitions are distributed across consumers in the group
* each partition is consumed by **at most one** consumer in the group at a time

---

## Consumer properties: auto.offset.reset

Controls what happens when there’s **no committed offset** for the group.

```scala
properties.setProperty("auto.offset.reset", "earliest")
```

Options:

* `none`: fail if no offset exists
* `earliest`: start from beginning of the topic
* `latest`: start from “now” (only new messages)

---

## Create consumer + subscribe

```scala
val consumer = new KafkaConsumer[String, String](properties)

val topic = "demo_scala"
consumer.subscribe(List(topic).asJava)
```

---

## Poll loop and logging

`poll()` returns immediately if data is available; otherwise it waits up to the timeout and may return empty.

```scala
while (true) {
  val records = consumer.poll(Duration.ofMillis(1000))

  for (record <- records.asScala) {
    log.info(
      "key={} value={} partition={} offset={}",
      record.key(),
      record.value(),
      record.partition(),
      record.offset()
    )
  }
}
```

What you observe on first run with a new group:

* consumer joins the group
* partitions assigned
* offsets reset according to `auto.offset.reset`
* then records arrive in batches (Kafka is efficient; large fetches are normal)

On later runs with the same `group.id`:

* consumer resumes from committed offsets
* you may see “polling” but no records until new data arrives

---

## Graceful shutdown: shutdown hook + wakeup

Stopping a consumer abruptly can delay rebalances and make restarts slow/noisy.
The standard pattern is:

* add a shutdown hook
* call `consumer.wakeup()` (interrupts `poll()` by throwing `WakeupException`)
* close consumer in `finally` so Kafka can rebalance cleanly

### Pattern

```scala
val mainThread = Thread.currentThread()

sys.addShutdownHook {
  log.info("Detected shutdown, calling consumer.wakeup()")
  consumer.wakeup()
  try mainThread.join()
  catch { case _: InterruptedException => () } // ignore
}

try {
  consumer.subscribe(List(topic).asJava)

  while (true) {
    val records = consumer.poll(Duration.ofMillis(1000))
    for (record <- records.asScala) {
      // process record
    }
  }
} catch {
  case _: WakeupException =>
    log.info("Consumer is starting to shut down")
  case e: Exception =>
    log.error("Unexpected exception in the consumer", e)
} finally {
  consumer.close() // also commits offsets (depending on config) and leaves group cleanly
  log.info("Consumer is now gracefully shut down")
}
```

Why this matters:

* cleanly revokes partitions
* leaves the group properly
* makes rebalances faster and more predictable

---

## Consumer groups: partition assignment and rebalances

A **rebalance** happens when:

* a consumer joins the group
* a consumer leaves the group
* partitions are added to a topic

With a topic of 3 partitions:

* 1 consumer → gets 3 partitions
* 2 consumers → partitions split (e.g., 2 + 1)
* 3 consumers → each gets 1 partition

When one consumer stops (clean shutdown), remaining consumers get reassigned partitions.

---

## Rebalance protocols: eager vs cooperative

### Eager rebalance (stop-the-world)

All consumers:

* stop consuming
* give up partitions
* rejoin and get new assignments

Downsides:

* temporary full pause in processing
* partitions may move a lot

### Cooperative (incremental) rebalance

Only a subset of partitions move at a time.
Consumers whose partitions don’t move can continue consuming.

Benefit:

* less disruption
* more stable consumption during scaling events

---

## Partition assignment strategies (consumer config)

Config: `partition.assignment.strategy`

Common assignors:

* `RangeAssignor` (default historically; per-topic, can be imbalanced)
* `RoundRobinAssignor` (better balance across consumers)
* `StickyAssignor` (tries to minimise partition movement, but still eager)
* `CooperativeStickyAssignor` (sticky + cooperative protocol)

In Kafka 3.x, defaults are often a **list** such as:

* `RangeAssignor, CooperativeStickyAssignor`

If `RangeAssignor` is present and supported, it may take precedence.
To force cooperative sticky:

```scala
properties.setProperty(
  "partition.assignment.strategy",
  "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
)
```

---

## Static group membership (reduce rebalances on restart)

If consumers restart frequently (e.g., Kubernetes), you can reduce rebalances using:

* `group.instance.id`

Idea:

* each consumer has a stable identity
* if it leaves briefly and comes back within `session.timeout.ms`,
  it can reclaim partitions without a full rebalance

This is especially useful when consumers maintain local caches/state.

---

## Offsets: auto commit basics (default behaviour)

By default, consumers often run with:

* `enable.auto.commit=true`
* `auto.commit.interval.ms=5000` (5 seconds)

Offsets are committed **periodically**, triggered around calls to `poll()`.

At-least-once implication:

* You should process the records you polled **before** the next commit happens.
* If you commit before processing finishes, you can lose “at least once” safety (rare but possible if processing is slow / failure timing is unlucky).

Manual commit (`commitSync`, `commitAsync`) exists but is considered more advanced.

---

## Advanced consumer demos (optional)

Examples you might see in “advanced” folders (not required for getting productive):

* **assign + seek** (read from specific offsets manually)
* **rebalance listener** (control/observe rebalance events precisely)
* **consumer in threads** (run poll loop in a worker thread; keep main thread free)

Only worth it if you specifically need those behaviours.
