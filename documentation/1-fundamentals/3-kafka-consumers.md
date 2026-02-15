# 📄 File 3 — Kafka Consumers: Pull Model, Ordering, Deserialization, Compatibility

## 🧩 1) Why this part exists in the pipeline

Consumers are how you turn the log into downstream actions:

* update a dashboard
* trigger notifications
* persist to DB/warehouse
* run stream processing

Kafka is pull-based so consumers control:

* how fast they read
* when they commit offsets
* backpressure behaviour

---

## 🧠 2) Core concepts

### ✅ Pull model

Consumers **request** data from brokers; brokers do not push by default.
This is fundamental for:

* backpressure (slow consumer doesn’t crash broker)
* scaling (consumer can control fetch size/poll loop)

### ✅ Ordering is per partition

Consumers read offsets in increasing order **within a partition**.
Across partitions, there is no global order.

### ✅ Deserialization

Consumers convert bytes → objects using **deserializers**.
Consumers must know expected formats for key/value.

**Lifecycle constraint (very exam-relevant):**
You must not casually change the data format in an existing topic because it breaks consumers. Safer pattern: create a new topic with the new format and migrate. (In practice you can also do schema evolution, but that’s beyond this transcript.)

---

## 🏗️ 3) Architectural patterns + when to use

### Pattern A — One consumer reads many partitions (simple, low scale)

* works when traffic is low
* easier operationally

### Pattern B — Scale via consumer groups (covered in next file)

* increase parallelism by adding consumers **up to partition count**

---

## ⚙️ 4) Key product features

### Broker discovery (operationally important)

Consumers can connect to one broker (bootstrap) then discover leaders for partitions (covered later in brokers section).

### Recovery behaviour

If a broker fails, consumers can recover and continue based on new leadership (requires replication; covered later).

---

## 🎯 5) Exam cheats

### High-yield facts

* Consumers are pull-based
* Ordering is per partition only
* Deserializers must match serializers

### Common traps

* “Consumer can read topic like SQL query” ❌
* “Kafka guarantees consumer can read from replicas” ⚠️ default is leader; reading from follower is optional and version-dependent (covered later) ([cwiki.apache.org][2])

---

## 🔥 6) Gotchas

* **Format drift kills consumers:** one producer quietly changes serialization → consumer exceptions or silent bad parsing.
* **Mixed formats in one topic** is operational pain unless you version your schema and handle both on the consumer side.
* **Assuming ordering across partitions** causes subtle bugs in downstream state.

---

## 🧭 Decision rules / exam triggers

* If you see **“need strict ordering for an entity”** → ensure entity events are in **one partition** (keyed).
* If you see **“consumer breaks after producer deployment”** → suspect **serializer/deserializer mismatch** or schema change.

---
[1]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A%2BSticky%2BPartitioner?utm_source=chatgpt.com "KIP-480: Sticky Partitioner"
[2]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A%2BAllow%2Bconsumers%2Bto%2Bfetch%2Bfrom%2Bclosest%2Breplica?utm_source=chatgpt.com "KIP-392: Allow consumers to fetch from closest replica"
[3]: https://kafka.apache.org/40/getting-started/upgrade/?utm_source=chatgpt.com "Upgrading | Apache Kafka"
[4]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-833%3A%2BMark%2BKRaft%2Bas%2BProduction%2BReady?utm_source=chatgpt.com "KIP-833: Mark KRaft as Production Ready - Apache Kafka"
