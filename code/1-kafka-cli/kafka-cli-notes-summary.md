# Kafka CLI Notes + Labs (Topics, Producer, Consumer, Groups, Offsets)

This document summarises the *Kafka CLI* section from the Conduktor course and maps it to the commands you already have in your project scripts:
- `0-kafka-topics.sh`
- `1-kafka-console-producer.sh`
- `2-kafka-console-consumer.sh`
- `3-kafka-console-consumer-in-groups.sh`
- `4-kafka-consumer-groups.sh`
- `5-reset-offsets.sh`

---

## 0) Before you start: CLI binaries, PATH, OS differences

Kafka ships CLI tools in the Kafka distribution under `bin/` (or `bin/windows/` on Windows).

Depending on how Kafka was installed, your command names differ:

- Linux / macOS (Kafka binaries): `kafka-topics.sh`, `kafka-console-producer.sh`, etc.
- Windows **without** WSL2: `kafka-topics.bat`, `kafka-console-producer.bat`, etc.
- Some package installs (Homebrew, apt): commands may be available without extension: `kafka-topics`, `kafka-console-producer`, …

If you get `command not found`, your PATH is not set. Two options:

1) Fix PATH so you can run `kafka-topics(.sh)` from anywhere.
2) Use the **full path** to the executable (e.g., `.../kafka_2.13-3.1.0/bin/kafka-topics.sh`).

---

## 1) Always use `--bootstrap-server` (not `--zookeeper`)

In older Kafka versions, some admin CLIs accepted `--zookeeper`. In Kafka 3+, CLIs were updated to use Kafka directly via:

```bash
--bootstrap-server <host:port>
````

This is the standard in modern Kafka CLI usage.

---

## 2) Secure cluster vs localhost: `--command-config`

### Localhost (usually unsecured for learning)

You typically only need:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Secure cluster (TLS/SASL)

You pass security properties via:

```bash
--command-config playground.config
```

**Important:** the `playground.config` file must be in the directory where you run the command (unless you give a path).
It usually contains properties like SASL username/password and security protocol (exact content depends on the cluster).

**Practical tip:** keep `playground.config` out of Git, and on Linux/macOS restrict permissions:

```bash
chmod 600 playground.config
```

---

## 3) Topics CLI (`kafka-topics`)

### 3.1 List topics

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 3.2 Create topics (be explicit)

```bash
# minimal (Kafka uses cluster defaults)
kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create

# explicit partitions
kafka-topics.sh --bootstrap-server localhost:9092 --topic second_topic --create --partitions 3

# explicit partitions + replication factor
kafka-topics.sh --bootstrap-server localhost:9092 --topic third_topic --create --partitions 3 --replication-factor 1
```

**Replication factor rule:**
`replication-factor` cannot exceed the number of brokers.
On a 1-broker localhost cluster, RF=2 will fail.

**Conduktor Playground note (from transcript):**
The platform may enforce a fixed replication factor (e.g., always 3) regardless of what you pass. This is a *platform constraint*, not Kafka in general.

### 3.3 Describe a topic

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --describe
```

Interpreting output:

* **Partition**: 0..N-1
* **Leader**: broker ID hosting the leader replica for that partition
* **Replicas**: broker IDs that host replicas (count = replication factor)
* **ISR** (In-Sync Replicas): replicas currently caught up

**Common confusion:**
`partition 0` is the partition number.
`leader 0` / `replica 0` refers to broker ID 0 (not the partition).

### 3.4 Increase partitions (you can’t decrease)

The transcript mentions increasing partitions. The CLI command is:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --topic my_topic --alter --partitions 6
```

Rules to remember:

* You can **increase**, never decrease.
* Increasing partitions can change message distribution and ordering guarantees for keys.

### 3.5 Delete a topic (danger / prerequisites)

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --delete
```

**Broker setting:** topic deletion only works if `delete.topic.enable=true` on the broker (your script notes this).

**Windows warning (from transcript):** On Windows **non-WSL2**, topic deletion may crash things (course-specific warning). If you’re using Windows without WSL2, avoid delete during the course.

---

## 4) Console Producer (`kafka-console-producer`)

### 4.1 Produce messages interactively

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic
```

Then type lines; each line becomes one record. Exit with `Ctrl + C`.

### 4.2 Producer properties (example: acks)

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic first_topic \
  --producer-property acks=all
```

**Meaning:** producer waits for the required acknowledgements (durability trade-off).
This is producer-side only.

### 4.3 Producing to a non-existent topic (auto-create)

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic new_topic
```

Two common behaviours (cluster-dependent):

* **Auto-create enabled (often on localhost):** Kafka may auto-create the topic on first produce; you may see temporary warnings like “Leader not available” until the topic exists.
* **Auto-create disabled (common in real clusters):** producer fails with an error like “topic not present in metadata”.

**Best practice:** disable auto-create in production and **create topics explicitly**.
(Cluster property is typically `auto.create.topics.enable=false`.)

### 4.4 Producing with keys

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic first_topic \
  --property parse.key=true \
  --property key.separator=:
```

Now you write:

```
user123:hello
user123:another message
```

* Left of `:` = key
* Right of `:` = value

**Watch out:** if you send a line without the separator, you’ll get an exception (because it can’t parse a key).

**Operational note:** keys matter because they typically control partitioning (same key → same partition → ordering for that key).

---

## 5) Console Consumer (`kafka-console-consumer`)

### 5.1 Consume “tail” (only new messages)

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic second_topic
```

This consumes from “now” (new records), not historical data.

### 5.2 Consume from beginning (read history)

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic second_topic --from-beginning
```

### 5.3 Print timestamp / key / value / partition

Useful to *debug ordering and partition distribution*:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic second_topic \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.value=true \
  --property print.partition=true \
  --from-beginning
```

### 5.4 Ordering: “in order per partition”

If the topic has multiple partitions, and you consume from beginning, you may observe messages “out of order” compared to how you typed them.

That is expected:

* Kafka guarantees ordering **within a partition**.
* Kafka does **not** guarantee global ordering across partitions.

### 5.5 RoundRobinPartitioner is for demos, not production

Your scripts use:

```bash
--producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner
```

This forces messages to alternate partitions so you can *see* distribution while learning.

**Best practice:** do not use RoundRobin in production; it’s inefficient and breaks common key-based ordering assumptions. The default partitioner is tuned for batching and throughput.

---

## 6) Consumers in a group (`--group`)

### 6.1 Start a consumer in a group

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic third_topic \
  --group my-first-application
```

Now start producing to `third_topic` and you’ll see messages.

### 6.2 Add more consumers to the same group

Start the same command again in a second terminal:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic third_topic \
  --group my-first-application
```

Kafka will rebalance partition assignments so the consumers share the partitions.

Key behaviours to remember:

* Partitions are assigned **exclusively** within a group.
* If there are **more consumers than partitions**, some consumers will be idle (assigned 0 partitions).
* When consumers join/leave, a **rebalance** happens.

### 6.3 `--from-beginning` and why it “stops working”

If you start a brand-new group:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic third_topic \
  --group my-second-application \
  --from-beginning
```

Kafka reads from the beginning because there are no committed offsets for that group.

But if you run it again later, even with `--from-beginning`, nothing “restarts”:

* offsets already exist for the group
* Kafka resumes from the committed offsets

To truly re-read, you must reset offsets (next section).

---

## 7) Consumer Groups CLI (`kafka-consumer-groups`)

### 7.1 List consumer groups

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### 7.2 Describe a group (offsets + lag)

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group my-second-application
```

How to read it:

* `CURRENT-OFFSET`: last committed offset
* `LOG-END-OFFSET`: latest offset in the partition
* `LAG`: how many records are not yet consumed/committed (`LOG-END-OFFSET - CURRENT-OFFSET`)
* `CONSUMER-ID` / `HOST` / `CLIENT-ID`: who is consuming which partitions

If you produce extra messages, lag increases. When you run the consumer again, lag should go back to zero after catch-up.

### 7.3 “console-consumer-XXXXX” groups

If you run `kafka-console-consumer` **without** `--group`, Kafka creates a temporary group name like:

```
console-consumer-10592
```

The transcript notes these may disappear later (ephemeral).
**Recommendation:** always use explicit group IDs for real workflows.

---

## 8) Resetting offsets (`--reset-offsets`)

### 8.1 Dry run first (always)

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-first-application \
  --reset-offsets --to-earliest \
  --topic third_topic \
  --dry-run
```

This prints what offsets *would* be set, without applying changes.

### 8.2 Execute (apply)

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-first-application \
  --reset-offsets --to-earliest \
  --topic third_topic \
  --execute
```

### 8.3 Critical rule: consumers must be stopped

You cannot reliably reset offsets while the group is actively consuming.
Stop all consumers in the group first, reset, then restart consumers.

### 8.4 What reset offsets is actually doing

It changes the committed offsets for the group. After that, when the consumer starts, it resumes from the new offsets (e.g., earliest → re-read everything that still exists on the topic, subject to retention).

---

## 9) Practical best practices (CLI + real life)

### Topics

* **Create topics explicitly** (name, partitions, replication factor). Avoid relying on auto-create.
* Pick partitions for expected parallelism and throughput. You can increase later, but you can’t decrease.
* Don’t delete topics casually; deletion is disruptive and may be disabled.

### Producers

* Prefer keys for records that must keep per-entity ordering (e.g., `customer_id`).
* Use `acks=all` + appropriate `min.insync.replicas` + retries/idempotence for stronger durability (production patterns).
* Do *not* use `RoundRobinPartitioner` in production.

### Consumers + groups

* Group = horizontal scaling unit. Max parallelism = number of partitions.
* Use `kafka-consumer-groups --describe` to debug lag and assignments.
* Remember: ordering is **per partition**, not global.

### Offset resets

* Always do `--dry-run` first.
* Reset only the topic/partitions you intend to change.
* Coordinate resets with your team; it can cause reprocessing or skipping.

---

## 10) Quick “what to run” mapping (your scripts)

* Topics management: `0-kafka-topics.sh`
* Producer basics / keys / auto-create: `1-kafka-console-producer.sh`
* Consumer basics / from-beginning / formatter: `2-kafka-console-consumer.sh`
* Consumer group behaviour: `3-kafka-console-consumer-in-groups.sh`
* Consumer group inspection: `4-kafka-consumer-groups.sh`
* Reset offsets: `5-reset-offsets.sh`

---

### Optional next steps (if you want to go beyond the course)

* Learn `kafka-configs.sh` (topic/broker configs) and `kafka-acls.sh` (authz).
* Learn `kcat` (formerly `kafkacat`) for richer debugging (headers, schemas, etc.).
* If you use transactions: practice `--isolation-level read_committed` on the consumer.
