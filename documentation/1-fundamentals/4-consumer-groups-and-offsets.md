# 📄 File 4 — Consumer Groups & Offsets: Scaling Reads + Delivery Semantics

## 🧩 1) Why this part exists in the pipeline

Consumer groups exist to solve two things at once:

1. **Scale-out consumption** of a topic (parallel reads)
2. **Independent applications** reading the same topic (fan-out)

Offsets exist to provide **progress tracking** and **recovery**:

* “where did this group get to in this partition?”

---

## 🧠 2) Core concepts

### ✅ Consumer group

A **named set of consumers** (same `group.id`) cooperating to read a topic.

**Rule:** within a group, each partition is assigned to **at most one consumer** at a time.

### ✅ Scaling rule: partitions limit parallelism

* If topic has **P partitions**, a consumer group can have at most **P active consumers** reading in parallel.
* Extra consumers beyond P are **idle/standby**.

### ✅ Multiple consumer groups on same topic

That is normal:

* each group is a separate application view of the stream
* each group tracks offsets independently

### ✅ Committed offsets and `__consumer_offsets`

Kafka stores committed offsets in an internal topic named `__consumer_offsets`. This is how Kafka can restart a consumer group and resume from the last committed position.

---

## ⚙️ 3) Delivery semantics (exam-critical)

Delivery semantics are not magic; they are mostly about **when offsets are committed** relative to processing.

### (A) At-least-once (duplicates possible)

* You commit **after** processing
* If consumer crashes after processing but before commit → record is re-read → duplicates
* Requires **idempotent processing** downstream (or dedup)

### (B) At-most-once (loss possible)

* You commit **before** processing (or too early)
* If processing fails → offset already advanced → record not re-read → loss

### (C) Exactly-once (hard boundary)

Your transcript introduces:

* Kafka→Kafka workflows: can use **transactions** (especially via Kafka Streams)
* Kafka→external systems: need idempotency / careful integration design

**Exam boundary you must state explicitly:**

* “Exactly-once” depends on **end-to-end system boundary**.
* Kafka can help with exactly-once *within Kafka* (transactions), but external sinks require sink-specific guarantees.

---

## 🏗️ 4) Architectural patterns + when to use

### Pattern A — One group per service

* location service group
* notification service group
  This isolates scaling and offset tracking per service.

### Pattern B — Increase partitions to increase max parallel consumers

If you need more consumer parallelism, you usually increase partitions first (and ensure brokers can handle it).

---

## 🎯 5) Exam cheats (high-yield + traps)

### High-yield facts

* A consumer group scales reads: partitions are split among consumers
* More consumers than partitions → idle consumers
* Offsets are stored in `__consumer_offsets`
* Commit timing defines at-least-once vs at-most-once

### Common traps

* “Adding consumers always increases throughput” ❌ (only until partitions are fully assigned)
* “Exactly-once is just a checkbox” ❌ (depends on transactional boundaries and sink)

---

## 🔥 6) Gotchas (“what breaks in real life”)

* **Commit strategy bugs** cause duplicates or data loss.
* **Rebalances** (not covered explicitly in your transcript) can pause consumption and cause operational jitter; you see lag spikes during group changes.
* **Idempotency is non-optional** in at-least-once pipelines (e.g., writing to DB needs upserts/dedup keys).

---

## 🧭 Decision rules / exam triggers

* If you see **“scale consumption”** → consumer group + enough partitions.
* If you see **“multiple services read same topic”** → multiple consumer groups.
* If you see **“duplicates are acceptable but loss is not”** → at-least-once + idempotent processing.
* If you see **“loss is acceptable but duplicates must be avoided”** → at-most-once (rare in serious data systems).

---
[1]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A%2BSticky%2BPartitioner?utm_source=chatgpt.com "KIP-480: Sticky Partitioner"
[2]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A%2BAllow%2Bconsumers%2Bto%2Bfetch%2Bfrom%2Bclosest%2Breplica?utm_source=chatgpt.com "KIP-392: Allow consumers to fetch from closest replica"
[3]: https://kafka.apache.org/40/getting-started/upgrade/?utm_source=chatgpt.com "Upgrading | Apache Kafka"
[4]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-833%3A%2BMark%2BKRaft%2Bas%2BProduction%2BReady?utm_source=chatgpt.com "KIP-833: Mark KRaft as Production Ready - Apache Kafka"
