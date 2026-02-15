# 📄 File 5 — Brokers, Replication, Leaders/ISR, Replica Fetching, Producer Acks

## 🧩 1) Why this part exists in the pipeline

This is Kafka’s *distributed systems core*:

* Brokers store partitions and serve reads/writes.
* Replication makes the log **survive failures**.
* Leaders coordinate writes/reads for each partition.
* Acks define producer durability guarantees.

If you don’t understand this, you can’t reason about **data loss**, **downtime**, or **latency**.

---

## 🧠 2) Core concepts

### ✅ Broker

A Kafka broker is a server in the cluster, identified by an ID. Partitions are **distributed** across brokers.

### ✅ Bootstrap server (discovery)

A client only needs to connect to one broker (bootstrap). It requests **metadata** and learns:

* what brokers exist
* where partition leaders are
* where to send produce/fetch requests

### ✅ Replication factor (RF)

Replication factor = how many copies of each partition exist across brokers.

**Rule-of-thumb from transcript:**

* RF=1 for local dev
* RF=2 or RF=3 in production (RF=3 is common)

**Durability fact:**
If replication factor is **N**, you can tolerate up to **N−1 broker failures** *and still have a copy of the data somewhere* — but only if replicas are healthy/in-sync and leadership election is safe.

### ✅ Leader + followers + ISR

For each partition:

* one broker is the **leader**
* others are replicas (followers)
* replicas that are caught up are in the **ISR** (in-sync replicas)

**Default behaviour:**

* Producers write to **leader only**
* Consumers read from **leader only** (classic model)

### ✅ Consumer replica fetching (Kafka 2.4+)

Kafka added the ability for consumers to fetch from the **closest replica** (follower fetching), improving latency/cost in rack/AZ-aware setups. This is KIP-392. ([cwiki.apache.org][2])

---

## ⚙️ 3) Producer acknowledgements (acks)

### acks=0

* Producer doesn’t wait for broker acknowledgement
* Highest throughput, **highest risk**
* Data can be lost silently

### acks=1

* Wait for leader acknowledgement
* Some durability, but data can still be lost if leader dies before followers replicate

### acks=all (or acks=-1)

* Wait for leader + **ISR** acknowledgements
* Stronger durability
* Higher latency

**Critical subtlety (exam + real life):**
acks=all is only “no data loss” under certain conditions (e.g., you also configure appropriate ISR requirements). Kafka 4.0 docs still emphasise KRaft/metadata requirements for upgrades, but durability mechanics remain based on replication/ISR. ([kafka.apache.org][3])

---

## 🏗️ 4) Architectural patterns + when to use

### Pattern A — RF=3 + acks=all for durable streams

Use for money/business-critical event streams.

### Pattern B — Replica fetching to reduce cross-AZ cost/latency

Use when you have rack/AZ-aware deployments and want consumers to read locally. ([cwiki.apache.org][2])

---

## 🎯 5) Exam cheats

### High-yield facts

* Broker stores partitions; topics are spread across brokers
* One leader per partition; ISR followers replicate
* RF=N ⇒ can tolerate N−1 broker failures (conceptually)
* acks controls producer durability vs latency
* Kafka 2.4+ supports fetching from closest replica (optional feature) ([cwiki.apache.org][2])

### Traps

* “Consumers always read from replicas” ❌ (only if configured and supported; default is leader)
* “acks=all always means zero data loss” ⚠️ depends on ISR health and cluster settings (and safe leader election)

---

## 🔥 6) Gotchas (“what breaks in real life”)

* **Under-replicated partitions:** ISR shrinks → acks=all might block or you accept weaker durability.
* **Hot partitions overload a leader broker:** even with many brokers, one partition’s leader can become a bottleneck.
* **Replica fetch misconfiguration:** if rack awareness isn’t configured, “closest replica” may not do what you expect.

---

## 🧭 Decision rules / exam triggers

* If you see **“must survive broker failures”** → replication factor > 1 (usually 3).
* If you see **“strong durability required”** → acks=all (and ensure ISR constraints are met).
* If you see **“reduce cross-AZ network costs / latency”** → consider follower fetching (KIP-392, Kafka 2.4+). ([cwiki.apache.org][2])

---
[1]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A%2BSticky%2BPartitioner?utm_source=chatgpt.com "KIP-480: Sticky Partitioner"
[2]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A%2BAllow%2Bconsumers%2Bto%2Bfetch%2Bfrom%2Bclosest%2Breplica?utm_source=chatgpt.com "KIP-392: Allow consumers to fetch from closest replica"
[3]: https://kafka.apache.org/40/getting-started/upgrade/?utm_source=chatgpt.com "Upgrading | Apache Kafka"
[4]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-833%3A%2BMark%2BKRaft%2Bas%2BProduction%2BReady?utm_source=chatgpt.com "KIP-833: Mark KRaft as Production Ready - Apache Kafka"
