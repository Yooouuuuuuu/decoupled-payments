# Deployment

Everything runs on **one Docker host** with Docker Compose. The goal is a runnable system that shows the real distributed topology, not reproducing the thesis's throughput numbers.

![Deployment](diagrams/deployment.svg)

Source: [`diagrams/deployment.d2`](diagrams/deployment.d2)

## Sites

A **site** is one bank's infrastructure: one Kafka node, one aggregator and one validator. This follows the thesis's vision that each bank hosts its own broker, aggregator and validator (§4.1.1). The trade generator is separate.

## Kafka in KRaft mode

Kafka nodes use **KRaft**, with no ZooKeeper. A node can be:
- a **broker**, which stores data and serves clients
- a **controller**, which manages cluster metadata (topics, partition leaders, live brokers) and agrees on it through a Raft quorum
- **both** (*combined*)

Raft needs a majority of controllers to agree, so 3 controllers tolerate the loss of 1.

## Defaults

All settings are configurable.

| Setting              | Default   | Meaning                                                              |
|----------------------|-----------|----------------------------------------------------------------------|
| `SITES`              | 3         | Number of sites, which means Kafka nodes, aggregators and validators |
| `CONTROLLERS`        | 3         | The first N Kafka nodes are combined broker + controller, the rest are pure brokers |
| `REPLICATION_FACTOR` | 1         | Replication for our topics (the thesis used 1)                       |
| Partitions per topic | = `SITES` | One partition per bank, one bank per validator                       |

3 is the smallest cluster that is genuinely distributed with a real Raft majority.

## How settings are applied

| What | Where |
|---|---|
| Cluster shape (`SITES`, `CONTROLLERS`) | A generator script writes the Compose file. In combined mode the controller list depends on the node count, so it's generated. The default output is committed. |
| Topic settings (partitions, replication) | The app's `init` role creates topics from the app config. Brokers also default to `default.replication.factor=1`. |
| Per-container settings (role, `SITE_ID`, model) | Environment variables in the Compose file, overriding the app's YAML config |
| Variable values (ports, image version) | `infra/.env` |

## Limits of a single host

- All nodes share one machine's CPU, so more brokers **don't add capacity**.
- What multiple nodes do give you: partition leadership spread across nodes, per-site network delay (`tc netem`, closer to geographic distribution than the thesis's whole-NIC delay), and per-site CPU limits (the closest local equivalent of the thesis's high-end vs lower-spec machines).
- Kafka's own failover is out of scope. The system relies on Kafka's guarantees rather than testing them.
