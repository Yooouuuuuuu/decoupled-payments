# Data model

The messages that flow through Kafka. The schemas are in [`/schemas`](../../schemas/), and both implementations are generated from or checked against them.

## Serialization

Messages are **Avro**, chosen in the thesis over JSON for a smaller payload and faster (de)serialization (§5.2). They are encoded with Avro's **single-object encoding**:

```
C3 01 | 8-byte schema fingerprint (CRC-64-AVRO, little-endian) | Avro binary payload
```

**No Schema Registry** is used. Plain Avro can encode without one, which saves running an extra service and works the same way in Go. (The thesis used Confluent's registry. Its wire format carries the same binary payload behind a 5-byte schema-ID header.) A reader checks the fingerprint and refuses a message written with a different schema.

[`/schemas/testdata`](../../schemas/testdata/) holds a reference encoding of one sample message per schema, made with Apache Avro's Java library. The Java and Go tests both decode and re-encode them byte for byte, so the two implementations always agree on the wire format and can be mixed in one cluster.

Record keys are plain UTF-8 strings.

## Schemas

### `Block` ([`block-v1.avsc`](../../schemas/block-v1.avsc))

A list of `Trade` records. Every record on `trades`, `blocks`, `credits` and `orders` is a block. A single trade is just a block of one.

An aggregator's block holds at most `aggregator.blockSize` trades, and at most 512 KiB of encoded trades. The size counts each trade's `timestamp2` at its largest, because the validator fills it in later. So the block, the `orders` record a validator makes from it, and its credits all stay well under Kafka's 1 MiB message limit. A trade takes about 60 to 65 bytes, so the byte limit only matters above about 8,000 trades per block.

`Trade` fields:

| Field | Type | Purpose |
|---|---|---|
| `serialNumber` | long | Unique trade ID (see [IDs](#ids)) |
| `outbank`, `outAccount` | string | Sender's bank and account |
| `inbank`, `inAccount` | string | Receiver's bank and account |
| `outbankPartition` | int | Partition of the sender's bank, used for [validation routing](../partitioning/#sender-side-then-receiver-side) |
| `inbankPartition` | int | Partition of the receiver's bank, used for credit routing |
| `amount` | long | Amount, in the smallest currency unit |
| `category` | enum | `SEND`, `RECEIVE` or `REJECTED`; see [the category lifecycle](#category-lifecycle) |
| `timestamp1` | long | When the trade was created (ms since the epoch) |
| `timestamp2` | long | When the trade was validated (ms since the epoch); 0 before |

### `LocalBalance` ([`localBalance-v1.avsc`](../../schemas/localBalance-v1.avsc))

One account's latest `balance`. Written to `localBalance` with the account as the key. See [consistency → recovery](../consistency/#recovery).

### `ValidatorOffset` ([`validatorOffset-v1.avsc`](../../schemas/validatorOffset-v1.avsc))

How far a bank's validator has read one partition of `blocks` or `credits`: `topic`, `partition`, and the next `offset` to read. Written to `validatorOffsets` with the topic name as the key, in the same Kafka transaction as the validator's results. See [consistency](../consistency/#where-the-validator-keeps-its-read-positions).

### `AccountInfo` ([`accountInfo-v1.avsc`](../../schemas/accountInfo-v1.avsc))

One bank: its ID, its partition (`bankPartition`), its `accounts`, and their `initialBalance`. It also records the cluster-wide `model` (`SEQUENTIAL` or `CONCURRENT`) and whether `kafkaTransactions` are on. Written to `accountInfo` once by `init`, with the bank as the key. Aggregators and validators refuse to start if their own settings differ, and the generator and report read the accounts from here.

## Category lifecycle

```
SEND ──(validator: sufficient funds)──► stays SEND in orders ("accepted"), and a RECEIVE copy becomes the credit
  │                                        └──(receiver's validator applies it)──► RECEIVE in orders ("completed")
  └──(validator: insufficient funds)──► REJECTED in orders
```

What a category means depends on the topic:

| Category | On `trades` / `blocks` | On `credits` | On `orders` |
|---|---|---|---|
| `SEND` | A new trade, waiting for validation | (never) | Validated and accepted |
| `RECEIVE` | A credit (Sequential model) | A credit | Credit applied: the trade is complete |
| `REJECTED` | (never) | (never) | Rejected for insufficient funds |

The category is essential in the Sequential model, where new trades and credits share `trades` and `blocks`. In the Concurrent model, credits have their own topic.

## IDs

| What | Format | Example |
|---|---|---|
| Bank | `bank<partition>` | `bank0` |
| Account | `<bank>-<number, 4+ digits>` | `bank2-0042` |
| Trade | `runId × 2³² + sequence`. `runId` is a random 31-bit number per generator run, and `sequence` counts from 1, so separate generator runs never collide. | `4294967297` (run 1, trade 1) |
