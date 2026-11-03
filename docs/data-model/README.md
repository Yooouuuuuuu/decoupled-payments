# Data model

The messages that flow through Kafka. The schemas are in [`/schemas`](../../schemas/).

## Serialization

Messages are **Avro**, chosen in the thesis over JSON for a smaller payload and faster (de)serialization (§5.2). They are encoded with Avro's **single-object encoding**: a 10-byte header (a 2-byte marker plus the schema's 64-bit fingerprint), then the binary payload. **No Schema Registry** is used. Plain Avro can encode without one, which saves running an extra service and works the same way in Go. (The thesis used Confluent's registry; its wire format carries the same binary payload behind a 5-byte schema-ID header.) Both sides compile the same schemas from `/schemas`.

## Schemas

### `Block` ([`block-v1.avsc`](../../schemas/block-v1.avsc))

A list of `Trade` records. This is the unit written to `trades`, `blocks`, `credits` and `orders`. A single trade is just a block of one.

`Trade` fields:

| Field | Type | Purpose |
|---|---|---|
| `serialNumber` | long | Unique trade ID |
| `outbank`, `outAccount` | string | Sender's bank and account |
| `inbank`, `inAccount` | string | Receiver's bank and account |
| `outbankPartition` | int | Partition of the sender's bank, used for [validation routing](../partitioning/#sender-side-then-receiver-side) |
| `inbankPartition` | int | Partition of the receiver's bank, used for credit routing |
| `amount` | long | Amount, in the smallest currency unit |
| `category` | int | Stage of the trade: *send* (new), *receive* (credit), or *rejected* |
| `timestamp1`, `timestamp2` | long | Timestamps recorded along the way, used for latency measurement |

### `LocalBalance` ([`localBalance-v1.avsc`](../../schemas/localBalance-v1.avsc))

One account's latest balance (`Balance`, long). Written to `localBalance` with the account as the key. See [consistency → recovery](../consistency/#recovery).

### `AccountInfo` ([`accountInfo-v1.avsc`](../../schemas/accountInfo-v1.avsc))

A bank, its partition (`bankPartition`), and its list of accounts. Written to `accountInfo` at init.

## Category lifecycle

```
send ──(validator: sufficient funds)──► receive (the credit) ──(receiver's validator)──► completed in orders
  └──(validator: insufficient funds)──► rejected in orders
```

The category is required in the Sequential model, where new trades and credits share `trades` and `blocks`. It's optional in the Concurrent model, where credits have their own topic.
