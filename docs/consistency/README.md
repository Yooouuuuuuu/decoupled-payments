# Consistency

What the system guarantees, how, and the pitfalls found while building it (thesis §4.1.2, §4.4, §5.1).

## Goals

- **No lost trades:** every record is sent and acknowledged at least once at every step.
- **No double spending:** every trade is processed at most once.
- Together: **exactly once**, even when brokers or processors fail.

## Exactly-once processing through Kafka

Rather than solve these problems by hand, the system relies on Kafka:

- **Kafka transactions** combine an idempotent producer with atomic writes. A processor's reads (how far it has consumed) and all its writes commit or abort together, and readers use `isolation.level=read_committed`, so they never see an aborted write.
- **Replication and consumer groups** keep processing going when some brokers or processors fail.
- **Processors never communicate directly.** All inter-layer traffic goes through Kafka, so its guarantees cover every hop. Inter-thread communication inside a processor turned out to be risky too (see [the shared balance map](#the-shared-balance-map)).

Kafka's transaction protocol is a two-phase commit: the producer registers with a transaction coordinator, which tracks state in an internal topic. On a LAN it adds little overhead. With high network latency it costs noticeably more (see [models → Concurrent w/o Tx](../models/#concurrent-wo-tx)).

How each processor uses it:

| Processor | Reads | One Kafka transaction contains | Fenced by |
|---|---|---|---|
| Trade generator | — | (none: an idempotent producer, so retries never duplicate a trade) | — |
| Aggregator | `trades` | all its pending blocks, plus its consumer offsets | its consumer-group generation, and its `transactional.id` when it restarts |
| Validator | `blocks` (and `credits`) | one bank's results (`orders`, credits, `localBalance`), plus its read position in `validatorOffsets` | the bank's `transactional.id` |

### The aggregator: emit everything, then commit

Kafka offsets are per partition of `trades`, but an aggregator buffers trades per *bank*. An offset may only be committed once every trade before it has been emitted. Otherwise a crash would lose the trades still sitting in other banks' buffers. So when one bank's block is full, or the block timeout passes, the aggregator emits **all** of its pending blocks in one Kafka transaction, together with its consumer offsets. A block can therefore be smaller than the block size. That's the price of exactly-once.

If a rebalance moves `trades` partitions away, the aggregator drops its buffers. No transaction is open between polls, and whoever takes over the partitions re-reads them from the last committed offsets.

### The validator: one producer per bank

A validator writes each bank's results through that bank's own transactional producer (`transactional.id = validator-<bank>`). This has two effects:
- **Fencing.** When a bank moves to another validator, the new owner calls `initTransactions()` with the same `transactional.id`. Kafka then fences the previous owner's producer, which might be a process that was paused and doesn't know it lost the bank yet, a "zombie". Kafka also finishes or aborts its unfinished transaction. From then on, nothing the old owner sends for that bank can be committed.
- **A clean starting point.** After `initTransactions()` returns, no transaction for that bank is still open. Reading the bank's `localBalance` partition to its end therefore gives exactly the committed state.

### Where the validator keeps its read positions

A validator reads two topics: `blocks` through its consumer group, and, in the Concurrent model, `credits` through a second, manually assigned consumer. Instead of committing consumer-group offsets, it writes how far it has read each partition to the **`validatorOffsets`** topic, in the same transaction as the results. Read positions, balances and outputs therefore always match. The consumer group is used only to decide which validator owns which bank.

The same design works in Java and Go, so validators written in either language can share a group.

### Unfinished transactions

A `read_committed` reader can't read past a Kafka transaction that is still open, so a process that stops in the middle of one holds up everyone who reads the partitions it wrote to. A validator's unfinished transaction is aborted as soon as the bank's next owner calls `initTransactions()`. An aggregator's is aborted as soon as the aggregator restarts with the same site ID, because it reuses its `transactional.id` (`aggregator-<siteId>`). If it doesn't come back, or is only paused, the broker aborts the transaction once `kafka.transactionTimeoutMs` has passed.

That timeout is Kafka's default, 60 s. It only decides how long such a transaction can hold readers up, never the outcome: either way the transaction is aborted, or completed if it had already reached its commit, and every trade is processed exactly once. It only matters when an aggregator stops in the middle of a transaction and neither restarts nor wakes up within that time. An aggregator's transaction usually holds blocks for every bank, so then every validator waits for up to 60 s, and trades complete late but correctly, which is all that [eventual consistency](#eventual-consistency) promises. A shorter timeout would shorten that wait, but it would also abort any transaction slower than the timeout, for example under heavy load with added network delay.

## Eventual consistency

The [credit model](../credit-model/) splits a trade into validation and credit, so a receiver's balance is updated **some time after** the sender's. Immediate consistency, as in an account model, isn't achievable. **Eventual** consistency is: every accepted trade's credit is eventually applied exactly once. Without congestion, the gap is small.

## Ordering

- **Linearizability is impossible:** trades are processed in parallel groups with no global order.
- **Serializability is achievable**, ignoring the possibility of negative balances.
- **The `orders` topic** gets a record after validation *and* after credit. This gives real-time feedback ("accepted") while still marking full completion. Validators can write `orders` in parallel when it's partitioned by bank, at the cost of inter-bank serializability.

## Recovery

Each validator backs up its balances to `localBalance`, and its read positions to `validatorOffsets`, within the same Kafka transaction as its other writes. When a validator is given a bank, at startup or after a rebalance:

1. It creates the bank's producer and calls `initTransactions()`, which fences the previous owner (see above).
2. It reads the bank's `localBalance` partition to its end, and the latest balance of each account wins.
3. It reads the bank's `validatorOffsets` partition and continues `blocks` (and `credits`) from exactly there.

"To its end" means until the consumer's *position* reaches the partition's end offset. Kafka's transaction markers, and records of aborted transactions, take up offsets but are never returned to a `read_committed` reader. The last record you see is therefore usually *not* at the end offset.

**Errors fail fast.** If anything goes wrong inside a validator's transaction, it aborts the transaction and exits. Docker restarts it, and it recovers as above, exactly as on a normal start. The in-memory balances never outlive a transaction that didn't commit, and recovery is the code path that runs every time a validator starts. Rebalances aren't errors: they are handled in the consumer's rebalance callbacks.

## The shared balance map

In the [Concurrent model](../models/#concurrent-model), a validator has two threads, one for `blocks` and one for `credits`, which share one in-memory balance map and the `localBalance` backup. Two approaches failed before the final design.

### Attempt 1: update the map inside the Kafka transaction → dirty write

![Dirty write](diagrams/dirty-write.svg)

Thread 0 deducts A −10 inside transaction 1 and updates the map to 90. Thread 1 then credits A +10 and commits transaction 2, recording 100. Transaction 1 then **aborts**, for example on an acknowledgement timeout. The trade will be reprocessed, but the committed backup already includes its effect. If the abort comes from a thread crash, there is no way to undo it.

### Attempt 2: update the map after the Kafka transaction → race condition

![Race condition](diagrams/race-condition.svg)

Both threads read A = 100 from the map, and each writes its own result (90 or 110) to the backup during its transaction. The last commit wins, and the backup shows 90 or 110 instead of 100. Updating the map and the backup isn't atomic.

Java's `ConcurrentHashMap` doesn't help: it makes single operations thread-safe, not a read–transaction–write sequence.

### Final design: lock per Kafka transaction

The balance map is **locked for the duration of each Kafka transaction**, and while a rebalance hands banks in or out. No thread ever sees another thread's uncommitted change. If a transaction aborts, the process exits before anyone can use the map again (see [recovery](#recovery)). This is correct, but the two threads no longer process in parallel. The Concurrent model is still worth it: credits get their own queue and need fewer network steps (see [models](../models/)).

Diagram sources: [`dirty-write.d2`](diagrams/dirty-write.d2), [`race-condition.d2`](diagrams/race-condition.d2) (thesis Fig. 5.1, 5.2)
