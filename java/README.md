# Java implementation

The thesis design ([architecture](../docs/architecture/)) in Java 21 with the plain Kafka client (4.3.2). One program, `dp`, runs one role per process: `init`, `generator`, `aggregator`, `validator` or `report`. Every setting and its default is in [`config/default.yaml`](../config/default.yaml); `dp --help` explains how to override them.

## Build and test

```sh
cd java
./mvnw -B package          # compiles, runs the unit tests, builds target/dp.jar
java -jar target/dp.jar --help
```

The build generates the Avro classes from [`schemas/`](../schemas/) and packages `config/default.yaml` into the jar. The unit tests need no Kafka: configuration layering, the command line, the golden encodings in `schemas/testdata`, and the validation, aggregation, sampling and report logic, which are written as plain code without Kafka calls.

## Run against a broker

Each line is one process (in its own terminal, or with `&`); add `--kafka.bootstrapServers=host:port` to every line if the broker isn't at `localhost:9092`.

```sh
dp() { java -jar target/dp.jar "$@"; }  # a shell function, so this works in bash and zsh alike
dp init --reset                         # topics, accounts, starting balances (deletes old data)
dp aggregator --siteId=1                # start as many as there are trades partitions
dp validator --siteId=1                 # one per bank at most; each takes over some banks
dp generator --trades=20000 --rps=2000
dp report                               # waits for in-flight trades, prints PASS or FAIL
```

Every aggregator and validator needs its own `siteId`; a process restarted with the same `siteId` takes the place of the old one. The model is chosen at init (`--model=sequential|concurrent`, `--kafkaTransactions=false` for the w/o-Tx variant), and aggregators and validators refuse to start with different settings. `dp report --json` adds one `REPORT_JSON {...}` line for scripts.

Exit status: 0 success, 1 runtime failure (or a failed report), 2 bad usage or settings (each problem on its own `dp: ...` line). `SIGTERM` or Ctrl-C stops the generator, an aggregator or a validator cleanly with status 0, also while it is still starting; a stopped aggregator or validator leaves its consumer group at once, so its partitions move right away and `init --reset` works right after. One that fails or crashes stays in the group until its session times out, so a restart with the same `siteId` gets its partitions back without a rebalance. A signal interrupts init and the report, which then exit with status 1: they have no result.

The same `generator.seed` repeats the same trades only within this implementation; the Go one draws its random numbers differently.

## Docker

```sh
docker build -f java/Dockerfile -t decoupled-payments-java .     # from the repository root
docker run --rm decoupled-payments-java --help
docker run --rm --network host decoupled-payments-java validator --siteId=2
```

The image runs `dp` as its entrypoint; `JAVA_OPTS` is passed to the JVM (default `-XX:MaxRAMPercentage=75`).

## Where the thesis lives in the code

Base package `io.github.yooouuuuuuu.dp` (under `src/main/java`):

| Package | Contents |
|---|---|
| `Main`, `cli` | Command line, usage text, exit statuses, clean shutdown on a signal |
| `config` | The four configuration layers (default.yaml, `--config` file, `DP_*` variables, flags) and validation |
| `kafka` | Topic names, IDs, client settings, Avro single-object (de)serializers, reading a partition to its end, the bank directory from `accountInfo` |
| `init` | Creates the topics and writes the accounts and starting balances |
| `generator` | The trade generator: uniform or Zipf accounts, inter-bank trades only, paced to `rps` |
| `aggregator` | Layer 1: blocks per bank, by block size or timeout |
| `validator` | Layer 2: validation, credits, recovery, the shared balance map |
| `report` | Correctness checks, throughput and latency |
| `avro` (generated) | `Block`, `Trade`, `LocalBalance`, `AccountInfo`, `ValidatorOffset` |

| Thesis concept | Code |
|---|---|
| Credit model: validation phase and credit phase | `validator.BankBatch` (`validate`, `applyCredits`) |
| One owner per balance: a bank is a partition | `validator.BlocksLoop` (consumer group `validators`, rebalance callbacks), `BankBatch.requireOwnBank` |
| Sequential vs Concurrent model: how credits travel | `validator.ValidatorRecords.credits`, `validator.CreditsLoop` (Concurrent only), `aggregator.PendingBlocks.targetPartition` |
| Blocks: block size and timeout | `aggregator.PendingBlocks`, `aggregator.Aggregator`; every block also stays under 512 KiB (`aggregator.BlockBytes`), so that the records a validator derives from it fit Kafka's 1 MiB limit |
| Exactly-once in the aggregator | `Aggregator.emitInTransaction`: the blocks and the consumer offsets in one Kafka transaction |
| Exactly-once in the validator | `validator.TransactionalBankWriter`: one transactional producer per bank (`validator-<bank>`); the read positions are records in `validatorOffsets`, written in the same transaction as the results |
| Fencing a previous owner ("zombie") | `TransactionalBankWriter.open` (`initTransactions`), `validator.BankTakeover` (fence first, then read) |
| Recovery from `localBalance` | `validator.BankTakeover`, `kafka.PartitionReader` |
| The shared balance map and its lock (thesis §5.1) | `validator.Ledger`, `validator.BankTransaction` (a failed transaction stops every later one: no dirty write) |
| Concurrent w/o Tx | `validator.PlainBankWriter`, `Aggregator.emitWithoutTransaction` |
| Zipf-distributed accounts | `generator.ZipfPicker` |
| TPS, latency and the correctness checks | `report.ReportChecks`, `report.Report` |
