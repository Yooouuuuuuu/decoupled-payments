# Go implementation

The decoupled payment system in Go: one binary, `dp`, with five roles. It takes the same settings and flags, and uses the same topics, consumer groups, transactional IDs and Avro messages as the Java implementation, so the two are interchangeable. The design is in [docs/architecture](../docs/architecture/).

Requires Go 1.26 or later. Libraries: [franz-go](https://github.com/twmb/franz-go) (Kafka), [hamba/avro](https://github.com/hamba/avro), [go.yaml.in/yaml/v3](https://github.com/yaml/go-yaml).

## Build and test

```sh
cd go
go build -o bin/dp ./cmd/dp
go test ./...          # no Kafka needed
go vet ./...
```

The tests that need a broker run the roles against [kfake](https://github.com/twmb/franz-go/tree/master/pkg/kfake), franz-go's in-process fake Kafka (a test-only dependency).

`internal/config/default.yaml` and `internal/avro/schemas/` are copies of [`config/default.yaml`](../config/default.yaml) and [`schemas/`](../schemas/), embedded in the binary. After changing the originals, run `go generate ./...`; a test fails while the copies differ.

The Docker image is built from the repository root. The build runs the tests first, with the originals of the schemas and defaults in its context, so an image can't be built from stale copies:

```sh
docker build -f go/Dockerfile -t decoupled-payments-go .
docker run --rm decoupled-payments-go --help
```

## Run

Every setting and its default is in [`config/default.yaml`](../config/default.yaml). Override them with `--config=<file>` (or `DP_CONFIG`), then environment variables (`DP_AGGREGATOR_BLOCK_SIZE`), then flags (`--aggregator.blockSize=500`, or `--blockSize=500` for the aggregator role). `dp <role> --help` lists the role's own settings.

Against a broker on `localhost:9092` (`--kafka.bootstrapServers=host:port` otherwise), each in its own terminal, with `dp` being `bin/dp` or the image:

```sh
dp init --reset                         # topics, accounts, initial balances
dp validator --siteId=1                 # the validators share the banks
dp validator --siteId=2                 # every aggregator and validator needs its own siteId
dp aggregator --siteId=1
dp aggregator --siteId=2
dp generator --trades=20000 --rps=2000  # then wait for it to finish
dp report                               # checks exactly-once processing: PASS or FAIL
```

Choose the model with `--model=sequential|concurrent` and `--kafkaTransactions=false` for every role, including `init`, which records them in `accountInfo`. `dp report --json` also prints one `REPORT_JSON {...}` line for scripts. A `--config` file is read as plain text, never typed by the YAML library: `010` is 10, `yes` is no boolean, and nulls, lists, anchors, duplicate keys and second documents are errors.

Exit status: 0 success, 1 failure (including a failed report), 2 usage or configuration error. SIGTERM or Ctrl-C stops the generator, an aggregator or a validator cleanly, with status 0, even while it starts; an aggregator or validator then leaves its consumer group at once, so its partitions move to the others and `init --reset` works right away. A failed or killed one stays in the group until `kafka.sessionTimeoutMs` passes, so that its restart with the same `siteId` gets its partitions back without a rebalance. An interrupted `init` or `report` did not finish: status 1.

`generator.seed` repeats a run's trades, but only with this implementation: the Java one uses another random generator.

## Layout

| Package | Contents |
|---|---|
| `cmd/dp` | `main`: signals, then `cli.Run` |
| `internal/cli` | Command line, help, exit statuses, role dispatch |
| `internal/config` | Layered settings (default.yaml, file, environment, flags) and validation |
| `internal/avro` | Message types and Avro single-object encoding |
| `internal/bank` | Bank and account IDs; the bank directory init records in `accountInfo` |
| `internal/kafka` | Topics, client settings (set explicitly to the Java client's defaults), records, reading a partition to its end, leaving a consumer group |
| `internal/roles/initrole` | `init` |
| `internal/roles/generator` | `generator` and its uniform and Zipf samplers |
| `internal/roles/aggregator` | `aggregator` (layer 1) |
| `internal/roles/validator` | `validator` (layer 2) |
| `internal/roles/report` | `report`: correctness checks, throughput, latency |

## Where the design lives

| Idea ([thesis](../docs/thesis/)) | Code |
|---|---|
| Credit model: validate the sender, credit the receiver later ([credit-model](../docs/credit-model/)) | `ledger.validate` and `ledger.applyCredits` in `roles/validator/ledger.go` |
| One owner per balance: a bank is a partition ([partitioning](../docs/partitioning/)) | `bank.ID`; `kafka.partitioner` names the partition of every record; `aggregator.target` routes trades by bank |
| Blocks: block size and timeout, and a cap of 512 KiB per block so that every record derived from it fits in Kafka | `roles/aggregator/buffer.go` |
| Exactly-once aggregation: all pending blocks and the consumed offsets in one transaction | `transactional.emit` in `roles/aggregator/aggregator.go` (`kgo.GroupTransactSession`); `fence` stops a previous process with the same `siteId` |
| Sequential vs Concurrent model ([models](../docs/models/)) | `validator.creditRecord` sends credits to `trades` or `credits`; thread C is `validator.runCredits` |
| The shared balance map, locked for each Kafka transaction ([consistency](../docs/consistency/#the-shared-balance-map)) | `validator.mu` and `validator.transact` |
| Fencing a bank's previous owner | `newOwnedBank` in `roles/validator/bank.go`: one `transactional.id` per bank |
| Recovery from `localBalance` ([consistency](../docs/consistency/#recovery)) | `takeOver`, `restoreBalances` and `restorePositions` in `roles/validator/bank.go`, `startOffsets` in `validator.go` |
| Read positions stored with the results | `ownedBank.commit`: the `validatorOffsets` record goes in the same transaction |
| Concurrent w/o Tx | the `kafkaTransactions=false` paths: `plain.emit`, and `ownedBank.commit` without a transaction |
| Messages and single-object encoding ([data-model](../docs/data-model/)) | `internal/avro` |
| Checking the results | `roles/report/analysis.go` |
