# Running

How to start the system, send trades and check the results. The design behind it is in [architecture](../architecture/).

## One app, five roles

Both implementations, [Java](../../java/) and [Go](../../go/), build a single program, `dp`, with one role per process. Their command lines, configuration, topics and wire format are identical, so they can be swapped or mixed freely. The one thing they don't share is the trade generator's random numbers (see [the seed](#the-trade-generators-seed)).

| Role | What it does |
|---|---|
| `init` | Creates the topics, writes the banks, accounts and starting balances, and records the model. Runs once; running it again with the same settings does nothing. |
| `generator` | The trade generator: sends trades to `trades` at a set rate |
| `aggregator` | Layer 1: groups trades by bank into blocks |
| `validator` | Layer 2: validates trades and applies credits for the banks it owns |
| `report` | Reads the results back, prints throughput and latency, and checks correctness. Exits with status 0 only if every check passes. |

```sh
dp <role> [--setting=value ...]
dp --help            # or -h: the roles and how settings are given
dp <role> --help     # the role's settings: flag, default and environment variable
```

The exit status is 0 for success and for a clean stop, 1 for a failure while running (including a report whose checks failed), and 2 for a usage or configuration error. Such an error is printed as `dp: <problem>`, followed by `Run 'dp --help' for usage.` The configuration is checked as a whole, so every invalid setting gets its own line, naming the setting and where its value came from.

### Stopping

SIGTERM or SIGINT (Ctrl-C) stops the generator, an aggregator or a validator cleanly at any moment, even while it is still starting, with exit status 0. The generator first finishes sending the trades it has already produced, for at most 30 s, and exits with 1 if some are still unsent. A stopped aggregator or validator leaves its consumer group at once: its partitions move to the others right away, and `init --reset` works straight after.

A process that crashes or fails (exit status 1) doesn't leave its group. Restarted with the same `--siteId` within the session timeout (`kafka.sessionTimeoutMs`, 10 s), it gets its partitions back without a rebalance. Otherwise the others take them over when the timeout runs out.

`init` and `report` stop on these signals too, but with exit status 1 and a log line saying they were interrupted: they didn't finish, and an interrupted report has no verdict.

## With Docker Compose

Needs only Docker. From the repository root:

```sh
# Kafka (3 nodes), init, and one aggregator and one validator per site
docker compose -f infra/docker-compose.yml up -d --build

# Send 20,000 trades at 2,000 per second
docker compose -f infra/docker-compose.yml run --rm generator --trades=20000 --rps=2000

# Results and correctness checks
docker compose -f infra/docker-compose.yml run --rm report

# Logs of one processor
docker compose -f infra/docker-compose.yml logs -f validator-1

# Stop and delete everything: the next "up" starts with empty topics
docker compose -f infra/docker-compose.yml --profile tools down -v
```

Kafka keeps its data inside its containers, so `down` deletes it, and no volumes are left behind. `--profile tools` includes the generator and the report. A generator or report still running from `docker compose run` is only removed if you also add `--remove-orphans`.

Choices for a run are environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `IMPL` | `java` | Which implementation to run: `java` or `go` |
| `AGGREGATOR_IMPL`, `VALIDATOR_IMPL`, `TOOLS_IMPL` | `$IMPL` | Per role (`TOOLS_IMPL` covers `init`, `generator` and `report`), to mix the two implementations |
| `AGGREGATOR_<i>_IMPL`, `VALIDATOR_<i>_IMPL` | per role | For a single site, e.g. `VALIDATOR_2_IMPL=go` |
| `MODEL` | `concurrent` | `concurrent` or `sequential` (see [models](../models/)) |
| `KAFKA_TRANSACTIONS` | `true` | `false` runs the model without Kafka transactions |
| `BANK_COUNT` | one per site | Number of banks, which is the number of partitions of `blocks`, `credits`, `orders`, `localBalance` and `validatorOffsets`. With more banks than validators, a validator owns several. |
| `TRADES_PARTITIONS` | one per site | Number of partitions of `trades`, which the aggregators share |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |

Export them, or put them in `infra/.env`, rather than setting them on the `up` command alone. The generator and the report are separate `docker compose run` commands, which choose their implementation from `TOOLS_IMPL` or `IMPL` too: with `IMPL=go` set only for `up`, they would run on the Java image, and build it first. For example, the Sequential model on the Go implementation:

```sh
docker compose -f infra/docker-compose.yml --profile tools down -v
export IMPL=go MODEL=sequential
docker compose -f infra/docker-compose.yml up -d --build
docker compose -f infra/docker-compose.yml run --rm generator --trades=20000
```

The model, the number of banks and the `trades` partitions are fixed when `init` runs. Only `init` reads `BANK_COUNT` and `TRADES_PARTITIONS`, so change them with a fresh cluster (`down -v`, then `up`). To change the number of sites, see [deployment](../deployment/#the-compose-file).

## Without Docker for the app

Run Kafka only, then the roles as local processes. The default `kafka.bootstrapServers` is `localhost:9092`, which is `kafka-1` of the Compose cluster:

```sh
docker compose -f infra/docker-compose.yml up -d kafka-1 kafka-2 kafka-3
```

Build and run with Java ([details](../../java/README.md)):

```sh
cd java && ./mvnw -B package
java -jar target/dp.jar init
java -jar target/dp.jar validator --siteId=1 &
java -jar target/dp.jar aggregator --siteId=1 &
java -jar target/dp.jar generator --trades=10000
java -jar target/dp.jar report
```

Or with Go ([details](../../go/README.md)):

```sh
cd go && go build -o bin/dp ./cmd/dp
bin/dp init
...
```

Every aggregator and validator needs its own `--siteId`.

## Configuration

All settings, with their defaults and a comment each, are in [`config/default.yaml`](../../config/default.yaml). Any of them can be overridden. Later sources win:

1. `config/default.yaml`, built into the program
2. a YAML file: `--config=my.yaml` (or `DP_CONFIG=my.yaml`; an empty `DP_CONFIG` counts as unset). It may only contain settings that exist in the defaults, and each value is parsed like the default's type, the same way as an environment variable or a flag.
3. environment variables: `DP_` and the setting in upper snake case, e.g. `aggregator.blockSize` → `DP_AGGREGATOR_BLOCK_SIZE`
4. command-line flags: `--aggregator.blockSize=500`. A flag that isn't a full setting name is looked up in the role's own section, so `dp aggregator --blockSize=500` works too, and `dp init --reset` means `--init.reset=true`.

The settings the thesis experimented with:

| Setting | Default | Thesis |
|---|---|---|
| `aggregator.blockSize` | 500 | Varied; small blocks limit throughput |
| `aggregator.blockTimeoutMs` | 1000 | 10 s; below 5 s recommended with network delay |
| `kafka.maxPartitionFetchBytes` | 1 MiB | "Max fetch bytes"; too low limits throughput |
| `validator.blocksPerTransaction` | 1 | 1 block per Kafka transaction |
| `generator.rps` | 1000 | Varied, up to ~250k |
| `generator.distribution` | `uniform` | Uniform and Zipf |

A block holds at most `aggregator.blockSize` trades, and at most 512 KiB of encoded trades, so that it and every record a validator makes from it stay well under Kafka's 1 MiB message limit. A trade takes about 60 to 65 bytes, so the byte limit only matters above about 8,000 trades per block.

### The trade generator's seed

`generator.seed` (default 42) makes the accounts and amounts of a run reproducible, but only within one implementation: with the same seed and settings, a generator picks the same accounts and amounts again, but Java and Go use different random number generators, so they pick different ones. To compare the implementations, compare runs with the same settings and distribution, not the same trades. The serial numbers differ in every run anyway, because each run gets a random run ID (see [data model → IDs](../data-model/#ids)).

## What the report checks

`report` reads `accountInfo`, `trades`, `orders` and `localBalance` from the beginning, with `read_committed` isolation. It reads `trades` up to the end it has when the report starts, and waits for the Kafka transactions still open at that moment to finish. It then follows `orders` until every trade it found is validated and every accepted one completed, or until `report.timeoutMs` passes. While it waits, it logs how many validations and completions are still missing. Each read gives up, with exit status 1, if it makes no progress for `kafka.transactionTimeoutMs` plus 60 s.

Every `SEND` trade on `trades` is a *generated* trade (the `RECEIVE` ones are credits of the Sequential model). The report classifies each generated trade, by serial number, once:

| Class | Meaning |
|---|---|
| accepted | `orders` has at least one record that accepted it (`SEND`) |
| rejected | `orders` has a record that rejected it (`REJECTED`), and none that accepted it |
| completed | Accepted, and `orders` has at least one record of its credit being applied (`RECEIVE`) |
| in flight | Everything else: not validated yet, or accepted but not completed |

So *generated = rejected + completed + in flight*. The in-flight count is what was still unfinished when the report stopped waiting; after a settled run it is 0. A trade that was, say, validated twice still counts once here. The checks catch that:

| Check | Meaning |
|---|---|
| `complete` | Every generated trade was validated, and every accepted trade was completed, within the timeout |
| `validated-once` | Each generated trade appears once on `trades` and has exactly one validation record (accepted or rejected), and no unknown trade was validated |
| `completed-once` | Each accepted trade has exactly one completion record, and no rejected, unvalidated or unknown trade was completed |
| `money-conserved` | All balances plus the trades accepted but not yet completed add up to the starting total |
| `no-negative-balances` | No balance written to `localBalance` was ever negative |
| `balances-match-orders` | Every account's balance equals its starting balance, minus what it sent in accepted trades, plus what it received in completed trades |

`money-conserved` and `balances-match-orders` need a settled run, so they're only evaluated when `complete` passed. Otherwise both fail with `not checked: N trades not finished`: the balances are read after the orders, so trades still moving would show differences that aren't errors.

It also prints throughput, and latency percentiles (p50, p95, p99, max). Throughput is the number of completed trades divided by the time from the earliest trade's creation to the last completion. Validation latency runs from a trade's creation to its first validation record, and completion latency to its first completion record. With `--json`, it adds one machine-readable line starting with `REPORT_JSON`: `result`, `model`, `kafkaTransactions`, the counts `generated`, `accepted`, `rejected`, `completed` and `inFlight`, `throughputTps`, `latencyMs`, `validationLatencyMs`, and the `checks` with their `name`, `passed` and `detail`.

## End-to-end tests

[`tests/`](../../tests/) has scripts that start a fresh cluster, run each model on each implementation (and mixed), optionally kill and pause processors mid-run, and require the report to pass and every processor to recover. See [`tests/README.md`](../../tests/README.md).
