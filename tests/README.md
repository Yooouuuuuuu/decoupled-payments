# End-to-end tests

The end-to-end tests run the whole system in Docker and check from the data in Kafka that it processed every trade correctly. They use the local cluster in [`infra/docker-compose.yml`](../infra/docker-compose.yml): by default 3 sites, each with a Kafka node, an aggregator and a validator (see [deployment](../docs/deployment/)).

## What a test run does

[`e2e.sh`](e2e.sh) runs one scenario:

1. Removes any previous cluster of the Compose project `decoupled-payments`, with its data.
2. Builds the images the scenario needs (`decoupled-payments-java`, `decoupled-payments-go`).
3. Starts the cluster. Compose starts the Kafka nodes, runs `init` once every node is healthy, and starts the aggregators and validators once `init` has created the topics and accounts.
4. Waits until every aggregator and validator has joined its consumer group, so no partitions move between processes that are still starting while the trades flow.
5. Runs the trade generator: by default 20,000 trades at 2,000 per second, with amounts from 1 to 100 and accounts picked uniformly.
6. With `--chaos`, while the generator runs:
   - SIGKILLs `validator-1` and `aggregator-1`, and restarts them 5 s later. They lose everything they hadn't committed and must recover from Kafka. The validator rebuilds its balances from `localBalance` and its read positions from `validatorOffsets`, and the aggregator reads `trades` again from its committed offsets.
   - Pauses `validator-2` and `aggregator-2` for 25 s, well over the 10 s session timeout. Both consumer groups drop them, and the other processes take over their partitions. The new owners of the paused validator's banks fence its producers. The paused aggregator's consumer-group generation is outdated, so Kafka refuses its offsets, and the broker aborts its open transaction, if any, after the 10 s transaction timeout. While the two sleep, the test checks that both groups are stable without them, so they really wake up as zombies. When they do, each finds itself fenced and exits (Docker restarts it), or rejoins its group. Either way no trade may be lost or processed twice.
7. Runs the report, which waits up to 180 s for trades still in flight, then checks the result. The report must pass, and it must have found exactly the number of trades the generator was asked to send. With `--amounts`, at least one trade must have been rejected.
8. Checks that within 60 s every aggregator and validator is running and both consumer groups are stable with all their members again, and that no process restarted more often than expected: a paused one at most once, the others never. Without this check, a process that keeps crashing would go unnoticed, because the others take over its partitions and the trades still complete.
9. Saves every container's log and the report, removes the cluster (unless `--keep`), and prints a one-line verdict.

The exit status is 0 if the test passed, 1 if it failed, and 2 for bad options.

## What the report checks

The report reads `accountInfo`, `trades`, `orders` and `localBalance` from the beginning, with `read_committed` isolation, and checks:

| Check | Meaning |
|---|---|
| `complete` | Every generated trade was validated, and every accepted trade was completed, within the timeout |
| `validated-once` | Every generated trade appears once on `trades` and has exactly one validation record (accepted or rejected), and there are none for unknown trades |
| `completed-once` | Every accepted trade has exactly one completion record, and no rejected, unvalidated or unknown trade was completed |
| `money-conserved` | The balances plus the trades accepted but not yet completed add up to the money created at init |
| `no-negative-balances` | No balance written to `localBalance` was ever negative |
| `balances-match-orders` | Every account's balance equals its initial balance, minus what it sent in accepted trades, plus what it received in completed trades |

The two balance checks are only evaluated once the run has settled (`complete` passed); otherwise they fail as `not checked`. How the report counts trades is in [running → what the report checks](../docs/running/#what-the-report-checks).

Together these are the [exactly-once and eventual consistency guarantees](../docs/consistency/) of the design. Without Kafka transactions (`--no-tx`) they only hold when nothing fails, which is why the matrix runs that case without `--chaos`.

## Running

```sh
tests/e2e.sh                                    # Java, Concurrent model
tests/e2e.sh --impl go --model sequential --chaos
tests/e2e.sh --aggregators java --validators go --tools go
tests/e2e.sh --mixed-aggregators --mixed-validators --chaos   # Java and Go in the same groups
tests/e2e.sh --amounts 500-5000 --zipf          # balances run short, so trades are rejected
tests/e2e.sh --banks 6 --chaos                  # two banks per validator
tests/e2e.sh --help                             # all options

tests/e2e-all.sh                                # the standard matrix of 16 scenarios
tests/e2e-all.sh --trades 5000                  # options are added to every scenario
```

| Option | Meaning |
|---|---|
| `--impl java\|go` | Implementation of every role (default `java`) |
| `--aggregators`, `--validators`, `--tools java\|go` | Implementation of one role; the tools are `init`, the generator and the report |
| `--mixed-aggregators`, `--mixed-validators` | Aggregators, or validators, of odd sites in Java, of even sites in Go |
| `--model concurrent\|sequential` | The [model](../docs/models/) (default `concurrent`) |
| `--no-tx` | Run without Kafka transactions |
| `--trades N`, `--rps R` | How many trades, and how many per second (default 20,000 at 2,000/s) |
| `--amounts MIN-MAX` | Trade amounts (default 1-100). Large amounts such as 500-5000 make balances run short, so that trades are rejected; with this option the test requires at least one rejected trade |
| `--zipf` | Pick the accounts of a trade with a Zipf distribution, so a few accounts trade much more than the rest |
| `--banks N` | Number of banks (default one per site); with more banks than sites, validators own several |
| `--trades-partitions N` | Number of partitions of `trades` (default one per site) |
| `--chaos` | Kill, restart and pause processes while the trades flow (needs at least 2 sites) |
| `--keep` | Leave the cluster running at the end. Remove it later with `docker compose -f infra/docker-compose.yml --profile tools down -v`. |

The matrix in [`e2e-all.sh`](e2e-all.sh) runs four implementation mixes (all Java; all Go; Java aggregators with Go validators and tools; mixed validators) with three models each: Concurrent with `--chaos`, Sequential with `--chaos`, and Concurrent with `--no-tx`. Four more scenarios, all with `--chaos`, cover what this grid doesn't:

| Scenario | What it adds |
|---|---|
| `--mixed-aggregators --mixed-validators --model concurrent --amounts 500-5000` | Java and Go aggregators in one group, and rejected trades |
| `--aggregators go --validators java --tools java --model sequential --amounts 500-5000 --zipf` | Go aggregators with Java validators, and rejections of busy accounts in the Sequential model |
| `--impl go --model concurrent --banks 6` | More banks than validators: each validator owns two, and a takeover moves two |
| `--impl java --model sequential --trades-partitions 5` | More `trades` partitions than aggregators and banks |

The matrix continues after a failure, prints a summary table and exits with 1 if any scenario failed. On a cluster of one site it skips the `--chaos` scenarios, which need two, and lists them as `SKIP`.

The options alone define a scenario. `e2e.sh` ignores `infra/.env`, and the variables in your shell that select implementations, the model or the topics (`IMPL`, `MODEL`, `BANK_COUNT`, ...), or that would change Compose's project or services (`COMPOSE_PROJECT_NAME`, `COMPOSE_PROFILES`, `COMPOSE_FILE`). Others still apply, for example `LOG_LEVEL=debug` or `KAFKA_HEAP_OPTS`. The test works with any cluster shape made by `infra/generate-compose.sh`.

### Requirements

- Docker Engine 25 or later with Compose v2 (`docker compose`), bash, and python3 (to read the report's JSON line).
- Host ports 9092–9094 free: the cluster publishes one port per Kafka node.
- A few GB of free memory: each Kafka node has a 512 MB heap, plus the aggregators and validators.

### How long it takes

- The first run builds the images, which takes a few minutes while Maven and Go download dependencies. Later builds come from the Docker cache.
- With the images built, a scenario without `--chaos` takes about 35 s: around 20 s to start the cluster and let the processes join their groups, 10 s of trades, then the report, the recovery check and the cleanup. `--chaos` adds about 20 s, mostly for the 25 s pause.
- The matrix takes about 15 minutes.

## Results

Each run saves its results to `results/e2e/<UTC timestamp>-<scenario>/` (`results/` is not committed). The scenario name says what ran, for example `java-concurrent-chaos`, `agg-java+val-go+tools-go-sequential-chaos` or `go-concurrent-banks6-chaos`.

| File | Contents |
|---|---|
| `scenario.txt` | The scenario, the variables it set and the trade generator's options |
| `report.log`, `report.json` | The report's output, and its `REPORT_JSON` line |
| `generator.log` | The trade generator's output |
| `<service>.log` | The log of every container: `kafka-1`, `init`, `aggregator-1`, `validator-2`, ... |
| `chaos.log` | What `--chaos` did and when, and the consumer groups during the pause |
| `containers.txt` | The final state of every container; the restart counts show which processes crashed or exited after being fenced |
| `build.log`, `up.log` | The output of the image build and of starting the cluster |
| `compose.yml` | The Compose configuration as resolved for this scenario |

`e2e-all.sh` also saves its summary table to `results/e2e/<UTC timestamp>-matrix.txt`.

When a scenario fails, its verdict line says which step or which check failed. The report's output names the failing checks, and the validator logs show what each validator owned and when.
