# decoupled-payments

A rewrite of the research prototype from the master's thesis **解耦交易：基於 Kafka 的分散式支付系統**
(*Decoupling Payments: A Distributed Payment System Based on Kafka*), National Chengchi University, 2024.

📄 [Thesis PDF](docs/thesis/decoupling-payments.pdf) · [Official record (NCCU)](https://ah.lib.nccu.edu.tw/item?item_id=171529)

> This is a research prototype for exploring and benchmarking the design, not a production payment system.

## About this code

This is not the code that produced the thesis's results. It implements the thesis design again from scratch, with current versions (Kafka 4.3 in KRaft mode and Java 21, where the original used Kafka 3.4 and Java 11), and adds a second implementation in Go. The two are interchangeable and can run side by side in one cluster. Some details differ from the original implementation; the [docs](docs/) describe the design as it is implemented here.

The thesis's numbers were measured with the original code on the thesis's machines. This code aims to show the same trends, not to reproduce those numbers.

## Idea

Parallel payment processing usually needs account locks to avoid dirty writes. This system avoids them with a **credit model**: each payment ("trade") is split into a *validation* phase, where the sender bank's processor checks and deducts the amount, and a *credit* phase, where the receiver bank's processor adds it later. Each bank maps to one Kafka partition, so every balance has exactly one owner. Kafka transactions give exactly-once processing, and balances are **eventually consistent**.

In the thesis, the *Concurrent* model reached about **200,000 trades/s on two laptops**.

More: [architecture](docs/architecture/) · [thesis summary](docs/thesis/)

## Layout

| Path       | Contents                                                    |
|------------|-------------------------------------------------------------|
| `docs/`    | Thesis, architecture and design docs                        |
| `schemas/` | Message schemas shared by both implementations              |
| `config/`  | Default configuration shared by both implementations        |
| `java/`    | Java implementation                                         |
| `go/`      | Go implementation                                           |
| `infra/`   | Docker Compose: a multi-broker Kafka cluster (KRaft) and the processors |
| `tests/`   | End-to-end tests                                            |
| `bench/`   | Benchmark scripts                                           |

## Quick start

Needs only Docker

```sh
docker compose -f infra/docker-compose.yml up -d --build                    # 3 Kafka nodes, aggregators, validators
docker compose -f infra/docker-compose.yml run --rm generator --trades=20000 # send trades
docker compose -f infra/docker-compose.yml run --rm report                   # throughput, latency, correctness checks
docker compose -f infra/docker-compose.yml --profile tools down -v           # stop and delete everything
```

Run `export IMPL=go` first for the Go implementation (every command reads it, also the generator and the report), or `export MODEL=sequential` for the other model. More in [running](docs/running/).

## Citation

See [docs/thesis](docs/thesis/README.md#citation).

## License

Code: [MIT](LICENSE). The thesis in [`docs/thesis/`](docs/thesis/) is © 陳亮酉 and is not covered by the MIT license.
