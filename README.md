# decoupled-payments

Research prototype for the master's thesis **解耦交易：基於 Kafka 的分散式支付系統**
(*Decoupling Payments: A Distributed Payment System Based on Kafka*), National Chengchi University, 2024.

📄 [Thesis PDF](docs/thesis/decoupling-payments.pdf) · [Official record (NCCU)](https://ah.lib.nccu.edu.tw/item?item_id=171529)

> This is a research prototype for exploring and benchmarking the design, not a production payment system.

## Idea

Parallel payment processing usually needs account locks to avoid dirty writes. This system avoids them with a **credit model**: each payment ("trade") is split into a *debit* phase, validated by the sender bank's processor, and a *credit* phase, applied later by the receiver bank's processor. Each bank maps to one Kafka partition, so every balance has exactly one owner. Kafka transactions give exactly-once processing, and balances are **eventually consistent**.

In the thesis, the *Concurrent* model reached about **200,000 trades/s on two laptops**.

More: [architecture](docs/architecture/) · [thesis summary](docs/thesis/)

## Layout

| Path       | Contents                                             |
|------------|------------------------------------------------------|
| `docs/`    | Thesis, architecture and design docs                 |
| `schemas/` | Message schemas shared by all implementations        |
| `infra/`   | Docker Compose: multi-broker Kafka cluster (KRaft)   |
| `java/`    | Java implementation                                  |
| `go/`      | Go implementation (planned)                          |
| `bench/`   | Benchmark scripts                                    |

## Quick start

```sh
docker compose -f infra/docker-compose.yml up -d
```

The application itself is still being written.

## Citation

See [docs/thesis](docs/thesis/README.md#citation).

## License

Code: [MIT](LICENSE). The thesis in [`docs/thesis/`](docs/thesis/) is © 陳亮酉 and is not covered by the MIT license.
