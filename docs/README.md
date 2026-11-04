# Documentation

Each topic has its own folder. A folder holds a `README.md` (the main document) plus any appendices, and D2 diagrams in a `diagrams/` subfolder.

| Folder                              | What it covers                                                          |
|-------------------------------------|-------------------------------------------------------------------------|
| [`thesis/`](thesis/)                | The thesis PDF, the official record link, citation, and a short summary |
| [`architecture/`](architecture/)    | Overview: the problem, the idea, topics, processors, a trade's lifecycle, glossary |
| [`credit-model/`](credit-model/)    | Fund representation: account vs UTXO vs the credit model                |
| [`partitioning/`](partitioning/)    | One balance, one owner: how trades are routed to partitions, and why    |
| [`models/`](models/)                | The Sequential and Concurrent models, and the thesis results            |
| [`consistency/`](consistency/)      | Exactly-once processing, eventual consistency, ordering, recovery, the shared balance map |
| [`data-model/`](data-model/)        | Messages, schemas and serialization                                     |
| [`deployment/`](deployment/)        | The local multi-broker cluster: sites, KRaft, defaults                  |
| [`running/`](running/)              | How to run it: the roles, stopping and exit statuses, Docker Compose, configuration, the report's checks |

Suggested reading order: architecture → credit-model → partitioning → models → consistency. To just run it, go to [running](running/).

## Conventions

- **Diagrams** are written in [D2](https://d2lang.com) and rendered to SVG next to their source (`d2 file.d2 file.svg`). Both are committed.
- **Terminology follows the thesis.** "Trade" means a payment. "Transaction" only ever means a *Kafka* transaction. See the [glossary](architecture/README.md#glossary).
