# Thesis

**解耦交易：基於 Kafka 的分散式支付系統**
**Decoupling Payments: A Distributed Payment System Based on Kafka**

- Author: 陳亮酉 (Liang-You Chen)
- Advisor: Dr. 郭桐惟 (Tung-Wei Kuo)
- Master's thesis, Department of Computer Science, National Chengchi University (NCCU), March 2024 (ROC 113)
- Official record: <https://ah.lib.nccu.edu.tw/item?item_id=171529>
- PDF: [`decoupling-payments.pdf`](decoupling-payments.pdf) (© 陳亮酉, not covered by the repository's MIT license)

## Summary

The thesis builds an inter-bank payment system on Apache Kafka. Its core idea is the **credit model**: a trade is split ("decoupled") into two independent phases.

1. **Validation:** the sender's validator checks the balance and deducts the amount (sender account update).
2. **Credit:** the amount is emitted as a *credit* record, which the receiver's validator later adds without any further checks (receiver account update).

Each bank's accounts are owned by exactly one validator (one Kafka partition per bank). That removes the need for account locks and allows high parallelism, at the cost of **eventual** rather than immediate consistency. Kafka transactions give exactly-once processing.

Two models are compared:

| Model          | How credits travel                                  | Result in the thesis                                                    |
|----------------|-----------------------------------------------------|-------------------------------------------------------------------------|
| **Sequential** | Back into the Trades topic, re-aggregated into blocks | Higher peak TPS on a LAN (~230k)                                      |
| **Concurrent** | A dedicated Credits topic, read by a second validator thread | ~200k TPS; degrades more gracefully under overload and network delay (>2× Sequential at 100 ms delay) |

Hardware: two laptops (i7-12700H), plus two i7-10510U machines for the scalability tests.

Details are in [`../architecture/`](../architecture/).

## Citation

```bibtex
@mastersthesis{chen2024decoupling,
  author = {Chen, Liang-You},
  title  = {Decoupling Payments: A Distributed Payment System Based on Kafka},
  school = {National Chengchi University},
  year   = {2024},
  url    = {https://ah.lib.nccu.edu.tw/item?item_id=171529}
}
```
