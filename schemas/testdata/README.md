# Golden test data

Reference encodings of one sample message per schema. They were produced with Apache Avro's Java library (`BinaryMessageEncoder`, i.e. Avro *single-object encoding*). Both implementations test against them, so a Java and a Go process always agree on the bytes on the wire.

| File | Contents |
|---|---|
| `<name>.bin` | The sample message, single-object encoded: `C3 01`, the schema's 8-byte CRC-64-AVRO fingerprint (little-endian), then the Avro binary payload |
| `<name>.canonical.json` | The schema's [Parsing Canonical Form](https://avro.apache.org/docs/1.12.0/specification/#parsing-canonical-form-for-schemas), which the fingerprint is computed from |
| `fingerprints.txt` | Each schema's full name and fingerprint |

A test must decode each `.bin` to exactly the values below, and encode those values to exactly the same bytes.

## Sample values

**`block.bin`** (`Block`) with two trades:

| Field | Trade 1 | Trade 2 |
|---|---|---|
| `serialNumber` | 4294967297 | 4294967298 |
| `outbank` | `bank0` | `bank1` |
| `outAccount` | `bank0-0007` | `bank1-0003` |
| `inbank` | `bank2` | `bank0` |
| `inAccount` | `bank2-0042` | `bank0-0099` |
| `outbankPartition` | 0 | 1 |
| `inbankPartition` | 2 | 0 |
| `amount` | 1250 | 7 |
| `category` | `SEND` | `REJECTED` |
| `timestamp1` | 1730700000000 | 1730700000123 |
| `timestamp2` | 0 | 1730700000456 |

**`localBalance.bin`** (`LocalBalance`): `balance` = 987654321

**`accountInfo.bin`** (`AccountInfo`): `bank` = `bank1`, `bankPartition` = 1, `accounts` = [`bank1-0000`, `bank1-0001`, `bank1-0002`], `initialBalance` = 10000, `model` = `CONCURRENT`, `kafkaTransactions` = true

**`validatorOffset.bin`** (`ValidatorOffset`): `topic` = `credits`, `partition` = 2, `offset` = 12345
