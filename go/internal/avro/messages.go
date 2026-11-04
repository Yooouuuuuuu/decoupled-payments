// Package avro defines the messages that flow through Kafka (docs/data-model)
// and encodes them with Avro single-object encoding. The schemas are embedded
// copies of /schemas.
package avro

// Category is the stage of a trade (docs/data-model, category lifecycle).
type Category string

const (
	Send     Category = "SEND"     // a new trade; on orders: validated (accepted)
	Receive  Category = "RECEIVE"  // a credit; on orders: completed
	Rejected Category = "REJECTED" // insufficient funds
)

// Trade is one payment from OutAccount (at bank Outbank) to InAccount (at
// bank Inbank).
type Trade struct {
	SerialNumber     int64    `avro:"serialNumber"`
	Outbank          string   `avro:"outbank"`
	OutAccount       string   `avro:"outAccount"`
	Inbank           string   `avro:"inbank"`
	InAccount        string   `avro:"inAccount"`
	OutbankPartition int32    `avro:"outbankPartition"` // where the trade is validated
	InbankPartition  int32    `avro:"inbankPartition"`  // where its credit is applied
	Amount           int64    `avro:"amount"`
	Category         Category `avro:"category"`
	Timestamp1       int64    `avro:"timestamp1"` // created, ms since the Unix epoch
	Timestamp2       int64    `avro:"timestamp2"` // validated, ms since the Unix epoch; 0 before
}

// Block is a batch of trades, the value of every record on the trades,
// blocks, credits and orders topics.
type Block struct {
	Trades []Trade `avro:"trades"`
}

// LocalBalance is the latest balance of one account (the record key).
type LocalBalance struct {
	Balance int64 `avro:"balance"`
}

// Model is the model the cluster runs, as init records it in AccountInfo.
type Model string

const (
	ModelSequential Model = "SEQUENTIAL"
	ModelConcurrent Model = "CONCURRENT"
)

// AccountInfo is one bank and its accounts (the record key is the bank).
type AccountInfo struct {
	Bank              string   `avro:"bank"`
	BankPartition     int32    `avro:"bankPartition"`
	Accounts          []string `avro:"accounts"`
	InitialBalance    int64    `avro:"initialBalance"`
	Model             Model    `avro:"model"`
	KafkaTransactions bool     `avro:"kafkaTransactions"`
}

// ValidatorOffset is how far a bank's validator has read its partition of
// blocks or credits (the record key is the topic name).
type ValidatorOffset struct {
	Topic     string `avro:"topic"`
	Partition int32  `avro:"partition"`
	Offset    int64  `avro:"offset"` // the next offset to read
}
