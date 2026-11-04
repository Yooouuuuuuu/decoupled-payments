// Package kafka holds what the roles share about Kafka: the topics, the
// client settings, building and decoding records, and reading partitions to
// their end.
package kafka

import "github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"

// The topics (docs/architecture).
const (
	Trades           = "trades"           // new trades; Sequential model: also credits on their way back
	Blocks           = "blocks"           // blocks of trades, one partition per bank
	Credits          = "credits"          // credits waiting to be applied (Concurrent model)
	Orders           = "orders"           // results: validated (SEND), rejected, completed (RECEIVE)
	LocalBalance     = "localBalance"     // latest balance per account, to rebuild a validator's state
	ValidatorOffsets = "validatorOffsets" // how far each bank's validator has read blocks and credits
	AccountInfo      = "accountInfo"      // the banks and the model, written by init
)

// The consumer groups.
const (
	AggregatorsGroup = "aggregators"
	ValidatorsGroup  = "validators"
)

// TopicSpec is how init creates a topic.
type TopicSpec struct {
	Name       string
	Partitions int32
	Configs    map[string]*string
}

// AllTopics returns the seven topics. Every per-bank topic has one partition
// per bank; credits exists in both models (the Sequential model leaves it
// empty).
func AllTopics(cfg config.Config) []TopicSpec {
	banks := cfg.Bank.Count
	compactedOften := func() map[string]*string {
		// Only the latest value per key matters, a balance per account or a
		// position per topic, and the old ones pile up fast: compact often.
		return map[string]*string{
			"cleanup.policy":            ptr("compact"),
			"segment.ms":                ptr("60000"),
			"min.cleanable.dirty.ratio": ptr("0.1"),
		}
	}
	return []TopicSpec{
		{Name: Trades, Partitions: cfg.Topics.TradesPartitions},
		{Name: Blocks, Partitions: banks},
		{Name: Credits, Partitions: banks},
		{Name: Orders, Partitions: banks},
		{Name: LocalBalance, Partitions: banks, Configs: compactedOften()},
		{Name: ValidatorOffsets, Partitions: banks, Configs: compactedOften()},
		// Compacted, keyed by bank ID: the banks are kept for as long as the
		// cluster exists, not deleted after the default seven days.
		{Name: AccountInfo, Partitions: 1, Configs: map[string]*string{"cleanup.policy": ptr("compact")}},
	}
}

// TopicNames returns the names of AllTopics.
func TopicNames() []string {
	return []string{Trades, Blocks, Credits, Orders, LocalBalance, ValidatorOffsets, AccountInfo}
}

func ptr(s string) *string { return &s }
