// Package bank names banks and accounts, and holds the directory of banks that
// init records in the accountInfo topic.
//
// A bank is the unit of partitioning (docs/partitioning): bank i owns
// partition i of blocks, credits, orders, localBalance and validatorOffsets,
// so every balance has exactly one owner.
package bank

import (
	"fmt"
	"math"
	"strconv"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

// ID is the ID of the bank that owns a partition: bank0, bank1, ...
func ID(partition int32) string { return "bank" + strconv.Itoa(int(partition)) }

// AccountID is the ID of account j (0-based) of a bank: bank2-0042. The number
// has at least four digits.
func AccountID(bankID string, j int) string { return fmt.Sprintf("%s-%04d", bankID, j) }

// Bank is one bank as recorded in accountInfo.
type Bank struct {
	ID        string
	Partition int32
	Accounts  []string
}

// Directory is the cluster as init recorded it: the banks, and the settings
// they all share.
type Directory struct {
	Banks             []Bank // Banks[p] owns partition p
	InitialBalance    int64
	Model             config.Model
	KafkaTransactions bool
}

// NewDirectory builds the directory from the accountInfo records, one per bank.
func NewDirectory(infos []avro.AccountInfo) (Directory, error) {
	if len(infos) == 0 {
		return Directory{}, fmt.Errorf("accountInfo is empty")
	}
	first := infos[0]
	model, err := ConfigModel(first.Model)
	if err != nil {
		return Directory{}, err
	}
	d := Directory{
		Banks:             make([]Bank, len(infos)),
		InitialBalance:    first.InitialBalance,
		Model:             model,
		KafkaTransactions: first.KafkaTransactions,
	}
	seen := make([]bool, len(infos))
	for _, info := range infos {
		p := info.BankPartition
		switch {
		case p < 0 || int(p) >= len(infos) || seen[p]:
			return Directory{}, fmt.Errorf("accountInfo: banks must have the partitions 0 to %d once each, got %d for %s", len(infos)-1, p, info.Bank)
		case info.Bank != ID(p):
			return Directory{}, fmt.Errorf("accountInfo: partition %d belongs to %s, not %s", p, ID(p), info.Bank)
		case info.Model != first.Model || info.KafkaTransactions != first.KafkaTransactions || info.InitialBalance != first.InitialBalance:
			return Directory{}, fmt.Errorf("accountInfo: %s disagrees with %s about the model, kafkaTransactions or initialBalance", info.Bank, first.Bank)
		}
		seen[p] = true
		d.Banks[p] = Bank{ID: info.Bank, Partition: p, Accounts: info.Accounts}
	}
	// Money only moves between accounts: if the total fits in 64 bits, no
	// balance and no sum of balances can overflow (config validation makes
	// init check the same).
	if d.InitialBalance < 0 || d.InitialBalance > 0 && int64(d.AccountCount()) > math.MaxInt64/d.InitialBalance {
		return Directory{}, fmt.Errorf("accountInfo: %d accounts of %d each don't add up to a total that fits in 64 bits", d.AccountCount(), d.InitialBalance)
	}
	return d, nil
}

// AccountCount is the number of accounts in all banks.
func (d Directory) AccountCount() int {
	n := 0
	for _, b := range d.Banks {
		n += len(b.Accounts)
	}
	return n
}

// TotalMoney is the money in the system: it never changes, because a trade
// only moves money between accounts. NewDirectory checked that it fits.
func (d Directory) TotalMoney() int64 { return int64(d.AccountCount()) * d.InitialBalance }

// AvroModel is how accountInfo records a model.
func AvroModel(m config.Model) avro.Model {
	if m == config.Sequential {
		return avro.ModelSequential
	}
	return avro.ModelConcurrent
}

// ConfigModel is the setting for a model recorded in accountInfo.
func ConfigModel(m avro.Model) (config.Model, error) {
	switch m {
	case avro.ModelSequential:
		return config.Sequential, nil
	case avro.ModelConcurrent:
		return config.Concurrent, nil
	}
	return "", fmt.Errorf("accountInfo: unknown model %q", m)
}
