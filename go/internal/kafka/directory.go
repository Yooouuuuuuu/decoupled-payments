package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

// ErrNotInitialized means there is no accountInfo to read.
var ErrNotInitialized = errors.New("accountInfo is empty: run init first")

// ReadDirectory reads the banks that init recorded in accountInfo. The key
// of a record is its bank's ID, and the latest record of each bank wins, as
// in the compacted topic.
func ReadDirectory(ctx context.Context, cfg config.Config, log *slog.Logger, clientID string) (bank.Directory, error) {
	latest := map[string]avro.AccountInfo{}
	err := ReadToEnd(ctx, cfg, log, clientID, map[string][]int32{AccountInfo: {0}}, StableEnd, func(r *kgo.Record) error {
		info, err := Decode(avro.AccountInfoCodec, r)
		if err != nil {
			return err
		}
		if string(r.Key) != info.Bank {
			return fmt.Errorf("%s offset %d: the key %q names another bank than the record, %s", AccountInfo, r.Offset, r.Key, info.Bank)
		}
		latest[info.Bank] = info
		return nil
	})
	switch {
	case errors.Is(err, kerr.UnknownTopicOrPartition), err == nil && len(latest) == 0:
		return bank.Directory{}, ErrNotInitialized
	case err != nil:
		return bank.Directory{}, fmt.Errorf("read %s: %w", AccountInfo, err)
	}
	return bank.NewDirectory(slices.Collect(maps.Values(latest)))
}

// CheckModel fails unless the cluster runs the model and Kafka transaction
// setting of cfg: aggregators and validators refuse to join a cluster that
// runs differently.
func CheckModel(dir bank.Directory, cfg config.Config) error {
	if dir.Model != cfg.Model || dir.KafkaTransactions != cfg.KafkaTransactions {
		return fmt.Errorf("the cluster was initialized with model=%s kafkaTransactions=%t, but this process has model=%s kafkaTransactions=%t",
			dir.Model, dir.KafkaTransactions, cfg.Model, cfg.KafkaTransactions)
	}
	return nil
}
