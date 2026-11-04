package validator

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

// fenceLimit bounds initializing a bank's producer, which fences the bank's
// previous owner, like Java's max.block.ms for initTransactions. It waits
// while the previous owner's last transaction is being completed; if that
// takes longer, the validator fails and its restart tries again.
const fenceLimit = 60 * time.Second

// ownedBank is a bank this validator owns: the bank's producer, and how far
// the validator has read the bank's partitions of blocks and credits.
type ownedBank struct {
	bank.Bank
	// producer writes everything for this bank. With Kafka transactions its
	// transactional.id is validator-<bank>, the same on every validator: the
	// bank's next owner fences this producer when it takes over.
	producer      *kgo.Client
	transactional bool
	timeout       time.Duration // the transaction timeout bounds every write
	nextBlock     int64         // next offset of blocks to validate
	nextCredit    int64         // next offset of credits to apply
}

// takeOver makes this validator the owner of the banks: it fences their
// previous owners, then restores their balances and read positions, which
// those owners wrote in the same transactions as their results. It returns
// the balances of each bank by partition.
func (v *validator) takeOver(ctx context.Context, partitions []int32) (map[int32]*ownedBank, map[int32]map[string]int64, error) {
	banks := map[int32]*ownedBank{}
	fail := func(err error) (map[int32]*ownedBank, map[int32]map[string]int64, error) {
		for _, b := range banks {
			b.producer.Close()
		}
		return nil, nil, err
	}
	for _, p := range partitions {
		if int(p) >= len(v.dir.Banks) {
			return fail(fmt.Errorf("assigned partition %d of blocks, but accountInfo has %d banks", p, len(v.dir.Banks)))
		}
		b, err := v.newOwnedBank(ctx, v.dir.Banks[p])
		if err != nil {
			return fail(err)
		}
		banks[p] = b
	}
	// Every previous owner is fenced before anything is read.
	balances, err := v.restoreBalances(ctx, banks)
	if err == nil {
		err = v.restorePositions(ctx, banks)
	}
	if err != nil {
		return fail(fmt.Errorf("restore %v: %w", slices.Sorted(maps.Keys(banks)), err))
	}
	return banks, balances, nil
}

// newOwnedBank creates the bank's producer. A transactional producer
// initializes its producer ID at once: that bumps the epoch of
// validator-<bank>, which fences the previous owner's producer (even one that
// is only paused) and completes or aborts its last transaction, so that the
// bank's partitions hold no open transaction when restore reads them.
func (v *validator) newOwnedBank(ctx context.Context, b bank.Bank) (*ownedBank, error) {
	opts := append(kafka.ClientOpts(v.cfg, v.log, kafka.ClientID(role, v.cfg.SiteID, b.ID)), kafka.ProducerOpts()...)
	if v.cfg.KafkaTransactions {
		opts = append(opts, kafka.TransactionalOpts(v.cfg, "validator-"+b.ID)...)
	}
	producer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	if v.cfg.KafkaTransactions {
		fenceCtx, cancel := context.WithTimeout(ctx, fenceLimit)
		defer cancel()
		if _, _, err := producer.ProducerID(fenceCtx); err != nil {
			producer.Close()
			return nil, fmt.Errorf("fence the previous owner of %s: %w", b.ID, err)
		}
	}
	return &ownedBank{Bank: b, producer: producer, transactional: v.cfg.KafkaTransactions, timeout: v.cfg.Kafka.TransactionTimeout}, nil
}

// restoreBalances reads the banks' partitions of localBalance: the latest
// balance of each account wins. It takes only the bank's own accounts, as
// accountInfo lists them, and every one of them must have a balance.
func (v *validator) restoreBalances(ctx context.Context, banks map[int32]*ownedBank) (map[int32]map[string]int64, error) {
	latest := map[string]int64{}
	err := kafka.ReadToEnd(ctx, v.cfg, v.log, kafka.ClientID(role, v.cfg.SiteID, "localBalance"),
		map[string][]int32{kafka.LocalBalance: slices.Sorted(maps.Keys(banks))}, kafka.StableEnd, func(r *kgo.Record) error {
			lb, err := kafka.Decode(avro.LocalBalanceCodec, r)
			latest[string(r.Key)] = lb.Balance
			return err
		})
	if err != nil {
		return nil, err
	}
	balances := map[int32]map[string]int64{}
	for p, b := range banks {
		balances[p] = make(map[string]int64, len(b.Accounts))
		for _, a := range b.Accounts {
			balance, ok := latest[a]
			if !ok {
				return nil, fmt.Errorf("localBalance has no balance for account %s of %s: run init --reset", a, b.ID)
			}
			balances[p][a] = balance
		}
	}
	return balances, nil
}

// restorePositions reads the banks' partitions of validatorOffsets: where to
// continue reading blocks and credits. A record's key names the topic of the
// position, and its value must agree.
func (v *validator) restorePositions(ctx context.Context, banks map[int32]*ownedBank) error {
	return kafka.ReadToEnd(ctx, v.cfg, v.log, kafka.ClientID(role, v.cfg.SiteID, "validatorOffsets"),
		map[string][]int32{kafka.ValidatorOffsets: slices.Sorted(maps.Keys(banks))}, kafka.StableEnd, func(r *kgo.Record) error {
			pos, err := kafka.Decode(avro.ValidatorOffsetCodec, r)
			if err != nil {
				return err
			}
			key := string(r.Key)
			if pos.Topic != key || pos.Partition != r.Partition {
				return fmt.Errorf("%s partition %d offset %d: the position of %s partition %d has the key %q",
					r.Topic, r.Partition, r.Offset, pos.Topic, pos.Partition, key)
			}
			switch b := banks[r.Partition]; key {
			case kafka.Blocks:
				b.nextBlock = pos.Offset
			case kafka.Credits:
				b.nextCredit = pos.Offset
			default:
				return fmt.Errorf("%s partition %d offset %d: unknown key %q", r.Topic, r.Partition, r.Offset, key)
			}
			return nil
		})
}

// commit writes one transaction of the bank: its results, then its new read
// position. With Kafka transactions they become visible together or not at
// all. Without, the results are flushed before the position is written, so a
// crash in between makes the next owner process those records again
// (duplicates) but never skips one.
func (b *ownedBank) commit(ctx context.Context, results []*kgo.Record, position *kgo.Record) error {
	// A started transaction runs to the end even if the validator is
	// stopping; the transaction timeout bounds it.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), b.timeout)
	defer cancel()
	if !b.transactional {
		if err := b.send(ctx, results...); err != nil {
			return err
		}
		return b.send(ctx, position)
	}
	if err := b.producer.BeginTransaction(); err != nil {
		return err
	}
	committed := false
	defer func() {
		// Whatever went wrong, even a panic: abort, so that the partitions
		// don't wait for the transaction timeout. The validator exits next.
		if !committed && b.producer.AbortBufferedRecords(ctx) == nil {
			_ = b.producer.EndTransaction(ctx, kgo.TryAbort)
		}
	}()
	err := b.send(ctx, append(results, position)...)
	if err == nil {
		err = b.producer.EndTransaction(ctx, kgo.TryCommit)
	}
	if err != nil {
		return fmt.Errorf("transaction of %s: %w", b.ID, err)
	}
	committed = true
	return nil
}

// send produces records and waits until each one is acknowledged.
func (b *ownedBank) send(ctx context.Context, records ...*kgo.Record) error {
	var sends kafka.SendErrors
	for _, r := range records {
		b.producer.Produce(ctx, r, sends.Promise)
	}
	if err := b.producer.Flush(ctx); err != nil {
		return err
	}
	return sends.Err()
}
