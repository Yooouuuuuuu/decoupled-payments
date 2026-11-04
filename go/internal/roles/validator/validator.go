// Package validator implements layer 2, the core of the system. A validator
// owns the banks whose partitions of blocks the consumer group assigns to it.
// It keeps their balances in memory, validates their trades, applies their
// credits, and writes the results to orders, credits (or trades) and
// localBalance, together with its read positions, in Kafka transactions.
//
// Thread B validates blocks. In the Concurrent model, thread C applies
// credits for the same banks. Both share the balances, so every Kafka
// transaction holds one lock from begin to commit (thesis §5.1): updating the
// balances inside a transaction that then aborts, or outside of it, would
// lose updates (docs/consistency).
package validator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

const (
	role    = "validator"
	maxPoll = 100 * time.Millisecond // the longest a poll waits for records
)

type validator struct {
	cfg config.Config
	log *slog.Logger
	id  string // validator-<siteId>: the group instance ID
	dir bank.Directory

	// mu is held for the whole of every Kafka transaction and during
	// rebalance callbacks. It guards the fields below.
	mu     sync.Mutex
	ledger *ledger
	banks  map[int32]*ownedBank // by partition
	epoch  uint64               // bumped when banks changes, see runCredits
	failed error                // the first failure; no transaction runs after it

	stop  context.CancelFunc // stops both threads
	stats stats
}

// stats counts what the validator did since the previous stats line. The
// stats goroutine reads them without the lock.
type stats struct {
	blocks, accepted, rejected, credited atomic.Int64
	owned                                atomic.Pointer[[]string] // the IDs of the banks owned
}

// Run validates and credits until ctx ends or an error occurs.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	dir, err := kafka.ReadDirectory(ctx, cfg, log, kafka.ClientID(role, cfg.SiteID, "accountInfo"))
	if err != nil {
		return err
	}
	if err := kafka.CheckModel(dir, cfg); err != nil {
		return err
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	v := &validator{cfg: cfg, log: log, id: fmt.Sprintf("%s-%d", role, cfg.SiteID), dir: dir, ledger: newLedger(),
		banks: map[int32]*ownedBank{}, stop: stop}
	v.stats.owned.Store(&[]string{})

	blocks, err := v.newBlocksConsumer(ctx)
	if err != nil {
		return err
	}
	var threads sync.WaitGroup
	if cfg.Model == config.Concurrent {
		credits, err := kgo.NewClient(append(kafka.ClientOpts(cfg, log, kafka.ClientID(role, cfg.SiteID, "credits")), kafka.ConsumerOpts(cfg)...)...)
		if err != nil {
			blocks.Close()
			return err
		}
		defer credits.Close()
		threads.Go(func() { v.runCredits(ctx, credits) })
	}
	threads.Go(func() { v.logStats(ctx) })

	v.runBlocks(ctx, blocks)
	stop()
	threads.Wait()
	if v.failure() == nil {
		// A clean stop: hand the banks to the other validators at once.
		// Leaving runs onRevoked, which closes the producers.
		v.leaveGroup(blocks)
	}
	blocks.CloseAllowingRebalance()

	v.mu.Lock()
	defer v.mu.Unlock()
	for _, b := range v.banks {
		b.producer.Close()
	}
	return v.failed
}

// leaveGroup leaves the consumer group after a clean stop. A failed or
// crashed validator stays a member, so that its restart within the session
// timeout gets its banks back without a rebalance.
func (v *validator) leaveGroup(blocks *kgo.Client) {
	// The group drops the member after the session timeout anyway.
	ctx, cancel := context.WithTimeout(context.Background(), v.cfg.Kafka.SessionTimeout)
	defer cancel()
	if err := kafka.LeaveGroup(ctx, blocks, kafka.ValidatorsGroup, v.id); err != nil {
		v.log.Warn("could not leave the consumer group; it drops this validator after kafka.sessionTimeoutMs", "err", err)
	}
}

func (v *validator) failure() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.failed
}

// fail records the first failure and stops both threads. Callers hold mu,
// so the other thread can't start a transaction on balances that may hold
// the effects of the failed one: the process exits instead, and its
// restart rebuilds the balances from Kafka. No undo logic is needed.
func (v *validator) fail(err error) {
	if v.failed == nil {
		v.failed = err
		v.log.Error("stopping", "err", err)
	}
	v.stop()
}

// transact runs fn under the lock, unless the validator has failed. An error
// from fn, or a panic, fails the validator before the lock is released.
func (v *validator) transact(fn func() error) (err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.failed != nil {
		return v.failed
	}
	defer func() {
		// A panic is a failed transaction like any error: the balances may
		// hold its effects. (ownedBank.commit has aborted it.)
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v\n%s", p, debug.Stack())
		}
		if err != nil {
			v.fail(err)
		}
	}()
	return fn()
}

// newBlocksConsumer creates thread B's consumer. The group only assigns
// partitions: the validator never commits group offsets. Its read positions
// are records in validatorOffsets, written in the same transactions as its
// results.
func (v *validator) newBlocksConsumer(ctx context.Context) (*kgo.Client, error) {
	opts := append(kafka.ClientOpts(v.cfg, v.log, kafka.ClientID(role, v.cfg.SiteID, "blocks")), kafka.ConsumerOpts(v.cfg)...)
	opts = append(opts, kafka.GroupOpts(v.cfg, kafka.ValidatorsGroup, v.id)...)
	onRevoked := func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) { v.onRevoked(revoked[kafka.Blocks]) }
	return kgo.NewClient(append(opts,
		kgo.ConsumeTopics(kafka.Blocks),
		// With BlockRebalanceOnPoll these callbacks never run while thread B
		// processes a poll. They may run while thread C commits, hence the
		// lock. franz-go calls OnPartitionsAssigned before it fetches offsets,
		// and AdjustFetchOffsetsFn after, before fetching begins.
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			v.onAssigned(ctx, assigned[kafka.Blocks])
		}),
		kgo.OnPartitionsRevoked(onRevoked),
		kgo.OnPartitionsLost(onRevoked),
		kgo.AdjustFetchOffsetsFn(v.startOffsets),
	)...)
}

// onAssigned takes over newly assigned banks.
func (v *validator) onAssigned(ctx context.Context, partitions []int32) {
	_ = v.transact(func() error {
		partitions := slices.DeleteFunc(slices.Clone(partitions), func(p int32) bool { return v.banks[p] != nil })
		if len(partitions) == 0 {
			return nil
		}
		banks, balances, err := v.takeOver(ctx, partitions)
		if err != nil && ctx.Err() != nil {
			return nil // interrupted because the validator is stopping
		}
		if err != nil {
			return err
		}
		for p, b := range banks {
			v.banks[p] = b
			v.ledger.add(b.Bank, balances[p])
		}
		v.banksChanged()
		v.log.Info("now owns", "banks", v.ownedIDs())
		return nil
	})
}

// onRevoked gives up banks: their producers close, and their balances are
// forgotten. No transaction is open: thread B is between polls, and thread
// C commits under the lock this holds.
func (v *validator) onRevoked(partitions []int32) {
	v.mu.Lock()
	defer v.mu.Unlock()
	var released []string
	for _, p := range partitions {
		if b, ok := v.banks[p]; ok {
			b.producer.Close()
			v.ledger.drop(b.Bank)
			delete(v.banks, p)
			released = append(released, b.ID)
		}
	}
	if len(released) > 0 {
		v.banksChanged()
		v.log.Info("no longer owns", "banks", released)
	}
}

func (v *validator) banksChanged() {
	v.epoch++
	ids := v.ownedIDs()
	v.stats.owned.Store(&ids)
}

func (v *validator) ownedIDs() []string {
	ids := []string{}
	for _, p := range slices.Sorted(maps.Keys(v.banks)) {
		ids = append(ids, v.banks[p].ID)
	}
	return ids
}

// startOffsets starts each assigned partition of blocks where its bank's
// previous owner stopped, as restored from validatorOffsets.
func (v *validator) startOffsets(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.failed != nil {
		return nil, v.failed
	}
	for p := range offsets[kafka.Blocks] {
		b, ok := v.banks[p]
		if !ok {
			return nil, fmt.Errorf("blocks partition %d was assigned, but its bank wasn't restored", p)
		}
		offsets[kafka.Blocks][p] = kgo.NewOffset().At(b.nextBlock)
	}
	return offsets, nil
}

// runBlocks is thread B: it validates blocks, one transaction per partition
// per poll.
func (v *validator) runBlocks(ctx context.Context, consumer *kgo.Client) {
	for ctx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(ctx, maxPoll)
		fetches := consumer.PollRecords(pollCtx, v.cfg.Validator.BlocksPerTransaction)
		cancel()
		err := v.checkPoll(fetches)
		if err == nil {
			err = v.validateBlocks(ctx, fetches)
		}
		consumer.AllowRebalance()
		if err != nil {
			return
		}
	}
}

// checkPoll logs the errors of a poll, which franz-go retries, except those
// that mean the group won't take this validator: they fail it.
func (v *validator) checkPoll(fetches kgo.Fetches) error {
	err := kafka.FetchErrors(fetches)
	if refused := kafka.GroupRefused(err, v.cfg.SiteID); refused != nil {
		return v.transact(func() error { return refused })
	}
	if err != nil {
		v.log.Warn("poll blocks", "err", err) // franz-go rejoins and retries
	}
	return nil
}

func (v *validator) validateBlocks(ctx context.Context, fetches kgo.Fetches) error {
	batches, err := decode(fetches)
	if err != nil {
		return v.transact(func() error { return err })
	}
	for _, batch := range batches {
		err := v.transact(func() error {
			b, err := v.owner(batch, func(b *ownedBank) int64 { return b.nextBlock })
			if err != nil {
				return err
			}
			out, err := v.ledger.validate(batch.partition, batch.blocks, v.cfg.Model, time.Now().UnixMilli())
			if err != nil {
				return err
			}
			if err := v.commit(ctx, b, out, kafka.Blocks, batch.next); err != nil {
				return err
			}
			b.nextBlock = batch.next
			v.stats.blocks.Add(int64(len(batch.blocks)))
			v.count(out)
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// runCredits is thread C (Concurrent model): it applies credits of the banks
// thread B owns. Its consumer has no group: thread B's callbacks change the
// banks and bump the epoch, and thread C follows by assigning itself their
// partitions of credits, each from the bank's position in credits.
func (v *validator) runCredits(ctx context.Context, consumer *kgo.Client) {
	var applied uint64   // the epoch of the banks consumer follows
	var assigned []int32 // the partitions consumer reads
	for ctx.Err() == nil {
		if epoch, positions := v.creditPositions(); epoch != applied {
			consumer.RemoveConsumePartitions(map[string][]int32{kafka.Credits: assigned})
			consumer.AddConsumePartitions(map[string]map[int32]kgo.Offset{kafka.Credits: positions})
			applied, assigned = epoch, slices.Collect(maps.Keys(positions))
		}
		pollCtx, cancel := context.WithTimeout(ctx, maxPoll)
		fetches := consumer.PollRecords(pollCtx, v.cfg.Validator.CreditsPerTransaction)
		cancel()
		if err := kafka.FetchErrors(fetches); err != nil {
			v.log.Warn("poll credits", "err", err)
		}
		if fetches.NumRecords() == 0 {
			continue
		}
		if err := v.applyCredits(ctx, fetches, applied); err != nil {
			return
		}
	}
}

// creditPositions returns the epoch and where to read each owned bank's
// partition of credits.
func (v *validator) creditPositions() (uint64, map[int32]kgo.Offset) {
	v.mu.Lock()
	defer v.mu.Unlock()
	positions := map[int32]kgo.Offset{}
	for p, b := range v.banks {
		positions[p] = kgo.NewOffset().At(b.nextCredit)
	}
	return v.epoch, positions
}

func (v *validator) applyCredits(ctx context.Context, fetches kgo.Fetches, epoch uint64) error {
	batches, err := decode(fetches)
	return v.transact(func() error {
		if err != nil {
			return err
		}
		if v.epoch != epoch {
			// The banks changed while polling: drop these records. They are
			// read again once thread C follows the new banks.
			return nil
		}
		for _, batch := range batches {
			b, err := v.owner(batch, func(b *ownedBank) int64 { return b.nextCredit })
			if err != nil {
				return err
			}
			out, err := v.ledger.applyCredits(batch.partition, batch.blocks)
			if err != nil {
				return err
			}
			if err := v.commit(ctx, b, out, kafka.Credits, batch.next); err != nil {
				return err
			}
			b.nextCredit = batch.next
			v.count(out)
		}
		return nil
	})
}

// owner returns the bank a batch belongs to. A batch from before the bank's
// read position would be processed twice; that must never happen.
func (v *validator) owner(batch batch, next func(*ownedBank) int64) (*ownedBank, error) {
	b, ok := v.banks[batch.partition]
	if !ok {
		return nil, fmt.Errorf("%s partition %d: got records for a bank this validator doesn't own", batch.topic, batch.partition)
	}
	if batch.first < next(b) {
		return nil, fmt.Errorf("%s partition %d: got offset %d, but %s has already processed up to %d", batch.topic, batch.partition, batch.first, b.ID, next(b))
	}
	return b, nil
}

// commit writes an outcome and the bank's new position in topic.
func (v *validator) commit(ctx context.Context, b *ownedBank, out outcome, topic string, next int64) error {
	var results []*kgo.Record
	add := func(r *kgo.Record, err error) error {
		results = append(results, r)
		return err
	}
	for _, block := range out.orders {
		if err := add(kafka.NewRecord(avro.BlockCodec, kafka.Orders, b.Partition, b.ID, block)); err != nil {
			return err
		}
	}
	for _, c := range out.credits {
		r, err := v.creditRecord(c)
		if err := add(r, err); err != nil {
			return err
		}
	}
	for _, account := range slices.Sorted(maps.Keys(out.balances)) {
		balance := avro.LocalBalance{Balance: out.balances[account]}
		if err := add(kafka.NewRecord(avro.LocalBalanceCodec, kafka.LocalBalance, b.Partition, account, balance)); err != nil {
			return err
		}
	}
	position := avro.ValidatorOffset{Topic: topic, Partition: b.Partition, Offset: next}
	r, err := kafka.NewRecord(avro.ValidatorOffsetCodec, kafka.ValidatorOffsets, b.Partition, topic, position)
	if err != nil {
		return err
	}
	return b.commit(ctx, results, r)
}

// creditRecord sends credits to the receiver's bank: through credits in the
// Concurrent model, back through trades in the Sequential model.
func (v *validator) creditRecord(c credit) (*kgo.Record, error) {
	if v.cfg.Model == config.Sequential {
		return kafka.NewTradesRecord(c.block)
	}
	return kafka.NewRecord(avro.BlockCodec, kafka.Credits, c.partition, c.block.Trades[0].Inbank, c.block)
}

func (v *validator) count(out outcome) {
	v.stats.accepted.Add(int64(out.accepted))
	v.stats.rejected.Add(int64(out.rejected))
	v.stats.credited.Add(int64(out.credited))
}

// logStats logs what the validator did since the previous line, every
// statsIntervalMs, until ctx ends. It runs on its own timer and doesn't
// take the lock, so a long takeover or transaction doesn't delay it.
func (v *validator) logStats(ctx context.Context) {
	ticker := time.NewTicker(v.cfg.StatsInterval)
	defer ticker.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now() // when the counts are taken
			s := &v.stats
			accepted, rejected := s.accepted.Swap(0), s.rejected.Swap(0)
			v.log.Info(fmt.Sprintf("last %.1f s: validated %d blocks with %d trades (%d accepted, %d rejected), applied %d credits; owns [%s]",
				now.Sub(last).Seconds(), s.blocks.Swap(0), accepted+rejected, accepted, rejected, s.credited.Swap(0),
				strings.Join(*s.owned.Load(), ", ")))
			last = now
		}
	}
}

// batch is the records one poll returned for one partition, decoded.
type batch struct {
	topic     string
	partition int32
	blocks    []avro.Block
	first     int64 // offset of the first record
	next      int64 // offset after the last record
}

func decode(fetches kgo.Fetches) ([]batch, error) {
	var batches []batch
	var errs []error
	fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
		if len(fp.Records) == 0 {
			return
		}
		b := batch{topic: fp.Topic, partition: fp.Partition, first: fp.Records[0].Offset, next: fp.Records[len(fp.Records)-1].Offset + 1}
		for _, r := range fp.Records {
			block, err := kafka.Decode(avro.BlockCodec, r)
			if err != nil {
				errs = append(errs, err)
				return
			}
			b.blocks = append(b.blocks, block)
		}
		batches = append(batches, b)
	})
	return batches, errors.Join(errs...)
}
