// Package aggregator implements layer 1: it consumes trades in any order,
// groups them by bank, and emits a block to the bank's partition of blocks
// when the block is full or after a timeout.
//
// Exactly once: a trades offset may only be committed once every trade before
// it has been emitted. Offsets are per partition of trades but the buffers
// are per bank, so the aggregator emits all its pending blocks together, in
// one Kafka transaction with the offsets of everything it consumed.
package aggregator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

const (
	role    = "aggregator"
	maxPoll = 100 * time.Millisecond // the longest a poll waits for trades
	// fenceLimit bounds fencing the previous process with this siteId, like
	// Java's max.block.ms for initTransactions.
	fenceLimit = 60 * time.Second
)

// emitter sends blocks and commits the offsets of every trade consumed so far.
type emitter interface {
	// emit returns false when it was aborted without an error: nothing was
	// emitted or committed (a rebalance, or the offsets couldn't be
	// committed), and the trades are consumed again from the committed
	// offsets, by this aggregator or by the partitions' new owner.
	emit(ctx context.Context, blocks []pendingBlock) (emitted bool, err error)
	// rewind sets every partition the aggregator still reads back to its
	// committed offset.
	rewind(ctx context.Context) error
}

type aggregator struct {
	cfg     config.Config
	log     *slog.Logger
	id      string      // aggregator-<siteId>: group instance ID and transactional.id
	cl      *kgo.Client // the group consumer; with Kafka transactions also the producer
	emitter emitter
	buf     *buffer

	// revoked is raised by the rebalance callbacks when the group takes
	// partitions away, and handled by the loop (see step).
	revoked atomic.Bool
	reading map[int32]bool // the partitions of trades assigned, for the log; only the callbacks use it

	blocks, trades atomic.Int64 // emitted since the last stats line
}

// Run consumes trades and emits blocks until ctx ends or an error occurs.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	dir, err := kafka.ReadDirectory(ctx, cfg, log, kafka.ClientID(role, cfg.SiteID, "accountInfo"))
	if err != nil {
		return err
	}
	if err := kafka.CheckModel(dir, cfg); err != nil {
		return err
	}
	a, err := newAggregator(ctx, cfg, log)
	if err != nil {
		return err
	}

	statsCtx, stopStats := context.WithCancel(ctx)
	var stats sync.WaitGroup
	stats.Go(func() { a.logStats(statsCtx) })
	err = a.run(ctx)
	stopStats()
	stats.Wait()
	if err == nil {
		a.leaveGroup(ctx)
	}
	a.cl.CloseAllowingRebalance()
	return err
}

// newAggregator creates the aggregator's client, which joins the group.
func newAggregator(ctx context.Context, cfg config.Config, log *slog.Logger) (*aggregator, error) {
	a := &aggregator{cfg: cfg, log: log, id: fmt.Sprintf("%s-%d", role, cfg.SiteID), reading: map[int32]bool{},
		buf: newBuffer(cfg.Aggregator.BlockSize, cfg.Aggregator.BlockTimeout)}
	// One client consumes and produces (a GroupTransactSession is one
	// client), so the client ID names what it reads, like Java's consumer.
	opts := append(kafka.ClientOpts(cfg, log, kafka.ClientID(role, cfg.SiteID, "trades")), kafka.ConsumerOpts(cfg)...)
	opts = append(opts, kafka.GroupOpts(cfg, kafka.AggregatorsGroup, a.id)...)
	opts = append(opts, kafka.ProducerOpts()...)
	opts = append(opts,
		kgo.ConsumeTopics(kafka.Trades),
		kgo.OnPartitionsAssigned(a.onAssigned),
		kgo.OnPartitionsRevoked(a.onRevoked),
		kgo.OnPartitionsLost(a.onRevoked),
	)
	if !cfg.KafkaTransactions {
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, err
		}
		a.cl, a.emitter = cl, plain{cl: cl, log: log}
		return a, nil
	}
	session, err := kgo.NewGroupTransactSession(append(opts, kafka.TransactionalOpts(cfg, a.id)...)...)
	if err != nil {
		return nil, err
	}
	a.cl, a.emitter = session.Client(), transactional{session}
	if err := fence(ctx, a.cl); err != nil {
		a.cl.Close()
		return nil, err
	}
	return a, nil
}

// fence initializes the producer ID of transactional.id aggregator-<siteId>
// before the first poll, like Java's initTransactions. That fences an
// earlier process with this siteId, even one that is only paused, and
// aborts its unfinished transaction. Until then, the offsets that
// transaction would commit stay pending, and no aggregator can start
// reading those partitions of trades.
func fence(ctx context.Context, cl *kgo.Client) error {
	ctx, cancel := context.WithTimeout(ctx, fenceLimit)
	defer cancel()
	if _, _, err := cl.ProducerID(ctx); err != nil {
		return fmt.Errorf("fence the previous process with this siteId: %w", err)
	}
	return nil
}

// leaveGroup leaves the consumer group after a clean stop, so that the
// partitions move to the other aggregators at once.
func (a *aggregator) leaveGroup(ctx context.Context) {
	// The group drops the member after the session timeout anyway.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.Kafka.SessionTimeout)
	defer cancel()
	if err := kafka.LeaveGroup(ctx, a.cl, kafka.AggregatorsGroup, a.id); err != nil {
		a.log.Warn("could not leave the consumer group; it drops this aggregator after kafka.sessionTimeoutMs", "err", err)
	}
}

func (a *aggregator) run(ctx context.Context) error {
	for ctx.Err() == nil {
		err := a.step(ctx)
		// Rebalances wait while a poll's records are being processed
		// (BlockRebalanceOnPoll); they may run now, between steps.
		a.cl.AllowRebalance()
		if err != nil {
			return err
		}
	}
	return nil
}

// step polls once, buffers the trades, and emits the blocks if they are due.
func (a *aggregator) step(ctx context.Context) error {
	pollCtx, cancel := context.WithTimeout(ctx, a.buf.pollTimeout(time.Now(), maxPoll))
	fetches := a.cl.PollRecords(pollCtx, a.cfg.Aggregator.MaxPollRecords)
	cancel()
	if err := kafka.FetchErrors(fetches); err != nil {
		if refused := kafka.GroupRefused(err, a.cfg.SiteID); refused != nil {
			return refused
		}
		a.log.Warn("poll trades", "err", err) // franz-go rejoins and retries
	}
	if a.revoked.Swap(false) {
		// The group took partitions of trades away. Their buffered trades
		// now belong to their next owner, which reads them again from the
		// committed offsets. The buffer doesn't know which partition each
		// trade came from, so it drops all of them, and the partitions this
		// aggregator keeps go back to their committed offsets too: it reads
		// their trades again, including those of this poll.
		a.buf.clear()
		ctx, cancel := a.unstoppable(ctx)
		defer cancel()
		return a.emitter.rewind(ctx)
	}

	now := time.Now()
	for it := fetches.RecordIter(); !it.Done(); {
		block, err := kafka.Decode(avro.BlockCodec, it.Next())
		if err != nil {
			return err
		}
		for _, t := range block.Trades {
			if err := a.buf.add(t, now); err != nil {
				return err
			}
		}
	}
	if !a.buf.due(now) {
		return nil
	}
	return a.emit(ctx)
}

// unstoppable returns the context of work that, once started, runs to the
// end even if the aggregator is stopping meanwhile: an emit or a rewind.
// Ending a transaction with a canceled context would leave its outcome
// unknown. The transaction timeout bounds the work.
func (a *aggregator) unstoppable(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), a.cfg.Kafka.TransactionTimeout)
}

// emit emits every buffered trade, or none.
func (a *aggregator) emit(ctx context.Context) error {
	blocks := a.buf.blocks()
	a.buf.clear()
	ctx, cancel := a.unstoppable(ctx)
	defer cancel()
	emitted, err := a.emitter.emit(ctx, blocks)
	switch {
	case err != nil:
		return err
	case !emitted && a.revoked.Load():
		a.log.Info("the group took partitions away while the blocks were being emitted, so the emit was aborted: the trades are read again from the committed offsets",
			"blocks", len(blocks))
	case !emitted:
		a.log.Warn("the emit was aborted because its offsets couldn't be committed (the group began rebalancing, or a coordinator was unavailable): the trades are read again from the committed offsets",
			"blocks", len(blocks))
	default:
		a.blocks.Add(int64(len(blocks)))
		for _, b := range blocks {
			a.trades.Add(int64(len(b.block.Trades)))
		}
	}
	return nil
}

// The rebalance callbacks run on franz-go's goroutine: between steps
// (BlockRebalanceOnPoll), or while a transactional emit ends (see
// transactional.emit). They only note what happened; step acts on it.

func (a *aggregator) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	for _, p := range assigned[kafka.Trades] {
		a.reading[p] = true
	}
	a.log.Info("reading trades partitions " + list(slices.Sorted(maps.Keys(a.reading))))
}

// onRevoked handles revoked and lost partitions. An empty revocation takes
// nothing away: the cooperative protocol ends every rebalance with one, and
// an aggregator that reads no partition of trades gets one. (The session
// still notes an empty revocation of the eager protocol, and aborts the
// first emit after it once; its trades are read again.)
func (a *aggregator) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	if len(revoked[kafka.Trades]) == 0 {
		return
	}
	for _, p := range revoked[kafka.Trades] {
		delete(a.reading, p)
	}
	a.revoked.Store(true)
}

// logStats logs what was emitted since the previous line, every
// statsIntervalMs, until ctx ends.
func (a *aggregator) logStats(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.StatsInterval)
	defer ticker.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now() // when the counts are taken
			a.log.Info(fmt.Sprintf("last %.1f s: emitted %d blocks with %d trades", now.Sub(last).Seconds(), a.blocks.Swap(0), a.trades.Swap(0)))
			last = now
		}
	}
}

// transactional emits the blocks and commits the consumed offsets in one
// Kafka transaction, through franz-go's GroupTransactSession.
type transactional struct{ session *kgo.GroupTransactSession }

func (t transactional) emit(ctx context.Context, blocks []pendingBlock) (bool, error) {
	if err := t.session.Begin(); err != nil {
		return false, err
	}
	var sends kafka.SendErrors
	err := produce(blocks, func(r *kgo.Record) { t.session.Produce(ctx, r, sends.Promise) })
	if err == nil {
		err = t.session.Client().Flush(ctx)
	}
	if err == nil {
		err = sends.Err()
	}
	if err != nil {
		// A block that failed to send doesn't fail the transaction:
		// committing it would commit the offsets of trades whose block is
		// missing. Abort, then exit; the restart reads those trades again.
		_, _ = t.session.End(ctx, kgo.TryAbort)
		return false, err
	}
	// End commits only if the group took no partitions away from this
	// member since the poll, and it learns of that from the rebalance
	// callbacks, which wait while the poll's records are being processed.
	// Let them run now: if this member was dropped from the group (it was
	// paused for longer than the session timeout), End would otherwise wait
	// until ctx expires for a heartbeat that never comes, while the open
	// transaction holds back the validators reading these blocks.
	t.session.AllowRebalance()
	// End commits, within the transaction, the offsets of everything
	// polled so far, which is all in these blocks, and then the
	// transaction. If the group took partitions away, or the offsets can't
	// be committed, it aborts instead and rewinds the consumer to the
	// committed offsets.
	return t.session.End(ctx, kgo.TryCommit)
}

// rewind ends an empty transaction with an abort. Besides rewinding the
// consumer to the committed offsets, that clears the session's note of the
// rebalance; otherwise the session would abort the next emit too, although
// its trades were all read after the rebalance.
func (t transactional) rewind(ctx context.Context) error {
	if err := t.session.Begin(); err != nil {
		return err
	}
	_, err := t.session.End(ctx, kgo.TryAbort)
	return err
}

// plain emits without Kafka transactions: blocks first, offsets after, so a
// crash in between emits some blocks twice but never loses a trade.
type plain struct {
	cl  *kgo.Client
	log *slog.Logger
}

func (p plain) emit(ctx context.Context, blocks []pendingBlock) (bool, error) {
	var sends kafka.SendErrors
	err := produce(blocks, func(r *kgo.Record) { p.cl.Produce(ctx, r, sends.Promise) })
	if err == nil {
		err = p.cl.Flush(ctx)
	}
	if err == nil {
		err = sends.Err()
	}
	if err != nil {
		return false, err
	}
	err = p.cl.CommitUncommittedOffsets(ctx)
	if errors.Is(err, kerr.RebalanceInProgress) || errors.Is(err, kerr.IllegalGeneration) || errors.Is(err, kerr.UnknownMemberID) {
		// The blocks are out, so they count as emitted.
		p.log.Warn("a rebalance interrupted the offset commit: the next owner of these partitions reads them again, so some blocks are emitted twice", "err", err)
		return true, nil
	}
	return err == nil, err
}

// rewind runs while this aggregator holds the poll's rebalance gate, so no
// revocation can run concurrently, as SetOffsets requires.
func (p plain) rewind(context.Context) error {
	p.cl.SetOffsets(p.cl.CommittedOffsets())
	return nil
}

// produce encodes the blocks and hands them to send.
func produce(blocks []pendingBlock, send func(*kgo.Record)) error {
	for _, b := range blocks {
		r, err := kafka.NewRecord(avro.BlockCodec, kafka.Blocks, b.partition, b.key, b.block)
		if err != nil {
			return err
		}
		send(r)
	}
	return nil
}

// list formats partition numbers as [0, 2].
func list(partitions []int32) string {
	texts := make([]string, len(partitions))
	for i, p := range partitions {
		texts[i] = fmt.Sprint(p)
	}
	return "[" + strings.Join(texts, ", ") + "]"
}
