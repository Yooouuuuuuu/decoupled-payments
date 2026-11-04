package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

// End is where reading a partition "to its end" stops.
type End int

const (
	// StableEnd is a partition's read_committed end, its last stable offset:
	// every transaction that wrote before it is complete.
	StableEnd End = iota
	// LogEnd is a partition's high watermark: everything written so far,
	// including transactions still open. Reading waits until they end.
	LogEnd
)

// TopicPartition names a partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string {
	return fmt.Sprintf("%s partition %d", tp.Topic, tp.Partition)
}

// noProgressLimit is how long reading to the end waits for a partition to
// advance before it gives up. A transaction that was open when the read
// started ends within the transaction timeout, when the broker aborts it;
// the extra minute covers the broker's own delays. A read that keeps
// advancing may take as long as it needs.
func noProgressLimit(cfg config.Config) time.Duration {
	return cfg.Kafka.TransactionTimeout + time.Minute
}

// ReadToEnd reads partitions from their start up to the end offset they have
// when it is called, with read_committed isolation, and passes every data
// record before that offset to handle, in order within each partition. A
// topic with a nil list of partitions is read in all its partitions.
//
// Transaction markers and aborted records occupy offsets but aren't returned
// to a read_committed consumer, so the last record returned can be any
// distance before the end offset: comparing record offsets with end-1, or
// with a guessed margin, doesn't work. franz-go has no position getter for a
// direct consumer; instead it returns the markers too (KeepControlRecords),
// and a partition is done once any record, data or marker, at offset end-1
// or later arrives. That record always comes: the last offset holds data, or
// a marker, or aborted data whose marker is written later.
func ReadToEnd(ctx context.Context, cfg config.Config, log *slog.Logger, clientID string,
	partitions map[string][]int32, end End, handle func(*kgo.Record) error,
) error {
	opts := append(ClientOpts(cfg, log, clientID), ConsumerOpts(cfg)...)
	// The client's context interrupts even a request that is still opening
	// its connection, which the request's own context doesn't: a stop
	// signal ends the read at once, also when a broker doesn't answer.
	cl, err := kgo.NewClient(append(opts, kgo.KeepControlRecords(), kgo.WithContext(ctx))...)
	if err != nil {
		return err
	}
	defer cl.Close()

	ends, err := endOffsets(ctx, cl, log, partitions, end)
	if err != nil {
		return err
	}
	consume := map[string]map[int32]kgo.Offset{}
	for tp := range ends {
		if consume[tp.Topic] == nil {
			consume[tp.Topic] = map[int32]kgo.Offset{}
		}
		consume[tp.Topic][tp.Partition] = kgo.NewOffset().AtStart()
	}
	cl.AddConsumePartitions(consume)

	return readOn(ctx, cl, ends, noProgressLimit(cfg), func(r *kgo.Record) error {
		if r.Attrs.IsControl() || r.Offset >= ends[TopicPartition{r.Topic, r.Partition}] {
			return nil
		}
		return handle(r)
	})
}

// readOn polls cl until every partition in ends has returned a record, data
// or transaction marker, at its end offset - 1 or later. Every record polled
// goes to handle. It gives up when none of these partitions advanced for
// limit.
func readOn(ctx context.Context, cl *kgo.Client, ends map[TopicPartition]int64, limit time.Duration, handle func(*kgo.Record) error) error {
	remaining := maps.Clone(ends)
	deadline := time.Now().Add(limit)
	for len(remaining) > 0 {
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := FetchErrors(fetches); err != nil {
			return err
		}
		advanced := false
		for it := fetches.RecordIter(); !it.Done(); {
			r := it.Next()
			if err := handle(r); err != nil {
				return err
			}
			tp := TopicPartition{r.Topic, r.Partition}
			if end, reading := remaining[tp]; reading {
				advanced = true
				if r.Offset >= end-1 {
					delete(remaining, tp)
				}
			}
		}
		if advanced {
			deadline = time.Now().Add(limit)
		} else if !time.Now().Before(deadline) {
			return fmt.Errorf("no progress for %v reading %s to its end: a transaction open that long, or a partition without a leader?",
				limit, joined(slices.Collect(maps.Keys(remaining))))
		}
	}
	return nil
}

// endOffsets returns the end offset of each of the partitions that holds
// records, i.e. whose end is after its start. A topic with a nil list of
// partitions means all its partitions. While no broker can be reached, it
// tries again for a while (see retryUnreachable).
func endOffsets(ctx context.Context, cl *kgo.Client, log *slog.Logger, partitions map[string][]int32, end End) (map[TopicPartition]int64, error) {
	var out map[TopicPartition]int64
	err := retryUnreachable(ctx, log, func() error {
		tps, err := listPartitions(ctx, cl, partitions)
		if err != nil {
			return err
		}
		starts, err := listOffsets(ctx, cl, tps, startOffset, readUncommitted)
		if err != nil {
			return fmt.Errorf("list the start offsets of %s: %w", joined(tps), err)
		}
		isolation := readCommitted // the last stable offset
		if end == LogEnd {
			isolation = readUncommitted // the high watermark
		}
		ends, err := listOffsets(ctx, cl, tps, endOffset, isolation)
		if err != nil {
			return fmt.Errorf("list the end offsets of %s: %w", joined(tps), err)
		}
		out = map[TopicPartition]int64{}
		for _, tp := range tps {
			if ends[tp] > starts[tp] {
				out[tp] = ends[tp]
			}
		}
		return nil
	})
	return out, err
}

// listPartitions names every partition to read: those listed, or all
// partitions of a topic listed as nil.
func listPartitions(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) ([]TopicPartition, error) {
	var tps []TopicPartition
	for _, topic := range slices.Sorted(maps.Keys(partitions)) {
		ps := partitions[topic]
		if ps == nil {
			details, err := kadm.NewClient(cl).ListTopics(ctx, topic)
			if err == nil {
				err = details[topic].Err
			}
			if err != nil {
				return nil, fmt.Errorf("list the partitions of %s: %w", topic, err)
			}
			ps = details[topic].Partitions.Numbers()
		}
		for _, p := range ps {
			tps = append(tps, TopicPartition{topic, p})
		}
	}
	return tps, nil
}

// ListOffsets arguments: which offset, and how to read the end.
const (
	startOffset, endOffset         int64 = -2, -1
	readUncommitted, readCommitted int8  = 0, 1
)

// listOffsets lists one offset of each of the partitions. It asks for these
// partitions only, so that a partition without a leader elsewhere in the
// topic (a broker is down) doesn't fail a validator taking over another bank.
func listOffsets(ctx context.Context, cl *kgo.Client, tps []TopicPartition, which int64, isolation int8) (map[TopicPartition]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = isolation
	topics := map[string]int{} // index in req.Topics
	for _, tp := range tps {
		i, ok := topics[tp.Topic]
		if !ok {
			i = len(req.Topics)
			topics[tp.Topic] = i
			t := kmsg.NewListOffsetsRequestTopic()
			t.Topic = tp.Topic
			req.Topics = append(req.Topics, t)
		}
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition, p.Timestamp = tp.Partition, which
		req.Topics[i].Partitions = append(req.Topics[i].Partitions, p)
	}
	offsets := map[TopicPartition]int64{}
	for _, shard := range cl.RequestSharded(ctx, req) {
		if shard.Err != nil {
			return nil, shard.Err
		}
		for _, t := range shard.Resp.(*kmsg.ListOffsetsResponse).Topics {
			for _, p := range t.Partitions {
				tp := TopicPartition{t.Topic, p.Partition}
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					return nil, fmt.Errorf("%s: %w", tp, err)
				}
				offsets[tp] = p.Offset
			}
		}
	}
	for _, tp := range tps {
		if _, ok := offsets[tp]; !ok {
			return nil, fmt.Errorf("%s: no offset listed", tp)
		}
	}
	return offsets, nil
}

// FetchErrors returns the errors of a poll, leaving out the ones that just
// mean the poll was cut short (its context ended, or the client closed).
func FetchErrors(fetches kgo.Fetches) error {
	var errs []error
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kgo.ErrClientClosed) {
			return
		}
		if topic == "" { // not about a partition, e.g. a group error
			errs = append(errs, err)
			return
		}
		errs = append(errs, fmt.Errorf("fetch %s partition %d: %w", topic, partition, err))
	})
	return errors.Join(errs...)
}

func joined[T fmt.Stringer](items []T) string {
	texts := make([]string, len(items))
	for i, item := range items {
		texts[i] = item.String()
	}
	slices.Sort(texts)
	return strings.Join(texts, ", ")
}
