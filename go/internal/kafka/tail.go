package kafka

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

// Tail follows every partition of a topic from its start, with
// read_committed isolation and no consumer group, for as long as its user
// wants, and then reads on to the topic's end. The report follows orders this
// way while it waits for the trades still in flight.
type Tail struct {
	cfg   config.Config
	log   *slog.Logger
	cl    *kgo.Client
	topic string
	last  map[int32]int64 // per partition, the offset of the last record read, data or marker
}

// NewTail starts following topic, until ctx ends or Close. Close it when
// done.
func NewTail(ctx context.Context, cfg config.Config, log *slog.Logger, clientID, topic string) (*Tail, error) {
	opts := append(ClientOpts(cfg, log, clientID), ConsumerOpts(cfg)...)
	cl, err := kgo.NewClient(append(opts, kgo.ConsumeTopics(topic), kgo.KeepControlRecords(), kgo.WithContext(ctx))...)
	if err != nil {
		return nil, err
	}
	return &Tail{cfg: cfg, log: log, cl: cl, topic: topic, last: map[int32]int64{}}, nil
}

// Poll waits until records arrive or ctx ends, and passes the data records
// to handle.
func (t *Tail) Poll(ctx context.Context, handle func(*kgo.Record) error) error {
	fetches := t.cl.PollFetches(ctx)
	if err := FetchErrors(fetches); err != nil {
		return err
	}
	for it := fetches.RecordIter(); !it.Done(); {
		if err := t.take(it.Next(), handle); err != nil {
			return err
		}
	}
	return nil
}

// ReadToEnd reads on until it has read every partition up to the high
// watermark it has now, so that it waits for the transactions open now (see
// ReadToEnd for how the end is detected).
func (t *Tail) ReadToEnd(ctx context.Context, handle func(*kgo.Record) error) error {
	ends, err := endOffsets(ctx, t.cl, t.log, map[string][]int32{t.topic: nil}, LogEnd)
	if err != nil {
		return err
	}
	for tp, end := range ends {
		if last, ok := t.last[tp.Partition]; ok && last >= end-1 {
			delete(ends, tp) // already read to its end
		}
	}
	return readOn(ctx, t.cl, ends, noProgressLimit(t.cfg), func(r *kgo.Record) error { return t.take(r, handle) })
}

func (t *Tail) take(r *kgo.Record, handle func(*kgo.Record) error) error {
	t.last[r.Partition] = r.Offset
	if r.Attrs.IsControl() {
		return nil
	}
	return handle(r)
}

// Close stops following the topic.
func (t *Tail) Close() { t.cl.Close() }
