package kafka

import (
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
)

// NewRecord builds a record for an explicit partition with a string key.
func NewRecord[T any](codec *avro.Codec[T], topic string, partition int32, key string, value T) (*kgo.Record, error) {
	data, err := codec.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("encode a record for %s partition %d: %w", topic, partition, err)
	}
	return &kgo.Record{Topic: topic, Partition: partition, Key: []byte(key), Value: data}, nil
}

// NewTradesRecord builds a record for the trades topic: no key, and the
// default partitioner picks the partition.
func NewTradesRecord(block avro.Block) (*kgo.Record, error) {
	data, err := avro.BlockCodec.Encode(block)
	if err != nil {
		return nil, fmt.Errorf("encode a record for %s: %w", Trades, err)
	}
	return &kgo.Record{Topic: Trades, Value: data}, nil
}

// Decode decodes a record's value. The error names the record: a value that
// doesn't decode is fatal for every role.
func Decode[T any](codec *avro.Codec[T], r *kgo.Record) (T, error) {
	v, err := codec.Decode(r.Value)
	if err != nil {
		return v, fmt.Errorf("decode the record at %s partition %d offset %d: %w", r.Topic, r.Partition, r.Offset, err)
	}
	return v, nil
}

// SendErrors remembers the first failure of asynchronous produce calls. Pass
// Promise to Produce; after Flush returns, every promise has run.
type SendErrors struct {
	mu  sync.Mutex
	err error
}

// Promise is a produce promise that records a failure.
func (s *SendErrors) Promise(r *kgo.Record, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = fmt.Errorf("send to %s partition %d: %w", r.Topic, r.Partition, err)
	}
}

// Err returns the first failure, or nil.
func (s *SendErrors) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}
