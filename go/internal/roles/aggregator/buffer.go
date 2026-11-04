package aggregator

import (
	"fmt"
	"maps"
	"math/bits"
	"slices"
	"time"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
)

// buffer holds the trades consumed since the last emit, grouped by the
// partition of blocks they go to, i.e. by bank.
type buffer struct {
	blockSize   int
	timeout     time.Duration
	trades      map[int32][]avro.Trade
	windowStart time.Time // when the oldest trade arrived; zero while empty
}

// pendingBlock is a block ready to be sent to a partition of blocks.
type pendingBlock struct {
	partition int32
	key       string // the bank's ID
	block     avro.Block
}

func newBuffer(blockSize int, timeout time.Duration) *buffer {
	return &buffer{blockSize: blockSize, timeout: timeout, trades: map[int32][]avro.Trade{}}
}

// target is the partition of blocks a trade goes to: its sender's bank for a
// new trade, which is validated there; its receiver's bank for a credit,
// which is applied there (Sequential model, where credits come back through
// trades).
func target(t avro.Trade) (partition int32, bankID string, err error) {
	switch t.Category {
	case avro.Send:
		return t.OutbankPartition, t.Outbank, nil
	case avro.Receive:
		return t.InbankPartition, t.Inbank, nil
	}
	return 0, "", fmt.Errorf("trade %d is %s; the trades topic only carries SEND and RECEIVE trades", t.SerialNumber, t.Category)
}

func (b *buffer) add(t avro.Trade, now time.Time) error {
	p, _, err := target(t)
	if err != nil {
		return err
	}
	if b.windowStart.IsZero() {
		b.windowStart = now
	}
	b.trades[p] = append(b.trades[p], t)
	return nil
}

// due reports whether it's time to emit: a bank has a full block, or the
// oldest trade has waited blockTimeoutMs.
func (b *buffer) due(now time.Time) bool {
	if b.windowStart.IsZero() {
		return false
	}
	if !now.Before(b.windowStart.Add(b.timeout)) {
		return true
	}
	for _, trades := range b.trades {
		if len(trades) >= b.blockSize {
			return true
		}
	}
	return false
}

// pollTimeout is how long to wait for more trades: at most limit, and no
// longer than until the oldest trade's timeout.
func (b *buffer) pollTimeout(now time.Time, limit time.Duration) time.Duration {
	if b.windowStart.IsZero() {
		return limit
	}
	return max(0, min(limit, b.windowStart.Add(b.timeout).Sub(now)))
}

// blocks splits the buffered trades into blocks, per partition in partition
// order, keeping the order in which the trades arrived.
func (b *buffer) blocks() []pendingBlock {
	var out []pendingBlock
	for _, p := range slices.Sorted(maps.Keys(b.trades)) {
		trades := b.trades[p]
		_, bankID, _ := target(trades[0]) // add checked every trade
		for len(trades) > 0 {
			n := blockLen(trades, b.blockSize)
			out = append(out, pendingBlock{partition: p, key: bankID, block: avro.Block{Trades: trades[:n]}})
			trades = trades[n:]
		}
	}
	return out
}

func (b *buffer) clear() {
	clear(b.trades)
	b.windowStart = time.Time{}
}

// A block holds at most blockSize trades and maxBlockBytes of encoded data,
// the same in Java. Every record a validator derives from a block is at
// most as big: orders holds the same trades with timestamp2 set, and the
// credits are subsets. So all of them stay far below Kafka's 1 MiB limit;
// without the byte limit, a block of about 16,500 trades would already make
// its orders record too big to write.
const (
	maxBlockBytes = 512 << 10
	// blockOverhead is the size of a block record besides its trades: the
	// single-object header (10 bytes) and the array's framing (at most 6).
	blockOverhead = 16
)

// blockLen is how many of trades, from the first, go into the next block:
// the next trade starts a new block when adding it would exceed either
// limit. A single trade always fits.
func blockLen(trades []avro.Trade, blockSize int) int {
	size := blockOverhead + tradeBytes(trades[0])
	n := 1
	for n < len(trades) && n < blockSize {
		size += tradeBytes(trades[n])
		if size > maxBlockBytes {
			break
		}
		n++
	}
	return n
}

// tradeBytes is the Avro binary size of a trade with timestamp2 at its
// largest, 10 bytes: the validator sets it later, in the orders record.
func tradeBytes(t avro.Trade) int {
	const category = 1 // an enum index: 1 byte
	const maxLong = 10 // a 64-bit zig-zag varint
	return longBytes(t.SerialNumber) +
		stringBytes(t.Outbank) + stringBytes(t.OutAccount) + stringBytes(t.Inbank) + stringBytes(t.InAccount) +
		longBytes(int64(t.OutbankPartition)) + longBytes(int64(t.InbankPartition)) +
		longBytes(t.Amount) + category + longBytes(t.Timestamp1) + maxLong
}

// longBytes is the size of an Avro long or int: its zig-zag encoding, in
// groups of 7 bits.
func longBytes(v int64) int {
	zigzag := uint64(v<<1) ^ uint64(v>>63)
	return max(1, (bits.Len64(zigzag)+6)/7)
}

// stringBytes is the size of an Avro string: its length in bytes, then the
// UTF-8 bytes.
func stringBytes(s string) int { return longBytes(int64(len(s))) + len(s) }
