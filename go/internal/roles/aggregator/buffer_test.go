package aggregator

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func send(serial int64, from int32) avro.Trade {
	return avro.Trade{SerialNumber: serial, Outbank: bankID(from), OutbankPartition: from, Inbank: "bank9", InbankPartition: 9, Category: avro.Send}
}

func credit(serial int64, to int32) avro.Trade {
	return avro.Trade{SerialNumber: serial, Outbank: "bank9", OutbankPartition: 9, Inbank: bankID(to), InbankPartition: to, Category: avro.Receive}
}

func bankID(p int32) string { return "bank" + string(rune('0'+p)) }

func mustAdd(t *testing.T, b *buffer, now time.Time, trades ...avro.Trade) {
	t.Helper()
	for _, tr := range trades {
		if err := b.add(tr, now); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTradesGoToTheirBank(t *testing.T) {
	b := newBuffer(10, time.Second)
	// A new trade goes to its sender's bank, a credit to its receiver's.
	mustAdd(t, b, t0, send(1, 0), credit(2, 0), send(3, 2), credit(4, 1))
	blocks := b.blocks()
	want := []struct {
		partition int32
		key       string
		serials   []int64
	}{{0, "bank0", []int64{1, 2}}, {1, "bank1", []int64{4}}, {2, "bank2", []int64{3}}}
	if len(blocks) != len(want) {
		t.Fatalf("got %d blocks, want %d", len(blocks), len(want))
	}
	for i, w := range want {
		got := blocks[i]
		if got.partition != w.partition || got.key != w.key || len(got.block.Trades) != len(w.serials) {
			t.Fatalf("block %d: %+v, want %+v", i, got, w)
		}
		for j, s := range w.serials {
			if got.block.Trades[j].SerialNumber != s {
				t.Errorf("block %d trade %d: serial %d, want %d", i, j, got.block.Trades[j].SerialNumber, s)
			}
		}
	}
}

func TestRejectedTradesDontBelongOnTrades(t *testing.T) {
	tr := send(1, 0)
	tr.Category = avro.Rejected
	if err := newBuffer(10, time.Second).add(tr, t0); err == nil {
		t.Error("buffered a REJECTED trade")
	}
}

func TestDueWhenABlockIsFull(t *testing.T) {
	b := newBuffer(3, time.Hour)
	mustAdd(t, b, t0, send(1, 0), send(2, 0), send(3, 1), send(4, 1))
	if b.due(t0) {
		t.Fatal("due before any bank has blockSize trades")
	}
	mustAdd(t, b, t0, send(5, 1))
	if !b.due(t0) {
		t.Fatal("not due with a full block")
	}
}

func TestDueAfterTheTimeout(t *testing.T) {
	b := newBuffer(100, time.Second)
	if b.due(t0.Add(time.Hour)) {
		t.Fatal("an empty buffer is never due")
	}
	mustAdd(t, b, t0, send(1, 0))
	mustAdd(t, b, t0.Add(900*time.Millisecond), send(2, 1)) // the window started with the first trade
	if b.due(t0.Add(999 * time.Millisecond)) {
		t.Fatal("due before the timeout")
	}
	if !b.due(t0.Add(time.Second)) {
		t.Fatal("not due at the timeout")
	}
	b.clear()
	if b.due(t0.Add(time.Hour)) || len(b.blocks()) != 0 {
		t.Fatal("clear left trades behind")
	}
	mustAdd(t, b, t0.Add(2*time.Second), send(3, 0))
	if b.due(t0.Add(2500 * time.Millisecond)) {
		t.Fatal("the window must restart with the first trade after clear")
	}
}

func TestPollTimeout(t *testing.T) {
	b := newBuffer(100, time.Second)
	if got := b.pollTimeout(t0, 100*time.Millisecond); got != 100*time.Millisecond {
		t.Errorf("empty buffer: %v, want the limit", got)
	}
	mustAdd(t, b, t0, send(1, 0))
	for now, want := range map[time.Time]time.Duration{
		t0:                             100 * time.Millisecond,
		t0.Add(950 * time.Millisecond): 50 * time.Millisecond,
		t0.Add(2 * time.Second):        0,
	} {
		if got := b.pollTimeout(now, 100*time.Millisecond); got != want {
			t.Errorf("at %v: %v, want %v", now.Sub(t0), got, want)
		}
	}
}

func TestBlocksAreSplitAtBlockSize(t *testing.T) {
	b := newBuffer(2, time.Second)
	mustAdd(t, b, t0, send(1, 0), send(2, 0), send(3, 0), send(4, 0), send(5, 0))
	var sizes []int
	var serials []int64
	for _, blk := range b.blocks() {
		sizes = append(sizes, len(blk.block.Trades))
		for _, tr := range blk.block.Trades {
			serials = append(serials, tr.SerialNumber)
		}
	}
	if len(sizes) != 3 || sizes[0] != 2 || sizes[1] != 2 || sizes[2] != 1 {
		t.Errorf("block sizes %v, want [2 2 1]", sizes)
	}
	for i, s := range serials {
		if s != int64(i+1) {
			t.Fatalf("trades out of order: %v", serials)
		}
	}
}

// largest sets timestamp2 to a value whose varint takes 10 bytes, as
// tradeBytes assumes.
func largest(t avro.Trade) avro.Trade {
	t.Timestamp2 = math.MaxInt64
	return t
}

func TestTradeBytesIsTheEncodedSize(t *testing.T) {
	typical := avro.Trade{
		SerialNumber: 1234567890<<32 | 42, Outbank: "bank0", OutAccount: "bank0-0042", Inbank: "bank2", InAccount: "bank2-0007",
		OutbankPartition: 0, InbankPartition: 2, Amount: 99, Category: avro.Send, Timestamp1: 1730700000123,
	}
	wide := avro.Trade{
		SerialNumber: math.MinInt64, Outbank: "bank12345", OutAccount: "bank12345-123456", Inbank: "bänk", InAccount: strings.Repeat("ü", 200),
		OutbankPartition: math.MaxInt32, InbankPartition: math.MinInt32, Amount: math.MaxInt64, Category: avro.Rejected, Timestamp1: -1,
	}
	empty := avro.Trade{Category: avro.Receive}
	for name, tr := range map[string]avro.Trade{"typical": typical, "wide": wide, "empty": empty} {
		encoded, err := avro.BlockCodec.Encode(avro.Block{Trades: []avro.Trade{largest(tr)}})
		if err != nil {
			t.Fatal(err)
		}
		// One trade: the header (10 bytes), the item count (1), the trade and
		// the end of the array (1).
		if got, want := tradeBytes(tr), len(encoded)-12; got != want {
			t.Errorf("%s: tradeBytes %d, encoded %d", name, got, want)
		}
	}
}

func TestBlocksStayUnderTheByteLimit(t *testing.T) {
	b := newBuffer(100000, time.Second)
	const n = 30000 // about three blocks' worth
	for i := range n {
		mustAdd(t, b, t0, avro.Trade{
			SerialNumber: 1730700000<<32 | int64(i), Outbank: "bank0", OutAccount: fmt.Sprintf("bank0-%04d", i%10000), Inbank: "bank1",
			InAccount: "bank1-0001", OutbankPartition: 0, InbankPartition: 1, Amount: int64(i%100 + 1), Category: avro.Send, Timestamp1: 1730700000123,
		})
	}
	blocks := b.blocks()
	if len(blocks) < 3 {
		t.Fatalf("%d blocks: the byte limit didn't split them", len(blocks))
	}
	var total int
	for i, blk := range blocks {
		trades := blk.block.Trades
		total += len(trades)
		var size int
		for _, tr := range trades {
			size += tradeBytes(tr)
		}
		if size+blockOverhead > maxBlockBytes {
			t.Errorf("block %d: %d bytes, more than %d", i, size+blockOverhead, maxBlockBytes)
		}
		if i < len(blocks)-1 {
			// The next trade would not have fitted.
			if next := blocks[i+1].block.Trades[0]; size+blockOverhead+tradeBytes(next) <= maxBlockBytes {
				t.Errorf("block %d ends early: %d bytes, and the next trade has %d", i, size+blockOverhead, tradeBytes(next))
			}
		}
		// The orders record a validator writes for this block, with
		// timestamp2 set on every trade, is no bigger.
		stamped := make([]avro.Trade, len(trades))
		for j, tr := range trades {
			stamped[j] = largest(tr)
		}
		encoded, err := avro.BlockCodec.Encode(avro.Block{Trades: stamped})
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > size+blockOverhead {
			t.Errorf("block %d encodes to %d bytes, more than its computed %d", i, len(encoded), size+blockOverhead)
		}
	}
	if total != n {
		t.Errorf("%d trades in the blocks, want %d", total, n)
	}
}

func TestATradeAlwaysFitsInABlock(t *testing.T) {
	b := newBuffer(10, time.Second)
	huge := send(1, 0)
	huge.OutAccount = strings.Repeat("x", maxBlockBytes)
	mustAdd(t, b, t0, send(0, 0), huge, send(2, 0))
	var sizes []int
	for _, blk := range b.blocks() {
		sizes = append(sizes, len(blk.block.Trades))
	}
	if !slices.Equal(sizes, []int{1, 1, 1}) {
		t.Errorf("block sizes %v, want [1 1 1]: a trade too big for the limit goes alone", sizes)
	}
}
