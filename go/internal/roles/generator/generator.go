// Package generator implements the trade generator: it sends trades between
// random accounts to the trades topic at a fixed rate.
package generator

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

const role = "generator"

// stopGrace is how long the trades already handed to the producer may take
// to be delivered after a stop signal, like the Java shutdown hook's wait.
const stopGrace = 30 * time.Second

// Run sends generator.trades trades (0 = until stopped) at generator.rps.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	dir, err := kafka.ReadDirectory(ctx, cfg, log, kafka.ClientID(role, cfg.SiteID, "accountInfo"))
	if err != nil {
		return err
	}
	s, err := newSampler(dir, cfg.Generator)
	if err != nil {
		return err
	}
	runID, err := newRunID()
	if err != nil {
		return err
	}
	cl, err := kgo.NewClient(append(kafka.ClientOpts(cfg, log, kafka.ClientID(role, cfg.SiteID, "producer")), kafka.ProducerOpts()...)...)
	if err != nil {
		return err
	}
	defer cl.Close()

	// A trade handed to the producer is still delivered after a stop
	// signal, so the records don't get the signal's context. The producer
	// blocks a produce call while it has the most records it may buffer,
	// and a stop signal couldn't end that wait: the loop only produces while
	// there is room. A record that can't be delivered fails after the
	// delivery timeout (see kafka.ProducerOpts).
	sendCtx := context.WithoutCancel(ctx)
	room := cl.OptValue(kgo.MaxBufferedRecords).(int64)
	var sends kafka.SendErrors
	limit, rps := cfg.Generator.Trades, cfg.Generator.RPS
	if limit == 0 {
		// Until stopped, but a run ends after 2^32-1 trades anyway: its
		// sequence numbers have 32 bits.
		limit = math.MaxUint32
	}
	pace := time.NewTicker(time.Millisecond)
	defer pace.Stop()
	stats := time.NewTicker(cfg.StatsInterval)
	defer stats.Stop()

	start := time.Now()
	var sent int64
send:
	for sent < limit {
		// Trade i is due at start + i/rps: send everything due by now.
		due := min(int64(time.Since(start).Seconds()*float64(rps))+1, limit)
		for ; sent < due && cl.BufferedProduceRecords() < room; sent++ {
			r, err := kafka.NewTradesRecord(avro.Block{Trades: []avro.Trade{s.trade(runID, sent+1)}})
			if err != nil {
				return err
			}
			cl.Produce(sendCtx, r, sends.Promise)
		}
		if err := sends.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			break send
		case <-stats.C:
			log.Info(fmt.Sprintf("sent %d trades (%.0f/s)", sent, rate(sent, time.Since(start))))
		case <-pace.C:
		}
	}

	if err := flush(ctx, cl); err != nil {
		return err
	}
	if err := sends.Err(); err != nil {
		return err
	}
	elapsed := time.Since(start)
	log.Info(fmt.Sprintf("sent %d trades in %.1f s (%.0f/s), runId %d", sent, elapsed.Seconds(), rate(sent, elapsed), runID))
	return nil
}

// flush waits until every trade handed to the producer is delivered (or has
// failed, after the delivery timeout). After a stop signal, it waits at most
// stopGrace more.
func flush(ctx context.Context, cl *kgo.Client) error {
	flushCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	flushed := make(chan error, 1)
	go func() { flushed <- cl.Flush(flushCtx) }()
	select {
	case err := <-flushed:
		return err
	case <-ctx.Done():
	}
	select {
	case err := <-flushed:
		return err
	case <-time.After(stopGrace):
		unsent := cl.BufferedProduceRecords()
		cancel()
		<-flushed
		return fmt.Errorf("%d trades were still not delivered %v after the stop signal", unsent, stopGrace)
	}
}

// trade makes the trade with sequence number seq (from 1). Its serial number
// is runID * 2^32 + seq: unique across runs without coordination.
func (s *sampler) trade(runID, seq int64) avro.Trade {
	from, to := s.pair()
	return avro.Trade{
		SerialNumber:     runID<<32 | seq,
		Outbank:          from.bank,
		OutAccount:       from.id,
		Inbank:           to.bank,
		InAccount:        to.id,
		OutbankPartition: from.partition,
		InbankPartition:  to.partition,
		Amount:           s.amount(),
		Category:         avro.Send,
		Timestamp1:       time.Now().UnixMilli(),
	}
}

// newRunID is a random 31-bit number from a secure source: generator.seed
// makes the trades repeatable, but every run needs its own serial numbers.
func newRunID() (int64, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint32(b[:]) >> 1), nil
}

func rate(n int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}
