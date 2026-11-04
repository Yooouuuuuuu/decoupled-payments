// Package report implements the report role. It reads what the system wrote,
// checks that every trade was validated and completed exactly once and that
// no money was created or lost, and measures throughput and latency.
package report

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

// ErrFailed means a check failed. The report has been printed.
var ErrFailed = errors.New("a check failed")

const role = "report"

// Run prints the report to out. It reads with read_committed isolation and
// no consumer group, from the beginning of each topic.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger, out io.Writer) error {
	dir, err := kafka.ReadDirectory(ctx, cfg, log, kafka.ClientID(role, cfg.SiteID, "accountInfo"))
	if err != nil {
		return err
	}
	generated, err := readGenerated(ctx, cfg, log)
	if err != nil {
		return err
	}
	log.Info("read the generated trades", "trades", len(generated))
	a := newAnalysis(generated)
	if err := followOrders(ctx, cfg, log, a); err != nil {
		return err
	}
	bal, err := readBalances(ctx, cfg, log)
	if err != nil {
		return err
	}

	r := a.result(dir, bal, cfg.Report.Timeout.String())
	writeSummary(out, r)
	if cfg.Report.JSON {
		if err := writeJSON(out, r); err != nil {
			return err
		}
	}
	if !r.Passed() {
		return ErrFailed
	}
	return nil
}

// Reading a topic "to its end" reads up to its high watermark when the read
// starts, not its last stable offset: it waits for the transactions open at
// that moment. In the Sequential model validators write credits to trades in
// transactions, and while one is open the last stable offset stays before
// it, and so maybe before the last generated trades; the same goes for the
// last orders and balances. A read gives up only when it makes no progress
// (see kafka.ReadToEnd).

// readGenerated reads trades to its end. Its SEND trades are the generated
// ones; RECEIVE trades are credits on their way back (Sequential model).
func readGenerated(ctx context.Context, cfg config.Config, log *slog.Logger) ([]avro.Trade, error) {
	var generated []avro.Trade
	err := kafka.ReadToEnd(ctx, cfg, log, kafka.ClientID(role, cfg.SiteID, "trades"), map[string][]int32{kafka.Trades: nil}, kafka.LogEnd,
		func(r *kgo.Record) error {
			block, err := kafka.Decode(avro.BlockCodec, r)
			for _, t := range block.Trades {
				if t.Category == avro.Send {
					generated = append(generated, t)
				}
			}
			return err
		})
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", kafka.Trades, err)
	}
	return generated, nil
}

// followOrders reads orders until every generated trade has been validated
// and every accepted trade completed, or report.timeoutMs has passed. Then it
// reads on to the end orders has at that moment, so that any duplicate
// already written is counted too.
func followOrders(ctx context.Context, cfg config.Config, log *slog.Logger, a *analysis) error {
	orders, err := kafka.NewTail(ctx, cfg, log, kafka.ClientID(role, cfg.SiteID, "orders"), kafka.Orders)
	if err != nil {
		return err
	}
	defer orders.Close()
	addOrders := func(r *kgo.Record) error {
		block, err := kafka.Decode(avro.BlockCodec, r)
		if err != nil {
			return err
		}
		for _, t := range block.Trades {
			if err := a.addOrder(t, r.Timestamp.UnixMilli()); err != nil {
				return err
			}
		}
		return nil
	}

	deadline := time.Now().Add(cfg.Report.Timeout)
	nextLine := time.Now().Add(cfg.StatsInterval)
	for !a.done() && time.Now().Before(deadline) {
		if !time.Now().Before(nextLine) {
			log.Info(fmt.Sprintf("waiting for %d validations and %d completions", a.unvalidated, a.uncompleted))
			nextLine = nextLine.Add(cfg.StatsInterval)
		}
		poll, cancel := context.WithDeadline(ctx, earlier(deadline, nextLine))
		err := orders.Poll(poll, addOrders)
		cancel()
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("read %s: %w", kafka.Orders, err)
		}
	}
	if err := orders.ReadToEnd(ctx, addOrders); err != nil {
		return fmt.Errorf("read %s to its end: %w", kafka.Orders, err)
	}
	return nil
}

// readBalances reads localBalance to its end: the latest balance of each
// account, and every record below zero.
func readBalances(ctx context.Context, cfg config.Config, log *slog.Logger) (balances, error) {
	bal := balances{latest: map[string]int64{}}
	err := kafka.ReadToEnd(ctx, cfg, log, kafka.ClientID(role, cfg.SiteID, "localBalance"), map[string][]int32{kafka.LocalBalance: nil}, kafka.LogEnd,
		func(r *kgo.Record) error {
			lb, err := kafka.Decode(avro.LocalBalanceCodec, r)
			account := string(r.Key)
			bal.latest[account] = lb.Balance
			if lb.Balance < 0 {
				bal.negative = append(bal.negative, fmt.Sprintf("%s: %d", account, lb.Balance))
			}
			return err
		})
	if err != nil {
		return balances{}, fmt.Errorf("read %s: %w", kafka.LocalBalance, err)
	}
	return bal, nil
}

func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
