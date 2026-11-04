package validator

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

// These tests run the validator against kfake, franz-go's in-process fake
// Kafka, with two banks of two accounts, 100 in each.

func cluster(t *testing.T, skipBalance string) (*kgo.Client, config.Config) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, kafka.AccountInfo),
		kfake.SeedTopics(2, kafka.Blocks, kafka.Credits, kafka.Orders, kafka.LocalBalance, kafka.ValidatorOffsets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	write := func(r *kgo.Record, err error) {
		t.Helper()
		if err == nil {
			err = cl.ProduceSync(context.Background(), r).FirstErr()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for p := range int32(2) {
		id := bank.ID(p)
		accounts := []string{bank.AccountID(id, 0), bank.AccountID(id, 1)}
		for _, a := range accounts {
			if a != skipBalance {
				write(kafka.NewRecord(avro.LocalBalanceCodec, kafka.LocalBalance, p, a, avro.LocalBalance{Balance: 100}))
			}
		}
		write(kafka.NewRecord(avro.AccountInfoCodec, kafka.AccountInfo, 0, id, avro.AccountInfo{Bank: id, BankPartition: p,
			Accounts: accounts, InitialBalance: 100, Model: avro.ModelConcurrent, KafkaTransactions: true}))
	}
	cfg, err := config.Load(config.Sources{Role: role, LookupEnv: func(string) (string, bool) { return "", false },
		Flags: []config.Flag{{Name: "kafka.bootstrapServers", Value: c.ListenAddrs()[0], HasValue: true}}})
	if err != nil {
		t.Fatal(err)
	}
	return cl, cfg
}

func testLog(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// read returns the decoded values of a topic's committed records by key,
// in order.
func read[T any](t *testing.T, cfg config.Config, codec *avro.Codec[T], topic string) map[string][]T {
	t.Helper()
	out := map[string][]T{}
	err := kafka.ReadToEnd(context.Background(), cfg, slog.New(slog.DiscardHandler), "check", map[string][]int32{topic: nil}, kafka.StableEnd,
		func(r *kgo.Record) error {
			v, err := kafka.Decode(codec, r)
			out[string(r.Key)] = append(out[string(r.Key)], v)
			return err
		})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func waitFor(t *testing.T, timeout time.Duration, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); !done(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("gave up after %v waiting for %s", timeout, what)
		}
	}
}

func TestTakeOverValidateCreditAndLeave(t *testing.T) {
	cl, cfg := cluster(t, "")
	// bank0-0000 pays 30 to bank1-0001, then 80, which it no longer has.
	trades := []avro.Trade{
		{SerialNumber: 1, Outbank: "bank0", OutAccount: "bank0-0000", Inbank: "bank1", InAccount: "bank1-0001", OutbankPartition: 0, InbankPartition: 1, Amount: 30, Category: avro.Send, Timestamp1: 1},
		{SerialNumber: 2, Outbank: "bank0", OutAccount: "bank0-0000", Inbank: "bank1", InAccount: "bank1-0001", OutbankPartition: 0, InbankPartition: 1, Amount: 80, Category: avro.Send, Timestamp1: 1},
	}
	r, err := kafka.NewRecord(avro.BlockCodec, kafka.Blocks, 0, "bank0", avro.Block{Trades: trades})
	if err == nil {
		err = cl.ProduceSync(context.Background(), r).FirstErr()
	}
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, testLog(t)) }()
	waitFor(t, 15*time.Second, "the credit to be applied", func() bool {
		return len(read(t, cfg, avro.BlockCodec, kafka.Orders)["bank1"]) == 1
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	orders := read(t, cfg, avro.BlockCodec, kafka.Orders)
	if v := orders["bank0"]; len(v) != 1 || v[0].Trades[0].Category != avro.Send || v[0].Trades[1].Category != avro.Rejected || v[0].Trades[0].Timestamp2 == 0 {
		t.Errorf("bank0's orders: %+v", v)
	}
	if v := orders["bank1"]; v[0].Trades[0].Category != avro.Receive || v[0].Trades[0].SerialNumber != 1 {
		t.Errorf("bank1's orders: %+v", v)
	}
	balances := read(t, cfg, avro.LocalBalanceCodec, kafka.LocalBalance)
	if b := balances["bank0-0000"]; b[len(b)-1].Balance != 70 {
		t.Errorf("bank0-0000: %+v", b)
	}
	if b := balances["bank1-0001"]; b[len(b)-1].Balance != 130 {
		t.Errorf("bank1-0001: %+v", b)
	}
	positions := read(t, cfg, avro.ValidatorOffsetCodec, kafka.ValidatorOffsets)
	if p := positions[kafka.Blocks]; len(p) != 1 || p[0] != (avro.ValidatorOffset{Topic: kafka.Blocks, Partition: 0, Offset: 1}) {
		t.Errorf("blocks positions: %+v", p)
	}
	if p := positions[kafka.Credits]; len(p) != 1 || p[0] != (avro.ValidatorOffset{Topic: kafka.Credits, Partition: 1, Offset: 1}) {
		t.Errorf("credits positions: %+v", p)
	}
	// A clean stop left the group, although the member is static.
	groups, err := kadm.NewClient(cl).DescribeGroups(context.Background(), kafka.ValidatorsGroup)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(groups[kafka.ValidatorsGroup].Members); n != 0 {
		t.Errorf("%d members after a clean stop, want 0", n)
	}
}

// A session timeout the broker doesn't allow: the validator can never join,
// so it fails instead of retrying forever.
func TestARefusedValidatorFails(t *testing.T) {
	_, cfg := cluster(t, "")
	cfg.Kafka.SessionTimeout = 3 * time.Second // kfake's minimum is 6 s, as Kafka's
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	if err := Run(ctx, cfg, testLog(t)); err == nil || !strings.Contains(err.Error(), "INVALID_SESSION_TIMEOUT") {
		t.Errorf("Run returned %v after %v, want INVALID_SESSION_TIMEOUT", err, time.Since(start))
	}
}

func TestATakeoverNeedsEveryBalance(t *testing.T) {
	_, cfg := cluster(t, "bank1-0001")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := Run(ctx, cfg, testLog(t))
	if err == nil || !strings.Contains(err.Error(), "localBalance has no balance for account bank1-0001 of bank1: run init --reset") {
		t.Errorf("Run returned %v", err)
	}
}
