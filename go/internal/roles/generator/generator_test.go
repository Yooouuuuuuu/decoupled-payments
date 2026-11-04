package generator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

// cluster starts kfake, franz-go's in-process fake Kafka, with two banks of
// two accounts, and returns the generator's configuration for it.
func cluster(t *testing.T, flags ...config.Flag) config.Config {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, kafka.AccountInfo), kfake.SeedTopics(3, kafka.Trades))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for p := range int32(2) {
		id := bank.ID(p)
		r, err := kafka.NewRecord(avro.AccountInfoCodec, kafka.AccountInfo, 0, id, avro.AccountInfo{Bank: id, BankPartition: p,
			Accounts: []string{bank.AccountID(id, 0), bank.AccountID(id, 1)}, InitialBalance: 100, Model: avro.ModelConcurrent, KafkaTransactions: true})
		if err == nil {
			err = cl.ProduceSync(context.Background(), r).FirstErr()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := config.Load(config.Sources{Role: role, LookupEnv: func(string) (string, bool) { return "", false },
		Flags: append(flags, config.Flag{Name: "kafka.bootstrapServers", Value: c.ListenAddrs()[0], HasValue: true})})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func generated(t *testing.T, cfg config.Config) []avro.Trade {
	t.Helper()
	var trades []avro.Trade
	err := kafka.ReadToEnd(context.Background(), cfg, slog.New(slog.DiscardHandler), "check", map[string][]int32{kafka.Trades: nil}, kafka.StableEnd,
		func(r *kgo.Record) error {
			b, err := kafka.Decode(avro.BlockCodec, r)
			trades = append(trades, b.Trades...)
			return err
		})
	if err != nil {
		t.Fatal(err)
	}
	return trades
}

func TestTheGeneratorSendsItsTrades(t *testing.T) {
	cfg := cluster(t, config.Flag{Name: "trades", Value: "300", HasValue: true}, config.Flag{Name: "rps", Value: "3000", HasValue: true})
	if err := Run(context.Background(), cfg, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	trades := generated(t, cfg)
	if len(trades) != 300 {
		t.Fatalf("%d trades on trades, want 300", len(trades))
	}
	serials := map[int64]bool{}
	for _, tr := range trades {
		serials[tr.SerialNumber] = true
		if tr.Category != avro.Send || tr.Outbank == tr.Inbank || tr.Amount < 1 || tr.Amount > 100 {
			t.Fatalf("trade %+v", tr)
		}
	}
	if len(serials) != 300 {
		t.Errorf("%d distinct serial numbers, want 300", len(serials))
	}
}

func TestAStopSignalStopsTheGenerator(t *testing.T) {
	cfg := cluster(t, config.Flag{Name: "trades", Value: "0", HasValue: true})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, slog.New(slog.DiscardHandler)) }()
	time.Sleep(500 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("stopped generator: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the generator didn't stop")
	}
	if n := len(generated(t, cfg)); n == 0 {
		t.Error("no trades sent before the stop")
	}
}
