package report

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

// A run on kfake, franz-go's in-process fake Kafka, with the banks of dir:
// t1 was validated and completed; t2 only generated.
func cluster(t *testing.T, flags ...config.Flag) config.Config {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, kafka.AccountInfo, kafka.Trades), kfake.SeedTopics(2, kafka.Orders, kafka.LocalBalance))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	write := func(r *kgo.Record, err error) {
		t.Helper()
		if err == nil {
			err = cl.ProduceSync(context.Background(), r).FirstErr()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range dir.Banks {
		write(kafka.NewRecord(avro.AccountInfoCodec, kafka.AccountInfo, 0, b.ID, avro.AccountInfo{Bank: b.ID, BankPartition: b.Partition,
			Accounts: b.Accounts, InitialBalance: dir.InitialBalance, Model: avro.ModelConcurrent, KafkaTransactions: true}))
		write(kafka.NewRecord(avro.LocalBalanceCodec, kafka.LocalBalance, b.Partition, b.Accounts[0], avro.LocalBalance{Balance: dir.InitialBalance}))
	}
	t1, t2 := generated(1, 30, 1000), generated(2, 10, 1000)
	for _, tr := range []avro.Trade{t1, t2} {
		write(kafka.NewRecord(avro.BlockCodec, kafka.Trades, 0, "", avro.Block{Trades: []avro.Trade{tr}}))
	}
	write(kafka.NewRecord(avro.BlockCodec, kafka.Orders, 0, "bank0", avro.Block{Trades: []avro.Trade{order(t1, avro.Send, 1100)}}))
	write(kafka.NewRecord(avro.LocalBalanceCodec, kafka.LocalBalance, 0, "bank0-0000", avro.LocalBalance{Balance: 70}))
	write(kafka.NewRecord(avro.BlockCodec, kafka.Orders, 1, "bank1", avro.Block{Trades: []avro.Trade{order(t1, avro.Receive, 1100)}}))
	write(kafka.NewRecord(avro.LocalBalanceCodec, kafka.LocalBalance, 1, "bank1-0000", avro.LocalBalance{Balance: 130}))

	cfg, err := config.Load(config.Sources{Role: role, LookupEnv: func(string) (string, bool) { return "", false },
		Flags: append(flags, config.Flag{Name: "kafka.bootstrapServers", Value: c.ListenAddrs()[0], HasValue: true})})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestAReportOfARunThatHasNotSettled(t *testing.T) {
	cfg := cluster(t, config.Flag{Name: "report.timeoutMs", Value: "300", HasValue: true},
		config.Flag{Name: "statsIntervalMs", Value: "100", HasValue: true}, config.Flag{Name: "json", HasValue: false})
	var out, logs bytes.Buffer
	err := Run(context.Background(), cfg, slog.New(slog.NewTextHandler(&logs, nil)), &out)
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("Run returned %v, want a failed check", err)
	}
	if !strings.Contains(logs.String(), "waiting for 1 validations and 0 completions") {
		t.Errorf("no waiting line in the log:\n%s", logs.String())
	}
	for _, want := range []string{
		"generated          2", "completed          1", "in flight          1",
		"✗ complete", "✗ money-conserved        not checked: 1 trades not finished", "✓ no-negative-balances", "FAIL",
		`REPORT_JSON {"result":"FAIL"`, `"generated":2,"accepted":1,"rejected":0,"completed":1,"inFlight":1`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the report doesn't say %q:\n%s", want, out.String())
		}
	}
}
