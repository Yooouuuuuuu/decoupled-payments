package initrole

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

func testConfig() config.Config {
	return config.Config{
		Model: config.Concurrent, KafkaTransactions: true,
		Bank: config.Bank{Count: 3, AccountsPerBank: 2, InitialBalance: 500},
	}
}

func TestDirectory(t *testing.T) {
	dir := directory(testConfig())
	if len(dir.Banks) != 3 || dir.TotalMoney() != 3000 || dir.Model != config.Concurrent || !dir.KafkaTransactions {
		t.Fatalf("directory %+v", dir)
	}
	b := dir.Banks[2]
	if b.ID != "bank2" || b.Partition != 2 || strings.Join(b.Accounts, ",") != "bank2-0000,bank2-0001" {
		t.Errorf("bank %+v", b)
	}
}

func TestDifferences(t *testing.T) {
	cfg := testConfig()
	if diffs := differences(directory(cfg), cfg); len(diffs) != 0 {
		t.Errorf("same configuration, differences %v", diffs)
	}
	other := cfg
	other.Model, other.KafkaTransactions = config.Sequential, false
	other.Bank = config.Bank{Count: 2, AccountsPerBank: 3, InitialBalance: 1}
	diffs := differences(directory(other), cfg)
	want := []string{
		"model is sequential, the configuration says concurrent",
		"kafkaTransactions is false, the configuration says true",
		"bank.count is 2, the configuration says 3",
		"bank.initialBalance is 1, the configuration says 500",
		"bank.accountsPerBank of bank0 is 3, the configuration says 2",
	}
	if strings.Join(diffs, "\n") != strings.Join(want, "\n") {
		t.Errorf("differences:\n%s\nwant:\n%s", strings.Join(diffs, "\n"), strings.Join(want, "\n"))
	}
}

// kfakeConfig starts kfake, franz-go's in-process fake Kafka, and returns the
// configuration of an init against it: 3 banks of 2 accounts.
func kfakeConfig(t *testing.T) (*kfake.Cluster, config.Config) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cfg, err := config.Load(config.Sources{Role: role, LookupEnv: func(string) (string, bool) { return "", false }, Flags: []config.Flag{
		{Name: "kafka.bootstrapServers", Value: c.ListenAddrs()[0], HasValue: true},
		{Name: "bank.accountsPerBank", Value: "2", HasValue: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return c, cfg
}

func TestInitWritesTheBalancesBeforeTheBanks(t *testing.T) {
	c, cfg := kfakeConfig(t)
	var produced []kadm.TopicID // the topics of the produce requests, in order
	var mu sync.Mutex
	c.ControlKey(kmsg.Produce.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		mu.Lock()
		defer mu.Unlock()
		for _, topic := range req.(*kmsg.ProduceRequest).Topics {
			produced = append(produced, topic.TopicID)
		}
		return nil, nil, false
	})
	log := slog.New(slog.DiscardHandler)
	if err := Run(context.Background(), cfg, log); err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.BootstrapServers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	topics, err := kadm.NewClient(cl).ListTopics(context.Background(), kafka.AccountInfo, kafka.LocalBalance)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	first := slices.Index(produced, topics[kafka.AccountInfo].ID)
	if first < 0 || slices.Contains(produced[first:], topics[kafka.LocalBalance].ID) || !slices.Contains(produced, topics[kafka.LocalBalance].ID) {
		t.Errorf("produce requests to topics %v: accountInfo (%v) must come after every balance (%v)", produced, topics[kafka.AccountInfo].ID, topics[kafka.LocalBalance].ID)
	}
	mu.Unlock()

	dir, err := kafka.ReadDirectory(context.Background(), cfg, log, "check")
	if err != nil || len(dir.Banks) != 3 || dir.AccountCount() != 6 || dir.TotalMoney() != 60000 {
		t.Errorf("accountInfo: %+v, %v", dir, err)
	}
	configs, err := kadm.NewClient(cl).DescribeTopicConfigs(context.Background(), kafka.AccountInfo)
	if err != nil {
		t.Fatal(err)
	}
	if policy, err := configs.On(kafka.AccountInfo, nil); err != nil || !slices.ContainsFunc(policy.Configs, func(c kadm.Config) bool {
		return c.Key == "cleanup.policy" && c.Value != nil && *c.Value == "compact"
	}) {
		t.Errorf("accountInfo is not compacted: %+v, %v", policy, err)
	}
	// A second init finds it initialized.
	if err := Run(context.Background(), cfg, log); err != nil {
		t.Errorf("second init: %v", err)
	}
}

func TestInitNoticesAnUnfinishedInit(t *testing.T) {
	_, cfg := kfakeConfig(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.BootstrapServers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	// The topics, but no accountInfo record: an init stopped while writing the balances.
	for _, topic := range kafka.AllTopics(cfg) {
		if _, err := kadm.NewClient(cl).CreateTopic(context.Background(), topic.Partitions, 1, topic.Configs, topic.Name); err != nil {
			t.Fatal(err)
		}
	}
	err = Run(context.Background(), cfg, slog.New(slog.DiscardHandler))
	if err == nil || err.Error() != "the topics exist but accountInfo is empty, so an earlier init didn't finish: run init --reset to start over (deletes all data)" {
		t.Errorf("got %v", err)
	}
}
