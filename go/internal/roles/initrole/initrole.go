// Package initrole implements the init role: it creates the topics, records
// the banks and the model in accountInfo, and gives every account its
// initial balance in localBalance.
package initrole

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

const role = "init"

// waitLimit bounds waiting for deleted topics to disappear and for new
// partitions to get a leader.
const waitLimit = 60 * time.Second

// Run initializes the cluster, or checks that it already is.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	// init polls metadata while topics are deleted and created; by default
	// franz-go answers from metadata up to 5 s old. The client's context
	// lets a stop signal interrupt even a request still opening its
	// connection.
	cl, err := kgo.NewClient(append(kafka.ClientOpts(cfg, log, kafka.ClientID(role, cfg.SiteID, "admin")),
		kgo.MetadataMinAge(100*time.Millisecond), kgo.WithContext(ctx))...)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := kafka.AwaitBrokers(ctx, cl, log); err != nil {
		return err
	}
	adm := kadm.NewClient(cl)

	if cfg.Init.Reset {
		if err := reset(ctx, adm, log); err != nil {
			return err
		}
	}

	existing, err := existingTopics(ctx, adm)
	if err != nil {
		return err
	}
	switch len(existing) {
	case 0:
	case len(kafka.TopicNames()):
		return checkInitialized(ctx, cfg, log)
	default:
		return fmt.Errorf("only some topics exist (%s): run init --reset to start over (deletes all data)", strings.Join(existing, ", "))
	}

	if err := createTopics(ctx, adm, cfg, log); err != nil {
		return err
	}
	dir := directory(cfg)
	if err := writeAccounts(ctx, cfg, log, dir); err != nil {
		return err
	}
	log.Info("initialized", "banks", len(dir.Banks), "accounts", dir.AccountCount(), "totalMoney", dir.TotalMoney(),
		"model", cfg.Model, "kafkaTransactions", cfg.KafkaTransactions)
	return nil
}

// reset deletes the consumer groups and the topics, and so all data.
func reset(ctx context.Context, adm *kadm.Client, log *slog.Logger) error {
	if err := waitFor(ctx, "the group coordinator", func() (bool, error) { return deleteGroups(ctx, adm) }); err != nil {
		return err
	}
	deleted, err := adm.DeleteTopics(ctx, kafka.TopicNames()...)
	if err != nil {
		return fmt.Errorf("delete the topics: %w", err)
	}
	for _, t := range deleted.Sorted() {
		if t.Err != nil && !errors.Is(t.Err, kerr.UnknownTopicOrPartition) {
			return fmt.Errorf("delete topic %s: %w", t.Topic, t.Err)
		}
	}
	err = waitFor(ctx, "the deleted topics to disappear", func() (bool, error) {
		existing, err := existingTopics(ctx, adm)
		return len(existing) == 0, err
	})
	if err == nil {
		log.Info("deleted all topics and consumer groups")
	}
	return err
}

// deleteGroups deletes the consumer groups. It reports false while the group
// coordinator is starting: in a new cluster, the first group request starts it.
func deleteGroups(ctx context.Context, adm *kadm.Client) (bool, error) {
	groups, err := adm.DeleteGroups(ctx, kafka.AggregatorsGroup, kafka.ValidatorsGroup)
	if err != nil {
		if kerr.IsRetriable(err) {
			return false, nil
		}
		return false, fmt.Errorf("delete the consumer groups: %w", err)
	}
	for _, g := range groups.Sorted() {
		switch {
		case g.Err == nil, errors.Is(g.Err, kerr.GroupIDNotFound):
		case errors.Is(g.Err, kerr.NonEmptyGroup):
			return false, fmt.Errorf("consumer group %s still has members: stop the aggregators and validators first", g.Group)
		case kerr.IsRetriable(g.Err):
			return false, nil
		default:
			return false, fmt.Errorf("delete consumer group %s: %w", g.Group, g.Err)
		}
	}
	return true, nil
}

// existingTopics returns which of the seven topics exist.
func existingTopics(ctx context.Context, adm *kadm.Client) ([]string, error) {
	details, err := adm.ListTopics(ctx, kafka.TopicNames()...)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	var existing []string
	for _, name := range kafka.TopicNames() {
		if t, ok := details[name]; ok && t.Err == nil {
			existing = append(existing, name)
		}
	}
	return existing, nil
}

// checkInitialized succeeds if accountInfo matches the configuration.
func checkInitialized(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	dir, err := kafka.ReadDirectory(ctx, cfg, log, kafka.ClientID(role, cfg.SiteID, "accountInfo"))
	switch {
	case errors.Is(err, kafka.ErrNotInitialized):
		// init writes accountInfo last.
		return errors.New("the topics exist but accountInfo is empty, so an earlier init didn't finish: run init --reset to start over (deletes all data)")
	case err != nil:
		return err
	}
	if diffs := differences(dir, cfg); len(diffs) > 0 {
		return fmt.Errorf("already initialized differently (%s): run init --reset to start over (deletes all data)", strings.Join(diffs, "; "))
	}
	log.Info("already initialized", "banks", len(dir.Banks), "accounts", dir.AccountCount())
	return nil
}

// differences lists how accountInfo differs from the configuration.
func differences(dir bank.Directory, cfg config.Config) []string {
	var diffs []string
	differ := func(name string, recorded, configured any) {
		if recorded != configured {
			diffs = append(diffs, fmt.Sprintf("%s is %v, the configuration says %v", name, recorded, configured))
		}
	}
	differ("model", dir.Model, cfg.Model)
	differ("kafkaTransactions", dir.KafkaTransactions, cfg.KafkaTransactions)
	differ("bank.count", len(dir.Banks), int(cfg.Bank.Count))
	differ("bank.initialBalance", dir.InitialBalance, cfg.Bank.InitialBalance)
	for _, b := range dir.Banks {
		if len(b.Accounts) != int(cfg.Bank.AccountsPerBank) {
			differ("bank.accountsPerBank of "+b.ID, len(b.Accounts), int(cfg.Bank.AccountsPerBank))
			break
		}
	}
	return diffs
}

// createTopics creates the seven topics and waits until every partition has
// a leader, so the first writes don't fail.
func createTopics(ctx context.Context, adm *kadm.Client, cfg config.Config, log *slog.Logger) error {
	for _, t := range kafka.AllTopics(cfg) {
		resp, err := adm.CreateTopic(ctx, t.Partitions, cfg.Topics.ReplicationFactor, t.Configs, t.Name)
		if err == nil {
			err = resp.Err
		}
		if err != nil {
			return fmt.Errorf("create topic %s: %w", t.Name, err)
		}
	}
	err := waitFor(ctx, "every partition to have a leader", func() (bool, error) {
		details, err := adm.ListTopics(ctx, kafka.TopicNames()...)
		if err != nil {
			return false, err
		}
		for _, t := range kafka.AllTopics(cfg) {
			d, ok := details[t.Name]
			if !ok || d.Err != nil || int32(len(d.Partitions)) != t.Partitions {
				return false, nil
			}
			for _, p := range d.Partitions {
				if p.Err != nil || p.Leader < 0 {
					return false, nil
				}
			}
		}
		return true, nil
	})
	if err == nil {
		log.Info("created the topics", "banks", cfg.Bank.Count, "tradesPartitions", cfg.Topics.TradesPartitions,
			"replicationFactor", cfg.Topics.ReplicationFactor)
	}
	return err
}

// directory is the bank directory the configuration describes.
func directory(cfg config.Config) bank.Directory {
	dir := bank.Directory{InitialBalance: cfg.Bank.InitialBalance, Model: cfg.Model, KafkaTransactions: cfg.KafkaTransactions}
	for p := range cfg.Bank.Count {
		id := bank.ID(p)
		accounts := make([]string, cfg.Bank.AccountsPerBank)
		for j := range accounts {
			accounts[j] = bank.AccountID(id, j)
		}
		dir.Banks = append(dir.Banks, bank.Bank{ID: id, Partition: p, Accounts: accounts})
	}
	return dir
}

// writeAccounts gives every account its initial balance in localBalance, in
// its bank's partition, and then records each bank in accountInfo. Only
// once every balance is written does accountInfo exist: a complete
// accountInfo means a finished init.
func writeAccounts(ctx context.Context, cfg config.Config, log *slog.Logger, dir bank.Directory) error {
	cl, err := kgo.NewClient(append(kafka.ClientOpts(cfg, log, kafka.ClientID(role, cfg.SiteID, "producer")), kafka.ProducerOpts()...)...)
	if err != nil {
		return err
	}
	defer cl.Close()
	var sends kafka.SendErrors
	produce := func(r *kgo.Record, err error) error {
		if err == nil {
			cl.Produce(ctx, r, sends.Promise)
		}
		return err
	}

	for _, b := range dir.Banks {
		for _, account := range b.Accounts {
			if err := produce(kafka.NewRecord(avro.LocalBalanceCodec, kafka.LocalBalance, b.Partition, account, avro.LocalBalance{Balance: dir.InitialBalance})); err != nil {
				return err
			}
		}
	}
	if err := flushed(ctx, cl, &sends); err != nil {
		return fmt.Errorf("write the initial balances: %w", err)
	}
	for _, b := range dir.Banks {
		info := avro.AccountInfo{
			Bank: b.ID, BankPartition: b.Partition, Accounts: b.Accounts, InitialBalance: dir.InitialBalance,
			Model: bank.AvroModel(dir.Model), KafkaTransactions: dir.KafkaTransactions,
		}
		if err := produce(kafka.NewRecord(avro.AccountInfoCodec, kafka.AccountInfo, 0, b.ID, info)); err != nil {
			return err
		}
	}
	if err := flushed(ctx, cl, &sends); err != nil {
		return fmt.Errorf("write %s: %w", kafka.AccountInfo, err)
	}
	return nil
}

// flushed waits until every record produced so far is acknowledged.
func flushed(ctx context.Context, cl *kgo.Client, sends *kafka.SendErrors) error {
	if err := cl.Flush(ctx); err != nil {
		return err
	}
	return sends.Err()
}

// waitFor polls until done reports true, for at most waitLimit.
func waitFor(ctx context.Context, what string, done func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, waitLimit)
	defer cancel()
	for {
		ok, err := done()
		if ok || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("gave up waiting for %s: %w", what, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}
