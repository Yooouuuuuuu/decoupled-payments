package aggregator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/kafka"
)

// These tests run the aggregator against kfake, franz-go's in-process fake
// Kafka.

// cluster starts kfake with trades (one partition), accountInfo and
// blocks, records banks banks in accountInfo, and returns a client for it.
func cluster(t *testing.T, banks int32, blocksPartitions int32) (*kfake.Cluster, *kgo.Client) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, kafka.Trades, kafka.AccountInfo), kfake.SeedTopics(blocksPartitions, kafka.Blocks))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	write := writer(t, cl)
	for p := range banks {
		id := bank.ID(p)
		info := avro.AccountInfo{Bank: id, BankPartition: p, Accounts: []string{bank.AccountID(id, 0)}, InitialBalance: 100,
			Model: avro.ModelConcurrent, KafkaTransactions: true}
		write(kafka.NewRecord(avro.AccountInfoCodec, kafka.AccountInfo, 0, id, info))
	}
	return c, cl
}

// writer returns a function that writes one record, as kafka.NewRecord
// builds it.
func writer(t *testing.T, cl *kgo.Client) func(*kgo.Record, error) {
	return func(r *kgo.Record, err error) {
		t.Helper()
		if err == nil {
			err = cl.ProduceSync(context.Background(), r).FirstErr()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// sendTrades writes n new trades of the bank of partition from to trades.
func sendTrades(t *testing.T, cl *kgo.Client, from int32, n int) {
	t.Helper()
	write := writer(t, cl)
	for i := range n {
		tr := avro.Trade{SerialNumber: int64(from)<<32 | int64(i), Outbank: bank.ID(from), OutAccount: bank.AccountID(bank.ID(from), 0),
			Inbank: "bank1", InAccount: "bank1-0000", OutbankPartition: from, InbankPartition: 1, Amount: 1, Category: avro.Send,
			Timestamp1: time.Now().UnixMilli()}
		write(kafka.NewRecord(avro.BlockCodec, kafka.Trades, 0, "", avro.Block{Trades: []avro.Trade{tr}}))
	}
}

func testConfig(t *testing.T, cl *kgo.Client, flags ...string) config.Config {
	t.Helper()
	fl := []config.Flag{{Name: "kafka.bootstrapServers", Value: strings.Join(cl.OptValue(kgo.SeedBrokers).([]string), ","), HasValue: true}}
	for _, f := range flags {
		name, value, _ := strings.Cut(f, "=")
		fl = append(fl, config.Flag{Name: name, Value: value, HasValue: true})
	}
	cfg, err := config.Load(config.Sources{Role: role, Flags: fl, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func testLog(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// committed is the aggregators' committed offset of trades partition 0, or
// -1 if there is none.
func committed(t *testing.T, cl *kgo.Client) int64 {
	t.Helper()
	offsets, err := kadm.NewClient(cl).FetchOffsets(context.Background(), kafka.AggregatorsGroup)
	if err != nil {
		t.Fatal(err)
	}
	if o, ok := offsets.Lookup(kafka.Trades, 0); ok {
		return o.At
	}
	return -1
}

// emitted is the number of trades in committed blocks.
func emitted(t *testing.T, cfg config.Config) int {
	t.Helper()
	n := 0
	err := kafka.ReadToEnd(context.Background(), cfg, slog.New(slog.DiscardHandler), "check", map[string][]int32{kafka.Blocks: nil}, kafka.StableEnd,
		func(r *kgo.Record) error {
			b, err := kafka.Decode(avro.BlockCodec, r)
			n += len(b.Trades)
			return err
		})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// waitFor polls until done or the timeout.
func waitFor(t *testing.T, timeout time.Duration, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); !done(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("gave up after %v waiting for %s", timeout, what)
		}
	}
}

// A block that fails to send must not let the offsets of its trades commit:
// they would be lost. Here accountInfo knows bank2, but blocks has no
// partition 2.
func TestAFailedSendCommitsNoOffsets(t *testing.T) {
	_, cl := cluster(t, 3, 2)
	sendTrades(t, cl, 0, 5)
	sendTrades(t, cl, 2, 5)
	cfg := testConfig(t, cl, "aggregator.blockTimeoutMs=200")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := Run(ctx, cfg, testLog(t))
	if err == nil || !strings.Contains(err.Error(), "send to blocks partition 2") {
		t.Fatalf("Run returned %v, want the failed send", err)
	}
	if offset := committed(t, cl); offset > 0 {
		t.Errorf("the trades offset %d was committed although a block failed to send", offset)
	}
	if n := emitted(t, cfg); n != 0 {
		t.Errorf("%d trades emitted, want none: the transaction must abort", n)
	}
}

// The previous process with this siteId was killed after committing offsets
// in its transaction, before ending it. The new one must fence it at once,
// not wait for the broker to abort the transaction.
func TestARestartFencesTheCrashedPredecessor(t *testing.T) {
	_, cl := cluster(t, 2, 2)
	sendTrades(t, cl, 0, 10)
	pendingOffsetCommit(t, cl, "aggregator-1", 5)

	cfg := testConfig(t, cl, "aggregator.blockTimeoutMs=200")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, testLog(t)) }()
	waitFor(t, 10*time.Second, "the 10 trades to be emitted", func() bool { return emitted(t, cfg) == 10 })
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
}

// pendingOffsetCommit leaves an open transaction of transactionalID with an
// offset commit for trades partition 0, as a process killed in the middle of
// an emit does.
func pendingOffsetCommit(t *testing.T, cl *kgo.Client, transactionalID string, offset int64) {
	t.Helper()
	ctx := context.Background()
	initReq := kmsg.NewPtrInitProducerIDRequest()
	initReq.TransactionalID = &transactionalID
	initReq.TransactionTimeoutMillis = 120000
	initResp, err := initReq.RequestWith(ctx, cl)
	if err == nil && initResp.ErrorCode != 0 {
		err = fmt.Errorf("error code %d", initResp.ErrorCode)
	}
	if err != nil {
		t.Fatalf("init producer ID: %v", err)
	}
	addReq := kmsg.NewPtrAddOffsetsToTxnRequest()
	addReq.TransactionalID, addReq.ProducerID, addReq.ProducerEpoch, addReq.Group = transactionalID, initResp.ProducerID, initResp.ProducerEpoch, kafka.AggregatorsGroup
	if addResp, err := addReq.RequestWith(ctx, cl); err != nil || addResp.ErrorCode != 0 {
		t.Fatalf("add offsets to the transaction: %v %v", err, addResp)
	}
	topics, err := kadm.NewClient(cl).ListTopics(ctx, kafka.Trades)
	if err != nil {
		t.Fatal(err)
	}
	commitReq := kmsg.NewPtrTxnOffsetCommitRequest()
	commitReq.TransactionalID, commitReq.Group = transactionalID, kafka.AggregatorsGroup
	commitReq.ProducerID, commitReq.ProducerEpoch, commitReq.Generation = initResp.ProducerID, initResp.ProducerEpoch, -1
	topic := kmsg.NewTxnOffsetCommitRequestTopic()
	topic.Topic, topic.TopicID = kafka.Trades, topics[kafka.Trades].ID
	partition := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	partition.Partition, partition.Offset = 0, offset
	topic.Partitions = append(topic.Partitions, partition)
	commitReq.Topics = append(commitReq.Topics, topic)
	if _, err := commitReq.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	}
}

// An aggregator paused for longer than its session (SIGSTOP) resumes: the
// poll it was in returns at once and holds the rebalance gate, its buffered
// trades are due, and the group has dropped the member. The emit must abort
// quickly, not wait for the transaction timeout, and the trades must still
// be emitted afterwards.
func TestADroppedMemberAbortsItsEmitAtOnce(t *testing.T) {
	_, cl := cluster(t, 2, 2)
	sendTrades(t, cl, 0, 10)
	cfg := testConfig(t, cl, "aggregator.blockTimeoutMs=2000", "kafka.sessionTimeoutMs=6000", "kafka.transactionTimeoutMs=15000")
	ctx := context.Background()
	a, err := newAggregator(ctx, cfg, testLog(t))
	if err != nil {
		t.Fatal(err)
	}
	defer a.cl.CloseAllowingRebalance()
	step := func() error {
		err := a.step(ctx)
		a.cl.AllowRebalance()
		return err
	}
	waitFor(t, 10*time.Second, "the trades to be buffered", func() bool { return step() == nil && len(a.buf.trades[0]) == 10 })

	// The pause: the poll in progress returns at once and holds the gate.
	stopped, cancel := context.WithCancel(ctx)
	cancel()
	a.cl.PollRecords(stopped, 1)
	// Meanwhile the group dropped the member, and its heartbeat learns it.
	left, err := kadm.NewClient(cl).LeaveGroup(ctx, kadm.LeaveGroup(kafka.AggregatorsGroup).InstanceIDs(a.id))
	if err == nil {
		err = left.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second) // a heartbeat every 2 s

	start := time.Now()
	err = a.step(ctx) // the rest of the resumed step: the window has expired, so it emits
	if took := time.Since(start); err != nil || took > 5*time.Second {
		t.Fatalf("the resumed step took %v and returned %v", took.Round(time.Millisecond), err)
	}
	a.cl.AllowRebalance()
	if n := emitted(t, cfg); n != 0 {
		t.Fatalf("%d trades emitted by a member that was dropped", n)
	}
	// It rejoins, reads the trades again and emits them.
	waitFor(t, 15*time.Second, "the trades to be emitted after rejoining", func() bool { return step() == nil && emitted(t, cfg) == 10 })
	if offset := committed(t, cl); offset != 10 {
		t.Errorf("committed offset %d, want 10", offset)
	}
}

// A session timeout the broker doesn't allow (kfake's minimum is 6 s, as
// Kafka's): the aggregator can never join, so it fails instead of retrying
// forever.
func TestARefusedMemberFails(t *testing.T) {
	_, cl := cluster(t, 2, 2)
	cfg := testConfig(t, cl, "kafka.sessionTimeoutMs=3000")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	err := Run(ctx, cfg, testLog(t))
	if err == nil || !strings.Contains(err.Error(), "INVALID_SESSION_TIMEOUT") {
		t.Errorf("Run returned %v after %v, want INVALID_SESSION_TIMEOUT", err, time.Since(start))
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("failed only after %v", took)
	}
}

// A clean stop leaves the group, although the member is static.
func TestACleanStopLeavesTheGroup(t *testing.T) {
	_, cl := cluster(t, 2, 2)
	cfg := testConfig(t, cl)
	adm := kadm.NewClient(cl)
	members := func() int {
		groups, err := adm.DescribeGroups(context.Background(), kafka.AggregatorsGroup)
		if err != nil {
			t.Fatal(err)
		}
		return len(groups[kafka.AggregatorsGroup].Members)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, testLog(t)) }()
	waitFor(t, 10*time.Second, "the aggregator to join", func() bool { return members() == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := members(); n != 0 {
		t.Errorf("%d members after a clean stop, want 0", n)
	}
}

func TestEmptyRevocationsTakeNothingAway(t *testing.T) {
	a := &aggregator{log: slog.New(slog.DiscardHandler), reading: map[int32]bool{}}
	a.onAssigned(context.Background(), nil, map[string][]int32{kafka.Trades: {0, 2}})
	a.onRevoked(context.Background(), nil, map[string][]int32{})
	if a.revoked.Load() {
		t.Error("an empty revocation raised the flag")
	}
	a.onRevoked(context.Background(), nil, map[string][]int32{kafka.Trades: {2}})
	if !a.revoked.Load() || !a.reading[0] || a.reading[2] {
		t.Errorf("after revoking partition 2: flag %v, reading %v", a.revoked.Load(), a.reading)
	}
}
