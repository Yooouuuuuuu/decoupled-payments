package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

func TestAllTopics(t *testing.T) {
	cfg := config.Config{Bank: config.Bank{Count: 4}, Topics: config.Topics{TradesPartitions: 3}}
	want := map[string]int32{Trades: 3, Blocks: 4, Credits: 4, Orders: 4, LocalBalance: 4, ValidatorOffsets: 4, AccountInfo: 1}
	specs := AllTopics(cfg)
	if len(specs) != len(want) || len(TopicNames()) != len(want) {
		t.Fatalf("got %d topics and %d names, want %d", len(specs), len(TopicNames()), len(want))
	}
	for i, s := range specs {
		if s.Name != TopicNames()[i] || s.Partitions != want[s.Name] {
			t.Errorf("topic %d: %s with %d partitions", i, s.Name, s.Partitions)
		}
		compacted := s.Name == LocalBalance || s.Name == ValidatorOffsets || s.Name == AccountInfo
		if got := s.Configs["cleanup.policy"]; (got != nil && *got == "compact") != compacted {
			t.Errorf("%s: cleanup.policy %v", s.Name, got)
		}
		if often := s.Name != AccountInfo; compacted && often && (*s.Configs["segment.ms"] != "60000" || *s.Configs["min.cleanable.dirty.ratio"] != "0.1") {
			t.Errorf("%s: configs %v", s.Name, s.Configs)
		}
	}
}

func TestClientID(t *testing.T) {
	if got := ClientID("validator", 2, "bank0"); got != "validator-2-bank0" {
		t.Errorf("got %s", got)
	}
}

func TestPartitioner(t *testing.T) {
	p := partitioner{spread: kgo.UniformBytesPartitioner(16<<10, true, true, nil)}
	// Every topic but trades goes to the partition the record names.
	for _, topic := range []string{Blocks, Credits, Orders, LocalBalance, ValidatorOffsets, AccountInfo} {
		if got := p.ForTopic(topic).Partition(&kgo.Record{Topic: topic, Partition: 2}, 4); got != 2 {
			t.Errorf("%s record went to partition %d, want 2", topic, got)
		}
	}
	// Records to trades are spread by the sticky partitioner, which franz-go
	// drives through PartitionByBackup.
	if _, ok := p.ForTopic(Trades).(kgo.TopicBackupPartitioner); !ok {
		t.Error("trades doesn't use the sticky partitioner")
	}
}

func TestClientSettingsAreJavasDefaults(t *testing.T) {
	cl, err := kgo.NewClient(append(append(ProducerOpts(), ConsumerOpts(config.Config{Kafka: config.Kafka{MaxPartitionFetchBytes: 1 << 20}})...),
		GroupOpts(config.Config{Kafka: config.Kafka{SessionTimeout: 9 * time.Second}}, "g", "i")...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for _, tc := range []struct {
		name      string
		opt, want any
	}{
		{"linger", kgo.ProducerLinger, 5 * time.Millisecond},
		{"max request size", kgo.ProducerBatchMaxBytes, int32(1048576)},
		{"delivery timeout", kgo.RecordDeliveryTimeout, 120 * time.Second},
		{"fetch max wait", kgo.FetchMaxWait, 500 * time.Millisecond},
		{"rebalance timeout", kgo.RebalanceTimeout, 5 * time.Minute},
		{"heartbeat interval", kgo.HeartbeatInterval, 3 * time.Second},
		// Idempotent: franz-go itself allows five once the broker supports it.
		{"in-flight requests", kgo.MaxProduceRequestsInflightPerBroker, 1},
	} {
		if got := cl.OptValue(tc.opt); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
	if got := fmt.Sprint(cl.OptValue(kgo.ProducerBatchCompression)); got != fmt.Sprint([]kgo.CompressionCodec{kgo.NoCompression()}) {
		t.Errorf("compression %v, want none", got)
	}
}

func TestLibraryLogLevel(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	for level, want := range map[slog.Level]kgo.LogLevel{
		slog.LevelDebug: kgo.LogLevelWarn, slog.LevelInfo: kgo.LogLevelWarn, slog.LevelWarn: kgo.LogLevelWarn, slog.LevelError: kgo.LogLevelError,
	} {
		if got := newLogger(log, level).Level(); got != want {
			t.Errorf("logLevel %v: franz-go logs at %v, want %v", level, got, want)
		}
	}
}

func TestFetchErrorsIgnoresCutShortPolls(t *testing.T) {
	cutShort := kgo.Fetches{
		kgo.NewErrFetch(context.DeadlineExceeded)[0],
		kgo.NewErrFetch(context.Canceled)[0],
		kgo.NewErrFetch(kgo.ErrClientClosed)[0],
	}
	if err := FetchErrors(cutShort); err != nil {
		t.Errorf("got %v for polls that were only cut short", err)
	}
	boom := errors.New("boom")
	if err := FetchErrors(append(cutShort, kgo.NewErrFetch(boom)...)); !errors.Is(err, boom) {
		t.Errorf("got %v, want boom", err)
	}
}

func TestGroupRefused(t *testing.T) {
	poll := func(err error) error { return FetchErrors(kgo.NewErrFetch(&kgo.ErrGroupSession{Err: err})) }
	for _, refusal := range []error{kerr.InvalidSessionTimeout, kerr.GroupAuthorizationFailed, kerr.InconsistentGroupProtocol, kerr.InvalidGroupID, kerr.GroupMaxSizeReached} {
		if err := GroupRefused(poll(refusal), 3); !errors.Is(err, refusal) || !strings.Contains(err.Error(), "the consumer group refused this process") {
			t.Errorf("%v: got %v", refusal, err)
		}
	}
	if err := GroupRefused(poll(kerr.FencedInstanceID), 3); !strings.Contains(fmt.Sprint(err), "another process uses siteId 3") {
		t.Errorf("fenced: got %v", err)
	}
	// After a pause the member was dropped: franz-go rejoins.
	for _, rejoin := range []error{kerr.UnknownMemberID, kerr.IllegalGeneration, kerr.RebalanceInProgress, kerr.CoordinatorNotAvailable, io.EOF} {
		if err := GroupRefused(poll(rejoin), 3); err != nil {
			t.Errorf("%v: got %v, want nil", rejoin, err)
		}
	}
}

func TestUnreachable(t *testing.T) {
	refused := fmt.Errorf("unable to dial: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")})
	for err, want := range map[error]bool{
		refused:                       true,
		io.EOF:                        true,
		kerr.LeaderNotAvailable:       true,
		kerr.NotLeaderForPartition:    true,
		kerr.UnknownTopicOrPartition:  false,
		kerr.TopicAuthorizationFailed: false,
		errors.New("invalid payload"): false,
		fmt.Errorf("list: %w", kerr.UnknownTopicOrPartition): false,
	} {
		if got := unreachable(err); got != want {
			t.Errorf("unreachable(%v) = %v, want %v", err, got, want)
		}
	}
}

// newCluster starts an in-process Kafka with topic t (two partitions).
func newCluster(t *testing.T, opts ...kfake.Opt) (*kfake.Cluster, config.Config) {
	t.Helper()
	c, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1), kfake.SeedTopics(2, "t")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, testConfig(c.ListenAddrs()...)
}

func testConfig(brokers ...string) config.Config {
	return config.Config{Kafka: config.Kafka{BootstrapServers: brokers, TransactionTimeout: 10 * time.Second, MaxPartitionFetchBytes: 1 << 20}}
}

// producer is a transactional producer for tests.
func producer(t *testing.T, cfg config.Config, id string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.BootstrapServers...), kgo.TransactionalID(id),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// transaction writes values to partition p of t in one transaction.
func transaction(t *testing.T, cl *kgo.Client, p int32, commit kgo.TransactionEndTry, values ...string) {
	t.Helper()
	ctx := context.Background()
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for _, v := range values {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "t", Partition: p, Value: []byte(v)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.EndTransaction(ctx, commit); err != nil {
		t.Fatal(err)
	}
}

func readAll(ctx context.Context, cfg config.Config, partitions map[string][]int32, end End) ([]string, error) {
	var got []string
	err := ReadToEnd(ctx, cfg, slog.New(slog.DiscardHandler), "test", partitions, end, func(r *kgo.Record) error {
		got = append(got, fmt.Sprintf("%d:%s", r.Partition, r.Value))
		return nil
	})
	slices.Sort(got)
	return got, err
}

func TestReadToEndSkipsMarkersAndAbortedRecords(t *testing.T) {
	_, cfg := newCluster(t)
	cl := producer(t, cfg, "p")
	transaction(t, cl, 0, kgo.TryCommit, "a", "b")
	transaction(t, cl, 0, kgo.TryAbort, "aborted")
	transaction(t, cl, 1, kgo.TryCommit, "c")
	transaction(t, cl, 1, kgo.TryAbort, "aborted too") // the partition ends with aborted records

	got, err := readAll(context.Background(), cfg, map[string][]int32{"t": nil}, StableEnd)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"0:a", "0:b", "1:c"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	got, err = readAll(context.Background(), cfg, map[string][]int32{"t": {1}}, StableEnd)
	if err != nil || !slices.Equal(got, []string{"1:c"}) {
		t.Errorf("partition 1 only: got %v, %v", got, err)
	}
}

func TestReadToEndOfTheLogWaitsForOpenTransactions(t *testing.T) {
	_, cfg := newCluster(t)
	cl := producer(t, cfg, "p")
	transaction(t, cl, 0, kgo.TryCommit, "a")
	// A transaction that is still open when the reads start.
	ctx := context.Background()
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "t", Partition: 0, Value: []byte("open")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	stable, err := readAll(ctx, cfg, map[string][]int32{"t": {0}}, StableEnd)
	if err != nil || !slices.Equal(stable, []string{"0:a"}) {
		t.Errorf("up to the last stable offset: got %v, %v", stable, err)
	}
	done := make(chan []string)
	go func() {
		got, err := readAll(ctx, cfg, map[string][]int32{"t": {0}}, LogEnd)
		if err != nil {
			t.Error(err)
		}
		done <- got
	}()
	time.Sleep(300 * time.Millisecond)
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	if got := <-done; !slices.Equal(got, []string{"0:a", "0:open"}) {
		t.Errorf("up to the high watermark: got %v", got)
	}
}

func TestReadOnGivesUpWithoutProgress(t *testing.T) {
	_, cfg := newCluster(t)
	cl := producer(t, cfg, "p")
	ctx := context.Background()
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "t", Partition: 0, Value: []byte("open")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.BootstrapServers...), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords(), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"t": {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	start := time.Now()
	err = readOn(ctx, reader, map[TopicPartition]int64{{"t", 0}: 1}, 500*time.Millisecond, func(*kgo.Record) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "no progress for 500ms reading t partition 0 to its end") {
		t.Errorf("got %v", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("gave up after %v", took)
	}
}

// A partition without a leader elsewhere in the topic doesn't stop reading
// the partitions asked for.
func TestReadToEndListsOnlyThePartitionsItReads(t *testing.T) {
	c, cfg := newCluster(t)
	transaction(t, producer(t, cfg, "p"), 0, kgo.TryCommit, "a")
	c.ControlKey(kmsg.ListOffsets.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		r := req.(*kmsg.ListOffsetsRequest)
		for _, topic := range r.Topics {
			for _, p := range topic.Partitions {
				if p.Partition == 1 {
					resp := r.ResponseKind().(*kmsg.ListOffsetsResponse)
					rt := kmsg.NewListOffsetsResponseTopic()
					rt.Topic = topic.Topic
					rp := kmsg.NewListOffsetsResponseTopicPartition()
					rp.Partition, rp.ErrorCode = 1, kerr.LeaderNotAvailable.Code
					rt.Partitions = append(rt.Partitions, rp)
					resp.Topics = append(resp.Topics, rt)
					return resp, nil, true
				}
			}
		}
		return nil, nil, false
	})
	got, err := readAll(context.Background(), cfg, map[string][]int32{"t": {0}}, StableEnd)
	if err != nil || !slices.Equal(got, []string{"0:a"}) {
		t.Errorf("got %v, %v", got, err)
	}
}

// A role started before the brokers waits for them.
func TestReadToEndWaitsForTheBrokers(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close() // nothing listens there yet
	cfg := testConfig(fmt.Sprintf("127.0.0.1:%d", port))
	started := make(chan *kfake.Cluster)
	go func() {
		time.Sleep(1500 * time.Millisecond)
		c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.Ports(port), kfake.SeedTopics(1, "t"))
		if err != nil {
			t.Error(err)
		}
		started <- c
	}()
	_, err = readAll(context.Background(), cfg, map[string][]int32{"t": {0}}, StableEnd)
	if c := <-started; c != nil {
		c.Close()
	}
	if err != nil {
		t.Errorf("got %v, want the read to wait for the broker", err)
	}
}
