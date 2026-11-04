package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

// ClientID names one Kafka client of a process: <role>-<siteId>-<suffix>,
// where the suffix says what the client does, e.g. validator-2-blocks. The
// Java implementation names its clients the same way.
func ClientID(role string, siteID int, suffix string) string {
	return fmt.Sprintf("%s-%d-%s", role, siteID, suffix)
}

// ClientOpts are the options of every client: the brokers, its client.id
// (see ClientID) and logging.
func ClientOpts(cfg config.Config, log *slog.Logger, clientID string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Kafka.BootstrapServers...),
		kgo.ClientID(clientID),
		kgo.WithLogger(newLogger(log.With("client", clientID), cfg.LogLevel)),
	}
}

// The client settings below are the Java client's defaults, set explicitly
// in both implementations: both put the same records on the wire, and a
// library upgrade can't change them (and so the benchmarks) silently.

// MaxRequestBytes is the largest record batch a producer sends, Java's
// max.request.size. A block stays far below it (see the aggregator).
const MaxRequestBytes = 1 << 20

// ConsumerOpts are the options of every consumer: read only committed
// transactions, start at the beginning of a partition with no position, and
// cap the bytes fetched per partition (the thesis's "max fetch bytes").
func ConsumerOpts(cfg config.Config) []kgo.Opt {
	return []kgo.Opt{
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxPartitionBytes(cfg.Kafka.MaxPartitionFetchBytes),
		kgo.FetchMaxWait(500 * time.Millisecond), // Java's fetch.max.wait.ms
	}
}

// GroupOpts are the options of a group consumer.
func GroupOpts(cfg config.Config, group, instanceID string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		// Static membership: a member that restarts with the same instance ID
		// within the session timeout gets its partitions back without a
		// rebalance. A clean stop leaves the group explicitly (LeaveGroup).
		kgo.InstanceID(instanceID),
		// The range assignor with the classic group protocol. franz-go would
		// only use the KIP-848 protocol if asked to with ServerSideBalancer.
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.SessionTimeout(cfg.Kafka.SessionTimeout),
		kgo.HeartbeatInterval(cfg.Kafka.SessionTimeout / 3),
		// Java sends max.poll.interval.ms, 5 minutes, as its rebalance timeout.
		kgo.RebalanceTimeout(5 * time.Minute),
		kgo.DisableAutoCommit(),
		// Once a poll returns, the rebalance callbacks wait until
		// AllowRebalance: they never run while the polled records are being
		// processed. This doesn't guarantee that the member still owns the
		// partitions: a member that missed its session (a paused process)
		// processes them without knowing that the group gave them away. Then
		// fencing stops it: the session's generation-checked offset commit
		// for an aggregator, the bank's transactional.id for a validator.
		kgo.BlockRebalanceOnPoll(),
	}
}

// ProducerOpts are the options of every producer.
func ProducerOpts() []kgo.Opt {
	return []kgo.Opt{
		// Idempotent (franz-go's default) with acks=all. An idempotent
		// franz-go producer keeps up to five produce requests in flight per
		// broker, like Java's max.in.flight.requests.per.connection=5; the
		// option can't be set with idempotence.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.ProducerBatchMaxBytes(MaxRequestBytes),
		// A record that can't be delivered fails after this long (Java's
		// delivery.timeout.ms) instead of being retried forever.
		kgo.RecordDeliveryTimeout(120 * time.Second),
		// Records without a key go to one partition until 16 KiB were sent
		// there: Java's default partitioner with batch.size=16384.
		kgo.RecordPartitioner(partitioner{spread: kgo.UniformBytesPartitioner(16<<10, true, true, nil)}),
	}
}

// TransactionalOpts make a producer transactional.
func TransactionalOpts(cfg config.Config, transactionalID string) []kgo.Opt {
	return []kgo.Opt{
		kgo.TransactionalID(transactionalID),
		kgo.TransactionTimeout(cfg.Kafka.TransactionTimeout),
	}
}

// partitioner sends a record to the partition it names: every topic except
// trades is partitioned by bank, and the code picks the partition. Records to
// trades have no key and are spread by the sticky partitioner, like Kafka's
// default for records without a key.
type partitioner struct{ spread kgo.Partitioner }

func (p partitioner) ForTopic(topic string) kgo.TopicPartitioner {
	if topic == Trades {
		return p.spread.ForTopic(topic)
	}
	return kgo.ManualPartitioner().ForTopic(topic)
}

// logger passes franz-go's log lines to slog. franz-go logs only warnings
// and errors, or only errors when logLevel is error.
type logger struct {
	log   *slog.Logger
	level kgo.LogLevel
}

func newLogger(log *slog.Logger, level slog.Level) logger {
	if level >= slog.LevelError {
		return logger{log, kgo.LogLevelError}
	}
	return logger{log, kgo.LogLevelWarn}
}

func (l logger) Level() kgo.LogLevel { return l.level }

func (l logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	lvl := slog.LevelWarn
	if level == kgo.LogLevelError {
		lvl = slog.LevelError
	}
	l.log.Log(context.Background(), lvl, msg, keyvals...)
}
