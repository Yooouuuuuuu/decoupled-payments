// Package config loads the settings of every role. The defaults are an
// embedded copy of config/default.yaml; a YAML file, environment variables
// and command-line flags override them, in that order (later wins).
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"time"
)

// Model is how credits travel from the sender's validator to the receiver's
// (docs/models).
type Model string

const (
	Sequential Model = "sequential" // back through the trades and blocks topics
	Concurrent Model = "concurrent" // through the credits topic, applied by a second thread
)

// ConfigEnv names a YAML file to load, like --config=<file>.
const ConfigEnv = "DP_CONFIG"

// Config is the typed view of the settings. config/default.yaml documents
// each one.
type Config struct {
	Model             Model
	KafkaTransactions bool
	SiteID            int
	LogLevel          slog.Level
	StatsInterval     time.Duration
	Kafka             Kafka
	Topics            Topics
	Bank              Bank
	Init              Init
	Generator         Generator
	Aggregator        Aggregator
	Validator         Validator
	Report            Report
}

type Kafka struct {
	BootstrapServers       []string
	SessionTimeout         time.Duration
	TransactionTimeout     time.Duration
	MaxPartitionFetchBytes int32
}

type Topics struct {
	ReplicationFactor int16
	TradesPartitions  int32
}

type Bank struct {
	Count           int32
	AccountsPerBank int32
	InitialBalance  int64
}

type Init struct {
	Reset bool
}

type Generator struct {
	RPS          int64
	Trades       int64 // 0 = until stopped
	Distribution string
	ZipfExponent float64
	MinAmount    int64
	MaxAmount    int64
	Seed         int64
}

type Aggregator struct {
	BlockSize      int
	BlockTimeout   time.Duration
	MaxPollRecords int
}

type Validator struct {
	BlocksPerTransaction  int
	CreditsPerTransaction int
}

type Report struct {
	Timeout time.Duration
	JSON    bool
}

// Generator distributions.
const (
	Uniform = "uniform"
	Zipf    = "zipf"
)

// Sources are the layers on top of the embedded defaults, lowest first.
type Sources struct {
	ConfigFile string // --config=<file>; when empty, $DP_CONFIG if set
	LookupEnv  func(string) (string, bool)
	ReadFile   func(string) ([]byte, error)
	Role       string // the role being run, for role-relative flags
	Flags      []Flag
}

// Defaults returns the settings of the embedded default.yaml.
func Defaults() (*Settings, error) { return parseDefaults(defaultYAML) }

// Load layers the sources over the defaults and validates the result. Every
// error it returns is a usage or configuration error (exit status 2); a
// failed validation lists every problem, one per line.
func Load(src Sources) (Config, error) {
	s, err := Defaults()
	if err != nil {
		return Config{}, err
	}
	file := src.ConfigFile
	if file == "" {
		file, _ = src.LookupEnv(ConfigEnv) // an empty DP_CONFIG counts as unset
	}
	if file != "" {
		data, err := src.ReadFile(file)
		if err != nil {
			return Config{}, fmt.Errorf("config file: %w", err)
		}
		if err := s.applyYAML(data, file); err != nil {
			return Config{}, err
		}
	}
	if err := s.applyEnv(src.LookupEnv); err != nil {
		return Config{}, err
	}
	if err := s.applyFlags(src.Role, src.Flags); err != nil {
		return Config{}, err
	}
	if err := validate(s); err != nil {
		return Config{}, err
	}
	return fromSettings(s), nil
}

// fromSettings converts validated settings, so the narrowing conversions
// below can't overflow.
func fromSettings(s *Settings) Config {
	return Config{
		Model:             Model(s.string("model")),
		KafkaTransactions: s.bool("kafkaTransactions"),
		SiteID:            int(s.int("siteId")),
		LogLevel:          logLevels[s.string("logLevel")],
		StatsInterval:     millis(s.int("statsIntervalMs")),
		Kafka: Kafka{
			BootstrapServers:       servers(s.string("kafka.bootstrapServers")),
			SessionTimeout:         millis(s.int("kafka.sessionTimeoutMs")),
			TransactionTimeout:     millis(s.int("kafka.transactionTimeoutMs")),
			MaxPartitionFetchBytes: int32(s.int("kafka.maxPartitionFetchBytes")),
		},
		Topics: Topics{
			ReplicationFactor: int16(s.int("topics.replicationFactor")),
			TradesPartitions:  int32(s.int("topics.tradesPartitions")),
		},
		Bank: Bank{
			Count:           int32(s.int("bank.count")),
			AccountsPerBank: int32(s.int("bank.accountsPerBank")),
			InitialBalance:  s.int("bank.initialBalance"),
		},
		Init: Init{Reset: s.bool("init.reset")},
		Generator: Generator{
			RPS:          s.int("generator.rps"),
			Trades:       s.int("generator.trades"),
			Distribution: s.string("generator.distribution"),
			ZipfExponent: s.float("generator.zipfExponent"),
			MinAmount:    s.int("generator.minAmount"),
			MaxAmount:    s.int("generator.maxAmount"),
			Seed:         s.int("generator.seed"),
		},
		Aggregator: Aggregator{
			BlockSize:      int(s.int("aggregator.blockSize")),
			BlockTimeout:   millis(s.int("aggregator.blockTimeoutMs")),
			MaxPollRecords: int(s.int("aggregator.maxPollRecords")),
		},
		Validator: Validator{
			BlocksPerTransaction:  int(s.int("validator.blocksPerTransaction")),
			CreditsPerTransaction: int(s.int("validator.creditsPerTransaction")),
		},
		Report: Report{
			Timeout: millis(s.int("report.timeoutMs")),
			JSON:    s.bool("report.json"),
		},
	}
}

var logLevels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

func millis(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

// servers splits kafka.bootstrapServers into its entries.
func servers(list string) []string {
	entries := strings.Split(list, ",")
	for i, e := range entries {
		entries[i] = strings.TrimSpace(e)
	}
	return entries
}

// maxMillis is the longest duration in ms that fits a time.Duration.
const maxMillis = math.MaxInt64 / int64(time.Millisecond)

// maxBanks and maxAccountsPerBank keep accountInfo records small: init
// writes each bank's account list as one record, which must stay under
// Kafka's 1 MiB message limit. With bank IDs up to bank9999 and at most this
// many accounts, it stays under 800 KB.
const (
	maxBanks           = 10_000
	maxAccountsPerBank = 50_000
)

// maxZipfExponent bounds generator.zipfExponent: with a higher exponent
// nearly every draw is the top-ranked account, and picking a receiver other
// than the sender would take almost forever.
const maxZipfExponent = 5

// validate checks the settings that would otherwise fail later or make no
// sense, and reports every problem at once. Java checks the same table.
func validate(s *Settings) error {
	var errs []error
	fail := func(key, format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s %s (from %s)", key, fmt.Sprintf(format, args...), s.source(key)))
	}
	oneOf := func(key string, allowed ...string) {
		if v := s.string(key); !slices.Contains(allowed, v) {
			fail(key, "must be one of %s, got %q", strings.Join(allowed, ", "), v)
		}
	}
	between := func(key string, lo, hi int64) bool {
		v := s.int(key)
		if v < lo || v > hi {
			fail(key, "must be between %d and %d, got %d", lo, hi, v)
		}
		return v >= lo && v <= hi
	}

	oneOf("model", string(Sequential), string(Concurrent))
	oneOf("logLevel", "debug", "info", "warn", "error")
	oneOf("generator.distribution", Uniform, Zipf)
	between("siteId", 0, math.MaxInt32)
	between("statsIntervalMs", 1, maxMillis)
	if list := s.string("kafka.bootstrapServers"); slices.Contains(servers(list), "") {
		fail("kafka.bootstrapServers", "must list brokers as host:port, separated by commas, with no empty entry, got %q", list)
	}
	between("kafka.sessionTimeoutMs", 1, math.MaxInt32)
	between("kafka.transactionTimeoutMs", 1, math.MaxInt32)
	between("kafka.maxPartitionFetchBytes", 1, math.MaxInt32)
	between("topics.replicationFactor", 1, math.MaxInt16)
	between("topics.tradesPartitions", 1, math.MaxInt32)
	banks := between("bank.count", 1, maxBanks)
	accounts := between("bank.accountsPerBank", 1, maxAccountsPerBank)
	balance := between("bank.initialBalance", 0, math.MaxInt64)
	// Money only moves between accounts, so if the total fits in 64 bits, no
	// balance and no sum of balances can overflow.
	if banks && accounts && balance && !totalMoneyFits(s.int("bank.count"), s.int("bank.accountsPerBank"), s.int("bank.initialBalance")) {
		errs = append(errs, fmt.Errorf("the total money bank.count × bank.accountsPerBank × bank.initialBalance must be at most %d, got %d × %d × %d (bank.count from %s, bank.accountsPerBank from %s, bank.initialBalance from %s)",
			int64(math.MaxInt64), s.int("bank.count"), s.int("bank.accountsPerBank"), s.int("bank.initialBalance"),
			s.source("bank.count"), s.source("bank.accountsPerBank"), s.source("bank.initialBalance")))
	}
	between("generator.rps", 1, math.MaxInt64)
	// The sequence number is the low 32 bits of a trade's serial number.
	between("generator.trades", 0, math.MaxUint32)
	if v := s.float("generator.zipfExponent"); !(v >= 0 && v <= maxZipfExponent) { // false for NaN too
		fail("generator.zipfExponent", "must be a number between 0 and %d, got %v", maxZipfExponent, v)
	}
	between("generator.minAmount", 1, math.MaxInt64)
	between("generator.maxAmount", max(1, s.int("generator.minAmount")), math.MaxInt64)
	between("aggregator.blockSize", 1, math.MaxInt32)
	between("aggregator.maxPollRecords", 1, math.MaxInt32)
	between("validator.blocksPerTransaction", 1, math.MaxInt32)
	between("validator.creditsPerTransaction", 1, math.MaxInt32)
	between("aggregator.blockTimeoutMs", 0, maxMillis)
	between("report.timeoutMs", 0, maxMillis)
	return errors.Join(errs...)
}

// totalMoneyFits reports whether banks × accountsPerBank × initialBalance,
// the total money, fits in an int64, computed without overflowing. The
// factors are not negative.
func totalMoneyFits(banks, accountsPerBank, initialBalance int64) bool {
	accounts := banks * accountsPerBank // both are at most 2^31-1: no overflow
	return initialBalance == 0 || accounts <= math.MaxInt64/initialBalance
}
