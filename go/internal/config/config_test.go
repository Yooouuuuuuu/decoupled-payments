package config

import (
	"bytes"
	"io/fs"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestEmbeddedDefaultsMatchOriginal(t *testing.T) {
	original, err := os.ReadFile("../../../config/default.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, defaultYAML) {
		t.Fatal("internal/config/default.yaml differs from config/default.yaml: run go generate ./...")
	}
}

// sources builds Sources from an environment map and in-memory files.
func sources(role string, env map[string]string, files map[string]string, flags ...Flag) Sources {
	return Sources{
		Role:  role,
		Flags: flags,
		LookupEnv: func(name string) (string, bool) {
			v, ok := env[name]
			return v, ok
		},
		ReadFile: func(name string) ([]byte, error) {
			if data, ok := files[name]; ok {
				return []byte(data), nil
			}
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		},
	}
}

func flag(name, value string) Flag { return Flag{Name: name, Value: value, HasValue: true} }

func mustLoad(t *testing.T, src Sources) Config {
	t.Helper()
	cfg, err := Load(src)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func loadErr(t *testing.T, src Sources, wantParts ...string) {
	t.Helper()
	_, err := Load(src)
	if err == nil {
		t.Fatalf("Load succeeded, want an error mentioning %q", wantParts)
	}
	for _, part := range wantParts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q doesn't mention %q", err, part)
		}
	}
}

func TestDefaults(t *testing.T) {
	cfg := mustLoad(t, sources("validator", nil, nil))
	want := Config{
		Model:             Concurrent,
		KafkaTransactions: true,
		SiteID:            1,
		LogLevel:          slog.LevelInfo,
		StatsInterval:     5 * time.Second,
		Kafka: Kafka{
			BootstrapServers:       []string{"localhost:9092"},
			SessionTimeout:         10 * time.Second,
			TransactionTimeout:     60 * time.Second,
			MaxPartitionFetchBytes: 1048576,
		},
		Topics:     Topics{ReplicationFactor: 1, TradesPartitions: 3},
		Bank:       Bank{Count: 3, AccountsPerBank: 100, InitialBalance: 10000},
		Generator:  Generator{RPS: 1000, Trades: 10000, Distribution: Uniform, ZipfExponent: 1.0, MinAmount: 1, MaxAmount: 100, Seed: 42},
		Aggregator: Aggregator{BlockSize: 500, BlockTimeout: time.Second, MaxPollRecords: 2000},
		Validator:  Validator{BlocksPerTransaction: 1, CreditsPerTransaction: 500},
		Report:     Report{Timeout: time.Minute},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("defaults:\n got %+v\nwant %+v", cfg, want)
	}
}

func TestEverySettingIsUsed(t *testing.T) {
	s, err := Defaults()
	if err != nil {
		t.Fatal(err)
	}
	fromSettings(s)
	for _, key := range s.Keys() {
		if !s.read[key] {
			t.Errorf("%s is in default.yaml but Config doesn't read it", key)
		}
	}
}

func TestEnvName(t *testing.T) {
	for key, want := range map[string]string{
		"aggregator.blockSize":       "DP_AGGREGATOR_BLOCK_SIZE",
		"kafkaTransactions":          "DP_KAFKA_TRANSACTIONS",
		"kafka.transactionTimeoutMs": "DP_KAFKA_TRANSACTION_TIMEOUT_MS",
		"siteId":                     "DP_SITE_ID",
		"bank.accountsPerBank":       "DP_BANK_ACCOUNTS_PER_BANK",
		"report.json":                "DP_REPORT_JSON",
		"a2B.c":                      "DP_A2_B_C",
	} {
		if got := EnvName(key); got != want {
			t.Errorf("EnvName(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestEnvNamesDontCollide(t *testing.T) {
	s, err := Defaults()
	if err != nil {
		t.Fatal(err)
	}
	owner := map[string]string{ConfigEnv: "the config file variable"}
	for _, key := range s.Keys() {
		name := EnvName(key)
		if other, ok := owner[name]; ok {
			t.Errorf("%s and %s both map to %s", key, other, name)
		}
		owner[name] = key
	}
}

func TestLayersLaterWins(t *testing.T) {
	files := map[string]string{"site.yaml": "siteId: 2\naggregator:\n  blockSize: 50\n  blockTimeoutMs: 20\n"}
	env := map[string]string{"DP_AGGREGATOR_BLOCK_SIZE": "60", "DP_AGGREGATOR_MAX_POLL_RECORDS": "70"}
	src := sources("aggregator", env, files, flag("maxPollRecords", "80"))
	src.ConfigFile = "site.yaml"
	cfg := mustLoad(t, src)
	if cfg.SiteID != 2 || cfg.Aggregator.BlockTimeout != 20*time.Millisecond {
		t.Errorf("config file not applied: %+v", cfg)
	}
	if cfg.Aggregator.BlockSize != 60 {
		t.Errorf("blockSize = %d, want 60 (environment beats the config file)", cfg.Aggregator.BlockSize)
	}
	if cfg.Aggregator.MaxPollRecords != 80 {
		t.Errorf("maxPollRecords = %d, want 80 (flags beat the environment)", cfg.Aggregator.MaxPollRecords)
	}
}

func TestConfigFileFromEnvironment(t *testing.T) {
	files := map[string]string{"a.yaml": "siteId: 3\n", "b.yaml": "siteId: 4\n"}
	env := map[string]string{ConfigEnv: "a.yaml"}
	if cfg := mustLoad(t, sources("validator", env, files)); cfg.SiteID != 3 {
		t.Errorf("siteId = %d, want 3 from $DP_CONFIG", cfg.SiteID)
	}
	src := sources("validator", env, files)
	src.ConfigFile = "b.yaml"
	if cfg := mustLoad(t, src); cfg.SiteID != 4 {
		t.Errorf("siteId = %d, want 4: --config beats $DP_CONFIG", cfg.SiteID)
	}
	src.ConfigFile = "missing.yaml"
	loadErr(t, src, "missing.yaml")
}

func TestRoleRelativeFlags(t *testing.T) {
	cfg := mustLoad(t, sources("aggregator", nil, nil, flag("blockSize", "7"), flag("bank.count", "4")))
	if cfg.Aggregator.BlockSize != 7 || cfg.Bank.Count != 4 {
		t.Errorf("got blockSize %d, bank.count %d; want 7, 4", cfg.Aggregator.BlockSize, cfg.Bank.Count)
	}
	cfg = mustLoad(t, sources("init", nil, nil, Flag{Name: "reset"}))
	if !cfg.Init.Reset {
		t.Error("dp init --reset didn't set init.reset")
	}
	cfg = mustLoad(t, sources("report", nil, nil, Flag{Name: "json"}, flag("kafkaTransactions", "false")))
	if !cfg.Report.JSON || cfg.KafkaTransactions {
		t.Errorf("got report.json %v, kafkaTransactions %v", cfg.Report.JSON, cfg.KafkaTransactions)
	}
	// A setting of another role's section needs its full key.
	loadErr(t, sources("validator", nil, nil, flag("blockSize", "7")), "unknown flag --blockSize")
	loadErr(t, sources("aggregator", nil, nil, flag("count", "4")), "unknown flag --count")
}

func TestParseErrorsNameKeyAndSource(t *testing.T) {
	loadErr(t, sources("validator", map[string]string{"DP_SITE_ID": "two"}, nil),
		"siteId", "environment variable DP_SITE_ID", "integer")
	loadErr(t, sources("init", nil, nil, flag("reset", "yes")), "init.reset", "flag --reset", "true or false")
	loadErr(t, sources("generator", nil, nil, flag("zipfExponent", "x")), "generator.zipfExponent", "number")
	loadErr(t, sources("generator", nil, nil, Flag{Name: "rps"}), "--rps needs a value")
	loadErr(t, sources("generator", map[string]string{"DP_GENERATOR_RPS": ""}, nil), "generator.rps", "DP_GENERATOR_RPS")

	files := map[string]string{
		"unknown.yaml": "aggregator:\n  blocksize: 5\n",
		"type.yaml":    "bank:\n  count: three\n",
		"list.yaml":    "kafka:\n  bootstrapServers: [a, b]\n",
		"broken.yaml":  "siteId: [\n",
	}
	for file, want := range map[string]string{
		"unknown.yaml": "unknown setting aggregator.blocksize (from config file unknown.yaml, line 2)",
		"type.yaml":    "invalid value \"three\" for bank.count (from config file type.yaml, line 2)",
		"list.yaml":    "kafka.bootstrapServers is a list, but a setting takes a single value (from config file list.yaml, line 2)",
		"broken.yaml":  "config file broken.yaml",
	} {
		src := sources("init", nil, files)
		src.ConfigFile = file
		loadErr(t, src, want)
	}
}

// TestValidationBoundaries checks every range of the validation table at
// and just beyond its limits (implementation spec §16.6, the same in Java).
func TestValidationBoundaries(t *testing.T) {
	for _, tc := range []struct {
		key          string
		okLo, okHi   string
		badLo, badHi string // "" when the limit is the type's own
		badLoMessage string
	}{
		{"siteId", "0", "2147483647", "-1", "2147483648", "siteId must be between 0 and 2147483647"},
		{"statsIntervalMs", "1", "9223372036854", "0", "9223372036855", "statsIntervalMs must be between 1 and 9223372036854"},
		{"kafka.sessionTimeoutMs", "1", "2147483647", "0", "2147483648", "kafka.sessionTimeoutMs must be between 1"},
		{"kafka.transactionTimeoutMs", "1", "2147483647", "0", "2147483648", "kafka.transactionTimeoutMs must be between 1"},
		{"kafka.maxPartitionFetchBytes", "1", "2147483647", "0", "2147483648", "kafka.maxPartitionFetchBytes must be between 1"},
		{"topics.replicationFactor", "1", "32767", "0", "32768", "topics.replicationFactor must be between 1 and 32767"},
		{"topics.tradesPartitions", "1", "2147483647", "0", "2147483648", "topics.tradesPartitions must be between 1"},
		{"bank.count", "1", "10000", "0", "10001", "bank.count must be between 1 and 10000"},
		{"bank.accountsPerBank", "1", "50000", "0", "50001", "bank.accountsPerBank must be between 1 and 50000"},
		{"generator.rps", "1", "9223372036854775807", "0", "", "generator.rps must be between 1"},
		{"generator.trades", "0", "4294967295", "-1", "4294967296", "generator.trades must be between 0 and 4294967295"},
		{"generator.zipfExponent", "0", "5", "-0.1", "5.0001", "generator.zipfExponent must be a number between 0 and 5"},
		{"aggregator.blockSize", "1", "2147483647", "0", "2147483648", "aggregator.blockSize must be between 1"},
		{"aggregator.maxPollRecords", "1", "2147483647", "0", "2147483648", "aggregator.maxPollRecords must be between 1"},
		{"validator.blocksPerTransaction", "1", "2147483647", "0", "2147483648", "validator.blocksPerTransaction must be between 1"},
		{"validator.creditsPerTransaction", "1", "2147483647", "0", "2147483648", "validator.creditsPerTransaction must be between 1"},
		{"aggregator.blockTimeoutMs", "0", "9223372036854", "-1", "9223372036855", "aggregator.blockTimeoutMs must be between 0 and 9223372036854"},
		{"report.timeoutMs", "0", "9223372036854", "-1", "9223372036855", "report.timeoutMs must be between 0 and 9223372036854"},
	} {
		for _, ok := range []string{tc.okLo, tc.okHi} {
			if _, err := Load(sources("validator", nil, nil, flag(tc.key, ok))); err != nil {
				t.Errorf("%s=%s: %v", tc.key, ok, err)
			}
		}
		loadErr(t, sources("validator", nil, nil, flag(tc.key, tc.badLo)), tc.badLoMessage, "(from flag --"+tc.key+")")
		if tc.badHi != "" {
			loadErr(t, sources("validator", nil, nil, flag(tc.key, tc.badHi)), tc.key, "(from flag --"+tc.key+")")
		}
	}
}

func TestValidationOfChoicesAndCombinations(t *testing.T) {
	for _, tc := range []struct {
		flags []Flag
		want  string
	}{
		{[]Flag{flag("model", "parallel")}, "model must be one of sequential, concurrent"},
		{[]Flag{flag("generator.distribution", "normal")}, "generator.distribution must be one of uniform, zipf"},
		{[]Flag{flag("logLevel", "trace")}, "logLevel must be one of debug, info, warn, error"},
		{[]Flag{flag("bank.initialBalance", "-1")}, "bank.initialBalance must be between 0"},
		{[]Flag{flag("generator.minAmount", "0")}, "generator.minAmount must be between 1"},
		{[]Flag{flag("generator.minAmount", "50"), flag("generator.maxAmount", "49")}, "generator.maxAmount must be between 50"},
		{[]Flag{flag("kafka.bootstrapServers", "")}, "kafka.bootstrapServers must list brokers"},
		{[]Flag{flag("kafka.bootstrapServers", " , ")}, "kafka.bootstrapServers must list brokers"},
		{[]Flag{flag("kafka.bootstrapServers", "a:1,")}, "kafka.bootstrapServers must list brokers"},
		{[]Flag{flag("kafka.bootstrapServers", ",a:1")}, "kafka.bootstrapServers must list brokers"},
	} {
		loadErr(t, sources("validator", nil, nil, tc.flags...), tc.want, "(from flag --")
	}
	loadErr(t, sources("validator", nil, nil, flag("bank.initialBalance", "30744573456182587")),
		"the total money bank.count × bank.accountsPerBank × bank.initialBalance must be at most 9223372036854775807, got 3 × 100 × 30744573456182587",
		"bank.count from default.yaml", "bank.initialBalance from flag --bank.initialBalance")
	// The total money must fit in 64 bits; with 3 banks × 100 accounts, this
	// is the largest initial balance that does.
	if _, err := Load(sources("validator", nil, nil, flag("bank.initialBalance", "30744573456182586"))); err != nil {
		t.Errorf("the largest total money that fits: %v", err)
	}
	// The most accounts there can be, 10,000 banks of 50,000: 18446744073 is
	// the largest initial balance whose total fits.
	if _, err := Load(sources("validator", nil, nil, flag("bank.count", "10000"), flag("bank.accountsPerBank", "50000"),
		flag("bank.initialBalance", "18446744073"))); err != nil {
		t.Errorf("the most accounts with the largest balance that fits: %v", err)
	}
	loadErr(t, sources("validator", nil, nil, flag("bank.count", "10000"), flag("bank.accountsPerBank", "50000"),
		flag("bank.initialBalance", "18446744074")), "the total money bank.count × bank.accountsPerBank × bank.initialBalance must be at most")
	// One huge bank account is fine too.
	if _, err := Load(sources("validator", nil, nil, flag("bank.count", "1"), flag("bank.accountsPerBank", "1"),
		flag("bank.initialBalance", "9223372036854775807"))); err != nil {
		t.Errorf("one account with the largest balance: %v", err)
	}
	// Every problem is reported at once, one per line.
	_, err := Load(sources("validator", nil, nil, flag("model", "x"), flag("bank.count", "0"), flag("siteId", "-1")))
	if lines := strings.Split(err.Error(), "\n"); len(lines) != 3 {
		t.Errorf("want 3 problems, got %d: %v", len(lines), err)
	}
}

func TestStrictNumberSyntax(t *testing.T) {
	// The same text means the same in every source and in Java: decimal only.
	for _, tc := range []struct{ key, value string }{
		{"aggregator.blockSize", "0x10"},
		{"aggregator.blockSize", "1_000"},
		{"aggregator.blockSize", "+5"},
		{"aggregator.blockSize", "5.0"},
		{"aggregator.blockSize", "99999999999999999999"},
		{"generator.zipfExponent", "0x1p-2"},
		{"generator.zipfExponent", "1_0.5"},
		{"generator.zipfExponent", "NaN"},
		{"generator.zipfExponent", "Inf"},
		{"generator.zipfExponent", ".5"},
		{"generator.zipfExponent", "5."},
		{"generator.zipfExponent", "1e400"},
		{"init.reset", "yes"},
		{"init.reset", "True"},
	} {
		loadErr(t, sources("init", nil, nil, flag(tc.key, tc.value)), "invalid value", tc.key)
	}
	cfg := mustLoad(t, sources("generator", map[string]string{"DP_AGGREGATOR_BLOCK_SIZE": "010"}, nil,
		flag("zipfExponent", "2.5e-1")))
	if cfg.Aggregator.BlockSize != 10 || cfg.Generator.ZipfExponent != 0.25 {
		t.Errorf("blockSize %d (010 is decimal 10), zipfExponent %v", cfg.Aggregator.BlockSize, cfg.Generator.ZipfExponent)
	}
}

func TestBootstrapServersList(t *testing.T) {
	cfg := mustLoad(t, sources("validator", map[string]string{"DP_KAFKA_BOOTSTRAP_SERVERS": " a:1, b:2 "}, nil))
	if !slices.Equal(cfg.Kafka.BootstrapServers, []string{"a:1", "b:2"}) {
		t.Errorf("got %q", cfg.Kafka.BootstrapServers)
	}
}

func TestEmptyConfigEnvIsUnset(t *testing.T) {
	if cfg := mustLoad(t, sources("validator", map[string]string{ConfigEnv: ""}, nil)); cfg.SiteID != 1 {
		t.Errorf("siteId = %d, want the default", cfg.SiteID)
	}
}

func TestHelpTextIsTheDefaultAsWritten(t *testing.T) {
	s, err := Defaults()
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"generator.zipfExponent": "1.0", "aggregator.blockSize": "500", "init.reset": "false", "model": "concurrent"} {
		if got := s.Text(key); got != want {
			t.Errorf("Text(%s) = %q, want %q", key, got, want)
		}
	}
}
