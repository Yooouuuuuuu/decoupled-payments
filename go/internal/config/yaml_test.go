package config

import (
	"strings"
	"testing"
)

// loadFile loads the defaults with one --config file.
func loadFile(content string) (Config, error) {
	src := sources("init", nil, map[string]string{"site.yaml": content})
	src.ConfigFile = "site.yaml"
	return Load(src)
}

func TestYAMLValuesAreParsedAsText(t *testing.T) {
	// The library never types a value: 010 is decimal 10 (YAML 1.1 says 8),
	// a quoted number is a number, and a string may look like anything.
	cfg, err := loadFile("topics:\n  tradesPartitions: 010\nsiteId: \"7\"\ngenerator:\n  zipfExponent: 2\nkafka:\n  bootstrapServers: yes:9092\n")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Topics.TradesPartitions != 10 || cfg.SiteID != 7 || cfg.Generator.ZipfExponent != 2 || cfg.Kafka.BootstrapServers[0] != "yes:9092" {
		t.Errorf("got %+v", cfg)
	}
}

func TestYAMLErrorsNameTheKeyAndTheFile(t *testing.T) {
	for name, tc := range map[string]struct{ content, want string }{
		"yes is not a boolean": {"init:\n  reset: yes\n", `invalid value "yes" for init.reset (from config file site.yaml, line 2): want true or false`},
		"underscores":          {"aggregator:\n  blockSize: 1_000\n", `invalid value "1_000" for aggregator.blockSize (from config file site.yaml, line 2)`},
		"hex":                  {"aggregator:\n  blockSize: 0x10\n", `invalid value "0x10" for aggregator.blockSize (from config file site.yaml, line 2)`},
		"~":                    {"siteId: ~\n", "siteId has no value (from config file site.yaml, line 1)"},
		"null":                 {"kafka:\n  bootstrapServers: null\n", "kafka.bootstrapServers has no value (from config file site.yaml, line 2)"},
		"an empty value":       {"siteId:\n", "siteId has no value (from config file site.yaml, line 1)"},
		"a list":               {"kafka:\n  bootstrapServers: [a:1, b:2]\n", "kafka.bootstrapServers is a list, but a setting takes a single value (from config file site.yaml, line 2)"},
		"a duplicate key":      {"siteId: 2\nsiteId: 3\n", "siteId appears twice (from config file site.yaml, line 2)"},
		"a duplicate section":  {"bank:\n  count: 2\nbank:\n  accountsPerBank: 5\n", "bank appears twice (from config file site.yaml, line 3)"},
		"an alias":             {"siteId: &id 2\naggregator:\n  blockSize: *id\n", "siteId uses a YAML anchor or alias"},
		"an alias only":        {"bank:\n  count: 2\n  accountsPerBank: *x\n", "config file site.yaml"},
		"two documents":        {"siteId: 2\n---\nsiteId: 3\n", "config file site.yaml: holds more than one YAML document"},
		"an unknown key":       {"aggregator:\n  blocksize: 5\n", "unknown setting aggregator.blocksize (from config file site.yaml, line 2)"},
		"a section as a value": {"kafka: localhost:9092\n", "kafka is a section of settings, not a single value (from config file site.yaml, line 1)"},
		"a value as a section": {"siteId:\n  value: 2\n", "siteId takes a single value, not a section (from config file site.yaml, line 2)"},
		"a top-level list":     {"- siteId: 2\n", "config file site.yaml: line 1: expected settings (key: value)"},
		"a top-level value":    {"siteId\n", "config file site.yaml: line 1: expected settings (key: value)"},
	} {
		_, err := loadFile(tc.content)
		if err == nil {
			t.Errorf("%s: loaded without an error", name)
			continue
		}
		if got := err.Error(); !strings.Contains(got, tc.want) {
			t.Errorf("%s: got %q, want it to contain %q", name, got, tc.want)
		}
	}
}

func TestYAMLFilesWithoutSettings(t *testing.T) {
	defaults, err := loadFile("")
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"an empty file":        "",
		"only comments":        "# nothing to override\n\n# really\n",
		"only ~":               "~\n",
		"only a document mark": "---\n",
		"an empty mapping":     "{}\n",
		"an empty section":     "kafka: {}\n",
	} {
		cfg, err := loadFile(content)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if cfg.SiteID != defaults.SiteID || cfg.Aggregator != defaults.Aggregator {
			t.Errorf("%s: changed a setting: %+v", name, cfg)
		}
	}
}
