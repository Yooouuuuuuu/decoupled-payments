// Package cli runs dp: it parses the command line, loads the configuration,
// and runs one role.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/roles/aggregator"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/roles/generator"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/roles/initrole"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/roles/report"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/roles/validator"
)

// Exit statuses.
const (
	ExitOK      = 0 // success, or a clean shutdown
	ExitFailure = 1 // a runtime failure, including a report that failed
	ExitUsage   = 2 // a usage or configuration error
)

// Env is what dp takes from the process: replaceable in tests.
type Env struct {
	Stdout, Stderr io.Writer
	LookupEnv      func(string) (string, bool)
	ReadFile       func(string) ([]byte, error)
}

type role struct {
	name    string
	summary string
	run     func(context.Context, config.Config, *slog.Logger, io.Writer) error
	// runsUntilStopped: a stop signal (SIGTERM, SIGINT) ends the role
	// cleanly, with exit status 0, even while it is still starting. The
	// other roles (init, report) have no result when interrupted: exit 1.
	runsUntilStopped bool
}

var roles = []role{
	{"init", "create the topics and the accounts (--reset deletes everything first)", quiet(initrole.Run), false},
	{"generator", "send trades between random accounts to the trades topic", quiet(generator.Run), true},
	{"aggregator", "group trades into blocks per bank (layer 1)", quiet(aggregator.Run), true},
	{"validator", "validate trades and apply credits for the banks it owns (layer 2)", quiet(validator.Run), true},
	{"report", "check that every trade was processed exactly once; measure throughput and latency", report.Run, false},
}

// quiet adapts a role that prints nothing but logs.
func quiet(run func(context.Context, config.Config, *slog.Logger) error) func(context.Context, config.Config, *slog.Logger, io.Writer) error {
	return func(ctx context.Context, cfg config.Config, log *slog.Logger, _ io.Writer) error {
		return run(ctx, cfg, log)
	}
}

func findRole(name string) (role, bool) {
	for _, r := range roles {
		if r.name == name {
			return r, true
		}
	}
	return role{}, false
}

// Run runs dp with the arguments after the program name and returns the exit
// status. A stop signal cancels ctx.
func Run(ctx context.Context, args []string, env Env) int {
	inv, err := parseArgs(args)
	if err != nil {
		return usageError(env.Stderr, err)
	}
	if inv.help {
		writeHelp(env.Stdout, inv.role)
		return ExitOK
	}
	cfg, err := config.Load(config.Sources{
		ConfigFile: inv.configFile,
		LookupEnv:  env.LookupEnv,
		ReadFile:   env.ReadFile,
		Role:       inv.role.name,
		Flags:      inv.flags,
	})
	if err != nil {
		return usageError(env.Stderr, err)
	}

	log := slog.New(slog.NewTextHandler(env.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	log.Info("starting", "role", inv.role.name, "siteId", cfg.SiteID, "model", cfg.Model,
		"kafkaTransactions", cfg.KafkaTransactions, "bootstrapServers", strings.Join(cfg.Kafka.BootstrapServers, ","))
	err = inv.role.run(ctx, cfg, log, env.Stdout)
	stopped := ctx.Err() != nil
	switch {
	case err == nil:
		return ExitOK
	case stopped && !inv.role.runsUntilStopped:
		log.Error("interrupted: " + inv.role.name + " did not finish")
		return ExitFailure
	case stopped && errors.Is(err, context.Canceled):
		return ExitOK // stopped while starting, e.g. while reading accountInfo
	case errors.Is(err, report.ErrFailed):
		return ExitFailure // the report says why
	default:
		log.Error("failed", "role", inv.role.name, "err", err)
		return ExitFailure
	}
}

// usageError prints a usage or configuration error, one line per problem,
// and returns the exit status for it.
func usageError(w io.Writer, err error) int {
	for _, problem := range strings.Split(err.Error(), "\n") {
		fmt.Fprintf(w, "dp: %s\n", problem)
	}
	fmt.Fprintln(w, "Run 'dp --help' for usage.")
	return ExitUsage
}

// invocation is a parsed command line.
type invocation struct {
	role       role // zero for "dp --help"
	help       bool
	configFile string
	flags      []config.Flag
}

// parseArgs parses "<role> [flags]" or "--help". Flags are --key=value or
// --key; config resolves the keys.
func parseArgs(args []string) (invocation, error) {
	var inv invocation
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		r, ok := findRole(args[0])
		if !ok {
			return inv, fmt.Errorf("unknown role %q", args[0])
		}
		inv.role, args = r, args[1:]
	}
	for _, arg := range args {
		name, value, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		switch {
		case arg == "--help" || arg == "-h":
			inv.help = true
		case !strings.HasPrefix(arg, "--") || name == "":
			return inv, fmt.Errorf("unexpected argument %q: flags look like --key=value", arg)
		case name == "help":
			return inv, fmt.Errorf("--help takes no value, got %q", arg)
		case name == "config" && (!hasValue || value == ""):
			return inv, errors.New("--config needs a file: --config=<file>")
		case name == "config":
			inv.configFile = value
		default:
			inv.flags = append(inv.flags, config.Flag{Name: name, Value: value, HasValue: hasValue})
		}
	}
	if inv.role.name == "" && !inv.help {
		return inv, errors.New("missing role")
	}
	return inv, nil
}

const usage = `Usage:
  dp <role> [flags]
  dp --help | dp <role> --help    (or -h)

Roles:
%s
Flags:
  --<key>=<value>   set a setting, e.g. --aggregator.blockSize=500 or --kafka.bootstrapServers=host:9092
  --<key>           set a boolean setting to true, e.g. --init.reset
  --config=<file>   read settings from a YAML file (or set DP_CONFIG=<file>)
A key that isn't a full key is looked up in the role's section:
  dp aggregator --blockSize=500   dp init --reset   dp report --json

Every setting and its default is in config/default.yaml. Later sources win:
config/default.yaml, the --config file, environment variables (DP_ and the key
in upper snake case, e.g. DP_AGGREGATOR_BLOCK_SIZE), then flags.

Exit status: 0 success, 1 failure (including a failed report), 2 usage or
configuration error. SIGTERM or Ctrl-C stops the generator, an aggregator or a
validator cleanly (0); an interrupted init or report did not finish (1).
`

func writeHelp(w io.Writer, r role) {
	var list strings.Builder
	for _, r := range roles {
		fmt.Fprintf(&list, "  %-11s %s\n", r.name, r.summary)
	}
	fmt.Fprintf(w, usage, list.String())
	if r.name == "" {
		return
	}
	defaults, err := config.Defaults()
	if err != nil {
		return
	}
	fmt.Fprintf(w, "\nSettings of the %s role (flag, default, environment variable):\n", r.name)
	for _, key := range defaults.Keys() {
		if strings.HasPrefix(key, r.name+".") {
			fmt.Fprintf(w, "  --%-28s %-10s %s\n", strings.TrimPrefix(key, r.name+"."), defaults.Text(key), config.EnvName(key))
		}
	}
}
