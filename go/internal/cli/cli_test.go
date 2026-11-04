package cli

import (
	"bytes"
	"context"
	"io/fs"
	"strings"
	"testing"
)

func run(t *testing.T, env map[string]string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	return runCtx(t, context.Background(), env, args...)
}

func runCtx(t *testing.T, ctx context.Context, env map[string]string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = Run(ctx, args, Env{
		Stdout: &out,
		Stderr: &errOut,
		LookupEnv: func(name string) (string, bool) {
			v, ok := env[name]
			return v, ok
		},
		ReadFile: func(name string) ([]byte, error) {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		},
	})
	return code, out.String(), errOut.String()
}

func TestHelp(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"-h"}, {"validator", "--help"}, {"validator", "-h"}, {"init", "--reset", "--help"}, {"--siteId=2", "-h"}} {
		code, out, _ := run(t, nil, args...)
		if code != ExitOK || !strings.Contains(out, "dp <role> [flags]") || !strings.Contains(out, "config/default.yaml") {
			t.Errorf("%v: exit %d, output:\n%s", args, code, out)
		}
		for _, r := range roles {
			if !strings.Contains(out, "  "+r.name+" ") {
				t.Errorf("%v: help doesn't list role %s", args, r.name)
			}
		}
	}
}

func TestRoleHelpListsFlagDefaultAndEnvironmentVariable(t *testing.T) {
	_, out, _ := run(t, nil, "generator", "--help")
	if !strings.Contains(out, "Settings of the generator role (flag, default, environment variable):") {
		t.Errorf("role help has no settings header:\n%s", out)
	}
	// The default is written as in default.yaml: 1.0, not 1.
	for _, line := range []string{"--zipfExponent", "--rps"} {
		found := false
		for _, l := range strings.Split(out, "\n") {
			fields := strings.Fields(l)
			if len(fields) == 3 && fields[0] == line {
				found = true
				if line == "--zipfExponent" && (fields[1] != "1.0" || fields[2] != "DP_GENERATOR_ZIPF_EXPONENT") {
					t.Errorf("zipfExponent row %q", l)
				}
				if line == "--rps" && (fields[1] != "1000" || fields[2] != "DP_GENERATOR_RPS") {
					t.Errorf("rps row %q", l)
				}
			}
		}
		if !found {
			t.Errorf("role help has no %s row:\n%s", line, out)
		}
	}
}

func TestUsageErrorsExitWith2(t *testing.T) {
	for _, tc := range []struct {
		env  map[string]string
		args []string
		want string
	}{
		{nil, nil, "missing role"},
		{nil, []string{"--siteId=2"}, "missing role"},
		{nil, []string{"bank"}, `unknown role "bank"`},
		{nil, []string{"--help=1"}, `--help takes no value, got "--help=1"`},
		{nil, []string{"validator", "--help=x"}, `--help takes no value, got "--help=x"`},
		{nil, []string{"validator", "-h=x"}, `unexpected argument "-h=x"`},
		{nil, []string{"validator", "siteId=2"}, `unexpected argument "siteId=2"`},
		{nil, []string{"validator", "-siteId=2"}, `unexpected argument "-siteId=2"`},
		{nil, []string{"validator", "--=2"}, `unexpected argument "--=2"`},
		{nil, []string{"validator", "--config"}, "--config needs a file"},
		{nil, []string{"validator", "--config=nope.yaml"}, "open nope.yaml"},
		{nil, []string{"validator", "--nope=1"}, "unknown flag --nope"},
		{nil, []string{"aggregator", "--blockSize=big"}, `invalid value "big" for aggregator.blockSize (from flag --blockSize)`},
		{map[string]string{"DP_MODEL": "both"}, []string{"report"}, "model must be one of sequential, concurrent"},
		{map[string]string{"DP_CONFIG": "missing.yaml"}, []string{"report"}, "open missing.yaml"},
	} {
		code, out, errOut := run(t, tc.env, tc.args...)
		if code != ExitUsage || !strings.Contains(errOut, "dp: ") || !strings.Contains(errOut, tc.want) || out != "" {
			t.Errorf("%v: exit %d, stderr %q; want exit 2 mentioning %q", tc.args, code, errOut, tc.want)
		}
		if !strings.HasSuffix(errOut, "\nRun 'dp --help' for usage.\n") {
			t.Errorf("%v: stderr %q doesn't end with the usage hint", tc.args, errOut)
		}
	}
}

func TestEveryConfigurationProblemOnItsOwnLine(t *testing.T) {
	code, _, errOut := run(t, nil, "aggregator", "--model=both", "--blockSize=0", "--siteId=-1")
	lines := strings.Split(strings.TrimSuffix(errOut, "\n"), "\n")
	if code != ExitUsage || len(lines) != 4 {
		t.Fatalf("exit %d, stderr:\n%s", code, errOut)
	}
	for i, want := range []string{"model must be one of", "siteId must be between", "aggregator.blockSize must be between"} {
		if !strings.HasPrefix(lines[i], "dp: ") || !strings.Contains(errOut, want) {
			t.Errorf("line %d: %q (want a line mentioning %q)", i, lines[i], want)
		}
	}
	if lines[3] != "Run 'dp --help' for usage." {
		t.Errorf("last line %q", lines[3])
	}
}

func TestEmptyConfigEnvironmentVariableCountsAsUnset(t *testing.T) {
	code, _, errOut := run(t, map[string]string{"DP_CONFIG": "", "DP_MODEL": "both"}, "report")
	if code != ExitUsage || strings.Contains(errOut, "config file") {
		t.Errorf("exit %d, stderr %q: an empty DP_CONFIG must not be read", code, errOut)
	}
}

// A stop signal while a role starts: the roles that run until stopped stop
// cleanly, init and report did not finish.
func TestStopSignalWhileStarting(t *testing.T) {
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	for role, want := range map[string]int{"generator": ExitOK, "aggregator": ExitOK, "validator": ExitOK, "init": ExitFailure, "report": ExitFailure} {
		code, _, errOut := runCtx(t, stopped, nil, role, "--kafka.bootstrapServers=127.0.0.1:1")
		if code != want {
			t.Errorf("%s: exit %d, want %d; stderr:\n%s", role, code, want, errOut)
		}
		if want == ExitFailure && !strings.Contains(errOut, "interrupted: "+role+" did not finish") {
			t.Errorf("%s: no line saying it was interrupted:\n%s", role, errOut)
		}
	}
}

func TestParseArgs(t *testing.T) {
	inv, err := parseArgs([]string{"init", "--reset", "--bank.count=4", "--config=site.yaml", "--kafka.bootstrapServers=a:1,b:2"})
	if err != nil {
		t.Fatal(err)
	}
	if inv.role.name != "init" || inv.help || inv.configFile != "site.yaml" || len(inv.flags) != 3 {
		t.Fatalf("got %+v", inv)
	}
	if f := inv.flags[0]; f.Name != "reset" || f.HasValue {
		t.Errorf("bare flag parsed as %+v", f)
	}
	if f := inv.flags[2]; f.Name != "kafka.bootstrapServers" || f.Value != "a:1,b:2" || !f.HasValue {
		t.Errorf("flag parsed as %+v", f)
	}
	inv, err = parseArgs([]string{"report", "--x="})
	if err != nil || inv.flags[0].Value != "" || !inv.flags[0].HasValue {
		t.Errorf("empty value parsed as %+v, %v", inv, err)
	}
}
