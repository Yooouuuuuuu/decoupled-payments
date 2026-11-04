package report

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

// Two banks with one account each, 100 in every account.
var dir = bank.Directory{
	Banks: []bank.Bank{
		{ID: "bank0", Partition: 0, Accounts: []string{"bank0-0000"}},
		{ID: "bank1", Partition: 1, Accounts: []string{"bank1-0000"}},
	},
	InitialBalance: 100, Model: config.Concurrent, KafkaTransactions: true,
}

func generated(serial, amount, created int64) avro.Trade {
	return avro.Trade{
		SerialNumber: serial, Outbank: "bank0", OutAccount: "bank0-0000", Inbank: "bank1", InAccount: "bank1-0000",
		OutbankPartition: 0, InbankPartition: 1, Amount: amount, Category: avro.Send, Timestamp1: created,
	}
}

// order is a trade as it appears on orders: validated at timestamp2.
func order(t avro.Trade, category avro.Category, validated int64) avro.Trade {
	t.Category, t.Timestamp2 = category, validated
	return t
}

type ordered struct {
	trade avro.Trade
	at    int64 // the orders record's timestamp
}

func analyze(t *testing.T, gen []avro.Trade, orders []ordered, latest map[string]int64, negative ...string) Result {
	t.Helper()
	a := newAnalysis(gen)
	for _, o := range orders {
		if err := a.addOrder(o.trade, o.at); err != nil {
			t.Fatal(err)
		}
	}
	return a.result(dir, balances{latest: latest, negative: negative}, "1m0s")
}

func checks(r Result) map[string]Check {
	out := map[string]Check{}
	for _, c := range r.Checks {
		out[c.Name] = c
	}
	return out
}

func wantFailed(t *testing.T, r Result, names ...string) {
	t.Helper()
	failed := map[string]bool{}
	for _, n := range names {
		failed[n] = true
	}
	for _, c := range r.Checks {
		if c.Passed == failed[c.Name] {
			t.Errorf("check %s passed=%v (%s), want passed=%v", c.Name, c.Passed, c.Detail, !failed[c.Name])
		}
	}
	if len(r.Checks) != 6 {
		t.Errorf("got %d checks, want 6", len(r.Checks))
	}
}

func TestEveryTradeProcessedOnce(t *testing.T) {
	t1, t2, t3 := generated(1, 30, 1000), generated(2, 500, 1010), generated(3, 20, 1020)
	orders := []ordered{
		{order(t1, avro.Send, 1100), 1150},
		{order(t1, avro.Receive, 1100), 2000}, // completed
		{order(t2, avro.Rejected, 1200), 1250},
		{order(t3, avro.Receive, 1300), 1500}, // the completion can be read before the validation
		{order(t3, avro.Send, 1300), 1350},
	}
	r := analyze(t, []avro.Trade{t1, t2, t3}, orders, map[string]int64{"bank0-0000": 50, "bank1-0000": 150})
	wantFailed(t, r)
	if !r.Passed() {
		t.Error("Passed() is false")
	}
	if r.Generated != 3 || r.Accepted != 2 || r.Rejected != 1 || r.Completed != 2 || r.InFlight != 0 {
		t.Errorf("counts %+v", r)
	}
	// completed 2 ÷ (last completion 2000 - first created 1000) ms
	if r.ThroughputTPS != 2 {
		t.Errorf("throughput %v, want 2", r.ThroughputTPS)
	}
	if want := (Percentiles{P50: 480, P95: 1000, P99: 1000, Max: 1000}); r.Latency != want {
		t.Errorf("completion latency %+v, want %+v", r.Latency, want)
	}
	if want := (Percentiles{P50: 190, P95: 280, P99: 280, Max: 280}); r.ValidationLatency != want {
		t.Errorf("validation latency %+v, want %+v", r.ValidationLatency, want)
	}
}

func TestTradesInFlight(t *testing.T) {
	t1, t2 := generated(1, 30, 1000), generated(2, 40, 1000)
	a := newAnalysis([]avro.Trade{t1, t2})
	if a.done() {
		t.Fatal("done before any order")
	}
	_ = a.addOrder(order(t1, avro.Send, 1100), 1100)
	r := a.result(dir, balances{latest: map[string]int64{"bank0-0000": 70, "bank1-0000": 100}}, "1m0s")
	// t1 is accepted but not completed: its money is in flight. t2 was never
	// validated. The balances aren't compared on a run that hasn't settled.
	wantFailed(t, r, "complete", "validated-once", "completed-once", "money-conserved", "balances-match-orders")
	if r.InFlight != 2 || r.Accepted != 1 || r.Completed != 0 {
		t.Errorf("counts %+v", r)
	}
	if c := checks(r)["complete"]; !strings.Contains(c.Detail, "1 generated trades not validated, 1 accepted trades not completed") {
		t.Errorf("complete: %s", c.Detail)
	}
	for _, name := range []string{"money-conserved", "balances-match-orders"} {
		if c := checks(r)[name]; c.Detail != "not checked: 2 trades not finished" {
			t.Errorf("%s: %s", name, c.Detail)
		}
	}
}

// The case both implementations test with the same expected values
// (implementation spec §16.9).
func TestSharedCase(t *testing.T) {
	var trades []avro.Trade
	for serial := int64(1); serial <= 8; serial++ {
		trades = append(trades, generated(serial, 10, 1000))
	}
	// T3, trades[2], has no orders at all.
	t1, t2, t4, t5, t6, t7, t8 := trades[0], trades[1], trades[3], trades[4], trades[5], trades[6], trades[7]
	u := generated(99, 10, 1000) // not generated
	orders := []ordered{
		{order(t1, avro.Send, 1100), 1100}, {order(t1, avro.Receive, 1100), 1200},
		{order(t2, avro.Rejected, 1100), 1100},
		{order(t4, avro.Send, 1100), 1100},
		{order(t5, avro.Send, 1100), 1100}, {order(t5, avro.Send, 1100), 1150}, {order(t5, avro.Receive, 1100), 1200}, {order(t5, avro.Receive, 1100), 1250},
		{order(t6, avro.Send, 1100), 1100}, {order(t6, avro.Receive, 1100), 1200},
		{order(t7, avro.Rejected, 1100), 1100}, {order(t7, avro.Receive, 1100), 1200},
		{order(t8, avro.Receive, 1100), 1200},
		{order(u, avro.Send, 1100), 1100}, {order(u, avro.Receive, 1100), 1200},
	}
	gen := append(trades, t6) // t6's serial number appears twice on trades
	r := analyze(t, gen, orders, map[string]int64{"bank0-0000": 60, "bank1-0000": 130})

	if r.Generated != 8 || r.Accepted != 4 || r.Rejected != 2 || r.Completed != 3 || r.InFlight != 3 {
		t.Errorf("generated %d accepted %d rejected %d completed %d inFlight %d, want 8 4 2 3 3",
			r.Generated, r.Accepted, r.Rejected, r.Completed, r.InFlight)
	}
	wantFailed(t, r, "complete", "validated-once", "completed-once", "money-conserved", "balances-match-orders")
	if r.Passed() || verdict(r) != "FAIL" {
		t.Error("the result must be FAIL")
	}
	c := checks(r)
	for _, name := range []string{"money-conserved", "balances-match-orders"} {
		if c[name].Detail != "not checked: 3 trades not finished" {
			t.Errorf("%s: %s", name, c[name].Detail)
		}
	}
	for name, want := range map[string]string{
		"validated-once": "1 duplicate serial numbers among generated trades, 2 trades without a validation record, 1 trades validated more than once, 1 validation records for unknown trades",
		"completed-once": "1 accepted trades without a completion record, 1 trades completed more than once, 1 rejected trades completed, 1 trades completed without a validation record, 1 completion records for unknown trades",
	} {
		if c[name].Detail != want {
			t.Errorf("%s: %q, want %q", name, c[name].Detail, want)
		}
	}
}

func TestThroughputIsRoundedHalfUp(t *testing.T) {
	for _, tc := range []struct {
		completed int
		ms        int64
		want      float64
	}{
		{1, 4000, 0.3}, // 0.25 rounds up
		{20000, 10234, 1954.3},
		{3, 7, 428.6},
	} {
		var gen []avro.Trade
		var orders []ordered
		for i := range tc.completed {
			tr := generated(int64(i+1), 1, 1000)
			gen = append(gen, tr)
			orders = append(orders, ordered{order(tr, avro.Send, 1000), 1000}, ordered{order(tr, avro.Receive, 1000), 1000 + tc.ms})
		}
		r := analyze(t, gen, orders, map[string]int64{})
		if r.ThroughputTPS != tc.want {
			t.Errorf("%d completed in %d ms: %v trades/s, want %v", tc.completed, tc.ms, r.ThroughputTPS, tc.want)
		}
	}
	if r := analyze(t, nil, nil, map[string]int64{}); r.ThroughputTPS != 0 {
		t.Errorf("nothing completed: %v", r.ThroughputTPS)
	}
}

func TestDuplicatesAreCaught(t *testing.T) {
	t1 := generated(1, 30, 1000)
	balancesAfter := map[string]int64{"bank0-0000": 70, "bank1-0000": 130}
	done := []ordered{{order(t1, avro.Send, 1100), 1100}, {order(t1, avro.Receive, 1100), 1200}}

	r := analyze(t, []avro.Trade{t1, t1}, done, balancesAfter)
	wantFailed(t, r, "validated-once")
	if c := checks(r)["validated-once"]; !strings.Contains(c.Detail, "1 duplicate serial numbers") {
		t.Errorf("validated-once: %s", c.Detail)
	}

	r = analyze(t, []avro.Trade{t1}, append(done, ordered{order(t1, avro.Send, 1300), 1300}), map[string]int64{"bank0-0000": 40, "bank1-0000": 130})
	wantFailed(t, r, "validated-once", "money-conserved", "balances-match-orders")

	r = analyze(t, []avro.Trade{t1}, append(done, ordered{order(t1, avro.Receive, 1100), 1400}), map[string]int64{"bank0-0000": 70, "bank1-0000": 160})
	wantFailed(t, r, "completed-once", "money-conserved", "balances-match-orders")
	if c := checks(r)["completed-once"]; !strings.Contains(c.Detail, "1 trades completed more than once") {
		t.Errorf("completed-once: %s", c.Detail)
	}
}

func TestUnknownAndRejectedTradesMustNotComplete(t *testing.T) {
	t1, stranger := generated(1, 30, 1000), generated(99, 5, 1000)
	r := analyze(t, []avro.Trade{t1}, []ordered{
		{order(t1, avro.Rejected, 1100), 1100},
		{order(t1, avro.Receive, 1100), 1200},
		{order(stranger, avro.Send, 1100), 1100},
		{order(stranger, avro.Receive, 1100), 1200},
	}, map[string]int64{"bank0-0000": 100, "bank1-0000": 100})
	// The balances didn't move, but orders say bank1-0000 received 30.
	wantFailed(t, r, "validated-once", "completed-once", "balances-match-orders")
	detail := checks(r)["completed-once"].Detail
	for _, want := range []string{"1 rejected trades completed", "1 completion records for unknown trades"} {
		if !strings.Contains(detail, want) {
			t.Errorf("completed-once %q doesn't mention %q", detail, want)
		}
	}
	if d := checks(r)["validated-once"].Detail; !strings.Contains(d, "1 validation records for unknown trades") {
		t.Errorf("validated-once: %s", d)
	}
}

func TestBalanceChecks(t *testing.T) {
	r := analyze(t, nil, nil, map[string]int64{"bank0-0000": 101, "bank1-0000": 99})
	wantFailed(t, r, "balances-match-orders")
	if d := checks(r)["balances-match-orders"].Detail; !strings.Contains(d, "2 accounts differ, first bank0-0000: orders say 100, localBalance has 101") {
		t.Errorf("balances-match-orders: %s", d)
	}

	r = analyze(t, nil, nil, map[string]int64{"bank0-0000": 100}, "bank1-0000: -5")
	wantFailed(t, r, "money-conserved", "no-negative-balances", "balances-match-orders")
	if d := checks(r)["balances-match-orders"].Detail; !strings.Contains(d, "bank1-0000: orders say 100, localBalance has none") {
		t.Errorf("balances-match-orders: %s", d)
	}
}

func TestPercentilesUseNearestRank(t *testing.T) {
	if got := percentiles(nil); got != (Percentiles{}) {
		t.Errorf("no values: %+v", got)
	}
	if got := percentiles([]int64{7}); got != (Percentiles{7, 7, 7, 7}) {
		t.Errorf("one value: %+v", got)
	}
	// 1..100 in reverse: the q-quantile is the value at index ceil(q·n)-1.
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(100 - i)
	}
	if got := percentiles(values); got != (Percentiles{P50: 50, P95: 95, P99: 99, Max: 100}) {
		t.Errorf("1..100: %+v", got)
	}
	// ceil(0.95 · 20000) - 1 = 18999 exactly, without floating point error.
	values = make([]int64, 20000)
	for i := range values {
		values[i] = int64(i)
	}
	if got := percentiles(values); got.P95 != 18999 || got.P50 != 9999 || got.P99 != 19799 {
		t.Errorf("0..19999: %+v", got)
	}
	if got := percentiles([]int64{1, 2, 3}); got.P50 != 2 || got.P95 != 3 {
		t.Errorf("1..3: %+v", got)
	}
}

func TestJSONLine(t *testing.T) {
	r := analyze(t, nil, nil, map[string]int64{"bank0-0000": 100, "bank1-0000": 100})
	var buf bytes.Buffer
	if err := writeJSON(&buf, r); err != nil {
		t.Fatal(err)
	}
	line := buf.String()
	if !strings.HasPrefix(line, "REPORT_JSON {") || strings.Count(line, "\n") != 1 || !strings.HasSuffix(line, "}\n") {
		t.Fatalf("not one REPORT_JSON line: %q", line)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "REPORT_JSON ")), &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"result", "model", "kafkaTransactions", "generated", "accepted", "rejected", "completed",
		"inFlight", "throughputTps", "latencyMs", "validationLatencyMs", "checks"} {
		if _, ok := got[key]; !ok {
			t.Errorf("key %s missing", key)
		}
	}
	if len(got) != 12 || got["result"] != "PASS" || got["model"] != "concurrent" {
		t.Errorf("got %v", got)
	}
	latency := got["latencyMs"].(map[string]any)
	for _, key := range []string{"p50", "p95", "p99", "max"} {
		if _, ok := latency[key]; !ok {
			t.Errorf("latencyMs.%s missing", key)
		}
	}
	first := got["checks"].([]any)[0].(map[string]any)
	if first["name"] != "complete" || first["passed"] != true || first["detail"] == "" {
		t.Errorf("checks[0] = %v", first)
	}
}

func TestSummaryEndsWithTheVerdict(t *testing.T) {
	var buf bytes.Buffer
	r := analyze(t, nil, nil, map[string]int64{})
	writeSummary(&buf, r)
	out := buf.String()
	if !strings.HasSuffix(out, "FAIL\n") || !strings.Contains(out, "✗ money-conserved") || !strings.Contains(out, "✓ complete") {
		t.Errorf("summary:\n%s", out)
	}
}
