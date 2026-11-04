package report

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

// Result is what the report prints. Each generated trade is counted once:
// generated = rejected + completed + inFlight.
type Result struct {
	Model             config.Model
	KafkaTransactions bool
	Generated         int         // distinct serial numbers among the SEND trades on trades
	Accepted          int         // generated trades with a SEND record on orders
	Rejected          int         // generated trades with a REJECTED record and no SEND record
	Completed         int         // accepted trades with a RECEIVE record on orders
	InFlight          int         // the others: not validated yet, or accepted and not completed
	ThroughputTPS     float64     // completed trades per second, to 1 decimal
	Latency           Percentiles // of completed trades: their first completion record's time - timestamp1
	ValidationLatency Percentiles // of validated trades: timestamp2 - timestamp1 of their first validation record
	Checks            []Check
}

// Percentiles of a latency, in ms.
type Percentiles struct{ P50, P95, P99, Max int64 }

// Check is one correctness check. The names are stable.
type Check struct {
	Name   string
	Passed bool
	Detail string
}

// Passed reports whether every check passed.
func (r Result) Passed() bool {
	for _, c := range r.Checks {
		if !c.Passed {
			return false
		}
	}
	return true
}

// tradeState is what orders says about one generated trade.
type tradeState struct {
	trade       avro.Trade // as generated
	accepted    int        // validation records saying SEND (accepted)
	rejected    int        // validation records saying REJECTED
	completed   int        // completion records (RECEIVE)
	validation  avro.Trade // the first validation record's trade
	completedAt int64      // the first completion record's Kafka timestamp, in ms
}

func (s *tradeState) validated() bool { return s.accepted+s.rejected > 0 }

// analysis follows the orders of the generated trades.
type analysis struct {
	trades     map[int64]*tradeState // by serial number
	duplicates int                   // generated trades whose serial number was already taken

	unknownValidations int // validation records for trades that weren't generated
	unknownCompletions int // completion records for trades that weren't generated
	unvalidated        int // generated trades without a validation record yet
	uncompleted        int // accepted trades without a completion record yet
}

func newAnalysis(generated []avro.Trade) *analysis {
	a := &analysis{trades: make(map[int64]*tradeState, len(generated))}
	for _, t := range generated {
		if _, ok := a.trades[t.SerialNumber]; ok {
			a.duplicates++
			continue
		}
		a.trades[t.SerialNumber] = &tradeState{trade: t}
	}
	a.unvalidated = len(a.trades)
	return a
}

// addOrder takes one trade of an orders record. Records from different
// partitions arrive in any order: a completion may come before its validation.
func (a *analysis) addOrder(t avro.Trade, recordTime int64) error {
	s, known := a.trades[t.SerialNumber]
	switch t.Category {
	case avro.Send, avro.Rejected:
		if !known {
			a.unknownValidations++
			return nil
		}
		if !s.validated() {
			a.unvalidated--
			s.validation = t
		}
		if t.Category == avro.Rejected {
			s.rejected++
			return nil
		}
		if s.accepted++; s.accepted == 1 && s.completed == 0 {
			a.uncompleted++
		}
	case avro.Receive:
		if !known {
			a.unknownCompletions++
			return nil
		}
		if s.completed++; s.completed == 1 {
			s.completedAt = recordTime
			if s.accepted > 0 {
				a.uncompleted--
			}
		}
	default:
		return fmt.Errorf("trade %d on orders has category %q", t.SerialNumber, t.Category)
	}
	return nil
}

// done reports whether every generated trade was validated and every
// accepted trade completed.
func (a *analysis) done() bool { return a.unvalidated == 0 && a.uncompleted == 0 }

// balances is what localBalance says.
type balances struct {
	latest   map[string]int64 // the latest balance of each account
	negative []string         // records with a balance below zero, as "account: balance"
}

// result runs the checks and computes the metrics.
func (a *analysis) result(dir bank.Directory, bal balances, timeout string) Result {
	r := Result{Model: dir.Model, KafkaTransactions: dir.KafkaTransactions, Generated: len(a.trades)}

	var validationLatencies, completionLatencies []int64
	var firstCreated, lastCompleted int64
	var multiValidated, multiCompleted, rejectedCompleted, unvalidatedCompleted int
	var inFlightMoney int64
	expected := map[string]int64{} // per account: initial - accepted sent + completed received
	for _, b := range dir.Banks {
		for _, account := range b.Accounts {
			expected[account] = dir.InitialBalance
		}
	}
	for _, s := range a.trades {
		t := s.trade
		if firstCreated == 0 || t.Timestamp1 < firstCreated {
			firstCreated = t.Timestamp1
		}
		if s.accepted+s.rejected > 1 {
			multiValidated++
		}
		if s.validated() {
			validationLatencies = append(validationLatencies, s.validation.Timestamp2-s.validation.Timestamp1)
		}
		switch {
		case s.accepted > 0:
			r.Accepted++
			expected[t.OutAccount] -= t.Amount
			if s.completed > 1 {
				multiCompleted++
			}
			if s.completed == 0 {
				inFlightMoney += t.Amount
			}
		case s.rejected > 0:
			r.Rejected++
			if s.completed > 0 {
				rejectedCompleted++
			}
		case s.completed > 0:
			unvalidatedCompleted++ // completed without any validation record
		}
		if s.completed > 0 {
			expected[t.InAccount] += t.Amount
		}
		if s.accepted > 0 && s.completed > 0 {
			r.Completed++
			completionLatencies = append(completionLatencies, s.completedAt-t.Timestamp1)
			lastCompleted = max(lastCompleted, s.completedAt)
		}
	}
	r.InFlight = r.Generated - r.Rejected - r.Completed
	if r.Completed > 0 && lastCompleted > firstCreated {
		// Trades per second rounded half up to 1 decimal, computed like
		// Java's Math.round(completed * 10000.0 / ms) / 10.0, so that both
		// implementations print the same number.
		r.ThroughputTPS = math.Round(float64(r.Completed)*10000/float64(lastCompleted-firstCreated)) / 10
	}
	r.Latency = percentiles(completionLatencies)
	r.ValidationLatency = percentiles(validationLatencies)

	var balanceSum int64
	for _, v := range bal.latest {
		balanceSum += v
	}
	var mismatches []string
	for _, account := range slices.Sorted(maps.Keys(expected)) {
		if got, ok := bal.latest[account]; !ok || got != expected[account] {
			mismatches = append(mismatches, fmt.Sprintf("%s: orders say %d, localBalance %s", account, expected[account], balanceText(got, ok)))
		}
	}

	r.Checks = []Check{
		check("complete", a.done(),
			fmt.Sprintf("all %d generated trades validated and all %d accepted trades completed", r.Generated, r.Accepted),
			fmt.Sprintf("after %s: %d generated trades not validated, %d accepted trades not completed", timeout, a.unvalidated, a.uncompleted)),
		check("validated-once", a.duplicates == 0 && a.unvalidated == 0 && multiValidated == 0 && a.unknownValidations == 0,
			fmt.Sprintf("each of the %d generated trades has exactly one validation record", r.Generated),
			problems(
				count(a.duplicates, "duplicate serial numbers among generated trades"),
				count(a.unvalidated, "trades without a validation record"),
				count(multiValidated, "trades validated more than once"),
				count(a.unknownValidations, "validation records for unknown trades"))),
		check("completed-once", a.uncompleted == 0 && multiCompleted == 0 && rejectedCompleted == 0 && unvalidatedCompleted == 0 && a.unknownCompletions == 0,
			fmt.Sprintf("each of the %d accepted trades has exactly one completion record", r.Accepted),
			problems(
				count(a.uncompleted, "accepted trades without a completion record"),
				count(multiCompleted, "trades completed more than once"),
				count(rejectedCompleted, "rejected trades completed"),
				count(unvalidatedCompleted, "trades completed without a validation record"),
				count(a.unknownCompletions, "completion records for unknown trades"))),
		settledCheck(a.done(), r.InFlight, check("money-conserved", balanceSum+inFlightMoney == dir.TotalMoney(),
			fmt.Sprintf("balances %d + in flight %d = total %d", balanceSum, inFlightMoney, dir.TotalMoney()),
			fmt.Sprintf("balances %d + in flight %d = %d, but the total is %d", balanceSum, inFlightMoney, balanceSum+inFlightMoney, dir.TotalMoney()))),
		check("no-negative-balances", len(bal.negative) == 0,
			"no balance record is below zero",
			fmt.Sprintf("%d balance records below zero, first %s", len(bal.negative), firstOf(bal.negative))),
		settledCheck(a.done(), r.InFlight, check("balances-match-orders", len(mismatches) == 0,
			fmt.Sprintf("all %d balances match the orders", len(expected)),
			fmt.Sprintf("%d accounts differ, first %s", len(mismatches), firstOf(mismatches)))),
	}
	return r
}

// settledCheck keeps a check that compares the balances with orders only if
// the run settled. localBalance is read after orders, and on a run that
// hasn't settled the validators commit more in between: the balances would
// show differences that aren't there.
func settledCheck(settled bool, unfinished int, c Check) Check {
	if settled {
		return c
	}
	return Check{Name: c.Name, Passed: false, Detail: fmt.Sprintf("not checked: %d trades not finished", unfinished)}
}

// percentiles uses the nearest-rank method: the q-quantile is the value at
// index ceil(q·n)-1 of the sorted list (computed in integers, q in percent).
func percentiles(values []int64) Percentiles {
	n := len(values)
	if n == 0 {
		return Percentiles{}
	}
	sorted := slices.Sorted(slices.Values(values))
	at := func(percent int) int64 { return sorted[(percent*n+99)/100-1] }
	return Percentiles{P50: at(50), P95: at(95), P99: at(99), Max: sorted[n-1]}
}

func check(name string, passed bool, ok, failed string) Check {
	if passed {
		return Check{Name: name, Passed: true, Detail: ok}
	}
	return Check{Name: name, Passed: false, Detail: failed}
}

func count(n int, what string) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("%d %s", n, what)
}

func problems(parts ...string) string {
	return strings.Join(slices.DeleteFunc(parts, func(s string) bool { return s == "" }), ", ")
}

func firstOf(items []string) string {
	if len(items) == 0 {
		return ""
	}
	return items[0]
}

func balanceText(v int64, ok bool) string {
	if !ok {
		return "has none"
	}
	return fmt.Sprintf("has %d", v)
}
