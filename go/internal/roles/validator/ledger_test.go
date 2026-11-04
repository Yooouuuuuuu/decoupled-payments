package validator

import (
	"maps"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

const now = 1730700000999

// trade builds a trade from bank "from" to bank "to"; the accounts are
// <bank>-a and <bank>-b.
func trade(serial int64, from, to int32, amount int64, category avro.Category) avro.Trade {
	out, in := bankName(from), bankName(to)
	return avro.Trade{
		SerialNumber: serial, Outbank: out, OutAccount: out + "-a", Inbank: in, InAccount: in + "-a",
		OutbankPartition: from, InbankPartition: to, Amount: amount, Category: category, Timestamp1: 1730700000000,
	}
}

func bankName(p int32) string { return "bank" + string(rune('0'+p)) }

// ledgerWith takes over the banks of the accounts: bank0-a belongs to bank0.
func ledgerWith(balances map[string]int64) *ledger {
	l := newLedger()
	banks := map[int32]*bank.Bank{}
	for account := range balances {
		p := int32(account[4] - '0')
		if banks[p] == nil {
			banks[p] = &bank.Bank{ID: bankName(p), Partition: p}
		}
		banks[p].Accounts = append(banks[p].Accounts, account)
	}
	for _, b := range banks {
		l.add(*b, balances)
	}
	return l
}

func TestValidateAcceptsAndRejects(t *testing.T) {
	l := ledgerWith(map[string]int64{"bank0-a": 100, "bank0-b": 5})
	block := avro.Block{Trades: []avro.Trade{
		trade(1, 0, 1, 60, avro.Send),
		trade(2, 0, 2, 50, avro.Send), // 40 left: rejected
		trade(3, 0, 2, 40, avro.Send), // exactly enough
		trade(4, 0, 1, 7, avro.Send),
	}}
	block.Trades[3].OutAccount = "bank0-b" // 5 < 7: rejected

	out, err := l.validate(0, []avro.Block{block}, config.Concurrent, now)
	if err != nil {
		t.Fatal(err)
	}
	categories := []avro.Category{avro.Send, avro.Rejected, avro.Send, avro.Rejected}
	if len(out.orders) != 1 || len(out.orders[0].Trades) != 4 {
		t.Fatalf("want one processed block of 4 trades, got %+v", out.orders)
	}
	for i, tr := range out.orders[0].Trades {
		if tr.Category != categories[i] || tr.Timestamp2 != now {
			t.Errorf("trade %d: category %s timestamp2 %d, want %s %d", tr.SerialNumber, tr.Category, tr.Timestamp2, categories[i], int64(now))
		}
	}
	if out.accepted != 2 || out.rejected != 2 || out.credited != 0 {
		t.Errorf("accepted %d rejected %d credited %d, want 2 2 0", out.accepted, out.rejected, out.credited)
	}
	if want := map[string]int64{"bank0-a": 0}; !maps.Equal(out.balances, want) {
		t.Errorf("changed balances %v, want %v (bank0-b didn't change)", out.balances, want)
	}
	if l.balances["bank0-a"] != 0 || l.balances["bank0-b"] != 5 {
		t.Errorf("ledger %v", l.balances)
	}
	// The input block is not modified: the processed trades are copies.
	if block.Trades[1].Category != avro.Send {
		t.Error("validate modified its input")
	}
}

func TestValidateGroupsCreditsByReceiverBank(t *testing.T) {
	l := ledgerWith(map[string]int64{"bank0-a": 1000})
	blocks := []avro.Block{
		{Trades: []avro.Trade{trade(1, 0, 2, 10, avro.Send), trade(2, 0, 1, 20, avro.Send), trade(3, 0, 2, 2000, avro.Send), trade(4, 0, 2, 30, avro.Send)}},
		{Trades: []avro.Trade{trade(5, 0, 1, 40, avro.Send)}},
	}
	out, err := l.validate(0, blocks, config.Concurrent, now)
	if err != nil {
		t.Fatal(err)
	}
	// One credit block per receiving bank per processed block, in bank order;
	// the rejected trade 3 has no credit.
	var got []string
	for _, c := range out.credits {
		var serials []string
		for _, tr := range c.block.Trades {
			if tr.Category != avro.Receive || tr.Timestamp2 != now || tr.InbankPartition != c.partition {
				t.Errorf("credit %+v in block for bank %d", tr, c.partition)
			}
			serials = append(serials, string(rune('0'+tr.SerialNumber)))
		}
		got = append(got, bankName(c.partition)+":"+strings.Join(serials, ","))
	}
	if want := []string{"bank1:2", "bank2:1,4", "bank1:5"}; !reflect.DeepEqual(got, want) {
		t.Errorf("credits %v, want %v", got, want)
	}
	if len(out.orders) != 2 {
		t.Errorf("want one orders block per input block, got %d", len(out.orders))
	}
}

func TestValidateAppliesCreditsInSequentialModel(t *testing.T) {
	l := ledgerWith(map[string]int64{"bank1-a": 10})
	credit := trade(7, 0, 1, 25, avro.Receive)
	credit.Timestamp2 = 123
	out, err := l.validate(1, []avro.Block{{Trades: []avro.Trade{credit}}}, config.Sequential, now)
	if err != nil {
		t.Fatal(err)
	}
	if l.balances["bank1-a"] != 35 || out.credited != 1 || out.accepted+out.rejected != 0 || len(out.credits) != 0 {
		t.Errorf("ledger %v, outcome %+v", l.balances, out)
	}
	// The completed credit goes to orders as it came: still RECEIVE, still validated at 123.
	if got := out.orders[0].Trades[0]; got.Category != avro.Receive || got.Timestamp2 != 123 {
		t.Errorf("orders got %+v", got)
	}
}

func TestValidateRejectsWrongInput(t *testing.T) {
	balances := map[string]int64{"bank0-a": 100, "bank1-a": 100, "bank1-b": math.MaxInt64 - 5}
	unknown := trade(1, 0, 1, 10, avro.Send)
	unknown.OutAccount = "bank0-x"
	// The validator owns bank1 too, but bank0's trade can't touch its accounts.
	foreign := trade(1, 0, 1, 10, avro.Send)
	foreign.OutAccount = "bank1-a"
	foreignCredit := trade(1, 0, 1, 10, avro.Receive)
	foreignCredit.InAccount = "bank0-a"
	overflow := trade(1, 0, 1, 10, avro.Receive)
	overflow.InAccount = "bank1-b"
	zero := trade(2, 0, 1, 0, avro.Send)
	negative := trade(2, 0, 1, -5, avro.Receive)
	for name, tc := range map[string]struct {
		partition int32
		trade     avro.Trade
		model     config.Model
		want      string
	}{
		"another bank's trade":       {1, trade(1, 0, 1, 10, avro.Send), config.Concurrent, "validated by bank 0, not 1"},
		"credit in Concurrent model": {1, trade(1, 0, 1, 10, avro.Receive), config.Concurrent, "only in the Sequential model"},
		"another bank's credit":      {0, trade(1, 0, 1, 10, avro.Receive), config.Sequential, "credited by bank 1, not 0"},
		"rejected trade":             {0, trade(1, 0, 1, 10, avro.Rejected), config.Concurrent, "only carry SEND and RECEIVE"},
		"unknown account":            {0, unknown, config.Concurrent, "bank0-x is not one of the accounts of bank0"},
		"another bank's account":     {0, foreign, config.Concurrent, "bank1-a is not one of the accounts of bank0"},
		"another bank's receiver":    {1, foreignCredit, config.Sequential, "bank0-a is not one of the accounts of bank1"},
		"a balance overflow":         {1, overflow, config.Sequential, "crediting 10 to bank1-b, which has 9223372036854775802, overflows"},
		"zero amount":                {0, zero, config.Concurrent, "non-positive amount"},
		"negative amount":            {1, negative, config.Sequential, "non-positive amount"},
	} {
		_, err := ledgerWith(balances).validate(tc.partition, []avro.Block{{Trades: []avro.Trade{tc.trade}}}, tc.model, now)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error containing %q", name, err, tc.want)
		}
	}
}

func TestApplyCredits(t *testing.T) {
	l := ledgerWith(map[string]int64{"bank2-a": 1, "bank2-b": 2})
	c1, c2, c3 := trade(1, 0, 2, 10, avro.Receive), trade(2, 1, 2, 20, avro.Receive), trade(3, 0, 2, 30, avro.Receive)
	c3.InAccount = "bank2-b"
	blocks := []avro.Block{{Trades: []avro.Trade{c1, c2}}, {Trades: []avro.Trade{c3}}}
	out, err := l.applyCredits(2, blocks)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]int64{"bank2-a": 31, "bank2-b": 32}; !maps.Equal(out.balances, want) || !maps.Equal(l.balances, want) {
		t.Errorf("balances %v, ledger %v, want %v", out.balances, l.balances, want)
	}
	if !reflect.DeepEqual(out.orders, blocks) || out.credited != 3 {
		t.Errorf("want the credit blocks unchanged on orders, got %+v", out.orders)
	}

	for name, credit := range map[string]avro.Trade{
		"a SEND trade":         trade(4, 0, 2, 10, avro.Send),
		"another bank's":       trade(5, 0, 1, 10, avro.Receive),
		"an unknown account's": func() avro.Trade { c := trade(6, 0, 2, 10, avro.Receive); c.InAccount = "x"; return c }(),
	} {
		if _, err := l.applyCredits(2, []avro.Block{{Trades: []avro.Trade{credit}}}); err == nil {
			t.Errorf("applied %s credit", name)
		}
	}
}

func TestLedgerAddAndDrop(t *testing.T) {
	bank0 := bank.Bank{ID: "bank0", Partition: 0, Accounts: []string{"bank0-a", "bank0-b"}}
	bank1 := bank.Bank{ID: "bank1", Partition: 1, Accounts: []string{"bank1-a"}}
	l := newLedger()
	// Only the bank's own accounts are taken over, whatever else localBalance held.
	l.add(bank0, map[string]int64{"bank0-a": 1, "bank0-b": 2, "bank1-a": 99})
	l.add(bank1, map[string]int64{"bank1-a": 3})
	l.drop(bank0)
	if want := map[string]int64{"bank1-a": 3}; !maps.Equal(l.balances, want) || len(l.bankOf) != 1 {
		t.Errorf("ledger %v (banks %v), want %v", l.balances, l.bankOf, want)
	}
}
