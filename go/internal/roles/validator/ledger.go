package validator

import (
	"fmt"
	"maps"
	"slices"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

// ledger is the in-memory balance of every account of the banks this
// validator owns. No other validator touches these accounts (each bank has
// exactly one owner), but the two threads of a validator share them, so the
// ledger is only used under validator.mu.
type ledger struct {
	balances map[string]int64
	bankOf   map[string]int32 // the partition of each account's bank
}

// outcome is everything one Kafka transaction writes for a bank, besides
// the bank's new read position.
type outcome struct {
	orders   []avro.Block     // to orders: one per block processed
	credits  []credit         // to credits (Concurrent model) or trades (Sequential)
	balances map[string]int64 // to localBalance: the final balance of each account that changed
	accepted int              // SEND trades with enough funds
	rejected int              // SEND trades without
	credited int              // credits applied
}

// credit is a block of accepted trades, turned into credits for one
// receiving bank.
type credit struct {
	partition int32
	block     avro.Block
}

func newLedger() *ledger { return &ledger{balances: map[string]int64{}, bankOf: map[string]int32{}} }

// validate processes blocks from partition p of blocks (bank p).
//
// A SEND trade is accepted if its sender has the funds: the amount is
// deducted, and a copy of the trade becomes a credit (category RECEIVE) for
// the receiver's bank. Otherwise it is REJECTED. Either way it is stamped
// with the validation time. In the Sequential model, credits come back
// through blocks as RECEIVE trades and are applied here.
func (l *ledger) validate(p int32, blocks []avro.Block, model config.Model, now int64) (outcome, error) {
	out := outcome{balances: map[string]int64{}}
	for _, block := range blocks {
		processed := avro.Block{Trades: make([]avro.Trade, 0, len(block.Trades))}
		credits := map[int32][]avro.Trade{}
		for _, t := range block.Trades {
			switch t.Category {
			case avro.Send:
				if t.OutbankPartition != p {
					return out, fmt.Errorf("trade %d is validated by bank %d, not %d", t.SerialNumber, t.OutbankPartition, p)
				}
				balance, err := l.balance(t.OutAccount, p, t)
				if err != nil {
					return out, err
				}
				t.Timestamp2 = now
				if balance >= t.Amount {
					l.set(&out, t.OutAccount, balance-t.Amount)
					c := t
					c.Category = avro.Receive
					credits[t.InbankPartition] = append(credits[t.InbankPartition], c)
					out.accepted++
				} else {
					t.Category = avro.Rejected
					out.rejected++
				}
			case avro.Receive:
				if model != config.Sequential {
					return out, fmt.Errorf("trade %d: credits come through blocks only in the Sequential model", t.SerialNumber)
				}
				if err := l.applyCredit(&out, p, t); err != nil {
					return out, err
				}
			default:
				return out, fmt.Errorf("trade %d is %s; blocks only carry SEND and RECEIVE trades", t.SerialNumber, t.Category)
			}
			processed.Trades = append(processed.Trades, t)
		}
		out.orders = append(out.orders, processed)
		for _, q := range slices.Sorted(maps.Keys(credits)) {
			out.credits = append(out.credits, credit{partition: q, block: avro.Block{Trades: credits[q]}})
		}
	}
	return out, nil
}

// applyCredits applies blocks of credits from partition p of credits
// (Concurrent model). Credits need no check: their trades were validated.
// Each block goes to orders unchanged; RECEIVE there means completed.
func (l *ledger) applyCredits(p int32, blocks []avro.Block) (outcome, error) {
	out := outcome{balances: map[string]int64{}}
	for _, block := range blocks {
		for _, t := range block.Trades {
			if t.Category != avro.Receive {
				return out, fmt.Errorf("trade %d is %s; credits only carry RECEIVE trades", t.SerialNumber, t.Category)
			}
			if err := l.applyCredit(&out, p, t); err != nil {
				return out, err
			}
		}
		out.orders = append(out.orders, block)
	}
	return out, nil
}

func (l *ledger) applyCredit(out *outcome, p int32, t avro.Trade) error {
	if t.InbankPartition != p {
		return fmt.Errorf("trade %d is credited by bank %d, not %d", t.SerialNumber, t.InbankPartition, p)
	}
	balance, err := l.balance(t.InAccount, p, t)
	if err != nil {
		return err
	}
	// The total money fits in 64 bits (init checked it), so only corrupt
	// input can overflow a balance.
	credited := balance + t.Amount
	if credited < balance {
		return fmt.Errorf("trade %d: crediting %d to %s, which has %d, overflows", t.SerialNumber, t.Amount, t.InAccount, balance)
	}
	l.set(out, t.InAccount, credited)
	out.credited++
	return nil
}

// balance returns the balance of an account of bank p, checking that the
// trade touching it is sound: a non-positive amount, or an account of
// another bank, means corrupt input.
func (l *ledger) balance(account string, p int32, t avro.Trade) (int64, error) {
	if t.Amount <= 0 {
		return 0, fmt.Errorf("trade %d has a non-positive amount %d", t.SerialNumber, t.Amount)
	}
	if owner, ok := l.bankOf[account]; !ok || owner != p {
		return 0, fmt.Errorf("trade %d: %s is not one of the accounts of %s", t.SerialNumber, account, bank.ID(p))
	}
	return l.balances[account], nil
}

func (l *ledger) set(out *outcome, account string, balance int64) {
	l.balances[account] = balance
	out.balances[account] = balance
}

// add takes over a bank: its accounts, with their restored balances.
func (l *ledger) add(b bank.Bank, balances map[string]int64) {
	for _, a := range b.Accounts {
		l.balances[a] = balances[a]
		l.bankOf[a] = b.Partition
	}
}

// drop forgets the accounts of a bank this validator no longer owns.
func (l *ledger) drop(b bank.Bank) {
	for _, a := range b.Accounts {
		delete(l.balances, a)
		delete(l.bankOf, a)
	}
}
