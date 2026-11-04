package generator

import (
	"errors"
	"math"
	"math/rand/v2"
	"sort"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

// account is an account the generator can pick as a sender or receiver.
type account struct {
	id        string
	bank      string
	partition int32
}

// sampler draws the senders, receivers and amounts of trades from a seeded
// random source, so a run can be repeated. Only by this implementation: the
// Java one uses another random generator, so the same generator.seed gives
// other trades there.
type sampler struct {
	rng       *rand.Rand
	accounts  []account // bank by bank, then shuffled once
	pick      func() int
	multiBank bool
	minAmount int64
	maxAmount int64
}

func newSampler(dir bank.Directory, gen config.Generator) (*sampler, error) {
	s := &sampler{
		rng:       rand.New(rand.NewPCG(uint64(gen.Seed), uint64(gen.Seed))),
		multiBank: len(dir.Banks) > 1,
		minAmount: gen.MinAmount,
		maxAmount: gen.MaxAmount,
	}
	for _, b := range dir.Banks {
		for _, id := range b.Accounts {
			s.accounts = append(s.accounts, account{id: id, bank: b.ID, partition: b.Partition})
		}
	}
	if len(s.accounts) < 2 {
		return nil, errors.New("a trade needs two accounts, and there is only one")
	}
	s.rng.Shuffle(len(s.accounts), func(i, j int) { s.accounts[i], s.accounts[j] = s.accounts[j], s.accounts[i] })

	if gen.Distribution == config.Zipf {
		s.pick = zipf(s.rng, len(s.accounts), gen.ZipfExponent)
	} else {
		n := len(s.accounts)
		s.pick = func() int { return s.rng.IntN(n) }
	}
	return s, nil
}

// zipf picks the index of rank k (1-based) with a weight of 1/k^exponent:
// a few accounts trade much more than the rest. It draws a point on the
// cumulative weights and finds its rank by binary search.
func zipf(rng *rand.Rand, n int, exponent float64) func() int {
	cumulative := make([]float64, n)
	total := 0.0
	for i := range cumulative {
		total += 1 / math.Pow(float64(i+1), exponent)
		cumulative[i] = total
	}
	return func() int {
		u := rng.Float64() * total
		return min(sort.Search(n, func(i int) bool { return cumulative[i] > u }), n-1)
	}
}

// pair picks a sender and a receiver: two different accounts, at different
// banks when there is more than one bank (the thesis only generates trades
// between banks).
func (s *sampler) pair() (from, to account) {
	from = s.accounts[s.pick()]
	for {
		to = s.accounts[s.pick()]
		if to.id != from.id && (!s.multiBank || to.bank != from.bank) {
			return from, to
		}
	}
}

// amount picks an amount in [minAmount, maxAmount].
func (s *sampler) amount() int64 {
	return s.minAmount + s.rng.Int64N(s.maxAmount-s.minAmount+1)
}
