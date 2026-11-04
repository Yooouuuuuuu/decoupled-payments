package generator

import (
	"math"
	"testing"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/bank"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

func directory(banks, accountsPerBank int) bank.Directory {
	var dir bank.Directory
	for p := range int32(banks) {
		b := bank.Bank{ID: bank.ID(p), Partition: p}
		for j := range accountsPerBank {
			b.Accounts = append(b.Accounts, bank.AccountID(b.ID, j))
		}
		dir.Banks = append(dir.Banks, b)
	}
	return dir
}

func settings(distribution string) config.Generator {
	return config.Generator{Distribution: distribution, ZipfExponent: 1.0, MinAmount: 1, MaxAmount: 100, Seed: 42}
}

func mustSampler(t *testing.T, dir bank.Directory, gen config.Generator) *sampler {
	t.Helper()
	s, err := newSampler(dir, gen)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPairsAreBetweenBanks(t *testing.T) {
	for _, dist := range []string{config.Uniform, config.Zipf} {
		s := mustSampler(t, directory(3, 10), settings(dist))
		for range 10000 {
			from, to := s.pair()
			if from.bank == to.bank || from.id == to.id {
				t.Fatalf("%s: trade from %+v to %+v: want different banks", dist, from, to)
			}
		}
	}
}

func TestOneBankPairsDifferentAccounts(t *testing.T) {
	for _, dist := range []string{config.Uniform, config.Zipf} {
		s := mustSampler(t, directory(1, 2), settings(dist))
		for range 1000 {
			if from, to := s.pair(); from.id == to.id {
				t.Fatalf("%s: trade from %s to itself", dist, from.id)
			}
		}
	}
	if _, err := newSampler(directory(1, 1), settings(config.Uniform)); err == nil {
		t.Error("a single account can't trade")
	}
}

func TestTradeFields(t *testing.T) {
	s := mustSampler(t, directory(3, 10), settings(config.Uniform))
	runID := int64(1<<31 - 1)
	tr := s.trade(runID, 7)
	if tr.SerialNumber != runID*(1<<32)+7 {
		t.Errorf("serial number %d, want runID * 2^32 + 7", tr.SerialNumber)
	}
	if tr.Category != avro.Send || tr.Timestamp1 == 0 || tr.Timestamp2 != 0 {
		t.Errorf("new trade %+v", tr)
	}
	if bank.ID(tr.OutbankPartition) != tr.Outbank || bank.ID(tr.InbankPartition) != tr.Inbank {
		t.Errorf("partitions don't match banks: %+v", tr)
	}
}

func TestAmountsAreInRange(t *testing.T) {
	gen := settings(config.Uniform)
	gen.MinAmount, gen.MaxAmount = 5, 7
	s := mustSampler(t, directory(2, 5), gen)
	seen := map[int64]bool{}
	for range 1000 {
		a := s.amount()
		if a < 5 || a > 7 {
			t.Fatalf("amount %d outside [5, 7]", a)
		}
		seen[a] = true
	}
	if len(seen) != 3 {
		t.Errorf("amounts seen %v, want all of 5, 6, 7", seen)
	}
}

func TestSeedRepeatsTheSequence(t *testing.T) {
	a := mustSampler(t, directory(3, 10), settings(config.Zipf))
	b := mustSampler(t, directory(3, 10), settings(config.Zipf))
	for range 100 {
		af, at := a.pair()
		bf, bt := b.pair()
		if af != bf || at != bt || a.amount() != b.amount() {
			t.Fatal("the same seed gave different trades")
		}
	}
}

func TestUniformPicksEveryAccountAlike(t *testing.T) {
	s := mustSampler(t, directory(2, 10), settings(config.Uniform))
	counts := make([]int, len(s.accounts))
	const draws = 100000
	for range draws {
		counts[s.pick()]++
	}
	for i, c := range counts {
		if math.Abs(float64(c)-draws/20.0) > draws/20.0*0.1 {
			t.Errorf("account %d picked %d times, want about %d", i, c, draws/20)
		}
	}
}

func TestZipfWeightsRanks(t *testing.T) {
	// Rank k (1-based, a position in the shuffled list) has weight 1/k^s:
	// with s = 1 and 4 accounts the probabilities are 12/25, 6/25, 4/25, 3/25.
	gen := settings(config.Zipf)
	s := mustSampler(t, directory(2, 2), gen)
	counts := make([]int, 4)
	const draws = 200000
	for range draws {
		counts[s.pick()]++
	}
	for k, want := range []float64{12.0 / 25, 6.0 / 25, 4.0 / 25, 3.0 / 25} {
		if got := float64(counts[k]) / draws; math.Abs(got-want) > 0.01 {
			t.Errorf("rank %d: frequency %.3f, want %.3f", k+1, got, want)
		}
	}
}

func TestZipfExponentZeroIsUniform(t *testing.T) {
	gen := settings(config.Zipf)
	gen.ZipfExponent = 0
	s := mustSampler(t, directory(2, 2), gen)
	counts := make([]int, 4)
	for range 40000 {
		counts[s.pick()]++
	}
	for k, c := range counts {
		if math.Abs(float64(c)-10000) > 500 {
			t.Errorf("rank %d picked %d times, want about 10000", k+1, c)
		}
	}
}
