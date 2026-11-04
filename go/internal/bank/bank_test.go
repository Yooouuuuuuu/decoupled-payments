package bank

import (
	"math"
	"strings"
	"testing"

	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/avro"
	"github.com/Yooouuuuuuu/decoupled-payments/go/internal/config"
)

func TestIDs(t *testing.T) {
	for got, want := range map[string]string{
		ID(0):                     "bank0",
		ID(12):                    "bank12",
		AccountID("bank0", 0):     "bank0-0000",
		AccountID("bank2", 42):    "bank2-0042",
		AccountID("bank1", 9999):  "bank1-9999",
		AccountID("bank1", 12345): "bank1-12345",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func info(p int32, accounts ...string) avro.AccountInfo {
	return avro.AccountInfo{Bank: ID(p), BankPartition: p, Accounts: accounts, InitialBalance: 100, Model: avro.ModelSequential, KafkaTransactions: true}
}

func TestNewDirectory(t *testing.T) {
	d, err := NewDirectory([]avro.AccountInfo{info(1, "bank1-0000"), info(0, "bank0-0000", "bank0-0001")})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Banks) != 2 || d.Banks[0].ID != "bank0" || d.Banks[1].Partition != 1 {
		t.Errorf("banks %+v", d.Banks)
	}
	if d.Model != config.Sequential || !d.KafkaTransactions || d.AccountCount() != 3 || d.TotalMoney() != 300 {
		t.Errorf("directory %+v", d)
	}
}

func TestNewDirectoryRejectsInconsistentRecords(t *testing.T) {
	differentModel := info(1)
	differentModel.Model = avro.ModelConcurrent
	wrongName := info(1)
	wrongName.Bank = "bankX"
	rich := func(p int32) avro.AccountInfo {
		i := info(p, "a", "b", "c")
		i.InitialBalance = math.MaxInt64 / 5 // 6 accounts: the total overflows
		return i
	}
	negative := info(0, "a")
	negative.InitialBalance = -1
	for name, infos := range map[string][]avro.AccountInfo{
		"empty":                      nil,
		"gap":                        {info(0), info(2)},
		"duplicate":                  {info(0), info(0)},
		"different model":            {info(0), differentModel},
		"wrong bank name":            {info(0), wrongName},
		"too much money":             {rich(0), rich(1)},
		"a negative initial balance": {negative},
	} {
		if _, err := NewDirectory(infos); err == nil {
			t.Errorf("%s: no error", name)
		} else if !strings.Contains(err.Error(), "accountInfo") {
			t.Errorf("%s: %v doesn't name accountInfo", name, err)
		}
	}
}

func TestTotalMoneyAtTheLimit(t *testing.T) {
	i := info(0, "a", "b", "c")
	i.InitialBalance = math.MaxInt64 / 3
	d, err := NewDirectory([]avro.AccountInfo{i})
	if err != nil || d.TotalMoney() != math.MaxInt64/3*3 {
		t.Errorf("total %d, %v", d.TotalMoney(), err)
	}
}

func TestModels(t *testing.T) {
	for _, m := range []config.Model{config.Sequential, config.Concurrent} {
		if back, err := ConfigModel(AvroModel(m)); err != nil || back != m {
			t.Errorf("%s: round trip gave %s, %v", m, back, err)
		}
	}
	if _, err := ConfigModel("PARALLEL"); err == nil {
		t.Error("accepted an unknown model")
	}
}
