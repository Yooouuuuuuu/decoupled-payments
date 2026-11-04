package avro

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// testdata holds the golden encodings written by Apache Avro's Java library.
const testdata = "../../../schemas/testdata"

func TestEmbeddedSchemasMatchOriginals(t *testing.T) {
	originals, err := filepath.Glob("../../../schemas/*.avsc")
	if err != nil || len(originals) == 0 {
		t.Fatalf("no schemas found in /schemas: %v", err)
	}
	embedded, err := schemaFiles.ReadDir("schemas")
	if err != nil {
		t.Fatal(err)
	}
	if len(embedded) != len(originals) {
		t.Errorf("%d embedded schemas, %d in /schemas: run go generate ./...", len(embedded), len(originals))
	}
	for _, path := range originals {
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		copied, err := schemaFiles.ReadFile("schemas/" + filepath.Base(path))
		if err != nil || !bytes.Equal(original, copied) {
			t.Errorf("internal/avro/schemas/%s differs from /schemas: run go generate ./...", filepath.Base(path))
		}
	}
}

// golden checks one codec against <name>.bin, <name>.canonical.json and
// fingerprints.txt: decoding gives want, and encoding want gives the same bytes.
func golden[T any](t *testing.T, codec *Codec[T], name string, want T) {
	t.Helper()
	bin := readFile(t, name+".bin")
	got, err := codec.Decode(bin)
	if err != nil {
		t.Fatalf("decode %s.bin: %v", name, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decode %s.bin:\n got %+v\nwant %+v", name, got, want)
	}
	encoded, err := codec.Encode(want)
	if err != nil {
		t.Fatalf("encode %s: %v", name, err)
	}
	if !bytes.Equal(encoded, bin) {
		t.Errorf("encode %s:\n got % x\nwant % x", name, encoded, bin)
	}
	if canonical := strings.TrimSpace(string(readFile(t, name+".canonical.json"))); codec.schema.String() != canonical {
		t.Errorf("%s canonical form:\n got %s\nwant %s", name, codec.schema.String(), canonical)
	}
	if fp := fingerprints(t)[name]; codec.fingerprint != fp {
		t.Errorf("%s fingerprint %#016x, fingerprints.txt says %#016x", name, codec.fingerprint, fp)
	}
}

func TestGoldenBlock(t *testing.T) {
	golden(t, BlockCodec, "block", Block{Trades: []Trade{
		{
			SerialNumber: 4294967297, Outbank: "bank0", OutAccount: "bank0-0007", Inbank: "bank2", InAccount: "bank2-0042",
			OutbankPartition: 0, InbankPartition: 2, Amount: 1250, Category: Send,
			Timestamp1: 1730700000000, Timestamp2: 0,
		},
		{
			SerialNumber: 4294967298, Outbank: "bank1", OutAccount: "bank1-0003", Inbank: "bank0", InAccount: "bank0-0099",
			OutbankPartition: 1, InbankPartition: 0, Amount: 7, Category: Rejected,
			Timestamp1: 1730700000123, Timestamp2: 1730700000456,
		},
	}})
}

func TestGoldenLocalBalance(t *testing.T) {
	golden(t, LocalBalanceCodec, "localBalance", LocalBalance{Balance: 987654321})
}

func TestGoldenAccountInfo(t *testing.T) {
	golden(t, AccountInfoCodec, "accountInfo", AccountInfo{
		Bank: "bank1", BankPartition: 1, Accounts: []string{"bank1-0000", "bank1-0001", "bank1-0002"},
		InitialBalance: 10000, Model: ModelConcurrent, KafkaTransactions: true,
	})
}

func TestGoldenValidatorOffset(t *testing.T) {
	golden(t, ValidatorOffsetCodec, "validatorOffset", ValidatorOffset{Topic: "credits", Partition: 2, Offset: 12345})
}

func TestDecodeRejectsForeignData(t *testing.T) {
	block := readFile(t, "block.bin")
	for name, data := range map[string][]byte{
		"empty":                 {},
		"short header":          block[:9],
		"wrong marker":          append([]byte{0xC3, 0x02}, block[2:]...),
		"another schema":        readFile(t, "localBalance.bin"),
		"truncated payload":     block[:len(block)-3],
		"bytes after the value": append(append([]byte{}, block...), 0),
		"plain Avro, no frame":  block[headerLen:],
	} {
		if _, err := BlockCodec.Decode(data); err == nil {
			t.Errorf("%s: decoded without error", name)
		}
	}
}

func TestLargeArraysRoundTrip(t *testing.T) {
	// Arrays are written as one block, like Apache Avro's Java encoder: the
	// count 250 (zig-zag 500 = f4 03), then the items.
	in := Block{Trades: make([]Trade, 250)}
	for i := range in.Trades {
		in.Trades[i] = Trade{SerialNumber: int64(i), Category: Receive}
	}
	data, err := BlockCodec.Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data[headerLen:headerLen+2], []byte{0xf4, 0x03}) {
		t.Errorf("array starts with % x, want f4 03", data[headerLen:headerLen+2])
	}
	out, err := BlockCodec.Decode(data)
	if err != nil || !reflect.DeepEqual(in, out) {
		t.Errorf("round trip changed the block (%v)", err)
	}
}

func TestEncodeRejectsUnknownCategory(t *testing.T) {
	if _, err := BlockCodec.Encode(Block{Trades: []Trade{{Category: "LOST"}}}); err == nil {
		t.Error("encoded an unknown category")
	}
}

func readFile(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(testdata, name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// fingerprints parses fingerprints.txt: "<name> <full name> 0x<fingerprint>".
func fingerprints(t *testing.T) map[string]uint64 {
	t.Helper()
	out := map[string]uint64{}
	sc := bufio.NewScanner(bytes.NewReader(readFile(t, "fingerprints.txt")))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 {
			continue
		}
		fp, err := strconv.ParseUint(strings.TrimPrefix(fields[2], "0x"), 16, 64)
		if err != nil {
			t.Fatalf("fingerprints.txt: %v", err)
		}
		out[fields[0]] = fp
	}
	return out
}
