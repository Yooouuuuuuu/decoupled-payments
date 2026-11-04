package avro

import (
	"embed"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	hamba "github.com/hamba/avro/v2"
)

// schemaFiles are copies of /schemas. go generate refreshes them, and a test
// fails if they differ from the originals.
//
//go:generate sh -c "rm -f schemas/*.avsc && cp ../../../schemas/*.avsc schemas/"
//go:embed schemas/*.avsc
var schemaFiles embed.FS

// Codecs for the record values of each topic.
var (
	BlockCodec           = mustCodec[Block]("block-v1.avsc")
	LocalBalanceCodec    = mustCodec[LocalBalance]("localBalance-v1.avsc")
	AccountInfoCodec     = mustCodec[AccountInfo]("accountInfo-v1.avsc")
	ValidatorOffsetCodec = mustCodec[ValidatorOffset]("validatorOffset-v1.avsc")
)

// api writes every array as a single block holding all its items, without a
// byte-size header, as Apache Avro's Java encoder does, so that both
// implementations produce the same bytes. (hamba's default, blocks of 100
// items with byte sizes, is also valid Avro.)
var api = hamba.Config{BlockLength: math.MaxInt32, DisableBlockSizeHeader: true}.Freeze()

// Avro single-object encoding: the marker C3 01, the schema's 8-byte
// CRC-64-AVRO fingerprint (of its Parsing Canonical Form) in little-endian
// order, then the Avro binary payload. The fingerprint lets a reader check
// that a value was written with the schema it expects, without a schema
// registry.
const (
	marker0, marker1 = 0xC3, 0x01
	headerLen        = 10
)

// Codec encodes and decodes the values of one schema.
type Codec[T any] struct {
	schema      hamba.Schema
	fingerprint uint64
	header      []byte
}

func mustCodec[T any](file string) *Codec[T] {
	c, err := newCodec[T](file)
	if err != nil {
		panic(fmt.Sprintf("avro: schema %s: %v", file, err))
	}
	return c
}

func newCodec[T any](file string) (*Codec[T], error) {
	src, err := schemaFiles.ReadFile("schemas/" + file)
	if err != nil {
		return nil, err
	}
	// A private cache: each schema file defines its own named types.
	schema, err := hamba.ParseBytesWithCache(src, "", &hamba.SchemaCache{})
	if err != nil {
		return nil, err
	}
	fp, err := schema.FingerprintUsing(hamba.CRC64Avro)
	if err != nil {
		return nil, err
	}
	// hamba returns the 64-bit fingerprint as big-endian bytes; the
	// single-object header carries it little-endian.
	fingerprint := binary.BigEndian.Uint64(fp)
	header := binary.LittleEndian.AppendUint64([]byte{marker0, marker1}, fingerprint)
	return &Codec[T]{schema: schema, fingerprint: fingerprint, header: header}, nil
}

// Encode returns v's single-object encoding.
func (c *Codec[T]) Encode(v T) ([]byte, error) {
	payload, err := api.Marshal(c.schema, v)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(c.header)+len(payload))
	return append(append(out, c.header...), payload...), nil
}

// Decode decodes a single-object encoded value. A value written with another
// schema is an error, not a guess.
func (c *Codec[T]) Decode(data []byte) (T, error) {
	var v T
	if len(data) < headerLen || data[0] != marker0 || data[1] != marker1 {
		return v, errors.New("not an Avro single-object encoded value")
	}
	if fp := binary.LittleEndian.Uint64(data[2:headerLen]); fp != c.fingerprint {
		return v, fmt.Errorf("written with schema fingerprint %#016x, want %#016x (%s)",
			fp, c.fingerprint, c.schema.(hamba.NamedSchema).FullName())
	}
	// A Reader instead of Unmarshal, which ignores io.EOF and would decode a
	// truncated payload without an error. The payload must hold exactly one
	// value: reading one more byte must fail.
	r := hamba.NewReader(nil, 0, hamba.WithReaderConfig(api)).Reset(data[headerLen:])
	r.ReadVal(c.schema, &v)
	if r.Error != nil {
		return v, fmt.Errorf("invalid payload: %w", r.Error)
	}
	if r.Read(make([]byte, 1)); r.Error == nil {
		return v, errors.New("invalid payload: bytes left after the value")
	}
	return v, nil
}
