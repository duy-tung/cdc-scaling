package serialize

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/event"
)

// JSON serializes events into a Debezium-style JSON envelope:
// {"id","op","before","after","source","ts_ms"}. The record key is the
// event's partition key so per-entity ordering carries into Kafka.
//
// The encoder is hand-rolled (append-based, no reflection): profiling
// showed encoding/json at ~35% of pipeline CPU and ~55% of allocations.
// The envelope layout is fixed, so reflection buys nothing. Row map keys
// are emitted in sorted order to keep payload bytes deterministic (useful
// for audit/dedupe comparisons downstream).
type JSON struct{}

func (JSON) Serialize(e *event.NomiosEvent) ([]byte, []byte, error) {
	buf := make([]byte, 0, eventSizeHint(e))
	buf, err := AppendEvent(buf, e)
	if err != nil {
		return nil, nil, err
	}
	return []byte(e.Key), buf, nil
}

// eventSizeHint estimates the encoded size to avoid growth reallocations.
func eventSizeHint(e *event.NomiosEvent) int {
	n := 192 + len(e.ID) + len(e.Source.Database) + len(e.Source.Table) +
		len(e.Source.GTID) + len(e.Source.File)
	n += 32 * (len(e.Before) + len(e.After))
	return n
}

// AppendEvent appends the JSON envelope of e to b.
func AppendEvent(b []byte, e *event.NomiosEvent) ([]byte, error) {
	var err error
	b = append(b, `{"id":`...)
	b = appendJSONString(b, e.ID)
	b = append(b, `,"op":`...)
	b = appendJSONString(b, string(e.Op))
	b = append(b, `,"before":`...)
	if b, err = appendRow(b, e.Before); err != nil {
		return nil, err
	}
	b = append(b, `,"after":`...)
	if b, err = appendRow(b, e.After); err != nil {
		return nil, err
	}
	b = append(b, `,"source":`...)
	b = appendSource(b, e.Source)
	b = append(b, `,"ts_ms":`...)
	b = strconv.AppendInt(b, e.OccurredAt.UnixMilli(), 10)
	b = append(b, '}')
	return b, nil
}

func appendSource(b []byte, m event.SourceMeta) []byte {
	// Field set and omitempty semantics mirror event.SourceMeta's json tags.
	b = append(b, `{"connector":`...)
	b = appendJSONString(b, m.Connector)
	if m.ServerID != 0 {
		b = append(b, `,"server_id":`...)
		b = strconv.AppendUint(b, uint64(m.ServerID), 10)
	}
	b = append(b, `,"db":`...)
	b = appendJSONString(b, m.Database)
	b = append(b, `,"table":`...)
	b = appendJSONString(b, m.Table)
	if m.GTID != "" {
		b = append(b, `,"gtid":`...)
		b = appendJSONString(b, m.GTID)
	}
	if m.File != "" {
		b = append(b, `,"file":`...)
		b = appendJSONString(b, m.File)
	}
	if m.Pos != 0 {
		b = append(b, `,"pos":`...)
		b = strconv.AppendUint(b, uint64(m.Pos), 10)
	}
	if m.TxOrder != 0 {
		b = append(b, `,"tx_order":`...)
		b = strconv.AppendInt(b, int64(m.TxOrder), 10)
	}
	return append(b, '}')
}

type kv struct {
	k string
	v any
}

func appendRow(b []byte, m map[string]any) ([]byte, error) {
	if m == nil {
		return append(b, "null"...), nil
	}
	// Sorted keys for deterministic output. Key/value pairs are collected
	// in one map pass (no per-key lookups later); small rows — the common
	// case — sort in a stack array without allocating.
	var arr [32]kv
	pairs := arr[:0]
	if len(m) > len(arr) {
		pairs = make([]kv, 0, len(m))
	}
	for k, v := range m {
		pairs = append(pairs, kv{k, v})
	}
	if len(pairs) <= len(arr) {
		insertionSort(pairs)
	} else {
		slices.SortFunc(pairs, func(a, b kv) int { return strings.Compare(a.k, b.k) })
	}

	var err error
	b = append(b, '{')
	for i := range pairs {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONString(b, pairs[i].k)
		b = append(b, ':')
		if b, err = appendValue(b, pairs[i].v); err != nil {
			return nil, err
		}
	}
	return append(b, '}'), nil
}

func insertionSort(s []kv) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].k < s[j-1].k; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// appendValue encodes the value types the MySQL source produces (plus a
// generic fallback for anything exotic).
func appendValue(b []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(b, "null"...), nil
	case string:
		return appendJSONString(b, x), nil
	case json.RawMessage:
		// Pre-validated JSON (MySQL JSON columns): embed as-is so documents
		// arrive as nested objects, not quoted strings.
		if len(x) == 0 {
			return append(b, "null"...), nil
		}
		return append(b, x...), nil
	case []byte:
		// True binary data (BLOB/VARBINARY/...): base64, exactly like
		// encoding/json.
		if x == nil {
			return append(b, "null"...), nil
		}
		b = append(b, '"')
		n := base64.StdEncoding.EncodedLen(len(x))
		b = append(b, make([]byte, n)...)
		base64.StdEncoding.Encode(b[len(b)-n:], x)
		return append(b, '"'), nil
	case bool:
		return strconv.AppendBool(b, x), nil
	case int:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int8:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int16:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int32:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(b, x, 10), nil
	case uint:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint8:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint16:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint32:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint64:
		return strconv.AppendUint(b, x, 10), nil
	case float32:
		return appendFloat(b, float64(x), 32)
	case float64:
		return appendFloat(b, x, 64)
	case time.Time:
		b = append(b, '"')
		b = x.AppendFormat(b, time.RFC3339Nano)
		return append(b, '"'), nil
	default:
		// Safety valve for types the fast paths don't cover.
		enc, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("serialize: value %T: %w", v, err)
		}
		return append(b, enc...), nil
	}
}

func appendFloat(b []byte, f float64, bits int) ([]byte, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("serialize: unsupported float value %v", f)
	}
	// Match encoding/json: shortest representation, 'e' only for very
	// large/small exponents.
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	return strconv.AppendFloat(b, f, format, -1, bits), nil
}

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a quoted, escaped JSON string.
func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		b = append(b, s[start:i]...)
		switch c {
		case '"', '\\':
			b = append(b, '\\', c)
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		default:
			b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		start = i + 1
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}
