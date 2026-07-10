package serialize

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/event"
)

// jsonEnvelope is the reference encoding: the hand-rolled encoder must be
// semantically identical to encoding/json marshalling this struct.
type jsonEnvelope struct {
	ID     string           `json:"id"`
	Op     event.Op         `json:"op"`
	Before map[string]any   `json:"before"`
	After  map[string]any   `json:"after"`
	Source event.SourceMeta `json:"source"`
	TsMs   int64            `json:"ts_ms"`
}

func referenceMarshal(t *testing.T, e *event.NomiosEvent) []byte {
	t.Helper()
	b, err := json.Marshal(jsonEnvelope{
		ID:     e.ID,
		Op:     e.Op,
		Before: e.Before,
		After:  e.After,
		Source: e.Source,
		TsMs:   e.OccurredAt.UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// assertSemanticallyEqual unmarshals both encodings and compares the
// resulting values (key order and float spelling may legally differ).
func assertSemanticallyEqual(t *testing.T, e *event.NomiosEvent) {
	t.Helper()
	_, got, err := (JSON{}).Serialize(e)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	want := referenceMarshal(t, e)

	var gotV, wantV any
	if err := json.Unmarshal(got, &gotV); err != nil {
		t.Fatalf("custom encoder produced invalid JSON: %v\n%s", err, got)
	}
	if err := json.Unmarshal(want, &wantV); err != nil {
		t.Fatalf("reference produced invalid JSON: %v", err)
	}
	if !reflect.DeepEqual(gotV, wantV) {
		t.Fatalf("encodings differ:\n got: %s\nwant: %s", got, want)
	}
}

// randomValue produces the value types the MySQL source emits.
func randomValue(r *rand.Rand) any {
	switch r.Intn(10) {
	case 0:
		return nil
	case 1:
		return r.Int63() - r.Int63()
	case 2:
		return int32(r.Int31() - r.Int31())
	case 3:
		return uint64(r.Uint64() >> 1)
	case 4:
		return r.NormFloat64() * math1e(r.Intn(25)-12)
	case 5:
		return r.Intn(2) == 1
	case 6:
		return randString(r, 0, 12)
	case 7:
		return randString(r, 0, 64) // includes escapes
	case 8:
		return float32(r.NormFloat64())
	default:
		return r.Intn(1000)
	}
}

func math1e(exp int) float64 {
	f := 1.0
	for i := 0; i < exp; i++ {
		f *= 10
	}
	for i := 0; i > exp; i-- {
		f /= 10
	}
	return f
}

// randString generates valid-UTF8 strings that exercise every escape path.
func randString(r *rand.Rand, min, max int) string {
	n := min + r.Intn(max-min+1)
	runes := make([]rune, n)
	for i := range runes {
		switch r.Intn(8) {
		case 0:
			runes[i] = rune(r.Intn(0x20)) // control chars
		case 1:
			runes[i] = []rune{'"', '\\', '\n', '\t', '\r', '/', '<', '&'}[r.Intn(8)]
		case 2:
			runes[i] = rune(0x80 + r.Intn(0x800)) // multi-byte
		case 3:
			runes[i] = []rune("âơ日本語🚀")[r.Intn(6)]
		default:
			runes[i] = rune('a' + r.Intn(26))
		}
	}
	return string(runes)
}

func randomRow(r *rand.Rand) map[string]any {
	if r.Intn(6) == 0 {
		return nil
	}
	m := make(map[string]any)
	for i, n := 0, r.Intn(40); i < n; i++ {
		m[fmt.Sprintf("%s_%d", randString(r, 1, 6), i)] = randomValue(r)
	}
	return m
}

// TestJSONDifferential fuzzes random events through both encoders and
// requires semantic equality. This is the correctness gate for replacing
// encoding/json (optimization O1).
func TestJSONDifferential(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	ops := []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete}
	for i := 0; i < 5000; i++ {
		e := &event.NomiosEvent{
			ID:     randString(r, 0, 30),
			Op:     ops[r.Intn(len(ops))],
			Key:    randString(r, 0, 20),
			Before: randomRow(r),
			After:  randomRow(r),
			Source: event.SourceMeta{
				Connector: "mysql",
				ServerID:  uint32(r.Intn(2)) * uint32(r.Int31()),
				Database:  randString(r, 1, 12),
				Table:     randString(r, 1, 12),
				GTID:      map[bool]string{true: randString(r, 1, 40), false: ""}[r.Intn(2) == 0],
				File:      map[bool]string{true: randString(r, 1, 16), false: ""}[r.Intn(2) == 0],
				Pos:       uint32(r.Intn(2)) * uint32(r.Int31()),
				TxOrder:   r.Intn(3),
			},
			OccurredAt: time.UnixMilli(r.Int63n(2e12)),
		}
		assertSemanticallyEqual(t, e)
	}
}

// TestJSONDeterministic verifies sorted-key output: identical events must
// produce identical bytes (payload comparability for audit/Makesure).
func TestJSONDeterministic(t *testing.T) {
	e := &event.NomiosEvent{
		ID: "x", Op: event.OpInsert, Key: "k",
		After:      map[string]any{"b": 1, "a": 2, "z": 3, "m": 4, "c": 5},
		Source:     event.SourceMeta{Connector: "mysql", Database: "d", Table: "t"},
		OccurredAt: time.UnixMilli(1),
	}
	_, first, err := (JSON{}).Serialize(e)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		_, again, err := (JSON{}).Serialize(e)
		if err != nil {
			t.Fatal(err)
		}
		if string(first) != string(again) {
			t.Fatalf("non-deterministic output:\n%s\n%s", first, again)
		}
	}
}
