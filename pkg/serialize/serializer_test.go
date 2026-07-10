package serialize

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/event"
)

func TestJSONSerialize(t *testing.T) {
	e := &event.NomiosEvent{
		ID:     "binlog.000001:456:0",
		Op:     event.OpUpdate,
		Key:    "db.items|5",
		Before: map[string]any{"id": 5, "qty": 1},
		After:  map[string]any{"id": 5, "qty": 2},
		Source: event.SourceMeta{
			Connector: "mysql", Database: "db", Table: "items",
			File: "binlog.000001", Pos: 456,
		},
		OccurredAt: time.UnixMilli(1657205700000),
	}
	key, value, err := JSON{}.Serialize(e)
	if err != nil {
		t.Fatal(err)
	}
	if string(key) != "db.items|5" {
		t.Fatalf("key = %q", key)
	}
	var env map[string]any
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatal(err)
	}
	if env["op"] != "u" || env["id"] != "binlog.000001:456:0" {
		t.Fatalf("envelope = %v", env)
	}
	if env["ts_ms"].(float64) != 1657205700000 {
		t.Fatalf("ts_ms = %v", env["ts_ms"])
	}
	after := env["after"].(map[string]any)
	if after["qty"].(float64) != 2 {
		t.Fatalf("after = %v", after)
	}
	src := env["source"].(map[string]any)
	if src["db"] != "db" || src["table"] != "items" {
		t.Fatalf("source = %v", src)
	}
}
