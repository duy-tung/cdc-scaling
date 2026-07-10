package dispatch

import (
	"context"
	"fmt"
	"testing"

	"github.com/duy-tung/cdc-scaling/pkg/event"
)

func ev(db, table, key string) *event.NomiosEvent {
	return &event.NomiosEvent{
		Key:    key,
		Source: event.SourceMeta{Database: db, Table: table},
		After:  map[string]any{"seller_id": 42, "deal_id": 7},
	}
}

func TestPickStableForSameKey(t *testing.T) {
	d := New(8, 16, DefaultKey(nil))
	first := d.Pick(ev("db", "t", "db.t|1"))
	for i := 0; i < 100; i++ {
		if got := d.Pick(ev("db", "t", "db.t|1")); got != first {
			t.Fatalf("same key routed to different queues: %d vs %d", got, first)
		}
	}
}

func TestPickSpreadsKeys(t *testing.T) {
	d := New(8, 16, DefaultKey(nil))
	seen := map[int]bool{}
	for i := 0; i < 1000; i++ {
		seen[d.Pick(ev("db", "t", fmt.Sprintf("db.t|%d", i)))] = true
	}
	if len(seen) != 8 {
		t.Fatalf("1000 keys hit only %d of 8 queues", len(seen))
	}
}

func TestKeyOverrides(t *testing.T) {
	kf := DefaultKey(map[string][]string{"db.t": {"seller_id"}})
	e := ev("db", "t", "db.t|pk")
	if got := kf(e); got != "db.t|42" {
		t.Fatalf("override key = %q, want db.t|42", got)
	}
	// No override: falls back to source-provided key.
	e2 := ev("db", "other", "db.other|pk")
	if got := kf(e2); got != "db.other|pk" {
		t.Fatalf("fallback key = %q", got)
	}
}

func TestRunRoutesAndCloses(t *testing.T) {
	// Queues are not drained until Run finishes, so capacity must hold all
	// events routed to any one queue.
	d := New(4, 128, DefaultKey(nil))
	in := make(chan *event.NomiosEvent)
	done := make(chan error, 1)
	go func() { done <- d.Run(context.Background(), in) }()

	byQueue := map[int][]string{}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("db.t|%d", i%10)
		e := ev("db", "t", k)
		byQueue[d.Pick(e)] = append(byQueue[d.Pick(e)], k)
		in <- e
	}
	close(in)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// All queues must be closed and hold their routed events in order.
	total := 0
	for i, q := range d.Queues() {
		var got []string
		for e := range q { // range ends only if closed
			got = append(got, e.Key)
		}
		total += len(got)
		want := byQueue[i]
		if len(got) != len(want) {
			t.Fatalf("queue %d: got %d events, want %d", i, len(got), len(want))
		}
		for j := range got {
			if got[j] != want[j] {
				t.Fatalf("queue %d out of order at %d: %s vs %s", i, j, got[j], want[j])
			}
		}
	}
	if total != 100 {
		t.Fatalf("total events = %d, want 100", total)
	}
}

func TestKeyFromColumns(t *testing.T) {
	row := map[string]any{"id": 5, "name": "x"}
	if got := KeyFromColumns("db.t", row, []string{"id"}); got != "db.t|5" {
		t.Fatalf("got %q", got)
	}
	// No key columns: whole row, deterministic.
	a := KeyFromColumns("db.t", row, nil)
	b := KeyFromColumns("db.t", row, nil)
	if a != b {
		t.Fatalf("keyless fallback not deterministic: %q vs %q", a, b)
	}
}
