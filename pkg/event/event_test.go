package event

import "testing"

func TestRowSelectsImage(t *testing.T) {
	before := map[string]any{"id": 1}
	after := map[string]any{"id": 2}

	cases := []struct {
		op   Op
		want map[string]any
	}{
		{OpInsert, after},
		{OpUpdate, after},
		{OpDelete, before},
	}
	for _, c := range cases {
		e := &NomiosEvent{Op: c.op, Before: before, After: after}
		if got := e.Row(); got["id"] != c.want["id"] {
			t.Errorf("op %s: Row() = %v, want %v", c.op, got, c.want)
		}
	}
}

func TestFQTN(t *testing.T) {
	m := SourceMeta{Database: "catalog", Table: "deals"}
	if got := m.FQTN(); got != "catalog.deals" {
		t.Fatalf("FQTN = %q", got)
	}
}
