package kafka

import (
	"strings"
	"testing"

	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/serialize"
)

func newTestSink(t *testing.T, cfg Config) *Sink {
	t.Helper()
	if cfg.Brokers == nil {
		// kgo connects lazily, so an unreachable seed is fine for tests
		// that never produce.
		cfg.Brokers = []string{"127.0.0.1:1"}
	}
	s, err := New(cfg, serialize.JSON{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func ev(db, table string) *event.NomiosEvent {
	return &event.NomiosEvent{Source: event.SourceMeta{Database: db, Table: table}}
}

func TestTopicTemplate(t *testing.T) {
	s := newTestSink(t, Config{})
	if got := s.Topic(ev("catalog", "deals")); got != "cdc.catalog.deals" {
		t.Fatalf("default template topic = %q", got)
	}

	s2 := newTestSink(t, Config{TopicTemplate: "nomios-{table}"})
	if got := s2.Topic(ev("catalog", "deals")); got != "nomios-deals" {
		t.Fatalf("custom template topic = %q", got)
	}
}

func TestTopicOverrides(t *testing.T) {
	s := newTestSink(t, Config{
		TopicOverrides: map[string]string{"catalog.deals": "deals-firehose"},
	})
	if got := s.Topic(ev("catalog", "deals")); got != "deals-firehose" {
		t.Fatalf("override topic = %q", got)
	}
	if got := s.Topic(ev("catalog", "products")); got != "cdc.catalog.products" {
		t.Fatalf("non-overridden topic = %q", got)
	}
}

func TestCompressionValidation(t *testing.T) {
	for _, c := range []string{"", "lz4", "snappy", "gzip", "zstd", "none"} {
		s, err := New(Config{Brokers: []string{"127.0.0.1:1"}, Compression: c}, serialize.JSON{})
		if err != nil {
			t.Fatalf("compression %q rejected: %v", c, err)
		}
		_ = s.Close()
	}
	if _, err := New(Config{Brokers: []string{"127.0.0.1:1"}, Compression: "brotli"}, serialize.JSON{}); err == nil ||
		!strings.Contains(err.Error(), "unknown compression") {
		t.Fatalf("bad compression not rejected: %v", err)
	}
}
