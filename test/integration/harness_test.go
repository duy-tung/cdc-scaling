//go:build integration

// Package integration contains end-to-end tests for the Nomios pipeline.
//
// Run with: go test -tags integration -race ./test/integration/...
//
// The Kafka side always runs against an in-memory kfake cluster (no broker
// install needed). The MySQL tests need a local MySQL 8 with ROW binlog +
// GTID and a "nomios" user; they skip with instructions when unavailable.
package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaCluster starts an in-memory Kafka cluster seeded with the topics.
func kafkaCluster(t *testing.T, topics ...string) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topics...))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// envelope mirrors the JSON serializer output.
type envelope struct {
	ID     string         `json:"id"`
	Op     string         `json:"op"`
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Source struct {
		Connector string `json:"connector"`
		Database  string `json:"db"`
		Table     string `json:"table"`
		GTID      string `json:"gtid"`
		File      string `json:"file"`
	} `json:"source"`
	TsMs int64 `json:"ts_ms"`

	key string // record key, filled by the consumer
}

// consumeUntil polls the topic and collects deduplicated envelopes (by
// event ID — the pipeline is at-least-once, so replays are legal) until
// `enough` returns true or the deadline passes. Per-key arrival order is
// preserved in the returned slice.
func consumeUntil(t *testing.T, brokers []string, topic string, timeout time.Duration, enough func(map[string]envelope) bool) []envelope {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer cl.Close()

	seen := map[string]envelope{}
	var ordered []envelope
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		if err := fetches.Err0(); err != nil && err != context.DeadlineExceeded {
			// Transient poll errors are fine; keep trying until deadline.
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var e envelope
			if err := json.Unmarshal(r.Value, &e); err != nil {
				t.Errorf("bad payload %q: %v", r.Value, err)
				return
			}
			e.key = string(r.Key)
			if _, dup := seen[e.ID]; dup {
				return
			}
			seen[e.ID] = e
			ordered = append(ordered, e)
		})
		if enough(seen) {
			return ordered
		}
	}
	t.Fatalf("timed out after %v waiting for records; got %d unique", timeout, len(seen))
	return nil
}

// atLeast returns an `enough` predicate for n unique events.
func atLeast(n int) func(map[string]envelope) bool {
	return func(seen map[string]envelope) bool { return len(seen) >= n }
}
