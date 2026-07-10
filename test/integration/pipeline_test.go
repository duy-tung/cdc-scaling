//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/hyperloop"
	"github.com/duy-tung/cdc-scaling/pkg/publish/kafka"
	"github.com/duy-tung/cdc-scaling/pkg/serialize"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// genSource emits `n` synthetic change events across `keys` entities.
type genSource struct {
	n, keys int
}

func (s *genSource) Start(ctx context.Context, _ state.Position, out chan<- []*event.NomiosEvent) error {
	const microBatch = 64
	perKey := map[int]int{}
	batch := make([]*event.NomiosEvent, 0, microBatch)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		select {
		case out <- batch:
			batch = make([]*event.NomiosEvent, 0, microBatch)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 1; i <= s.n; i++ {
		k := i % s.keys
		perKey[k]++
		batch = append(batch, &event.NomiosEvent{
			ID:  fmt.Sprintf("gen:%d", i),
			Op:  event.OpInsert,
			Key: fmt.Sprintf("gen.items|%d", k),
			After: map[string]any{
				"id":      k,
				"key_seq": perKey[k],
			},
			Source:     event.SourceMeta{Connector: "gen", Database: "gen", Table: "items"},
			OccurredAt: time.Now(),
			Position:   state.Position{SeqNo: uint64(i), File: "gen", Offset: uint32(i)},
		})
		if len(batch) == microBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// TestPipelineToKafka runs the full hyperloop (dispatcher, publisher pool,
// state manager, JSON serializer, real franz-go producer) against an
// in-memory Kafka cluster and verifies completeness, per-key ordering and
// the final checkpoint.
func TestPipelineToKafka(t *testing.T) {
	const total, keys = 20000, 101
	topic := "cdc.gen.items"
	cluster := kafkaCluster(t, topic)

	store, err := state.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sink, err := kafka.New(kafka.Config{
		Brokers:       cluster.ListenAddrs(),
		TopicTemplate: "cdc.{database}.{table}",
		Compression:   "none", // kfake compatibility across codecs
	}, serialize.JSON{})
	if err != nil {
		t.Fatal(err)
	}

	hl, err := hyperloop.New(hyperloop.Config{
		ID:             "pipe-test",
		Queues:         8,
		QueueCapacity:  1024,
		BatchMaxSize:   200,
		BatchMaxWait:   10 * time.Millisecond,
		CommitInterval: 20 * time.Millisecond,
	}, hyperloop.Deps{
		Source: &genSource{n: total, keys: keys},
		Sink:   sink,
		Store:  store,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := hl.Run(context.Background()); err != nil {
		t.Fatalf("hyperloop: %v", err)
	}
	elapsed := time.Since(start)

	events := consumeUntil(t, cluster.ListenAddrs(), topic, 30*time.Second, atLeast(total))
	if len(events) != total {
		t.Fatalf("got %d unique events, want %d", len(events), total)
	}

	// Per-key ordering must survive the whole pipeline including Kafka.
	lastSeq := map[string]float64{}
	for _, e := range events {
		seq := e.After["key_seq"].(float64)
		if seq <= lastSeq[e.key] {
			t.Fatalf("key %s out of order: %v after %v", e.key, seq, lastSeq[e.key])
		}
		lastSeq[e.key] = seq
	}

	// Checkpoint must equal the last emitted SeqNo after a clean finish.
	ckpt, err := store.Load(context.Background(), "pipe-test")
	if err != nil {
		t.Fatal(err)
	}
	if ckpt.SeqNo != total {
		t.Fatalf("checkpoint = %d, want %d", ckpt.SeqNo, total)
	}

	t.Logf("published %d events in %v (%.0f ev/s)", total, elapsed, float64(total)/elapsed.Seconds())
}

// TestPipelineResume verifies that a second run resumes from the persisted
// checkpoint: the store hands the source the saved position.
type positionRecorder struct {
	genSource
	got chan state.Position
}

func (s *positionRecorder) Start(ctx context.Context, from state.Position, out chan<- []*event.NomiosEvent) error {
	s.got <- from
	return s.genSource.Start(ctx, from, out)
}

func TestPipelineResume(t *testing.T) {
	topic := "cdc.gen.items"
	cluster := kafkaCluster(t, topic)
	dir := t.TempDir()

	run := func(src *positionRecorder) state.Position {
		store, err := state.NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		sink, err := kafka.New(kafka.Config{Brokers: cluster.ListenAddrs(), Compression: "none"}, serialize.JSON{})
		if err != nil {
			t.Fatal(err)
		}
		hl, err := hyperloop.New(hyperloop.Config{ID: "resume-test", CommitInterval: 10 * time.Millisecond},
			hyperloop.Deps{Source: src, Sink: sink, Store: store})
		if err != nil {
			t.Fatal(err)
		}
		if err := hl.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		p, err := store.Load(context.Background(), "resume-test")
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	src1 := &positionRecorder{genSource{n: 500, keys: 7}, make(chan state.Position, 1)}
	ckpt := run(src1)
	if from := <-src1.got; !from.IsZero() {
		t.Fatalf("first run should start from zero position, got %+v", from)
	}
	if ckpt.SeqNo != 500 {
		t.Fatalf("checkpoint after first run = %d, want 500", ckpt.SeqNo)
	}

	src2 := &positionRecorder{genSource{n: 100, keys: 7}, make(chan state.Position, 1)}
	_ = run(src2)
	if from := <-src2.got; from.SeqNo != 500 || from.Offset != 500 {
		t.Fatalf("second run resumed from %+v, want the first run's checkpoint", from)
	}
}
