package hyperloop

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/dispatch"
	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// fakeSource emits n events across k keys, then either returns (finite
// stream) or blocks until cancelled.
type fakeSource struct {
	n, keys    int
	blockAtEnd bool
}

func (s *fakeSource) Start(ctx context.Context, _ state.Position, out chan<- *event.NomiosEvent) error {
	perKey := map[string]int{}
	for i := 1; i <= s.n; i++ {
		key := fmt.Sprintf("db.t|%d", i%s.keys)
		perKey[key]++
		e := &event.NomiosEvent{
			ID:  fmt.Sprintf("e%d", i),
			Op:  event.OpInsert,
			Key: key,
			After: map[string]any{
				"key": key,
				"seq": perKey[key], // per-key sequence for order assertions
			},
			Source:   event.SourceMeta{Connector: "fake", Database: "db", Table: "t"},
			Position: state.Position{SeqNo: uint64(i), File: "fake", Offset: uint32(i)},
		}
		select {
		case out <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.blockAtEnd {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

// memSink records published events, optionally failing.
type memSink struct {
	mu     sync.Mutex
	events []*event.NomiosEvent
	fail   error
}

func (s *memSink) PublishBatch(_ context.Context, evs []*event.NomiosEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.events = append(s.events, evs...)
	return nil
}
func (s *memSink) Close() error { return nil }

func (s *memSink) byKey() map[string][]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]int{}
	for _, e := range s.events {
		out[e.Key] = append(out[e.Key], e.After["seq"].(int))
	}
	return out
}

// memStore is an in-memory state store.
type memStore struct {
	mu    sync.Mutex
	saved map[string]state.Position
	loads int
}

func newMemStore() *memStore { return &memStore{saved: map[string]state.Position{}} }

func (s *memStore) Load(_ context.Context, id string) (state.Position, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	return s.saved[id], nil
}
func (s *memStore) Save(_ context.Context, id string, p state.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved[id] = p
	return nil
}
func (s *memStore) get(id string) state.Position {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saved[id]
}

func newTestLoop(t *testing.T, src *fakeSource, sink Sink, store state.Store) *Hyperloop {
	t.Helper()
	hl, err := New(Config{
		ID:              "test",
		Queues:          4,
		QueueCapacity:   64,
		BatchMaxSize:    10,
		BatchMaxWait:    5 * time.Millisecond,
		CommitInterval:  10 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
	}, Deps{Source: src, Sink: sink, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	return hl
}

func TestHyperloopEndToEnd(t *testing.T) {
	const total, keys = 5000, 37
	src := &fakeSource{n: total, keys: keys}
	sink := &memSink{}
	store := newMemStore()
	hl := newTestLoop(t, src, sink, store)

	// Finite stream: Run returns when the source finishes and the pipeline
	// has fully drained.
	if err := hl.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := len(sink.events); got != total {
		t.Fatalf("published %d events, want %d", got, total)
	}
	// Per-key ordering: each key's seq values must be strictly increasing.
	for key, seqs := range sink.byKey() {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] != seqs[i-1]+1 {
				t.Fatalf("key %s out of order: %v", key, seqs)
			}
		}
	}
	// Final checkpoint == last event.
	if ckpt := store.get("test"); ckpt.SeqNo != total {
		t.Fatalf("checkpoint seq = %d, want %d", ckpt.SeqNo, total)
	}
	if hl.Status().Status != StatusStopped {
		t.Fatalf("status = %s, want stopped", hl.Status().Status)
	}
}

func TestHyperloopGracefulStopDrains(t *testing.T) {
	const total = 2000
	src := &fakeSource{n: total, keys: 10, blockAtEnd: true}
	sink := &memSink{}
	store := newMemStore()
	hl := newTestLoop(t, src, sink, store)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- hl.Run(ctx) }()

	// Wait until everything was published, then stop.
	deadline := time.After(10 * time.Second)
	for {
		sink.mu.Lock()
		n := len(sink.events)
		sink.mu.Unlock()
		if n == total {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for publishes, got %d", n)
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run returned error on graceful stop: %v", err)
	}
	if ckpt := store.get("test"); ckpt.SeqNo != total {
		t.Fatalf("checkpoint seq = %d, want %d", ckpt.SeqNo, total)
	}
}

func TestHyperloopSinkFailureAborts(t *testing.T) {
	src := &fakeSource{n: 100, keys: 5, blockAtEnd: true}
	sink := &memSink{fail: errors.New("broker down")}
	hl := newTestLoop(t, src, sink, newMemStore())

	done := make(chan error, 1)
	go func() { done <- hl.Run(context.Background()) }()

	select {
	case err := <-done:
		if err == nil || !errors.Is(err, sink.fail) {
			t.Fatalf("expected sink failure, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("hyperloop did not abort on sink failure")
	}
	if hl.Status().Status != StatusFailed {
		t.Fatalf("status = %s, want failed", hl.Status().Status)
	}
}

func TestDrainBatchRespectsMaxAndClose(t *testing.T) {
	q := make(chan *event.NomiosEvent, 10)
	for i := 0; i < 10; i++ {
		q <- &event.NomiosEvent{Key: dispatch.KeyFromColumns("t", map[string]any{"i": i}, nil)}
	}
	batch, open := drainBatch(context.Background(), q, 4, 50*time.Millisecond)
	if len(batch) != 4 || !open {
		t.Fatalf("batch=%d open=%v, want 4,true", len(batch), open)
	}
	close(q)
	batch, open = drainBatch(context.Background(), q, 100, 50*time.Millisecond)
	if len(batch) != 6 || open {
		t.Fatalf("batch=%d open=%v, want 6,false", len(batch), open)
	}
	batch, open = drainBatch(context.Background(), q, 100, 50*time.Millisecond)
	if len(batch) != 0 || open {
		t.Fatalf("batch=%d open=%v, want 0,false", len(batch), open)
	}
}
