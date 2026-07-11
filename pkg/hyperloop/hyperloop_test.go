package hyperloop

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

func (s *fakeSource) Start(ctx context.Context, _ state.Position, out chan<- []*event.NomiosEvent) error {
	const microBatch = 64
	perKey := map[string]int{}
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
		key := fmt.Sprintf("db.t|%d", i%s.keys)
		perKey[key]++
		batch = append(batch, &event.NomiosEvent{
			ID:  fmt.Sprintf("e%d", i),
			Op:  event.OpInsert,
			Key: key,
			After: map[string]any{
				"key": key,
				"seq": perKey[key], // per-key sequence for order assertions
			},
			Source:   event.SourceMeta{Connector: "fake", Database: "db", Table: "t"},
			Position: state.Position{SeqNo: uint64(i), File: "fake", Offset: uint32(i)},
		})
		if len(batch) == microBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
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

func (s *memSink) PublishBatch(_ context.Context, evs []*event.NomiosEvent, done func([]state.Position)) error {
	s.mu.Lock()
	if s.fail != nil {
		s.mu.Unlock()
		return s.fail
	}
	s.events = append(s.events, evs...)
	ps := make([]state.Position, len(evs))
	for i, e := range evs {
		ps[i] = e.Position
	}
	s.mu.Unlock()
	done(ps)
	return nil
}
func (s *memSink) Flush(context.Context) error { return nil }
func (s *memSink) Close() error                { return nil }

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

// flakyStore fails its first N saves, simulating a temporarily unreachable
// state store.
type flakyStore struct {
	*memStore
	failMu    sync.Mutex
	failFirst int
}

func (s *flakyStore) Save(ctx context.Context, id string, p state.Position) error {
	s.failMu.Lock()
	if s.failFirst > 0 {
		s.failFirst--
		s.failMu.Unlock()
		return errors.New("state store unavailable")
	}
	s.failMu.Unlock()
	return s.memStore.Save(ctx, id, p)
}

// TestHyperloopRetriesFailedStateSave: a failed periodic checkpoint save
// must be retried on the next commit tick even when the checkpoint has not
// advanced (Tracker.Redirty), and the failure must be counted.
func TestHyperloopRetriesFailedStateSave(t *testing.T) {
	const total = 500
	src := &fakeSource{n: total, keys: 7, blockAtEnd: true}
	sink := &memSink{}
	store := &flakyStore{memStore: newMemStore(), failFirst: 2}
	hl := newTestLoop(t, src, sink, store)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- hl.Run(ctx) }()

	// Wait until the periodic committer succeeded despite the initial
	// failures — the stream is idle after `total`, so only Redirty makes
	// the retry happen before shutdown.
	deadline := time.After(10 * time.Second)
	for store.get("test").SeqNo != total {
		select {
		case <-deadline:
			t.Fatalf("checkpoint never persisted; store=%+v", store.get("test"))
		case <-time.After(5 * time.Millisecond):
		}
	}
	if got := hl.Status().StateSaveFailures; got < 2 {
		t.Fatalf("StateSaveFailures = %d, want >= 2", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

// asyncSink acknowledges batches from separate goroutines after a small
// random-ish delay, exercising the asynchronous Sink contract: done fires
// after PublishBatch returned, possibly out of order across batches.
type asyncSink struct {
	memSink
	wg sync.WaitGroup
}

func (s *asyncSink) PublishBatch(_ context.Context, evs []*event.NomiosEvent, done func([]state.Position)) error {
	s.mu.Lock()
	s.events = append(s.events, evs...)
	ps := make([]state.Position, len(evs))
	for i, e := range evs {
		ps[i] = e.Position
	}
	delay := time.Duration(len(s.events)%7) * time.Millisecond // staggered acks
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		time.Sleep(delay)
		done(ps)
	}()
	return nil
}

func (s *asyncSink) Flush(context.Context) error {
	s.wg.Wait()
	return nil
}

// TestHyperloopAsyncSink verifies the pipeline with a sink that completes
// batches asynchronously and out of order: all events published, the final
// checkpoint still reaches the last SeqNo (Flush-before-commit), and the
// contiguous-prefix tracker handles the reordering.
func TestHyperloopAsyncSink(t *testing.T) {
	const total = 3000
	src := &fakeSource{n: total, keys: 17}
	sink := &asyncSink{}
	store := newMemStore()
	hl := newTestLoop(t, src, sink, store)

	if err := hl.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(sink.events); got != total {
		t.Fatalf("published %d events, want %d", got, total)
	}
	if ckpt := store.get("test"); ckpt.SeqNo != total {
		t.Fatalf("checkpoint seq = %d, want %d (async acks must be flushed before final commit)", ckpt.SeqNo, total)
	}
}

// stuckSink blocks forever in PublishBatch, ignoring its context — the
// worst case the bounded-shutdown path must survive.
type stuckSink struct {
	entered chan struct{}
	once    sync.Once
}

func (s *stuckSink) PublishBatch(context.Context, []*event.NomiosEvent, func([]state.Position)) error {
	s.once.Do(func() { close(s.entered) })
	select {} // never returns
}
func (s *stuckSink) Flush(context.Context) error { return nil }
func (s *stuckSink) Close() error                { return nil }

// TestHyperloopAbandonsStuckShutdown: with a sink that ignores
// cancellation, a graceful stop must still return within roughly
// 2×ShutdownTimeout, reporting the abandonment instead of hanging forever.
func TestHyperloopAbandonsStuckShutdown(t *testing.T) {
	src := &fakeSource{n: 10, keys: 2, blockAtEnd: true}
	sink := &stuckSink{entered: make(chan struct{})}
	hl, err := New(Config{
		ID:              "stuck",
		Queues:          1,
		BatchMaxWait:    time.Millisecond,
		CommitInterval:  10 * time.Millisecond,
		ShutdownTimeout: 200 * time.Millisecond,
	}, Deps{Source: src, Sink: sink, Store: newMemStore()})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- hl.Run(ctx) }()

	<-sink.entered // the publisher is now wedged inside the sink
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "abandoned") {
			t.Fatalf("expected abandonment error, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return: shutdown is not bounded")
	}
	if hl.Status().Status != StatusFailed {
		t.Fatalf("status = %s, want failed", hl.Status().Status)
	}
}

func TestDrainBatchRespectsMaxAndClose(t *testing.T) {
	mk := func(n int) []*event.NomiosEvent {
		out := make([]*event.NomiosEvent, n)
		for i := range out {
			out[i] = &event.NomiosEvent{Key: dispatch.KeyFromColumns("t", map[string]any{"i": i}, nil)}
		}
		return out
	}
	q := make(chan []*event.NomiosEvent, 10)
	q <- mk(3)
	q <- mk(3)
	q <- mk(4)

	// maxSize=4: first micro-batch (3) is under, second tops it to 6 —
	// drain may exceed maxSize by up to one micro-batch.
	batch, open := drainBatch(context.Background(), q, nil, 4, 50*time.Millisecond)
	if len(batch) != 6 || !open {
		t.Fatalf("batch=%d open=%v, want 6,true", len(batch), open)
	}
	close(q)
	batch, open = drainBatch(context.Background(), q, nil, 100, 50*time.Millisecond)
	if len(batch) != 4 || open {
		t.Fatalf("batch=%d open=%v, want 4,false", len(batch), open)
	}
	batch, open = drainBatch(context.Background(), q, nil, 100, 50*time.Millisecond)
	if len(batch) != 0 || open {
		t.Fatalf("batch=%d open=%v, want 0,false", len(batch), open)
	}

	// The caller's buffer is reused: same backing array when capacity fits.
	buf := make([]*event.NomiosEvent, 0, 8)
	q2 := make(chan []*event.NomiosEvent, 1)
	q2 <- mk(2)
	batch, _ = drainBatch(context.Background(), q2, buf, 2, time.Millisecond)
	if len(batch) != 2 || cap(batch) != cap(buf) {
		t.Fatalf("buffer not reused: len=%d cap=%d want cap=%d", len(batch), cap(batch), cap(buf))
	}
}
