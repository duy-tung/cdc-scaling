package state

import (
	"context"
	"sync"
	"testing"
)

func TestTrackerContiguousCheckpoint(t *testing.T) {
	tr := NewTracker()

	if _, ok := tr.Checkpoint(); ok {
		t.Fatal("expected no checkpoint before any Done")
	}

	// Complete 2 before 1: checkpoint must not advance past the gap.
	tr.Done(Position{SeqNo: 2, File: "b2"})
	if _, ok := tr.Checkpoint(); ok {
		t.Fatal("checkpoint advanced across a gap")
	}
	if tr.Outstanding() != 1 {
		t.Fatalf("outstanding = %d, want 1", tr.Outstanding())
	}

	tr.Done(Position{SeqNo: 1, File: "b1"})
	ckpt, ok := tr.Checkpoint()
	if !ok || ckpt.SeqNo != 2 || ckpt.File != "b2" {
		t.Fatalf("checkpoint = %+v, want seq 2", ckpt)
	}

	// Dirty flag semantics.
	if _, dirty := tr.TakeDirty(); !dirty {
		t.Fatal("expected dirty after advance")
	}
	if _, dirty := tr.TakeDirty(); dirty {
		t.Fatal("expected clean after TakeDirty")
	}

	tr.Done(Position{SeqNo: 3, File: "b3"})
	ckpt, _ = tr.Checkpoint()
	if ckpt.SeqNo != 3 {
		t.Fatalf("checkpoint seq = %d, want 3", ckpt.SeqNo)
	}
}

func TestTrackerRedirty(t *testing.T) {
	tr := NewTracker()

	// Redirty before any checkpoint exists is a no-op.
	tr.Redirty()
	if _, dirty := tr.TakeDirty(); dirty {
		t.Fatal("empty tracker must not become dirty")
	}

	tr.Done(Position{SeqNo: 1, File: "b1"})
	if p, dirty := tr.TakeDirty(); !dirty || p.SeqNo != 1 {
		t.Fatalf("TakeDirty = %+v, %v", p, dirty)
	}
	if _, dirty := tr.TakeDirty(); dirty {
		t.Fatal("second TakeDirty should be clean")
	}

	// A failed save calls Redirty: the SAME checkpoint must be retryable
	// on the next tick without the checkpoint advancing.
	tr.Redirty()
	p, dirty := tr.TakeDirty()
	if !dirty || p.SeqNo != 1 || p.File != "b1" {
		t.Fatalf("after Redirty, TakeDirty = %+v, %v; want the same checkpoint", p, dirty)
	}
}

func TestTrackerConcurrent(t *testing.T) {
	tr := NewTracker()
	const n = 10000
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for s := uint64(w + 1); s <= n; s += 8 {
				tr.Done(Position{SeqNo: s})
			}
		}(w)
	}
	wg.Wait()
	ckpt, ok := tr.Checkpoint()
	if !ok || ckpt.SeqNo != n {
		t.Fatalf("checkpoint seq = %d, want %d", ckpt.SeqNo, n)
	}
	if tr.Outstanding() != 0 {
		t.Fatalf("outstanding = %d, want 0", tr.Outstanding())
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	p, err := s.Load(ctx, "hl1")
	if err != nil || !p.IsZero() {
		t.Fatalf("fresh load = %+v, %v; want zero, nil", p, err)
	}

	want := Position{GTIDSet: "uuid:1-9", File: "binlog.000002", Offset: 4321, SeqNo: 77}
	if err := s.Save(ctx, "hl1", want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(ctx, "hl1")
	if err != nil || got != want {
		t.Fatalf("load = %+v, %v; want %+v", got, err, want)
	}
}
