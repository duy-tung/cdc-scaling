package state

import (
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkTrackerDoneInOrder is the fast path: events complete in SeqNo
// order, the contiguous prefix advances every call.
func BenchmarkTrackerDoneInOrder(b *testing.B) {
	tr := NewTracker()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		tr.Done(Position{SeqNo: uint64(i)})
	}
	if ckpt, ok := tr.Checkpoint(); !ok || ckpt.SeqNo != uint64(b.N) {
		b.Fatalf("checkpoint = %v", ckpt.SeqNo)
	}
}

// BenchmarkTrackerDoneConcurrent models the real pipeline: many publishers
// completing interleaved SeqNos concurrently.
func BenchmarkTrackerDoneConcurrent(b *testing.B) {
	tr := NewTracker()
	var next atomic.Uint64
	b.ResetTimer()
	var wg sync.WaitGroup
	workers := 8
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				s := next.Add(1)
				if s > uint64(b.N) {
					return
				}
				tr.Done(Position{SeqNo: s})
			}
		}()
	}
	wg.Wait()
	if ckpt, ok := tr.Checkpoint(); !ok || ckpt.SeqNo != uint64(b.N) {
		b.Fatalf("checkpoint = %v", ckpt.SeqNo)
	}
}
