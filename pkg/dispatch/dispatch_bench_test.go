package dispatch

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/duy-tung/cdc-scaling/pkg/event"
)

// BenchmarkPick measures the routing cost per event (key extraction + hash).
func BenchmarkPick(b *testing.B) {
	d := New(8, 1024, 64, DefaultKey(nil))
	events := make([]*event.NomiosEvent, 1024)
	for i := range events {
		events[i] = ev("db", "t", fmt.Sprintf("db.t|%d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.Pick(events[i%len(events)])
	}
}

// BenchmarkDispatchThroughput measures end-to-end dispatcher throughput
// with concurrent drainers, across queue counts.
func BenchmarkDispatchThroughput(b *testing.B) {
	for _, queues := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("queues=%d", queues), func(b *testing.B) {
			const microBatch = 128
			d := New(queues, 4096, 256, DefaultKey(nil))
			in := make(chan []*event.NomiosEvent, 32)

			var wg sync.WaitGroup
			for _, q := range d.Queues() {
				wg.Add(1)
				go func(q <-chan []*event.NomiosEvent) {
					defer wg.Done()
					for range q {
					}
				}(q)
			}
			done := make(chan error, 1)
			go func() { done <- d.Run(context.Background(), in) }()

			events := make([]*event.NomiosEvent, 1024)
			for i := range events {
				events[i] = ev("db", "t", fmt.Sprintf("db.t|%d", i))
			}

			b.ResetTimer()
			sent := 0
			for sent < b.N {
				n := microBatch
				if b.N-sent < n {
					n = b.N - sent
				}
				batch := make([]*event.NomiosEvent, n)
				for j := 0; j < n; j++ {
					batch[j] = events[(sent+j)%len(events)]
				}
				in <- batch
				sent += n
			}
			close(in)
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			wg.Wait()
		})
	}
}
