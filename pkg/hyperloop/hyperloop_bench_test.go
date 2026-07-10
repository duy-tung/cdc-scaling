package hyperloop

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/serialize"
)

// serializeSink performs the real per-event publisher CPU work
// (serialization) and discards the result. It isolates pipeline scaling
// from broker I/O, which is what the pool-size sweep is about: Debezium's
// limit was the single thread doing parse/serialize/publish.
type serializeSink struct {
	ser   serialize.Serializer
	count atomic.Int64
	bytes atomic.Int64
}

func (s *serializeSink) PublishBatch(_ context.Context, evs []*event.NomiosEvent) error {
	var n int64
	for _, e := range evs {
		_, v, err := s.ser.Serialize(e)
		if err != nil {
			return err
		}
		n += int64(len(v))
	}
	s.count.Add(int64(len(evs)))
	s.bytes.Add(n)
	return nil
}

func (s *serializeSink) Close() error { return nil }

// BenchmarkHyperloop sweeps publisher pool size and batch size over the
// full pipeline (source → dispatcher → buffer queues → publisher pool →
// JSON serialization), reporting events/sec. This is the benchmark matrix
// from the design doc: pool size 1..16, batch size 100/500.
func BenchmarkHyperloop(b *testing.B) {
	for _, queues := range []int{1, 2, 4, 8, 16} {
		for _, batch := range []int{100, 500} {
			b.Run(fmt.Sprintf("queues=%d/batch=%d", queues, batch), func(b *testing.B) {
				src := &fakeSource{n: b.N, keys: 512}
				sink := &serializeSink{ser: serialize.JSON{}}
				hl, err := New(Config{
					ID:             "bench",
					Queues:         queues,
					QueueCapacity:  4096,
					BatchMaxSize:   batch,
					BatchMaxWait:   time.Millisecond,
					CommitInterval: 100 * time.Millisecond,
				}, Deps{
					Source: src, Sink: sink, Store: newMemStore(),
					Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
				})
				if err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				start := time.Now()
				if err := hl.Run(context.Background()); err != nil {
					b.Fatal(err)
				}
				elapsed := time.Since(start)
				b.StopTimer()

				if got := sink.count.Load(); got != int64(b.N) {
					b.Fatalf("published %d, want %d", got, b.N)
				}
				b.SetBytes(sink.bytes.Load() / int64(b.N))
				b.ReportMetric(float64(b.N)/elapsed.Seconds(), "events/s")
			})
		}
	}
}
