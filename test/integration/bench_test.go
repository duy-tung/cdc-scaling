//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/duy-tung/cdc-scaling/pkg/hyperloop"
	"github.com/duy-tung/cdc-scaling/pkg/publish/kafka"
	"github.com/duy-tung/cdc-scaling/pkg/serialize"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// BenchmarkPipelineToKafka runs the complete pipeline — generated source,
// dispatcher, publisher pool, JSON serialization, real franz-go producer —
// against an in-memory Kafka cluster, sweeping the publisher pool size.
//
// Run with:
//
//	go test -tags integration -bench BenchmarkPipelineToKafka -run xxx ./test/integration/
func BenchmarkPipelineToKafka(b *testing.B) {
	for _, queues := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("queues=%d", queues), func(b *testing.B) {
			benchPipeline(b, queues, "none")
		})
	}
}

// BenchmarkPipelineCompression sweeps producer compression codecs at a
// fixed pool size (the "try compression" item from the original TODO).
func BenchmarkPipelineCompression(b *testing.B) {
	for _, codec := range []string{"none", "lz4", "snappy", "gzip"} {
		b.Run("codec="+codec, func(b *testing.B) {
			benchPipeline(b, 8, codec)
		})
	}
}

func benchPipeline(b *testing.B, queues int, compression string) {
	topic := "cdc.gen.items"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(8, topic))
	if err != nil {
		b.Fatal(err)
	}
	defer cluster.Close()

	store, err := state.NewFileStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	sink, err := kafka.New(kafka.Config{
		Brokers:     cluster.ListenAddrs(),
		Compression: compression,
	}, serialize.JSON{})
	if err != nil {
		b.Fatal(err)
	}

	hl, err := hyperloop.New(hyperloop.Config{
		ID:             "bench",
		Queues:         queues,
		QueueCapacity:  4096,
		BatchMaxSize:   500,
		BatchMaxWait:   time.Millisecond,
		CommitInterval: 100 * time.Millisecond,
	}, hyperloop.Deps{
		Source: &genSource{n: b.N, keys: 512},
		Sink:   sink,
		Store:  store,
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

	// The final checkpoint doubles as the completeness check.
	ckpt, err := store.Load(context.Background(), "bench")
	if err != nil {
		b.Fatal(err)
	}
	if ckpt.SeqNo != uint64(b.N) {
		b.Fatalf("checkpoint = %d, want %d", ckpt.SeqNo, b.N)
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "events/s")
}
