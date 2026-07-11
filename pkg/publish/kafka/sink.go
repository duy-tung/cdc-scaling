// Package kafka implements the Kafka sink used by the publisher pool.
package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/serialize"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// Config holds Kafka producer settings. Defaults mirror the benchmarked
// original Nomios producer: acks=all, idempotence on, lz4 compression.
type Config struct {
	Brokers []string `yaml:"brokers"`
	// TopicTemplate routes events to topics; {database} and {table} are
	// substituted per event. Default: "cdc.{database}.{table}".
	TopicTemplate string `yaml:"topicTemplate"`
	// Compression: "lz4" (default), "snappy", "gzip", "zstd", "none".
	Compression string `yaml:"compression"`
	// TopicOverrides maps "db.table" to an explicit topic name.
	TopicOverrides map[string]string `yaml:"topicOverrides"`
	// AllowAutoTopicCreation asks brokers to auto-create topics.
	AllowAutoTopicCreation bool `yaml:"allowAutoTopicCreation"`
	// MaxInflightBatches bounds the total number of submitted-but-unacked
	// batches across all publishers (memory/backpressure bound for the
	// asynchronous produce path). Default 32.
	MaxInflightBatches int `yaml:"maxInflightBatches"`
}

func (c *Config) withDefaults() {
	if c.TopicTemplate == "" {
		c.TopicTemplate = "cdc.{database}.{table}"
	}
	if c.Compression == "" {
		c.Compression = "lz4"
	}
	if c.MaxInflightBatches <= 0 {
		c.MaxInflightBatches = 32
	}
}

// Sink publishes serialized NomiosEvents to Kafka asynchronously: a batch
// is submitted to the client and acknowledged via per-record promises.
// Waiting out the produce round-trip in the publisher loop capped a single
// publisher at ~208k ev/s; with async submission the publisher keeps
// draining while previous batches are in flight. Per-partition ordering is
// preserved by franz-go's idempotent producer (on by default), so per-key
// ordering survives multiple in-flight batches.
//
// Safe for use by many publisher goroutines concurrently.
type Sink struct {
	cl  *kgo.Client
	ser serialize.Serializer
	cfg Config

	inflight chan struct{}  // semaphore: bounds unacked batches
	pending  sync.WaitGroup // one unit per in-flight batch

	errMu    sync.Mutex
	firstErr error // first permanent produce failure since the last Reset

	// topics caches resolved topic names per table so the hot path does no
	// template substitution (strings.NewReplacer per event showed up in
	// allocation profiles).
	topicMu sync.RWMutex
	topics  map[tableKey]string
}

type tableKey struct{ db, table string }

func New(cfg Config, ser serialize.Serializer) (*Sink, error) {
	cfg.withDefaults()
	// The per-key ordering guarantee depends on producer idempotence
	// (per-partition ordering across in-flight requests) and deterministic
	// key hashing. Both are franz-go defaults, but they are load-bearing
	// here, so pin them explicitly rather than inherit whatever a future
	// dependency default might be. Do NOT add kgo.DisableIdempotentWrite.
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}
	if cfg.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	switch cfg.Compression {
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	case "none":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	default:
		return nil, fmt.Errorf("kafka: unknown compression %q", cfg.Compression)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: client: %w", err)
	}
	return &Sink{
		cl:       cl,
		ser:      ser,
		cfg:      cfg,
		inflight: make(chan struct{}, cfg.MaxInflightBatches),
		topics:   make(map[tableKey]string),
	}, nil
}

// Topic resolves the destination topic for an event.
func (s *Sink) Topic(e *event.NomiosEvent) string {
	k := tableKey{e.Source.Database, e.Source.Table}
	s.topicMu.RLock()
	t, ok := s.topics[k]
	s.topicMu.RUnlock()
	if ok {
		return t
	}
	if t, ok = s.cfg.TopicOverrides[e.Source.FQTN()]; !ok {
		r := strings.NewReplacer("{database}", e.Source.Database, "{table}", e.Source.Table)
		t = r.Replace(s.cfg.TopicTemplate)
	}
	s.topicMu.Lock()
	s.topics[k] = t
	s.topicMu.Unlock()
	return t
}

// Err reports the first asynchronous produce failure since the last
// Reset. The hyperloop's committer polls it so an idle pipeline surfaces
// a failed producer instead of appearing healthy until the next publish.
func (s *Sink) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.firstErr
}

func (s *Sink) setErr(err error) {
	s.errMu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	s.errMu.Unlock()
}

// Reset clears the sticky produce error. The hyperloop calls it when a
// run starts: after a failure the checkpoint never advanced past the
// unacknowledged events, so the restarted run replays them — keeping the
// old error would make every Manager restart fail instantly and
// permanently against a recovered broker.
func (s *Sink) Reset() {
	s.errMu.Lock()
	s.firstErr = nil
	s.errMu.Unlock()
}

// PublishBatch serializes and submits a batch. It returns once the batch
// is handed to the producer (bounded by MaxInflightBatches); done fires
// with the batch's positions when every record is acknowledged. On any
// permanent record failure done is never called and the error surfaces on
// the next PublishBatch/Flush.
func (s *Sink) PublishBatch(ctx context.Context, events []*event.NomiosEvent, done func([]state.Position)) error {
	if err := s.Err(); err != nil {
		return err
	}
	// Serialize before taking an in-flight slot so a serialization error
	// submits nothing.
	records := make([]*kgo.Record, len(events))
	positions := make([]state.Position, len(events))
	for i, e := range events {
		key, value, err := s.ser.Serialize(e)
		if err != nil {
			return fmt.Errorf("kafka: serialize event %s: %w", e.ID, err)
		}
		records[i] = &kgo.Record{Topic: s.Topic(e), Key: key, Value: value}
		positions[i] = e.Position
	}

	select {
	case s.inflight <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.pending.Add(1)

	var remaining atomic.Int64
	remaining.Store(int64(len(records)))
	var failed atomic.Bool
	promise := func(_ *kgo.Record, err error) {
		if err != nil {
			failed.Store(true)
			s.setErr(fmt.Errorf("kafka: produce: %w", err))
		}
		if remaining.Add(-1) == 0 {
			<-s.inflight
			if !failed.Load() {
				done(positions)
			}
			s.pending.Done()
		}
	}
	for _, r := range records {
		s.cl.Produce(ctx, r, promise)
	}
	return nil
}

// Flush waits until every submitted batch is acknowledged and its done
// callback has returned, then reports any produce failure.
func (s *Sink) Flush(ctx context.Context) error {
	if err := s.cl.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush: %w", err)
	}
	waited := make(chan struct{})
	go func() { s.pending.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.Err()
}

func (s *Sink) Close() error {
	s.cl.Close()
	return nil
}
