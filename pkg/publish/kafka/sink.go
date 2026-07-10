// Package kafka implements the Kafka sink used by the publisher pool.
package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/serialize"
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
}

func (c *Config) withDefaults() {
	if c.TopicTemplate == "" {
		c.TopicTemplate = "cdc.{database}.{table}"
	}
	if c.Compression == "" {
		c.Compression = "lz4"
	}
}

// Sink publishes serialized NomiosEvents to Kafka. It is safe for use by
// many publisher goroutines concurrently; franz-go batches per partition
// internally and its idempotent producer (on by default) preserves
// per-partition ordering.
type Sink struct {
	cl  *kgo.Client
	ser serialize.Serializer
	cfg Config

	// topics caches resolved topic names per table so the hot path does no
	// template substitution (strings.NewReplacer per event showed up in
	// allocation profiles).
	topicMu sync.RWMutex
	topics  map[tableKey]string
}

type tableKey struct{ db, table string }

func New(cfg Config, ser serialize.Serializer) (*Sink, error) {
	cfg.withDefaults()
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
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
	return &Sink{cl: cl, ser: ser, cfg: cfg, topics: make(map[tableKey]string)}, nil
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

// PublishBatch serializes and produces a batch, returning only after every
// record in the batch is acknowledged (or any fails).
func (s *Sink) PublishBatch(ctx context.Context, events []*event.NomiosEvent) error {
	records := make([]*kgo.Record, 0, len(events))
	for _, e := range events {
		key, value, err := s.ser.Serialize(e)
		if err != nil {
			return fmt.Errorf("kafka: serialize event %s: %w", e.ID, err)
		}
		records = append(records, &kgo.Record{Topic: s.Topic(e), Key: key, Value: value})
	}
	if err := s.cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce: %w", err)
	}
	return nil
}

func (s *Sink) Close() error {
	s.cl.Close()
	return nil
}
