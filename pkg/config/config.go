// Package config loads Nomios configuration (YAML) and builds runnable
// hyperloops from it.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/duy-tung/cdc-scaling/pkg/hyperloop"
	"github.com/duy-tung/cdc-scaling/pkg/publish/kafka"
	"github.com/duy-tung/cdc-scaling/pkg/serialize"
	mysqlsource "github.com/duy-tung/cdc-scaling/pkg/source/mysql"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// File is the root config document.
type File struct {
	Listen     string      `yaml:"listen"` // HTTP control plane address, default ":8080"
	Hyperloops []Hyperloop `yaml:"hyperloops"`
}

// Hyperloop is the declarative definition of one CDC flow.
type Hyperloop struct {
	ID        string   `yaml:"id"`
	AutoStart *bool    `yaml:"autoStart"` // default true
	Source    Source   `yaml:"source"`
	Pipeline  Pipeline `yaml:"pipeline"`
	Sink      Sink     `yaml:"sink"`
	State     State    `yaml:"state"`

	ShutdownTimeoutSec int `yaml:"shutdownTimeoutSec"`
}

type Source struct {
	MySQL *mysqlsource.Config `yaml:"mysql"`
}

type Pipeline struct {
	Queues        int                 `yaml:"queues"`
	QueueCapacity int                 `yaml:"queueCapacity"`
	Batch         Batch               `yaml:"batch"`
	KeyOverrides  map[string][]string `yaml:"keyOverrides"`
}

type Batch struct {
	MaxSize   int `yaml:"maxSize"`
	MaxWaitMs int `yaml:"maxWaitMs"`
}

type Sink struct {
	Kafka *kafka.Config `yaml:"kafka"`
}

type State struct {
	Store            string `yaml:"store"` // "mysql" | "file"
	Dir              string `yaml:"dir"`   // file store directory
	Addr             string `yaml:"addr"`  // mysql store address host:port
	User             string `yaml:"user"`
	Password         string `yaml:"password"`
	Database         string `yaml:"database"`
	CommitIntervalMs int    `yaml:"commitIntervalMs"`
}

// Load reads and parses a YAML config file, expanding ${ENV_VAR} references.
func Load(path string) (*File, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read: %w", err)
	}
	expanded := os.Expand(string(b), os.Getenv)
	var f File
	if err := yaml.Unmarshal([]byte(expanded), &f); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	if f.Listen == "" {
		f.Listen = ":8080"
	}
	return &f, nil
}

// AutoStartEnabled reports whether the hyperloop should start at boot.
func (h *Hyperloop) AutoStartEnabled() bool {
	return h.AutoStart == nil || *h.AutoStart
}

// Build constructs a runnable hyperloop from its declarative definition.
func (h *Hyperloop) Build(logger *slog.Logger) (*hyperloop.Hyperloop, error) {
	if h.ID == "" {
		return nil, fmt.Errorf("config: hyperloop id is required")
	}
	if h.Source.MySQL == nil {
		return nil, fmt.Errorf("config: hyperloop %s: source.mysql is required", h.ID)
	}
	if h.Sink.Kafka == nil {
		return nil, fmt.Errorf("config: hyperloop %s: sink.kafka is required", h.ID)
	}

	src := mysqlsource.New(*h.Source.MySQL, logger)

	sink, err := kafka.New(*h.Sink.Kafka, serialize.JSON{})
	if err != nil {
		return nil, fmt.Errorf("config: hyperloop %s: %w", h.ID, err)
	}

	var store state.Store
	switch h.State.Store {
	case "file":
		store, err = state.NewFileStore(h.State.Dir)
	case "mysql", "":
		addr := h.State.Addr
		user := h.State.User
		password := h.State.Password
		if addr == "" {
			// Default to the source database for state storage.
			addr = fmt.Sprintf("%s:%d", h.Source.MySQL.Host, h.Source.MySQL.Port)
			user = h.Source.MySQL.User
			password = h.Source.MySQL.Password
		}
		db := h.State.Database
		if db == "" {
			return nil, fmt.Errorf("config: hyperloop %s: state.database is required for the mysql store", h.ID)
		}
		store, err = state.NewMySQLStore(addr, user, password, db)
	default:
		return nil, fmt.Errorf("config: hyperloop %s: unknown state store %q", h.ID, h.State.Store)
	}
	if err != nil {
		return nil, fmt.Errorf("config: hyperloop %s: state store: %w", h.ID, err)
	}

	cfg := hyperloop.Config{
		ID:              h.ID,
		Queues:          h.Pipeline.Queues,
		QueueCapacity:   h.Pipeline.QueueCapacity,
		BatchMaxSize:    h.Pipeline.Batch.MaxSize,
		BatchMaxWait:    time.Duration(h.Pipeline.Batch.MaxWaitMs) * time.Millisecond,
		KeyOverrides:    h.Pipeline.KeyOverrides,
		CommitInterval:  time.Duration(h.State.CommitIntervalMs) * time.Millisecond,
		ShutdownTimeout: time.Duration(h.ShutdownTimeoutSec) * time.Second,
	}
	return hyperloop.New(cfg, hyperloop.Deps{
		Source: src,
		Sink:   sink,
		Store:  store,
		Logger: logger,
	})
}
