// Package hyperloop implements the UNIT of CDC: one complete flow of
// stream event → parse → serialize → publish, managed as a single
// instance that can be configured, started, monitored and stopped. A node
// can run many hyperloops. On error or stop, a hyperloop performs a
// graceful shutdown: stop the source, drain the buffer queues, flush the
// publishers, commit the final state.
package hyperloop

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/dispatch"
	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/source"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// Status of a hyperloop.
type Status string

const (
	StatusCreated  Status = "created"
	StatusStarting Status = "starting"
	StatusRunning  Status = "running"
	StatusStopping Status = "stopping"
	StatusStopped  Status = "stopped"
	StatusFailed   Status = "failed"
)

// Sink is where publishers deliver serialized events (Kafka in v1).
type Sink interface {
	PublishBatch(ctx context.Context, events []*event.NomiosEvent) error
	Close() error
}

// Config controls the pipeline shape of one hyperloop.
type Config struct {
	ID string `yaml:"id"`
	// Queues is the number of buffer queues == publishers in the pool.
	Queues int `yaml:"queues"`
	// QueueCapacity is the buffered size of each queue (and of the source
	// output channel).
	QueueCapacity int `yaml:"queueCapacity"`
	// BatchMaxSize is the max events a publisher drains per publish batch.
	BatchMaxSize int `yaml:"batchMaxSize"`
	// BatchMaxWait is how long a publisher waits to top up a partial batch.
	BatchMaxWait time.Duration `yaml:"batchMaxWait"`
	// KeyOverrides maps "db.table" to the columns used as the entity key.
	KeyOverrides map[string][]string `yaml:"keyOverrides"`
	// CommitInterval is the state checkpoint cadence.
	CommitInterval time.Duration `yaml:"commitInterval"`
	// ShutdownTimeout bounds the graceful drain on stop before the pipeline
	// is hard-cancelled.
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout"`
}

func (c *Config) withDefaults() {
	if c.Queues <= 0 {
		c.Queues = 4
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 4096
	}
	if c.BatchMaxSize <= 0 {
		c.BatchMaxSize = 500
	}
	if c.BatchMaxWait <= 0 {
		c.BatchMaxWait = 20 * time.Millisecond
	}
	if c.CommitInterval <= 0 {
		c.CommitInterval = time.Second
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
}

// Deps are the wired components of a hyperloop.
type Deps struct {
	Source source.Source
	Sink   Sink
	Store  state.Store
	Logger *slog.Logger
}

// Hyperloop wires Source → Dispatcher → publisher pool → Sink, with the
// state manager committing checkpoints on the side.
type Hyperloop struct {
	cfg  Config
	deps Deps
	log  *slog.Logger

	status     atomic.Value // Status
	dispatcher atomic.Pointer[dispatch.Dispatcher]
	tracker    atomic.Pointer[state.Tracker]
	published  atomic.Uint64
	lastErr    atomic.Value // string
}

// StatusReport is a point-in-time view of a hyperloop for monitoring.
type StatusReport struct {
	ID          string         `json:"id"`
	Status      Status         `json:"status"`
	Published   uint64         `json:"published_events"`
	Checkpoint  state.Position `json:"checkpoint"`
	QueueDepths []int          `json:"queue_depths,omitempty"`
	LastError   string         `json:"last_error,omitempty"`
}

func New(cfg Config, deps Deps) (*Hyperloop, error) {
	cfg.withDefaults()
	if cfg.ID == "" {
		return nil, errors.New("hyperloop: id is required")
	}
	if deps.Source == nil || deps.Sink == nil || deps.Store == nil {
		return nil, errors.New("hyperloop: source, sink and store are required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	h := &Hyperloop{cfg: cfg, deps: deps, log: deps.Logger.With("hyperloop", cfg.ID)}
	h.status.Store(StatusCreated)
	h.lastErr.Store("")
	return h, nil
}

func (h *Hyperloop) ID() string { return h.cfg.ID }

// Status returns a monitoring snapshot.
func (h *Hyperloop) Status() StatusReport {
	rep := StatusReport{
		ID:        h.cfg.ID,
		Status:    h.status.Load().(Status),
		Published: h.published.Load(),
		LastError: h.lastErr.Load().(string),
	}
	if t := h.tracker.Load(); t != nil {
		rep.Checkpoint, _ = t.Checkpoint()
	}
	if d := h.dispatcher.Load(); d != nil {
		rep.QueueDepths = d.Depths()
	}
	return rep
}

// Run executes the full CDC flow until ctx is cancelled (graceful stop) or
// the source ends / a component fails. It blocks for the lifetime of the
// pipeline and always attempts a final state commit before returning.
func (h *Hyperloop) Run(ctx context.Context) error {
	h.status.Store(StatusStarting)
	err := h.run(ctx)
	if err != nil {
		h.lastErr.Store(err.Error())
		h.status.Store(StatusFailed)
	} else {
		h.status.Store(StatusStopped)
	}
	return err
}

func (h *Hyperloop) run(ctx context.Context) error {
	// Load last committed state: on restart/deploy/crash the hyperloop
	// continues the event stream from exactly the saved position.
	pos, err := h.deps.Store.Load(ctx, h.cfg.ID)
	if err != nil {
		return fmt.Errorf("hyperloop: load state: %w", err)
	}
	h.log.Info("starting", "from_gtid", pos.GTIDSet, "from_file", pos.File, "from_offset", pos.Offset)

	tracker := state.NewTracker()
	h.tracker.Store(tracker)

	// Micro-batch size for the channel hops: bounded by the publisher batch
	// so one queue batch never overshoots a publish batch by much.
	flushSize := h.cfg.BatchMaxSize
	if flushSize > 256 {
		flushSize = 256
	}
	d := dispatch.New(h.cfg.Queues, h.cfg.QueueCapacity, flushSize, dispatch.DefaultKey(h.cfg.KeyOverrides))
	h.dispatcher.Store(d)

	outBatches := h.cfg.QueueCapacity / flushSize
	if outBatches < 2 {
		outBatches = 2
	}
	out := make(chan []*event.NomiosEvent, outBatches)

	// Two cancellation levels: srcCtx stops the source first (graceful —
	// the rest of the pipeline drains naturally when the stream closes);
	// hardCtx aborts everything (component failure or drain timeout).
	srcCtx, cancelSrc := context.WithCancel(context.Background())
	defer cancelSrc()
	hardCtx, cancelHard := context.WithCancel(context.Background())
	defer cancelHard()

	var (
		errOnce  sync.Once
		firstErr error
	)
	fail := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			h.log.Error("component failed, aborting pipeline", "err", err)
			cancelSrc()
			cancelHard()
		})
	}

	var wg sync.WaitGroup

	// Source: separate goroutine, owns the stream. Closing out on return is
	// what lets the rest of the pipeline drain and finish.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(out)
		fail(h.deps.Source.Start(srcCtx, pos, out))
	}()

	// Dispatcher: partitions events into the buffer queues.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.Run(hardCtx, out); err != nil {
			fail(fmt.Errorf("dispatcher: %w", err))
		}
	}()

	// Publisher pool: one worker per buffer queue.
	for i, q := range d.Queues() {
		wg.Add(1)
		go func(i int, q <-chan []*event.NomiosEvent) {
			defer wg.Done()
			if err := h.runPublisher(hardCtx, i, q, tracker); err != nil {
				fail(fmt.Errorf("publisher %d: %w", i, err))
			}
		}(i, q)
	}

	// State manager: periodically persists the checkpoint (earliest
	// last-processed event across publishers).
	commitStop := make(chan struct{})
	var commitWg sync.WaitGroup
	commitWg.Add(1)
	go func() {
		defer commitWg.Done()
		h.runCommitter(commitStop, tracker)
	}()

	h.status.Store(StatusRunning)

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()

	select {
	case <-ctx.Done():
		// Graceful stop: stop the source, then let the pipeline drain.
		h.status.Store(StatusStopping)
		h.log.Info("graceful shutdown: stopping source and draining")
		cancelSrc()
		select {
		case <-finished:
		case <-time.After(h.cfg.ShutdownTimeout):
			h.log.Warn("drain timed out, hard-cancelling", "timeout", h.cfg.ShutdownTimeout)
			cancelHard()
			<-finished
		}
	case <-finished:
		// Source ended on its own (fatal error or finite stream).
		h.status.Store(StatusStopping)
	}

	close(commitStop)
	commitWg.Wait()

	// Final commit so a clean stop resumes with minimal replay.
	if ckpt, ok := tracker.Checkpoint(); ok {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.deps.Store.Save(cctx, h.cfg.ID, ckpt); err != nil {
			h.log.Error("final state commit failed", "err", err)
			if firstErr == nil {
				firstErr = err
			}
		} else {
			h.log.Info("final state committed", "seq", ckpt.SeqNo, "gtid", ckpt.GTIDSet, "file", ckpt.File, "offset", ckpt.Offset)
		}
	}
	return firstErr
}

// runPublisher drains one buffer queue in batches, publishes each batch to
// the sink, and reports completed positions to the tracker. The batch
// buffer is reused across iterations; the Sink contract is that it may
// retain events but not the batch slice itself.
func (h *Hyperloop) runPublisher(ctx context.Context, id int, q <-chan []*event.NomiosEvent, tracker *state.Tracker) error {
	positions := make([]state.Position, 0, h.cfg.BatchMaxSize)
	buf := make([]*event.NomiosEvent, 0, h.cfg.BatchMaxSize)
	for {
		batch, open := drainBatch(ctx, q, buf, h.cfg.BatchMaxSize, h.cfg.BatchMaxWait)
		if len(batch) > 0 {
			if err := h.deps.Sink.PublishBatch(ctx, batch); err != nil {
				if ctx.Err() != nil {
					return nil // hard shutdown, not a publisher fault
				}
				return err
			}
			positions = positions[:0]
			for _, e := range batch {
				positions = append(positions, e.Position)
			}
			tracker.DoneBatch(positions)
			h.published.Add(uint64(len(batch)))
		}
		if !open {
			return nil
		}
	}
}

// drainBatch blocks for the first micro-batch, then tops the batch up
// until at least maxSize events are collected, maxWait elapses, or the
// queue closes. Collected micro-batches are flattened into buf (reused by
// the caller). open=false means the queue is closed and fully drained (or
// ctx died). The returned batch may exceed maxSize by up to one
// micro-batch.
func drainBatch(ctx context.Context, q <-chan []*event.NomiosEvent, buf []*event.NomiosEvent, maxSize int, maxWait time.Duration) (batch []*event.NomiosEvent, open bool) {
	batch = buf[:0]
	select {
	case bs, ok := <-q:
		if !ok {
			return batch, false
		}
		batch = append(batch, bs...)
	case <-ctx.Done():
		return batch, false
	}

	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	for len(batch) < maxSize {
		select {
		case bs, ok := <-q:
			if !ok {
				return batch, false
			}
			batch = append(batch, bs...)
		case <-timer.C:
			return batch, true
		case <-ctx.Done():
			return batch, false
		}
	}
	return batch, true
}

// runCommitter persists the checkpoint every CommitInterval when it moved.
func (h *Hyperloop) runCommitter(stop <-chan struct{}, tracker *state.Tracker) {
	ticker := time.NewTicker(h.cfg.CommitInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if ckpt, ok := tracker.TakeDirty(); ok {
				cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := h.deps.Store.Save(cctx, h.cfg.ID, ckpt); err != nil {
					h.log.Error("state commit failed", "err", err)
				}
				cancel()
			}
		case <-stop:
			return
		}
	}
}
