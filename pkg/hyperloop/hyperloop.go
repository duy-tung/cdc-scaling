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
//
// PublishBatch may be asynchronous: it can return before the events are
// durable. The sink must invoke done exactly once with the batch's
// positions after every event in the batch is durably published — possibly
// concurrently from sink-internal goroutines, possibly after PublishBatch
// returned. If any event in the batch fails permanently, done must NOT be
// called (the checkpoint then never advances past the failure) and a
// subsequent PublishBatch or Flush call must return the error. The sink
// must not retain the events slice itself (the caller reuses it), though
// it may retain the events.
//
// Asynchronous publishing removes the produce round-trip from the
// publisher loop: end-to-end profiling showed a single sync publisher
// capped at ~208k ev/s by RTT stalls vs ~1.1M in-process. Out-of-order
// completion across batches is safe: the state tracker computes the
// checkpoint as a contiguous prefix, and per-key ordering is preserved
// because a key maps to one publisher (submission order) and one Kafka
// partition (franz-go's idempotent producer keeps per-partition order).
type Sink interface {
	PublishBatch(ctx context.Context, events []*event.NomiosEvent, done func([]state.Position)) error
	// Flush blocks until every previously submitted event is durable and
	// its done callback has returned, or ctx expires.
	Flush(ctx context.Context) error
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

	status       atomic.Value // Status
	dispatcher   atomic.Pointer[dispatch.Dispatcher]
	tracker      atomic.Pointer[state.Tracker]
	published    atomic.Uint64
	lastErr      atomic.Value // string
	persisted    atomic.Value // state.Position: last successfully saved
	lastEventMs  atomic.Int64 // OccurredAt of the newest published event
	saveFailures atomic.Uint64
}

// StatusReport is a point-in-time view of a hyperloop for monitoring.
type StatusReport struct {
	ID        string `json:"id"`
	Status    Status `json:"status"`
	Published uint64 `json:"published_events"`
	// Checkpoint is the in-memory committable position (everything up to
	// it is durably published); Persisted is the last position actually
	// written to the state store. They differ when a state save is behind
	// or failing — resume happens from Persisted, so monitoring must see
	// both.
	Checkpoint state.Position `json:"checkpoint"`
	Persisted  state.Position `json:"persisted_checkpoint"`
	// LastEventUnixMs is the event time (binlog timestamp) of the newest
	// DURABLY PUBLISHED (sink-acknowledged) event. "Now minus this" is
	// event freshness/age: it grows on a healthy but idle database, so it
	// is an upper bound on replication lag, not a strict measurement.
	LastEventUnixMs int64 `json:"last_event_unix_ms,omitempty"`
	// StateSaveFailures counts failed checkpoint persists over the
	// lifetime of this hyperloop object (it is not reset between Run
	// restarts — counter semantics).
	StateSaveFailures uint64 `json:"state_save_failures,omitempty"`
	QueueDepths       []int  `json:"queue_depths,omitempty"`
	LastError         string `json:"last_error,omitempty"`
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
		ID:                h.cfg.ID,
		Status:            h.status.Load().(Status),
		Published:         h.published.Load(),
		LastError:         h.lastErr.Load().(string),
		LastEventUnixMs:   h.lastEventMs.Load(),
		StateSaveFailures: h.saveFailures.Load(),
	}
	if t := h.tracker.Load(); t != nil {
		rep.Checkpoint, _ = t.Checkpoint()
	}
	if p, ok := h.persisted.Load().(state.Position); ok {
		rep.Persisted = p
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
	// A restarted run replays everything after the checkpoint, so a sticky
	// error from the previous run's sink must not fail this run instantly.
	if r, ok := h.deps.Sink.(interface{ Reset() }); ok {
		r.Reset()
	}

	pos, err := h.deps.Store.Load(ctx, h.cfg.ID)
	if err != nil {
		return fmt.Errorf("hyperloop: load state: %w", err)
	}
	h.persisted.Store(pos)
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

	// firstErr is mutex-guarded (not sync.Once) because the abandoned-
	// shutdown path reads it while late goroutines may still be failing.
	var (
		errMu    sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		errMu.Lock()
		first := firstErr == nil
		if first {
			firstErr = err
		}
		errMu.Unlock()
		if first {
			h.log.Error("component failed, aborting pipeline", "err", err)
			cancelSrc()
			cancelHard()
		}
	}
	getErr := func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
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
		h.runCommitter(commitStop, tracker, fail)
	}()

	h.status.Store(StatusRunning)

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()

	abandoned := false
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
			// Shutdown must be bounded even if a component ignores its
			// context (e.g. a produce stuck resolving an in-flight
			// idempotent request). After a second timeout the goroutines
			// are abandoned: state stays at-least-once safe because the
			// checkpoint only ever covers acknowledged events.
			select {
			case <-finished:
			case <-time.After(h.cfg.ShutdownTimeout):
				h.log.Error("pipeline goroutines did not exit after hard cancel; abandoning them", "timeout", h.cfg.ShutdownTimeout)
				abandoned = true
			}
		}
	case <-finished:
		// Source ended on its own (fatal error or finite stream).
		h.status.Store(StatusStopping)
	}

	runErr := getErr()

	// All workers have submitted their last batches; wait for the sink's
	// in-flight publishes to become durable so the final checkpoint covers
	// them. Skipping this on error/hard-cancel is safe (at-least-once).
	if runErr == nil && !abandoned {
		fctx, fcancel := context.WithTimeout(context.Background(), h.cfg.ShutdownTimeout)
		if err := h.deps.Sink.Flush(fctx); err != nil {
			h.log.Error("sink flush failed", "err", err)
			runErr = err
		}
		fcancel()
	}
	if abandoned && runErr == nil {
		runErr = fmt.Errorf("hyperloop: shutdown timed out after 2x%s; pipeline goroutines abandoned", h.cfg.ShutdownTimeout)
	}

	close(commitStop)
	commitWg.Wait()

	// Final commit so a clean stop resumes with minimal replay.
	if ckpt, ok := tracker.Checkpoint(); ok {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.deps.Store.Save(cctx, h.cfg.ID, ckpt); err != nil {
			h.log.Error("final state commit failed", "err", err)
			h.saveFailures.Add(1)
			if runErr == nil {
				runErr = err
			}
		} else {
			h.persisted.Store(ckpt)
			h.log.Info("final state committed", "seq", ckpt.SeqNo, "gtid", ckpt.GTIDSet, "file", ckpt.File, "offset", ckpt.Offset)
		}
	}
	return runErr
}

// runPublisher drains one buffer queue in batches and submits each batch
// to the sink. Positions reach the tracker via the sink's async done
// callback once the batch is durable. The batch buffer is reused across
// iterations; the Sink contract is that it may retain events but not the
// batch slice itself.
func (h *Hyperloop) runPublisher(ctx context.Context, id int, q <-chan []*event.NomiosEvent, tracker *state.Tracker) error {
	buf := make([]*event.NomiosEvent, 0, h.cfg.BatchMaxSize)
	for {
		batch, open := drainBatch(ctx, q, buf, h.cfg.BatchMaxSize, h.cfg.BatchMaxWait)
		if len(batch) > 0 {
			// The batch's newest event time is captured before submission
			// but recorded only inside the ack callback: LastEventUnixMs
			// means "newest DURABLY PUBLISHED event", and async
			// PublishBatch returns before acknowledgement.
			var maxMs int64
			for _, e := range batch {
				if ms := e.OccurredAt.UnixMilli(); ms > maxMs {
					maxMs = ms
				}
			}
			done := func(ps []state.Position) {
				tracker.DoneBatch(ps)
				h.published.Add(uint64(len(ps)))
				h.recordLastEvent(maxMs)
			}
			if err := h.deps.Sink.PublishBatch(ctx, batch, done); err != nil {
				if ctx.Err() != nil {
					return nil // hard shutdown, not a publisher fault
				}
				return err
			}
		}
		if !open {
			return nil
		}
	}
}

// recordLastEvent tracks the newest acknowledged event time (atomic max:
// queues and ack callbacks progress independently).
func (h *Hyperloop) recordLastEvent(ms int64) {
	if ms <= 0 {
		return
	}
	for {
		cur := h.lastEventMs.Load()
		if ms <= cur || h.lastEventMs.CompareAndSwap(cur, ms) {
			return
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

// runCommitter persists the checkpoint every CommitInterval when it moved,
// and doubles as the async-error watchdog: a produce failure on an
// otherwise idle pipeline is reported by the sink only on the next call,
// so the committer polls the sink's sticky error and fails the pipeline
// promptly instead of letting it show "running" forever.
func (h *Hyperloop) runCommitter(stop <-chan struct{}, tracker *state.Tracker, fail func(error)) {
	ticker := time.NewTicker(h.cfg.CommitInterval)
	defer ticker.Stop()
	sinkErr, pollable := h.deps.Sink.(interface{ Err() error })
	for {
		select {
		case <-ticker.C:
			if pollable {
				if err := sinkErr.Err(); err != nil {
					fail(fmt.Errorf("sink: %w", err))
				}
			}
			if ckpt, ok := tracker.TakeDirty(); ok {
				cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := h.deps.Store.Save(cctx, h.cfg.ID, ckpt); err != nil {
					h.log.Error("state commit failed", "err", err)
					h.saveFailures.Add(1)
					// TakeDirty cleared the flag: re-mark so the save is
					// retried next tick even if the checkpoint is idle.
					tracker.Redirty()
				} else {
					h.persisted.Store(ckpt)
				}
				cancel()
			}
		case <-stop:
			return
		}
	}
}
