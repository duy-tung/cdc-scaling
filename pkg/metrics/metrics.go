// Package metrics defines the Nomios Prometheus collectors (the custom
// metrics promised by the design doc's observability section). Collectors
// are fed from hyperloop status snapshots by a poller (see
// server.Manager.PollMetrics), which keeps the hot pipeline path free of
// metric bookkeeping.
package metrics

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/duy-tung/cdc-scaling/pkg/hyperloop"
)

// Nomios bundles the per-hyperloop collectors. All methods are safe on a
// nil receiver so components can be wired without metrics in tests.
type Nomios struct {
	published     *prometheus.CounterVec
	checkpointSeq *prometheus.GaugeVec
	persistedSeq  *prometheus.GaugeVec
	stateGauge    *prometheus.GaugeVec
	queueDepth    *prometheus.GaugeVec
	sourceLag     *prometheus.GaugeVec
	lastEventTS   *prometheus.GaugeVec
	saveFailures  *prometheus.CounterVec

	mu           sync.Mutex
	lastPub      map[string]uint64 // per hyperloop, to convert snapshots to counter increments
	lastSaveFail map[string]uint64
}

// New registers the Nomios collectors with reg (use
// prometheus.DefaultRegisterer for the default /metrics endpoint).
func New(reg prometheus.Registerer) *Nomios {
	m := &Nomios{
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "nomios_publish_events_total",
			Help: "Events durably published to the sink.",
		}, []string{"hyperloop"}),
		checkpointSeq: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "nomios_checkpoint_seq",
			Help: "In-memory committable checkpoint sequence number.",
		}, []string{"hyperloop"}),
		persistedSeq: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "nomios_persisted_checkpoint_seq",
			Help: "Last checkpoint sequence number successfully written to the state store.",
		}, []string{"hyperloop"}),
		stateGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "nomios_hyperloop_state",
			Help: "Hyperloop lifecycle state (0=created 1=starting 2=running 3=stopping 4=stopped 5=failed).",
		}, []string{"hyperloop"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "nomios_queue_depth",
			Help: "Approximate buffered events per queue.",
		}, []string{"hyperloop", "queue"}),
		sourceLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "nomios_source_lag_seconds",
			Help: "Event freshness: now minus the binlog timestamp of the newest durably published event. Grows on an idle-but-healthy source, so treat it as an upper bound on replication lag.",
		}, []string{"hyperloop"}),
		lastEventTS: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "nomios_last_published_event_timestamp_seconds",
			Help: "Binlog timestamp (unix seconds) of the newest durably published event.",
		}, []string{"hyperloop"}),
		saveFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "nomios_state_save_failures_total",
			Help: "Failed checkpoint persists.",
		}, []string{"hyperloop"}),
		lastPub:      make(map[string]uint64),
		lastSaveFail: make(map[string]uint64),
	}
	reg.MustRegister(m.published, m.checkpointSeq, m.persistedSeq, m.stateGauge,
		m.queueDepth, m.sourceLag, m.lastEventTS, m.saveFailures)
	return m
}

var stateValues = map[hyperloop.Status]float64{
	hyperloop.StatusCreated:  0,
	hyperloop.StatusStarting: 1,
	hyperloop.StatusRunning:  2,
	hyperloop.StatusStopping: 3,
	hyperloop.StatusStopped:  4,
	hyperloop.StatusFailed:   5,
}

// Observe records a status snapshot; it is cheap and idempotent. The
// published counter advances by the delta between snapshots (a hyperloop
// restart resets its Published counter, which is detected and treated as
// a fresh baseline).
func (m *Nomios) Observe(s hyperloop.StatusReport) {
	if m == nil {
		return
	}
	m.checkpointSeq.WithLabelValues(s.ID).Set(float64(s.Checkpoint.SeqNo))
	m.persistedSeq.WithLabelValues(s.ID).Set(float64(s.Persisted.SeqNo))
	m.stateGauge.WithLabelValues(s.ID).Set(stateValues[s.Status])
	for i, d := range s.QueueDepths {
		m.queueDepth.WithLabelValues(s.ID, strconv.Itoa(i)).Set(float64(d))
	}

	if s.LastEventUnixMs > 0 {
		lag := time.Since(time.UnixMilli(s.LastEventUnixMs)).Seconds()
		if lag < 0 {
			lag = 0
		}
		m.sourceLag.WithLabelValues(s.ID).Set(lag)
		m.lastEventTS.WithLabelValues(s.ID).Set(float64(s.LastEventUnixMs) / 1000)
	}

	m.mu.Lock()
	pubDelta := counterDelta(m.lastPub, s.ID, s.Published)
	failDelta := counterDelta(m.lastSaveFail, s.ID, s.StateSaveFailures)
	m.mu.Unlock()
	if pubDelta > 0 {
		m.published.WithLabelValues(s.ID).Add(float64(pubDelta))
	}
	if failDelta > 0 {
		m.saveFailures.WithLabelValues(s.ID).Add(float64(failDelta))
	}
}

// counterDelta converts monotonic snapshot values into counter increments,
// treating a decrease (hyperloop restart) as a fresh baseline.
func counterDelta(last map[string]uint64, id string, now uint64) uint64 {
	prev := last[id]
	last[id] = now
	if now < prev {
		return now
	}
	return now - prev
}
