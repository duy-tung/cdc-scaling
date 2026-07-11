// Package metrics defines the Nomios Prometheus collectors (the custom
// metrics promised by the design doc's observability section). Collectors
// are fed from hyperloop status snapshots by a poller (see
// server.Manager.PollMetrics), which keeps the hot pipeline path free of
// metric bookkeeping.
package metrics

import (
	"strconv"
	"sync"

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

	mu      sync.Mutex
	lastPub map[string]uint64 // per hyperloop, to convert snapshots to counter increments
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
		lastPub: make(map[string]uint64),
	}
	reg.MustRegister(m.published, m.checkpointSeq, m.persistedSeq, m.stateGauge, m.queueDepth)
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

	m.mu.Lock()
	last := m.lastPub[s.ID]
	delta := s.Published - last
	if s.Published < last {
		delta = s.Published
	}
	m.lastPub[s.ID] = s.Published
	m.mu.Unlock()
	if delta > 0 {
		m.published.WithLabelValues(s.ID).Add(float64(delta))
	}
}
