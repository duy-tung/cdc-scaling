package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/duy-tung/cdc-scaling/pkg/hyperloop"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

func report(published, saveFails uint64, ckptSeq uint64, lastEventMs int64) hyperloop.StatusReport {
	return hyperloop.StatusReport{
		ID:                "hl",
		Status:            hyperloop.StatusRunning,
		Published:         published,
		StateSaveFailures: saveFails,
		Checkpoint:        state.Position{SeqNo: ckptSeq},
		Persisted:         state.Position{SeqNo: ckptSeq - 1},
		LastEventUnixMs:   lastEventMs,
		QueueDepths:       []int{3, 0},
	}
}

func TestObserve(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	eventTime := time.Now().Add(-2 * time.Second)
	m.Observe(report(10, 1, 5, eventTime.UnixMilli()))

	if got := testutil.ToFloat64(m.published.WithLabelValues("hl")); got != 10 {
		t.Fatalf("published = %v, want 10", got)
	}
	if got := testutil.ToFloat64(m.saveFailures.WithLabelValues("hl")); got != 1 {
		t.Fatalf("save failures = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.checkpointSeq.WithLabelValues("hl")); got != 5 {
		t.Fatalf("checkpoint seq = %v, want 5", got)
	}
	if got := testutil.ToFloat64(m.persistedSeq.WithLabelValues("hl")); got != 4 {
		t.Fatalf("persisted seq = %v, want 4", got)
	}
	if got := testutil.ToFloat64(m.stateGauge.WithLabelValues("hl")); got != 2 { // running
		t.Fatalf("state = %v, want 2", got)
	}
	if lag := testutil.ToFloat64(m.sourceLag.WithLabelValues("hl")); lag < 1.5 || lag > 30 {
		t.Fatalf("lag = %v, want ~2s", lag)
	}
	if ts := testutil.ToFloat64(m.lastEventTS.WithLabelValues("hl")); int64(ts) != eventTime.Unix() {
		t.Fatalf("last event ts = %v, want %v", ts, eventTime.Unix())
	}
	if got := testutil.ToFloat64(m.queueDepth.WithLabelValues("hl", "0")); got != 3 {
		t.Fatalf("queue depth = %v, want 3", got)
	}

	// Counters advance by snapshot deltas.
	m.Observe(report(25, 3, 6, eventTime.UnixMilli()))
	if got := testutil.ToFloat64(m.published.WithLabelValues("hl")); got != 25 {
		t.Fatalf("published after delta = %v, want 25", got)
	}
	if got := testutil.ToFloat64(m.saveFailures.WithLabelValues("hl")); got != 3 {
		t.Fatalf("save failures after delta = %v, want 3", got)
	}

	// A snapshot decrease (hyperloop restart) is a fresh baseline, and the
	// counter keeps increasing monotonically.
	m.Observe(report(5, 0, 1, eventTime.UnixMilli()))
	if got := testutil.ToFloat64(m.published.WithLabelValues("hl")); got != 30 {
		t.Fatalf("published after restart = %v, want 30", got)
	}
}

// TestObserveNilSafe: a nil collector set must be ignorable by callers.
func TestObserveNilSafe(t *testing.T) {
	var m *Nomios
	m.Observe(report(1, 0, 1, 0)) // must not panic
}
