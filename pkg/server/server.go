// Package server is the Nomios control plane: an HTTP API to create,
// start, stop and monitor hyperloops on this node.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/duy-tung/cdc-scaling/pkg/config"
	"github.com/duy-tung/cdc-scaling/pkg/hyperloop"
)

// managed is one registered hyperloop and its run lifecycle.
type managed struct {
	hl     *hyperloop.Hyperloop
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// Manager owns all hyperloops on the node.
type Manager struct {
	mu    sync.Mutex
	loops map[string]*managed
	log   *slog.Logger
}

func NewManager(logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{loops: make(map[string]*managed), log: logger}
}

// Register adds a hyperloop without starting it.
func (m *Manager) Register(hl *hyperloop.Hyperloop) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.loops[hl.ID()]; ok {
		return fmt.Errorf("hyperloop %q already exists", hl.ID())
	}
	m.loops[hl.ID()] = &managed{hl: hl}
	return nil
}

func (m *Manager) get(id string) (*managed, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.loops[id]
	return l, ok
}

// Start launches a hyperloop's Run loop. No-op if already running.
func (m *Manager) Start(id string) error {
	l, ok := m.get(id)
	if !ok {
		return fmt.Errorf("hyperloop %q not found", id)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done != nil {
		select {
		case <-l.done:
			// previous run finished; fall through to restart
		default:
			return nil // already running
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	l.cancel, l.done = cancel, done
	go func() {
		defer close(done)
		if err := l.hl.Run(ctx); err != nil {
			m.log.Error("hyperloop exited with error", "hyperloop", id, "err", err)
		}
	}()
	return nil
}

// Stop gracefully stops a hyperloop and waits for it to drain.
func (m *Manager) Stop(ctx context.Context, id string) error {
	l, ok := m.get(id)
	if !ok {
		return fmt.Errorf("hyperloop %q not found", id)
	}
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("hyperloop %q did not stop in time", id)
	}
}

// StopAll gracefully stops every hyperloop (used on process shutdown).
func (m *Manager) StopAll(ctx context.Context) {
	m.mu.Lock()
	ids := make([]string, 0, len(m.loops))
	for id := range m.loops {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := m.Stop(ctx, id); err != nil {
				m.log.Error("stop failed", "hyperloop", id, "err", err)
			}
		}(id)
	}
	wg.Wait()
}

// Statuses returns status reports for all hyperloops.
func (m *Manager) Statuses() []hyperloop.StatusReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]hyperloop.StatusReport, 0, len(m.loops))
	for _, l := range m.loops {
		out = append(out, l.hl.Status())
	}
	return out
}

// Handler builds the HTTP control plane.
func (m *Manager) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /v1/hyperloops", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, m.Statuses())
	})

	mux.HandleFunc("POST /v1/hyperloops", func(w http.ResponseWriter, r *http.Request) {
		var def config.Hyperloop
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		hl, err := def.Build(m.log)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if err := m.Register(hl); err != nil {
			writeErr(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusCreated, hl.Status())
	})

	mux.HandleFunc("GET /v1/hyperloops/{id}", func(w http.ResponseWriter, r *http.Request) {
		l, ok := m.get(r.PathValue("id"))
		if !ok {
			writeErr(w, http.StatusNotFound, fmt.Errorf("not found"))
			return
		}
		writeJSON(w, http.StatusOK, l.hl.Status())
	})

	mux.HandleFunc("POST /v1/hyperloops/{id}/start", func(w http.ResponseWriter, r *http.Request) {
		if err := m.Start(r.PathValue("id")); err != nil {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		l, _ := m.get(r.PathValue("id"))
		writeJSON(w, http.StatusOK, l.hl.Status())
	})

	mux.HandleFunc("POST /v1/hyperloops/{id}/stop", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		if err := m.Stop(ctx, r.PathValue("id")); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		l, ok := m.get(r.PathValue("id"))
		if !ok {
			writeErr(w, http.StatusNotFound, fmt.Errorf("not found"))
			return
		}
		writeJSON(w, http.StatusOK, l.hl.Status())
	})

	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
