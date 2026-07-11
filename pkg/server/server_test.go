package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/hyperloop"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// idleSource blocks until cancelled (a healthy, quiet stream); failSource
// fails immediately.
type idleSource struct{}

func (idleSource) Start(ctx context.Context, _ state.Position, _ chan<- []*event.NomiosEvent) error {
	<-ctx.Done()
	return ctx.Err()
}

type failSource struct{}

func (failSource) Start(context.Context, state.Position, chan<- []*event.NomiosEvent) error {
	return errors.New("source exploded")
}

type nopSink struct{}

func (nopSink) PublishBatch(_ context.Context, evs []*event.NomiosEvent, done func([]state.Position)) error {
	ps := make([]state.Position, len(evs))
	for i, e := range evs {
		ps[i] = e.Position
	}
	done(ps)
	return nil
}
func (nopSink) Flush(context.Context) error { return nil }
func (nopSink) Close() error                { return nil }

type mapStore struct {
	mu sync.Mutex
	m  map[string]state.Position
}

func newMapStore() *mapStore { return &mapStore{m: map[string]state.Position{}} }
func (s *mapStore) Load(_ context.Context, id string) (state.Position, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[id], nil
}
func (s *mapStore) Save(_ context.Context, id string, p state.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[id] = p
	return nil
}

func newLoop(t *testing.T, id string, src interface {
	Start(context.Context, state.Position, chan<- []*event.NomiosEvent) error
}) *hyperloop.Hyperloop {
	t.Helper()
	hl, err := hyperloop.New(hyperloop.Config{ID: id, ShutdownTimeout: 2 * time.Second},
		hyperloop.Deps{Source: src, Sink: nopSink{}, Store: newMapStore()})
	if err != nil {
		t.Fatal(err)
	}
	return hl
}

func waitStatus(t *testing.T, m *Manager, id string, want hyperloop.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range m.Statuses() {
			if s.ID == id && s.Status == want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("hyperloop %s never reached status %s", id, want)
}

func TestHTTPLifecycleAndReadiness(t *testing.T) {
	m := NewManager(nil)
	m.MaxRestarts = 0 // fail immediately, no backoff loops in tests
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	if err := m.Register(newLoop(t, "good", idleSource{})); err != nil {
		t.Fatal(err)
	}

	// Start via API and observe it running.
	resp, err := http.Post(srv.URL+"/v1/hyperloops/good/start", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("start: %v %v", err, resp.Status)
	}
	resp.Body.Close()
	waitStatus(t, m, "good", hyperloop.StatusRunning)

	// List shows it.
	resp, err = http.Get(srv.URL + "/v1/hyperloops")
	if err != nil {
		t.Fatal(err)
	}
	var list []hyperloop.StatusReport
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(list) != 1 || list[0].ID != "good" {
		t.Fatalf("list = %+v", list)
	}

	// Healthy node is ready.
	resp, err = http.Get(srv.URL + "/readyz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz healthy: %v %v", err, resp.Status)
	}
	resp.Body.Close()

	// A failed hyperloop turns readiness off with the failing ID listed.
	if err := m.Register(newLoop(t, "bad", failSource{})); err != nil {
		t.Fatal(err)
	}
	if err := m.Start("bad"); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, "bad", hyperloop.StatusFailed)
	resp, err = http.Get(srv.URL + "/readyz")
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz with failed loop: %v %v", err, resp.Status)
	}
	var body struct {
		Failed []string `json:"failed_hyperloops"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if len(body.Failed) != 1 || body.Failed[0] != "bad" {
		t.Fatalf("failed list = %v", body.Failed)
	}

	// Graceful stop through the API.
	resp, err = http.Post(srv.URL+"/v1/hyperloops/good/stop", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("stop: %v %v", err, resp.Status)
	}
	resp.Body.Close()
	waitStatus(t, m, "good", hyperloop.StatusStopped)

	// Unknown ID is a 404.
	resp, err = http.Get(srv.URL + "/v1/hyperloops/nope")
	if err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown id: %v %v", err, resp.Status)
	}
	resp.Body.Close()
}

func TestManagerRestartBackoff(t *testing.T) {
	m := NewManager(nil)
	m.MaxRestarts = 2
	m.RestartBackoff = time.Millisecond

	if err := m.Register(newLoop(t, "flappy", failSource{})); err != nil {
		t.Fatal(err)
	}
	if err := m.Start("flappy"); err != nil {
		t.Fatal(err)
	}
	// Runs 1 + MaxRestarts times, then stays failed.
	waitStatus(t, m, "flappy", hyperloop.StatusFailed)
	l, _ := m.get("flappy")
	select {
	case <-l.done:
	case <-time.After(5 * time.Second):
		t.Fatal("run loop did not give up after restart budget")
	}
}
