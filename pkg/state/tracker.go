package state

import "sync"

// Tracker computes the committable checkpoint across many concurrent
// publishers. Publishers report each event's position after the event has
// been durably published. Because the Source assigns contiguous SeqNos
// starting at 1, the checkpoint is the position of the highest SeqNo N such
// that every event 1..N has been reported. This is the "earliest
// last-processed event across publishers" rule from the Nomios design:
// restarting from the checkpoint can never skip an event, only replay a few.
type Tracker struct {
	mu      sync.Mutex
	next    uint64 // lowest SeqNo not yet part of the contiguous prefix
	pending map[uint64]Position
	ckpt    Position
	has     bool
	dirty   bool
}

// NewTracker returns a Tracker expecting SeqNos to start at 1.
func NewTracker() *Tracker {
	return &Tracker{next: 1, pending: make(map[uint64]Position)}
}

// Done records that the event at position p has been durably published.
func (t *Tracker) Done(p Position) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending[p.SeqNo] = p
	t.advance()
}

// DoneBatch records a whole batch of published positions under a single
// lock acquisition. Publishers complete events in batches; reporting them
// one Done at a time made the tracker mutex a measurable share of pipeline
// CPU (~5% cum).
func (t *Tracker) DoneBatch(ps []Position) {
	if len(ps) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range ps {
		t.pending[p.SeqNo] = p
	}
	t.advance()
}

// advance moves the checkpoint across the contiguous completed prefix.
// Callers must hold t.mu.
func (t *Tracker) advance() {
	for {
		q, ok := t.pending[t.next]
		if !ok {
			return
		}
		delete(t.pending, t.next)
		t.ckpt = q
		t.has = true
		t.dirty = true
		t.next++
	}
}

// Checkpoint returns the current committable position, if any event has
// completed the contiguous prefix yet.
func (t *Tracker) Checkpoint() (Position, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ckpt, t.has
}

// TakeDirty returns the checkpoint if it advanced since the previous call,
// clearing the dirty flag. Used by the periodic committer to avoid
// rewriting an unchanged state.
func (t *Tracker) TakeDirty() (Position, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.dirty {
		return Position{}, false
	}
	t.dirty = false
	return t.ckpt, true
}

// Redirty re-marks the checkpoint dirty. The committer calls it when a
// state save fails: TakeDirty already cleared the flag, and without
// re-marking, the failed save would only be retried after the checkpoint
// advances again — on an idle stream that could postpone persistence
// indefinitely and enlarge the crash-replay window.
func (t *Tracker) Redirty() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.has {
		t.dirty = true
	}
}

// Outstanding reports how many completed events are waiting for an earlier
// SeqNo to complete (useful for diagnostics).
func (t *Tracker) Outstanding() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}
