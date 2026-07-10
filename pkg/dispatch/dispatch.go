// Package dispatch partitions the source event stream into buffer queues.
//
// To avoid the Source being blocked by slow consumers, Nomios holds events
// in N in-memory buffer queues. Events are partitioned by hashing an entity
// key provided by the KeyFunc, which guarantees all change events of one
// entity land in the same queue — preserving per-entity ordering end to end.
// When every queue is full the dispatcher blocks, applying natural
// backpressure to the binlog reader instead of dropping events.
package dispatch

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/duy-tung/cdc-scaling/pkg/event"
)

// KeyFunc extracts the partition key for an event.
type KeyFunc func(e *event.NomiosEvent) string

// DefaultKey returns a KeyFunc that uses per-table column overrides when
// configured (keyed by "db.table"), and otherwise falls back to the key the
// Source attached to the event (primary key values). If neither is
// available it falls back to the table name, which serializes that table's
// events through a single queue — slow but always correct.
func DefaultKey(overrides map[string][]string) KeyFunc {
	return func(e *event.NomiosEvent) string {
		if cols, ok := overrides[e.Source.FQTN()]; ok {
			row := e.Row()
			parts := make([]string, 0, len(cols)+1)
			parts = append(parts, e.Source.FQTN())
			for _, c := range cols {
				parts = append(parts, fmt.Sprintf("%v", row[c]))
			}
			return strings.Join(parts, "|")
		}
		if e.Key != "" {
			return e.Key
		}
		return e.Source.FQTN()
	}
}

// KeyFromColumns builds a stable key string from named columns of a row.
// Exported for sources to build the default primary-key based key.
func KeyFromColumns(fqtn string, row map[string]any, cols []string) string {
	parts := make([]string, 0, len(cols)+1)
	parts = append(parts, fqtn)
	if len(cols) == 0 {
		// No usable key columns: fall back to the whole row, sorted, so
		// identical rows at least hash consistently.
		names := make([]string, 0, len(row))
		for k := range row {
			names = append(names, k)
		}
		sort.Strings(names)
		cols = names
	}
	for _, c := range cols {
		parts = append(parts, fmt.Sprintf("%v", row[c]))
	}
	return strings.Join(parts, "|")
}

// Dispatcher routes events from the source stream into N buffer queues.
type Dispatcher struct {
	queues []chan *event.NomiosEvent
	key    KeyFunc
}

// New creates a dispatcher with n buffer queues of the given capacity.
func New(n, capacity int, key KeyFunc) *Dispatcher {
	if n < 1 {
		n = 1
	}
	qs := make([]chan *event.NomiosEvent, n)
	for i := range qs {
		qs[i] = make(chan *event.NomiosEvent, capacity)
	}
	return &Dispatcher{queues: qs, key: key}
}

// Queues exposes the buffer queues for the consumer pool to drain.
func (d *Dispatcher) Queues() []chan *event.NomiosEvent { return d.queues }

// Pick returns the queue index for an event.
func (d *Dispatcher) Pick(e *event.NomiosEvent) int {
	k := d.key(e)
	e.Key = k // ensure the exact routed key is also the Kafka record key
	h := fnv.New32a()
	_, _ = h.Write([]byte(k))
	return int(h.Sum32() % uint32(len(d.queues)))
}

// Run consumes events from in until it is closed or ctx is cancelled,
// routing each event to its queue (blocking when the queue is full). On
// return it closes all queues so downstream publishers drain and exit.
func (d *Dispatcher) Run(ctx context.Context, in <-chan *event.NomiosEvent) error {
	defer func() {
		for _, q := range d.queues {
			close(q)
		}
	}()
	for {
		select {
		case e, ok := <-in:
			if !ok {
				return nil
			}
			select {
			case d.queues[d.Pick(e)] <- e:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Depths returns the current number of buffered events per queue.
func (d *Dispatcher) Depths() []int {
	out := make([]int, len(d.queues))
	for i, q := range d.queues {
		out[i] = len(q)
	}
	return out
}
