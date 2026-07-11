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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

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
		if len(overrides) > 0 {
			fqtn := e.Source.FQTN()
			if cols, ok := overrides[fqtn]; ok {
				return KeyFromColumns(fqtn, e.Row(), cols)
			}
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
	b := make([]byte, 0, len(fqtn)+16*len(cols))
	b = append(b, fqtn...)
	for _, c := range cols {
		b = append(b, '|')
		b = appendKeyValue(b, row[c])
	}
	return string(b)
}

// appendKeyValue renders a column value into a key without fmt reflection
// (fmt.Sprintf was 6.6% of pipeline allocations). The rendering only needs
// to be deterministic and collision-free per column, not human-canonical.
func appendKeyValue(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return b
	case string:
		return append(b, x...)
	case []byte:
		return append(b, x...)
	case json.RawMessage:
		return append(b, x...)
	case int:
		return strconv.AppendInt(b, int64(x), 10)
	case int8:
		return strconv.AppendInt(b, int64(x), 10)
	case int16:
		return strconv.AppendInt(b, int64(x), 10)
	case int32:
		return strconv.AppendInt(b, int64(x), 10)
	case int64:
		return strconv.AppendInt(b, x, 10)
	case uint:
		return strconv.AppendUint(b, uint64(x), 10)
	case uint8:
		return strconv.AppendUint(b, uint64(x), 10)
	case uint16:
		return strconv.AppendUint(b, uint64(x), 10)
	case uint32:
		return strconv.AppendUint(b, uint64(x), 10)
	case uint64:
		return strconv.AppendUint(b, x, 10)
	case bool:
		return strconv.AppendBool(b, x)
	case float32:
		return strconv.AppendFloat(b, float64(x), 'g', -1, 32)
	case float64:
		return strconv.AppendFloat(b, x, 'g', -1, 64)
	default:
		return fmt.Appendf(b, "%v", v)
	}
}

// Dispatcher routes events from the source stream into N buffer queues.
//
// Events travel through the queues as micro-batches ([]*NomiosEvent): the
// dispatcher stages routed events per queue and flushes a queue's staging
// slice when it reaches flushSize, when the input pauses, or on shutdown.
// This amortizes channel/select overhead (~25% of pipeline CPU when events
// crossed channels one at a time) without adding latency under load.
type Dispatcher struct {
	queues    []chan []*event.NomiosEvent
	staging   [][]*event.NomiosEvent
	key       KeyFunc
	flushSize int
}

// New creates a dispatcher with n buffer queues. eventCapacity is the
// approximate number of events (not batches) a queue buffers before the
// dispatcher — and transitively the source — blocks; flushSize is the max
// micro-batch size staged per queue.
func New(n, eventCapacity, flushSize int, key KeyFunc) *Dispatcher {
	if n < 1 {
		n = 1
	}
	if flushSize < 1 {
		flushSize = 256
	}
	batches := eventCapacity / flushSize
	if batches < 2 {
		batches = 2
	}
	d := &Dispatcher{
		queues:    make([]chan []*event.NomiosEvent, n),
		staging:   make([][]*event.NomiosEvent, n),
		key:       key,
		flushSize: flushSize,
	}
	for i := range d.queues {
		d.queues[i] = make(chan []*event.NomiosEvent, batches)
		d.staging[i] = make([]*event.NomiosEvent, 0, flushSize)
	}
	return d
}

// Queues exposes the buffer queues for the consumer pool to drain.
func (d *Dispatcher) Queues() []chan []*event.NomiosEvent { return d.queues }

// Pick returns the queue index for an event. The hash is an inlined
// FNV-1a over the key string (identical results to hash/fnv, without the
// hasher object and []byte conversion allocations).
func (d *Dispatcher) Pick(e *event.NomiosEvent) int {
	k := d.key(e)
	e.Key = k // ensure the exact routed key is also the Kafka record key
	h := uint32(2166136261)
	for i := 0; i < len(k); i++ {
		h = (h ^ uint32(k[i])) * 16777619
	}
	return int(h % uint32(len(d.queues)))
}

// Run consumes event batches from in until it is closed or ctx is
// cancelled, routing each event to its queue (blocking when the queue is
// full). On return it flushes staged events and closes all queues so
// downstream publishers drain and exit.
func (d *Dispatcher) Run(ctx context.Context, in <-chan []*event.NomiosEvent) error {
	defer func() {
		for _, q := range d.queues {
			close(q)
		}
	}()
	for {
		select {
		case batch, ok := <-in:
			if !ok {
				return d.flushAll(ctx)
			}
			for _, e := range batch {
				i := d.Pick(e)
				d.staging[i] = append(d.staging[i], e)
				if len(d.staging[i]) >= d.flushSize {
					if err := d.flush(ctx, i); err != nil {
						return err
					}
				}
			}
			// Input momentarily idle: don't sit on staged events.
			if len(in) == 0 {
				if err := d.flushAll(ctx); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *Dispatcher) flush(ctx context.Context, i int) error {
	if len(d.staging[i]) == 0 {
		return nil
	}
	select {
	case d.queues[i] <- d.staging[i]:
		d.staging[i] = make([]*event.NomiosEvent, 0, d.flushSize)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Dispatcher) flushAll(ctx context.Context) error {
	for i := range d.queues {
		if err := d.flush(ctx, i); err != nil {
			return err
		}
	}
	return nil
}

// Depths returns the approximate number of buffered events per queue
// (buffered batches × flush size). Staged-but-unflushed events are not
// counted: staging is owned by the Run goroutine and Depths is called
// concurrently from the status endpoint.
func (d *Dispatcher) Depths() []int {
	out := make([]int, len(d.queues))
	for i, q := range d.queues {
		out[i] = len(q) * d.flushSize
	}
	return out
}
