// Package source defines the Source interface: the component responsible
// for providing events. A Source pulls changes from a data store and
// initiates the event stream of a Nomios pipeline. Events are filtered,
// mapped and transformed into NomiosEvents before reaching other
// components. The interface keeps additional data stores easy to add;
// MysqlSource is the only implementation in v1.
package source

import (
	"context"

	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// Source streams change events into out.
type Source interface {
	// Start begins streaming from the given position (a zero Position means
	// "current end of the log") and pushes events to out until ctx is
	// cancelled or a fatal error occurs. Start blocks for the lifetime of
	// the stream; it must return promptly (nil or ctx.Err) once ctx is
	// cancelled. Start must NOT close out — the caller owns the channel.
	//
	// Events are sent in micro-batches: the natural unit the source reads
	// (e.g. all rows of one binlog RowsEvent). Batching the channel hop
	// amortizes scheduler and select overhead, which profiling showed at
	// ~25% of pipeline CPU when events crossed one at a time. A batch is
	// owned by the receiver once sent.
	//
	// SeqNos on emitted events must start at 1 and be contiguous within one
	// Start call.
	Start(ctx context.Context, from state.Position, out chan<- []*event.NomiosEvent) error
}
