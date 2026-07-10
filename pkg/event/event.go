// Package event defines NomiosEvent, the standard data format used to
// communicate between all components in a Nomios stream.
package event

import (
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// Op is the kind of row change an event represents.
type Op string

const (
	OpInsert Op = "c" // create
	OpUpdate Op = "u"
	OpDelete Op = "d"
)

// NomiosEvent represents one change event (insert, update, delete). It
// carries the before/after images of the changed entity, a globally unique
// identifier, metadata about the source of the event, and the time the
// event occurred.
type NomiosEvent struct {
	// ID is globally unique and deterministic (derived from the binlog
	// coordinates), so downstream consumers can deduplicate replays.
	ID     string         `json:"id"`
	Op     Op             `json:"op"`
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Source SourceMeta     `json:"source"`
	// OccurredAt is the event time from the binlog header.
	OccurredAt time.Time `json:"-"`

	// Key is the partition/ordering key for the entity this event belongs
	// to. The Source sets a default (primary key values); the dispatcher may
	// override it per table. All events with the same Key flow through the
	// same buffer queue, publisher and Kafka partition.
	Key string `json:"-"`

	// Position is the resume coordinate attached to this event, used by the
	// state manager to compute the checkpoint.
	Position state.Position `json:"-"`
}

// SourceMeta describes where an event came from.
type SourceMeta struct {
	Connector string `json:"connector"` // "mysql"
	ServerID  uint32 `json:"server_id,omitempty"`
	Database  string `json:"db"`
	Table     string `json:"table"`
	GTID      string `json:"gtid,omitempty"` // GTID of the enclosing transaction
	File      string `json:"file,omitempty"` // binlog file
	Pos       uint32 `json:"pos,omitempty"`  // binlog offset of the event
	TxOrder   int    `json:"tx_order,omitempty"`
}

// FQTN returns the fully qualified table name "db.table".
func (m SourceMeta) FQTN() string { return m.Database + "." + m.Table }

// Row returns the current row image: After for inserts/updates, Before for
// deletes.
func (e *NomiosEvent) Row() map[string]any {
	if e.Op == OpDelete {
		return e.Before
	}
	return e.After
}
