// Package state defines binlog positions, the checkpoint tracker and
// persistent state stores used by Nomios to resume streaming after a
// restart, deploy or crash.
package state

// Position identifies a point in the source change stream.
//
// GTIDSet is the preferred resume coordinate: the executed GTID set of the
// source as of the last fully-committed transaction *before* this event.
// Resuming from it replays the enclosing transaction, guaranteeing no gaps
// (at-least-once). File/Offset are the file-position fallback with the same
// semantics: the end of the last committed transaction.
//
// SeqNo is a monotonic, contiguous sequence number assigned by the Source to
// every event it emits within one run. It exists only to order positions and
// to compute the contiguous checkpoint; it is not persisted across source
// restarts in a meaningful way.
type Position struct {
	GTIDSet string `json:"gtid_set,omitempty"`
	File    string `json:"file,omitempty"`
	Offset  uint32 `json:"offset,omitempty"`
	SeqNo   uint64 `json:"seq_no,omitempty"`
}

// IsZero reports whether the position carries no resume coordinates.
func (p Position) IsZero() bool {
	return p.GTIDSet == "" && p.File == "" && p.Offset == 0
}
