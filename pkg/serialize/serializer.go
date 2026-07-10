// Package serialize converts NomiosEvents to sink wire formats. Nomios
// provides a Serializer interface so multiple formats can be supported;
// JSON is the format used in v1.
package serialize

import (
	"encoding/json"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/event"
)

// Serializer turns an event into a (key, value) record payload.
type Serializer interface {
	Serialize(e *event.NomiosEvent) (key []byte, value []byte, err error)
}

// JSON serializes events into a Debezium-style JSON envelope:
// {"id","op","before","after","source","ts_ms"}. The record key is the
// event's partition key so per-entity ordering carries into Kafka.
type JSON struct{}

type jsonEnvelope struct {
	ID     string           `json:"id"`
	Op     event.Op         `json:"op"`
	Before map[string]any   `json:"before"`
	After  map[string]any   `json:"after"`
	Source event.SourceMeta `json:"source"`
	TsMs   int64            `json:"ts_ms"`
}

func (JSON) Serialize(e *event.NomiosEvent) ([]byte, []byte, error) {
	v, err := json.Marshal(jsonEnvelope{
		ID:     e.ID,
		Op:     e.Op,
		Before: e.Before,
		After:  e.After,
		Source: e.Source,
		TsMs:   e.OccurredAt.UnixNano() / int64(time.Millisecond),
	})
	if err != nil {
		return nil, nil, err
	}
	return []byte(e.Key), v, nil
}
