// Package serialize converts NomiosEvents to sink wire formats. Nomios
// provides a Serializer interface so multiple formats can be supported;
// JSON is the format used in v1.
package serialize

import "github.com/duy-tung/cdc-scaling/pkg/event"

// Serializer turns an event into a (key, value) record payload.
type Serializer interface {
	Serialize(e *event.NomiosEvent) (key []byte, value []byte, err error)
}
