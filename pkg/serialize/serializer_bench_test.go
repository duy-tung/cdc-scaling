package serialize

import (
	"fmt"
	"testing"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/event"
)

func benchEvent(cols int) *event.NomiosEvent {
	before := make(map[string]any, cols)
	after := make(map[string]any, cols)
	for i := 0; i < cols; i++ {
		c := fmt.Sprintf("col_%d", i)
		before[c] = i
		after[c] = i + 1
	}
	return &event.NomiosEvent{
		ID:     "binlog.000001:456:0",
		Op:     event.OpUpdate,
		Key:    "db.items|5",
		Before: before,
		After:  after,
		Source: event.SourceMeta{
			Connector: "mysql", Database: "db", Table: "items",
			GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:23",
			File: "binlog.000001", Pos: 456,
		},
		OccurredAt: time.UnixMilli(1657205700000),
	}
}

// BenchmarkJSONSerialize measures serialization cost per event — the
// dominant CPU cost of a publisher — for narrow and wide rows.
func BenchmarkJSONSerialize(b *testing.B) {
	for _, cols := range []int{5, 20, 100} {
		b.Run(fmt.Sprintf("cols=%d", cols), func(b *testing.B) {
			e := benchEvent(cols)
			s := JSON{}
			b.ReportAllocs()
			b.ResetTimer()
			var bytes int64
			for i := 0; i < b.N; i++ {
				_, v, err := s.Serialize(e)
				if err != nil {
					b.Fatal(err)
				}
				bytes += int64(len(v))
			}
			b.SetBytes(bytes / int64(b.N))
		})
	}
}
