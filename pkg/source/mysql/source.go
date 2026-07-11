// Package mysql implements MysqlSource: Nomios acts as a MySQL replication
// slave, streams raw binlog events, and maps them (parse + schema info) to
// NomiosEvents. The source runs on a single goroutine by design — stream
// and parse speed of one reader is sufficient; parallelism is applied
// downstream where it pays (serialize/publish).
package mysql

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/duy-tung/cdc-scaling/pkg/dispatch"
	"github.com/duy-tung/cdc-scaling/pkg/event"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

// Config for MysqlSource.
type Config struct {
	Host     string `yaml:"host"`
	Port     uint16 `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	// ServerID must be unique among all replicas (real and Nomios) attached
	// to the MySQL server.
	ServerID uint32 `yaml:"serverID"`
	// GTID enables GTID-based positions (strongly preferred; robust across
	// master failover behind the same host URI).
	GTID    bool     `yaml:"gtid"`
	Include []string `yaml:"include"`
	Exclude []string `yaml:"exclude"`
	// HeartbeatPeriod for the replication connection (default 30s).
	HeartbeatPeriod time.Duration `yaml:"heartbeatPeriod"`
}

func (c *Config) withDefaults() {
	if c.Port == 0 {
		c.Port = 3306
	}
	if c.HeartbeatPeriod == 0 {
		c.HeartbeatPeriod = 30 * time.Second
	}
}

func (c *Config) addr() string { return fmt.Sprintf("%s:%d", c.Host, c.Port) }

// Source is the MySQL binlog source.
type Source struct {
	cfg    Config
	filter *tableFilter
	log    *slog.Logger
}

func New(cfg Config, logger *slog.Logger) *Source {
	cfg.withDefaults()
	if logger == nil {
		logger = slog.Default()
	}
	// System schemas and Nomios's own state table are always excluded:
	// capturing nomios_state would feed the state manager's commits back
	// into the pipeline as an infinite loop.
	exclude := append([]string{
		"mysql.*", "sys.*", "performance_schema.*", "information_schema.*", "*.nomios_state",
	}, cfg.Exclude...)
	return &Source{
		cfg:    cfg,
		filter: newTableFilter(cfg.Include, exclude),
		log:    logger.With("component", "mysql-source"),
	}
}

// Start implements source.Source.
func (s *Source) Start(ctx context.Context, from state.Position, out chan<- []*event.NomiosEvent) error {
	if err := s.validateServer(); err != nil {
		return err
	}

	registry, err := newSchemaRegistry(s.cfg.addr(), s.cfg.User, s.cfg.Password)
	if err != nil {
		return err
	}
	defer registry.Close()

	start, err := s.resolveStart(from)
	if err != nil {
		return err
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:        s.cfg.ServerID,
		Flavor:          gomysql.MySQLFlavor,
		Host:            s.cfg.Host,
		Port:            s.cfg.Port,
		User:            s.cfg.User,
		Password:        s.cfg.Password,
		HeartbeatPeriod: s.cfg.HeartbeatPeriod,
	})
	defer syncer.Close()

	var streamer *replication.BinlogStreamer
	if start.gtidSet != nil {
		s.log.Info("starting binlog sync", "gtid_set", start.gtidSet.String())
		// The syncer takes ownership of the set it is given and mutates it
		// on its own goroutine — hand it a clone, keep ours for txn tracking.
		streamer, err = syncer.StartSyncGTID(start.gtidSet.Clone())
	} else {
		s.log.Info("starting binlog sync", "file", start.file, "offset", start.offset)
		streamer, err = syncer.StartSync(gomysql.Position{Name: start.file, Pos: start.offset})
	}
	if err != nil {
		return fmt.Errorf("mysql source: start sync: %w", err)
	}

	return s.run(ctx, streamer, registry, start, out)
}

// startPoint is the resolved streaming start.
type startPoint struct {
	gtidSet gomysql.GTIDSet
	file    string
	offset  uint32
}

// resolveStart turns a saved Position (possibly zero) into concrete binlog
// coordinates, querying the server's current position for fresh starts.
func (s *Source) resolveStart(from state.Position) (startPoint, error) {
	if s.cfg.GTID {
		gtid := from.GTIDSet
		if gtid == "" && from.IsZero() {
			var err error
			gtid, _, _, err = s.masterStatus()
			if err != nil {
				return startPoint{}, err
			}
		}
		set, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, gtid)
		if err != nil {
			return startPoint{}, fmt.Errorf("mysql source: parse gtid set %q: %w", gtid, err)
		}
		return startPoint{gtidSet: set}, nil
	}
	if from.File != "" {
		return startPoint{file: from.File, offset: from.Offset}, nil
	}
	_, file, pos, err := s.masterStatus()
	if err != nil {
		return startPoint{}, err
	}
	return startPoint{file: file, offset: pos}, nil
}

// validateServer fails fast when the MySQL server is configured in a way
// that would silently corrupt payloads instead of erroring later:
// binlog_format must be ROW (statement/mixed events carry no row images)
// and binlog_row_image must be FULL — with MINIMAL, go-mysql still decodes
// full-width rows but fills omitted columns with nil, so events would
// carry wrong nulls and, worse, wrong partition keys, undetected.
func (s *Source) validateServer() error {
	conn, err := client.Connect(s.cfg.addr(), s.cfg.User, s.cfg.Password, "")
	if err != nil {
		return fmt.Errorf("mysql source: connect for validation: %w", err)
	}
	defer conn.Close()

	get := func(name string) (string, error) {
		r, err := conn.Execute("SHOW VARIABLES LIKE '" + name + "'")
		if err != nil {
			return "", fmt.Errorf("mysql source: show variables %s: %w", name, err)
		}
		defer r.Close()
		if r.RowNumber() == 0 {
			return "", nil
		}
		v, _ := r.GetString(0, 1)
		// GetString is zero-copy into pooled buffers; clone anything that
		// outlives Result.Close.
		return strings.Clone(v), nil
	}

	format, err := get("binlog_format")
	if err != nil {
		return err
	}
	if !strings.EqualFold(format, "ROW") {
		return fmt.Errorf("mysql source: binlog_format is %q, need ROW", format)
	}
	image, err := get("binlog_row_image")
	if err != nil {
		return err
	}
	if image != "" && !strings.EqualFold(image, "FULL") {
		return fmt.Errorf("mysql source: binlog_row_image is %q, need FULL (MINIMAL/NOBLOB would silently produce wrong row images and partition keys)", image)
	}
	if s.cfg.GTID {
		mode, err := get("gtid_mode")
		if err != nil {
			return err
		}
		if !strings.EqualFold(mode, "ON") {
			return fmt.Errorf("mysql source: gtid requested but gtid_mode is %q", mode)
		}
	}
	return nil
}

// masterStatus queries the server's current binlog coordinates.
func (s *Source) masterStatus() (gtidSet, file string, pos uint32, err error) {
	conn, err := client.Connect(s.cfg.addr(), s.cfg.User, s.cfg.Password, "")
	if err != nil {
		return "", "", 0, fmt.Errorf("mysql source: connect: %w", err)
	}
	defer conn.Close()
	r, err := conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		return "", "", 0, fmt.Errorf("mysql source: show master status: %w", err)
	}
	defer r.Close()
	if r.RowNumber() == 0 {
		return "", "", 0, errors.New("mysql source: binary logging is not enabled on the server")
	}
	// Clone: GetString values are backed by pooled buffers freed on Close.
	file, _ = r.GetString(0, 0)
	file = strings.Clone(file)
	p, _ := r.GetUint(0, 1)
	if r.ColumnNumber() > 4 {
		gtidSet, _ = r.GetString(0, 4)
		gtidSet = strings.Clone(gtidSet)
	}
	return gtidSet, file, uint32(p), nil
}

// txnState tracks the resume coordinates of the last fully committed
// transaction. Every emitted event is stamped with these *committed*
// coordinates (not its own), so resuming replays at most the in-progress
// transaction — never skipping events.
type txnState struct {
	committedGTID string // serialized executed set through the last commit
	committedFile string
	committedPos  uint32
	currentFile   string

	set         gomysql.GTIDSet // running executed set (nil in file mode)
	pendingGTID string          // GTID of the currently open transaction
	seq         uint64
	txOrder     int
}

func (s *Source) run(ctx context.Context, streamer *replication.BinlogStreamer, registry *schemaRegistry, start startPoint, out chan<- []*event.NomiosEvent) error {
	ts := &txnState{
		committedFile: start.file,
		committedPos:  start.offset,
		currentFile:   start.file,
	}
	if start.gtidSet != nil {
		ts.set = start.gtidSet.Clone()
		ts.committedGTID = ts.set.String()
	}

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("mysql source: stream: %w", err)
		}
		if err := s.handleEvent(ctx, ev, ts, registry, out); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}
}

func (s *Source) handleEvent(ctx context.Context, ev *replication.BinlogEvent, ts *txnState, registry *schemaRegistry, out chan<- []*event.NomiosEvent) error {
	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		ts.currentFile = string(e.NextLogName)

	case *replication.GTIDEvent:
		ts.pendingGTID = fmt.Sprintf("%s:%d", formatSID(e.SID), e.GNO)
		ts.txOrder = 0

	case *replication.XIDEvent:
		ts.commit(ev.Header.LogPos)

	case *replication.QueryEvent:
		q := strings.TrimSpace(string(e.Query))
		if strings.EqualFold(q, "BEGIN") {
			return nil
		}
		// Any other query event is DDL (or COMMIT for non-transactional
		// tables): it commits its own transaction and may change schemas.
		s.log.Info("ddl observed, invalidating schema cache", "query", truncate(q, 120))
		registry.InvalidateAll()
		ts.commit(ev.Header.LogPos)

	case *replication.RowsEvent:
		op, ok := opForEventType(ev.Header.EventType)
		if !ok {
			return nil
		}
		return s.emitRows(ctx, ev, e, op, ts, registry, out)
	}
	return nil
}

// commit marks the currently open transaction as fully committed and folds
// its GTID into the running executed set.
func (ts *txnState) commit(logPos uint32) {
	if ts.set != nil && ts.pendingGTID != "" {
		if err := ts.set.Update(ts.pendingGTID); err == nil {
			ts.committedGTID = ts.set.String()
		}
		ts.pendingGTID = ""
	}
	ts.committedFile = ts.currentFile
	ts.committedPos = logPos
}

func (s *Source) emitRows(ctx context.Context, ev *replication.BinlogEvent, re *replication.RowsEvent, op event.Op, ts *txnState, registry *schemaRegistry, out chan<- []*event.NomiosEvent) error {
	step := 1
	if op == event.OpUpdate {
		step = 2 // update rows come in (before, after) pairs
	}
	db, table := string(re.Table.Schema), string(re.Table.Table)
	fqtn := db + "." + table
	if !s.filter.match(fqtn) {
		// Filtered-out rows still advance txOrder: an event's tx_order (and
		// therefore its GTID-based ID) must denote the row's position within
		// the whole transaction, not within this hyperloop's capture subset —
		// otherwise two hyperloops with different table filters would assign
		// the same ID to different rows.
		ts.txOrder += len(re.Rows) / step
		return nil
	}
	schema, err := registry.Get(db, table)
	if err != nil {
		return err
	}
	// Defensive re-fetch: if the cached column count no longer matches the
	// binlog row width (schema changed under us), reload once.
	if len(re.Rows) > 0 && len(re.Rows[0]) != len(schema.Columns) {
		schema, err = registry.Refresh(db, table)
		if err != nil {
			return err
		}
	}

	occurred := time.Unix(int64(ev.Header.Timestamp), 0)
	// All rows of one binlog event travel as one micro-batch: a single
	// channel send instead of one per row.
	batch := make([]*event.NomiosEvent, 0, len(re.Rows)/step)
	for i := 0; i+step-1 < len(re.Rows); i += step {
		var before, after map[string]any
		switch op {
		case event.OpInsert:
			after = mapRow(schema, re.Rows[i])
		case event.OpDelete:
			before = mapRow(schema, re.Rows[i])
		case event.OpUpdate:
			before = mapRow(schema, re.Rows[i])
			after = mapRow(schema, re.Rows[i+1])
		}

		ts.seq++
		ts.txOrder++
		// Event IDs must be stable across MySQL failover so downstream
		// dedupe keeps working: a GTID survives a master change, while the
		// same transaction can land at a different file/offset on the new
		// master. File coordinates are only the non-GTID fallback.
		var id string
		if ts.pendingGTID != "" {
			id = fmt.Sprintf("%s#%d", ts.pendingGTID, ts.txOrder)
		} else {
			id = fmt.Sprintf("%s:%d:%d", ts.currentFile, ev.Header.LogPos, i/step)
		}
		ne := &event.NomiosEvent{
			ID:         id,
			Op:         op,
			Before:     before,
			After:      after,
			OccurredAt: occurred,
			Source: event.SourceMeta{
				Connector: "mysql",
				ServerID:  ev.Header.ServerID,
				Database:  db,
				Table:     table,
				GTID:      ts.pendingGTID,
				File:      ts.currentFile,
				Pos:       ev.Header.LogPos,
				TxOrder:   ts.txOrder,
			},
			Position: state.Position{
				GTIDSet: ts.committedGTID,
				File:    ts.committedFile,
				Offset:  ts.committedPos,
				SeqNo:   ts.seq,
			},
		}
		row := ne.Row()
		ne.Key = dispatch.KeyFromColumns(fqtn, row, schema.PK)
		batch = append(batch, ne)
	}
	if len(batch) == 0 {
		return nil
	}
	select {
	case out <- batch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// mapRow maps positional binlog values to named columns, interpreting
// []byte values by the column's information_schema type: character data
// becomes string, JSON documents stay raw JSON (serialized as nested
// objects, not quoted strings), and true binary data stays []byte (the
// serializer emits it as base64). The binlog alone cannot make these
// distinctions — TEXT and BLOB share a wire type.
func mapRow(schema *TableSchema, vals []any) map[string]any {
	n := len(vals)
	if len(schema.Columns) < n {
		n = len(schema.Columns)
	}
	m := make(map[string]any, n)
	for i := 0; i < n; i++ {
		v := vals[i]
		// go-mysql is inconsistent about []byte vs string across column
		// types and versions (e.g. v1.15 returns JSON documents and
		// VARBINARY as string), so both representations are normalized by
		// the schema kind.
		switch schema.Kinds[i] {
		case KindJSON:
			switch b := v.(type) {
			case []byte:
				if len(b) == 0 {
					v = nil // empty JSON binary means SQL-inserted empty value
				} else {
					v = json.RawMessage(b)
				}
			case string:
				if b == "" {
					v = nil
				} else {
					v = json.RawMessage(b)
				}
			}
		case KindBinary:
			if s, ok := v.(string); ok {
				v = []byte(s) // serialized as base64 downstream
			}
		default:
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
		}
		m[schema.Columns[i]] = v
	}
	return m
}

func opForEventType(t replication.EventType) (event.Op, bool) {
	switch t {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return event.OpInsert, true
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return event.OpUpdate, true
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return event.OpDelete, true
	}
	return "", false
}

// formatSID renders a 16-byte server UUID as the canonical 8-4-4-4-12 form.
func formatSID(sid []byte) string {
	if len(sid) != 16 {
		return hex.EncodeToString(sid)
	}
	h := hex.EncodeToString(sid)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
