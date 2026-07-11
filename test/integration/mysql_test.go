//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"

	"github.com/duy-tung/cdc-scaling/pkg/hyperloop"
	"github.com/duy-tung/cdc-scaling/pkg/publish/kafka"
	"github.com/duy-tung/cdc-scaling/pkg/serialize"
	mysqlsource "github.com/duy-tung/cdc-scaling/pkg/source/mysql"
	"github.com/duy-tung/cdc-scaling/pkg/state"
)

const testDB = "nomios_test"

func mysqlAddr() string {
	if a := os.Getenv("NOMIOS_TEST_MYSQL_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:3306"
}

func mysqlUser() string {
	if u := os.Getenv("NOMIOS_TEST_MYSQL_USER"); u != "" {
		return u
	}
	return "nomios"
}

func mysqlPassword() string {
	if p := os.Getenv("NOMIOS_TEST_MYSQL_PASSWORD"); p != "" {
		return p
	}
	return "nomios-test"
}

// mysqlConn connects to the test MySQL or skips the test with setup
// instructions when the server isn't available.
func mysqlConn(t *testing.T) *client.Conn {
	t.Helper()
	conn, err := client.Connect(mysqlAddr(), mysqlUser(), mysqlPassword(), testDB)
	if err != nil {
		t.Skipf("MySQL not available at %s (%v) — need MySQL 8 with ROW binlog, gtid_mode=ON, "+
			"user %q (REPLICATION SLAVE, REPLICATION CLIENT, SELECT on *.*, ALL on %s.*)",
			mysqlAddr(), err, mysqlUser(), testDB)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func mustExec(t *testing.T, conn *client.Conn, q string, args ...any) {
	t.Helper()
	if _, err := conn.Execute(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// sourceHostPort splits the configured test address for the source config.
func sourceHostPort(t *testing.T) (string, uint16) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(mysqlAddr())
	if err != nil {
		t.Fatalf("bad NOMIOS_TEST_MYSQL_ADDR %q: %v", mysqlAddr(), err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("bad port in %q: %v", mysqlAddr(), err)
	}
	return host, uint16(port)
}

// masterFilePos reads the server's current binlog file and offset (the
// file-position-mode analogue of executedGTIDSet).
func masterFilePos(t *testing.T, conn *client.Conn) (string, uint32) {
	t.Helper()
	r, err := conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		t.Fatalf("show master status: %v", err)
	}
	defer r.Close()
	if r.RowNumber() == 0 {
		t.Fatal("binary logging not enabled on test server")
	}
	file, _ := r.GetString(0, 0)
	pos, _ := r.GetUint(0, 1)
	// GetString is zero-copy into pooled buffers freed by Close.
	return strings.Clone(file), uint32(pos)
}

// executedGTIDSet reads the server's current executed GTID set, used to
// pre-seed the state store so the pipeline deterministically captures
// everything written after this point (no startup race).
func executedGTIDSet(t *testing.T, conn *client.Conn) string {
	t.Helper()
	r, err := conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		t.Fatalf("show master status: %v", err)
	}
	defer r.Close()
	if r.RowNumber() == 0 || r.ColumnNumber() < 5 {
		t.Fatal("binary logging/GTID not enabled on test server")
	}
	set, _ := r.GetString(0, 4)
	if set == "" {
		t.Fatal("executed GTID set is empty; is gtid_mode=ON?")
	}
	// GetString is zero-copy into pooled buffers freed by Close.
	return strings.Clone(set)
}

// fileStore is the default state store for tests that don't exercise
// persistence specifics.
func fileStore(t *testing.T) state.Store {
	t.Helper()
	s, err := state.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// startLoop builds and starts a hyperloop over the given tables, returning
// a stop function that gracefully stops it and asserts a clean exit.
func startLoop(t *testing.T, id string, serverID uint32, include []string, brokers []string, store state.Store) (stop func()) {
	return startLoopMode(t, id, serverID, include, brokers, store, true)
}

// startLoopMode is startLoop with an explicit GTID/file-position choice.
func startLoopMode(t *testing.T, id string, serverID uint32, include []string, brokers []string, store state.Store, gtid bool) (stop func()) {
	t.Helper()
	// Pre-seed the state with the current position if none is saved yet.
	if p, err := store.Load(context.Background(), id); err != nil {
		t.Fatal(err)
	} else if p.IsZero() {
		conn := mysqlConn(t)
		var seed state.Position
		if gtid {
			seed = state.Position{GTIDSet: executedGTIDSet(t, conn)}
		} else {
			file, pos := masterFilePos(t, conn)
			seed = state.Position{File: file, Offset: pos}
		}
		if err := store.Save(context.Background(), id, seed); err != nil {
			t.Fatal(err)
		}
	}

	host, port := sourceHostPort(t)
	src := mysqlsource.New(mysqlsource.Config{
		Host:     host,
		Port:     port,
		User:     mysqlUser(),
		Password: mysqlPassword(),
		ServerID: serverID,
		GTID:     gtid,
		Include:  include,
	}, nil)

	sink, err := kafka.New(kafka.Config{Brokers: brokers, Compression: "none"}, serialize.JSON{})
	if err != nil {
		t.Fatal(err)
	}

	hl, err := hyperloop.New(hyperloop.Config{
		ID:             id,
		Queues:         4,
		BatchMaxSize:   100,
		BatchMaxWait:   10 * time.Millisecond,
		CommitInterval: 50 * time.Millisecond,
	}, hyperloop.Deps{Source: src, Sink: sink, Store: store})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- hl.Run(ctx) }()

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("hyperloop %s exited with error: %v", id, err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("hyperloop %s did not stop in time", id)
		}
	}
	t.Cleanup(stop)
	return stop
}

// TestMySQLCaptureInsertUpdateDelete streams real binlog changes through
// the full pipeline into Kafka and checks ops, payloads and per-key order.
func TestMySQLCaptureInsertUpdateDelete(t *testing.T) {
	conn := mysqlConn(t)
	mustExec(t, conn, "DROP TABLE IF EXISTS items")
	mustExec(t, conn, `CREATE TABLE items (
		id INT NOT NULL PRIMARY KEY,
		name VARCHAR(64) NOT NULL,
		qty INT NOT NULL DEFAULT 0
	)`)

	topic := "cdc." + testDB + ".items"
	cluster := kafkaCluster(t, topic)
	stop := startLoop(t, "it-crud", 5501, []string{testDB + ".items"}, cluster.ListenAddrs(), fileStore(t))

	// 60 inserts (some multi-row), 20 updates, 10 deletes = 90 change events.
	for i := 1; i <= 60; i += 3 {
		mustExec(t, conn, fmt.Sprintf(
			"INSERT INTO items (id, name, qty) VALUES (%d,'n%d',1),(%d,'n%d',1),(%d,'n%d',1)",
			i, i, i+1, i+1, i+2, i+2))
	}
	mustExec(t, conn, "UPDATE items SET qty = qty + 41 WHERE id <= 20")
	mustExec(t, conn, "DELETE FROM items WHERE id > 50")

	events := consumeUntil(t, cluster.ListenAddrs(), topic, 60*time.Second, atLeast(90))
	stop()

	counts := map[string]int{}
	lastOpByKey := map[string]string{}
	for _, e := range events {
		counts[e.Op]++
		// Per-key lifecycle order: c before u before d, nothing after d.
		if last, ok := lastOpByKey[e.key]; ok {
			if last == "d" {
				t.Fatalf("key %s: event %s after delete", e.key, e.Op)
			}
			if last == "u" && e.Op == "c" {
				t.Fatalf("key %s: insert after update", e.key)
			}
		} else if e.Op != "c" {
			t.Fatalf("key %s: first event is %s, want insert", e.key, e.Op)
		}
		lastOpByKey[e.key] = e.Op

		if e.Source.Connector != "mysql" || e.Source.Database != testDB || e.Source.Table != "items" {
			t.Fatalf("bad source meta: %+v", e.Source)
		}
		if e.Source.GTID == "" || e.Source.File == "" {
			t.Fatalf("missing binlog coordinates in source meta: %+v", e.Source)
		}

		switch e.Op {
		case "c":
			if e.Before != nil || e.After == nil {
				t.Fatalf("insert images wrong: before=%v after=%v", e.Before, e.After)
			}
			id := int(e.After["id"].(float64))
			if e.After["name"] != fmt.Sprintf("n%d", id) || e.After["qty"].(float64) != 1 {
				t.Fatalf("insert payload wrong: %v", e.After)
			}
		case "u":
			if e.Before == nil || e.After == nil {
				t.Fatalf("update images wrong: before=%v after=%v", e.Before, e.After)
			}
			if e.Before["qty"].(float64) != 1 || e.After["qty"].(float64) != 42 {
				t.Fatalf("update payload wrong: before=%v after=%v", e.Before, e.After)
			}
		case "d":
			if e.Before == nil || e.After != nil {
				t.Fatalf("delete images wrong: before=%v after=%v", e.Before, e.After)
			}
			if int(e.Before["id"].(float64)) <= 50 {
				t.Fatalf("unexpected delete of id %v", e.Before["id"])
			}
		}
	}
	if counts["c"] != 60 || counts["u"] != 20 || counts["d"] != 10 {
		t.Fatalf("op counts = %v, want c:60 u:20 d:10", counts)
	}
}

// TestMySQLResumeAfterRestart stops the pipeline, keeps writing while it is
// down, restarts it from the persisted checkpoint and verifies no gaps.
func TestMySQLResumeAfterRestart(t *testing.T) {
	conn := mysqlConn(t)
	mustExec(t, conn, "DROP TABLE IF EXISTS resume_items")
	mustExec(t, conn, "CREATE TABLE resume_items (id INT NOT NULL PRIMARY KEY, v VARCHAR(32))")

	topic := "cdc." + testDB + ".resume_items"
	cluster := kafkaCluster(t, topic)
	include := []string{testDB + ".resume_items"}

	// The resume test doubles as the MySQLStore integration test: state is
	// persisted in the source database itself (the production default) and
	// must survive across the two pipeline runs.
	store, err := state.NewMySQLStore(mysqlAddr(), mysqlUser(), mysqlPassword(), testDB)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	mustExec(t, conn, "DELETE FROM nomios_state WHERE hyperloop_id = 'it-resume'")

	// Phase 1: capture 30 inserts, then stop gracefully (commits state).
	stop1 := startLoop(t, "it-resume", 5502, include, cluster.ListenAddrs(), store)
	for i := 1; i <= 30; i++ {
		mustExec(t, conn, "INSERT INTO resume_items VALUES (?, 'a')", i)
	}
	consumeUntil(t, cluster.ListenAddrs(), topic, 60*time.Second, atLeast(30))
	stop1()

	// Downtime: 40 more inserts while no pipeline is running.
	for i := 31; i <= 70; i++ {
		mustExec(t, conn, "INSERT INTO resume_items VALUES (?, 'b')", i)
	}

	// Phase 2: restart from the checkpoint; the 40 downtime rows must all
	// arrive (replays of phase-1 rows are legal, gaps are not).
	startLoop(t, "it-resume", 5503, include, cluster.ListenAddrs(), store)
	events := consumeUntil(t, cluster.ListenAddrs(), topic, 60*time.Second, func(seen map[string]envelope) bool {
		ids := map[int]bool{}
		for _, e := range seen {
			if e.Op == "c" && e.After != nil {
				ids[int(e.After["id"].(float64))] = true
			}
		}
		return len(ids) >= 70
	})

	ids := map[int]bool{}
	for _, e := range events {
		if e.Op != "c" {
			t.Fatalf("unexpected op %s", e.Op)
		}
		ids[int(e.After["id"].(float64))] = true
	}
	for i := 1; i <= 70; i++ {
		if !ids[i] {
			t.Fatalf("gap after restart: id %d never captured", i)
		}
	}
}

// TestMySQLTypedColumns verifies the type-faithful wire mapping: JSON
// columns arrive as nested JSON documents (not quoted strings), binary
// columns as base64, and TEXT as plain strings — distinctions the binlog
// alone cannot make (TEXT and BLOB share a wire type).
func TestMySQLTypedColumns(t *testing.T) {
	conn := mysqlConn(t)
	mustExec(t, conn, "DROP TABLE IF EXISTS typed_items")
	mustExec(t, conn, `CREATE TABLE typed_items (
		id INT NOT NULL PRIMARY KEY,
		doc JSON,
		bin VARBINARY(16),
		txt TEXT
	)`)

	topic := "cdc." + testDB + ".typed_items"
	cluster := kafkaCluster(t, topic)
	startLoop(t, "it-typed", 5505, []string{testDB + ".typed_items"}, cluster.ListenAddrs(), fileStore(t))

	mustExec(t, conn,
		`INSERT INTO typed_items VALUES (1, '{"a": 1, "b": [true, null]}', X'DEADBEEF', 'hello world')`)
	mustExec(t, conn, `INSERT INTO typed_items VALUES (2, NULL, NULL, NULL)`)

	events := consumeUntil(t, cluster.ListenAddrs(), topic, 60*time.Second, atLeast(2))
	for _, e := range events {
		switch int(e.After["id"].(float64)) {
		case 1:
			doc, ok := e.After["doc"].(map[string]any)
			if !ok {
				t.Fatalf("JSON column arrived as %T (%v), want nested object", e.After["doc"], e.After["doc"])
			}
			if doc["a"].(float64) != 1 {
				t.Fatalf("JSON payload wrong: %v", doc)
			}
			bin, _ := e.After["bin"].(string)
			raw, err := base64.StdEncoding.DecodeString(bin)
			if err != nil || !bytes.Equal(raw, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
				t.Fatalf("binary column = %q (decoded %x, err %v), want base64 of deadbeef", bin, raw, err)
			}
			if e.After["txt"] != "hello world" {
				t.Fatalf("text column = %v", e.After["txt"])
			}
		case 2:
			for _, col := range []string{"doc", "bin", "txt"} {
				if e.After[col] != nil {
					t.Fatalf("column %s = %v, want null", col, e.After[col])
				}
			}
		}
	}
}

// TestMySQLReplayFromStaleCheckpoint simulates crash recovery: the state
// store is rewound to an old position (as if the process died before its
// latest commits) and the pipeline restarted. Everything after the stale
// checkpoint must be replayed — with identical event IDs, so downstream
// dedupe by ID absorbs the duplicates.
func TestMySQLReplayFromStaleCheckpoint(t *testing.T) {
	conn := mysqlConn(t)
	mustExec(t, conn, "DROP TABLE IF EXISTS crash_items")
	mustExec(t, conn, "CREATE TABLE crash_items (id INT NOT NULL PRIMARY KEY, v INT)")

	topic := "cdc." + testDB + ".crash_items"
	cluster := kafkaCluster(t, topic)
	include := []string{testDB + ".crash_items"}
	store := fileStore(t)

	// Capture the pre-insert position, run the pipeline over 40 rows, stop.
	seed := state.Position{GTIDSet: executedGTIDSet(t, conn)}
	if err := store.Save(context.Background(), "it-crash", seed); err != nil {
		t.Fatal(err)
	}
	stop1 := startLoop(t, "it-crash", 5506, include, cluster.ListenAddrs(), store)
	for i := 1; i <= 40; i++ {
		mustExec(t, conn, "INSERT INTO crash_items VALUES (?, ?)", i, i)
	}
	firstRun := consumeUntil(t, cluster.ListenAddrs(), topic, 60*time.Second, atLeast(40))
	stop1()

	firstIDs := map[int]string{}
	for _, e := range firstRun {
		firstIDs[int(e.After["id"].(float64))] = e.ID
	}

	// "Crash": rewind the checkpoint to before the 40 inserts, restart.
	if err := store.Save(context.Background(), "it-crash", seed); err != nil {
		t.Fatal(err)
	}
	startLoop(t, "it-crash", 5507, include, cluster.ListenAddrs(), store)

	// The topic now receives the replay: same rows again. Count raw
	// (non-deduplicated) copies of row 1 to prove the replay happened, and
	// verify replayed IDs match the originals exactly.
	deadline := time.Now().Add(60 * time.Second)
	for {
		replayed := rawRecords(t, cluster.ListenAddrs(), topic)
		byID := map[string]int{}
		rowIDs := map[int]string{}
		for _, e := range replayed {
			byID[e.ID]++
			rowIDs[int(e.After["id"].(float64))] = e.ID
		}
		if byID[firstIDs[1]] >= 2 && len(rowIDs) == 40 {
			for row, id := range rowIDs {
				if firstIDs[row] != id {
					t.Fatalf("row %d replayed with different ID: %q vs %q — downstream dedupe would break", row, id, firstIDs[row])
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replay never observed: dup count %d, distinct rows %d", byID[firstIDs[1]], len(rowIDs))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestMySQLFilePositionResume exercises the non-GTID mode end to end:
// capture with file/offset coordinates, resume across a restart, and
// file-based fallback event IDs.
func TestMySQLFilePositionResume(t *testing.T) {
	conn := mysqlConn(t)
	mustExec(t, conn, "DROP TABLE IF EXISTS fp_items")
	mustExec(t, conn, "CREATE TABLE fp_items (id INT NOT NULL PRIMARY KEY)")

	topic := "cdc." + testDB + ".fp_items"
	cluster := kafkaCluster(t, topic)
	include := []string{testDB + ".fp_items"}
	store := fileStore(t)

	// Phase 1: capture 20 inserts in file-position mode, stop gracefully.
	stop1 := startLoopMode(t, "it-filepos", 5509, include, cluster.ListenAddrs(), store, false)
	for i := 1; i <= 20; i++ {
		mustExec(t, conn, "INSERT INTO fp_items VALUES (?)", i)
	}
	events := consumeUntil(t, cluster.ListenAddrs(), topic, 60*time.Second, atLeast(20))
	stop1()

	// Note on IDs: file-position mode changes how the stream RESUMES, not
	// how events are identified. The test server runs gtid_mode=ON, so
	// GTID events still appear in the stream and IDs keep the stable
	// gtid#order form; the file:pos:row fallback applies only on servers
	// without GTIDs in the binlog. Assert uniqueness, not form.
	seenIDs := map[string]bool{}
	for _, e := range events {
		if e.ID == "" || seenIDs[e.ID] {
			t.Fatalf("event ID empty or duplicated: %q", e.ID)
		}
		seenIDs[e.ID] = true
	}
	ckpt, err := store.Load(context.Background(), "it-filepos")
	if err != nil {
		t.Fatal(err)
	}
	if ckpt.File == "" || ckpt.GTIDSet != "" {
		t.Fatalf("file-mode checkpoint should carry file coordinates only: %+v", ckpt)
	}

	// Phase 2: 15 rows while down, restart from the file checkpoint.
	for i := 21; i <= 35; i++ {
		mustExec(t, conn, "INSERT INTO fp_items VALUES (?)", i)
	}
	startLoopMode(t, "it-filepos", 5510, include, cluster.ListenAddrs(), store, false)
	events = consumeUntil(t, cluster.ListenAddrs(), topic, 60*time.Second, func(seen map[string]envelope) bool {
		ids := map[int]bool{}
		for _, e := range seen {
			ids[int(e.After["id"].(float64))] = true
		}
		return len(ids) >= 35
	})
	ids := map[int]bool{}
	for _, e := range events {
		ids[int(e.After["id"].(float64))] = true
	}
	for i := 1; i <= 35; i++ {
		if !ids[i] {
			t.Fatalf("gap after file-position restart: id %d never captured", i)
		}
	}
}

// TestMySQLTxOrderStableAcrossFilters verifies that tx_order (and thus the
// GTID-based event ID) denotes the row's position within the whole
// transaction, independent of this hyperloop's table filter: a pipeline
// capturing only table B must count table A's rows in the same
// transaction.
func TestMySQLTxOrderStableAcrossFilters(t *testing.T) {
	conn := mysqlConn(t)
	mustExec(t, conn, "DROP TABLE IF EXISTS txo_a")
	mustExec(t, conn, "DROP TABLE IF EXISTS txo_b")
	mustExec(t, conn, "CREATE TABLE txo_a (id INT NOT NULL PRIMARY KEY)")
	mustExec(t, conn, "CREATE TABLE txo_b (id INT NOT NULL PRIMARY KEY)")

	topic := "cdc." + testDB + ".txo_b"
	cluster := kafkaCluster(t, topic)
	// Capture ONLY txo_b.
	startLoop(t, "it-txo", 5508, []string{testDB + ".txo_b"}, cluster.ListenAddrs(), fileStore(t))

	// One transaction: two rows into A (filtered out), then one into B.
	mustExec(t, conn, "BEGIN")
	mustExec(t, conn, "INSERT INTO txo_a VALUES (1), (2)")
	mustExec(t, conn, "INSERT INTO txo_b VALUES (10)")
	mustExec(t, conn, "COMMIT")

	events := consumeUntil(t, cluster.ListenAddrs(), topic, 60*time.Second, atLeast(1))
	e := events[0]
	if e.Source.GTID == "" {
		t.Fatal("expected GTID on event")
	}
	// B's row is the 3rd row of the transaction; a filter-dependent counter
	// would have said 1 and produced an ID that collides with A's first row
	// in a differently-filtered hyperloop.
	if want := e.Source.GTID + "#3"; e.ID != want {
		t.Fatalf("event ID = %q, want %q (tx_order must count filtered-out rows)", e.ID, want)
	}
}

// TestMySQLSchemaChange alters the table mid-stream (add a column) and
// verifies subsequent events carry the new schema.
func TestMySQLSchemaChange(t *testing.T) {
	conn := mysqlConn(t)
	mustExec(t, conn, "DROP TABLE IF EXISTS ddl_items")
	mustExec(t, conn, "CREATE TABLE ddl_items (id INT NOT NULL PRIMARY KEY, name VARCHAR(32) NOT NULL)")

	topic := "cdc." + testDB + ".ddl_items"
	cluster := kafkaCluster(t, topic)
	startLoop(t, "it-ddl", 5504, []string{testDB + ".ddl_items"}, cluster.ListenAddrs(), fileStore(t))

	for i := 1; i <= 5; i++ {
		mustExec(t, conn, "INSERT INTO ddl_items (id, name) VALUES (?, 'old')", i)
	}
	mustExec(t, conn, "ALTER TABLE ddl_items ADD COLUMN extra VARCHAR(32) NOT NULL DEFAULT 'none'")
	for i := 6; i <= 10; i++ {
		mustExec(t, conn, "INSERT INTO ddl_items (id, name, extra) VALUES (?, 'new', 'filled')", i)
	}

	events := consumeUntil(t, cluster.ListenAddrs(), topic, 60*time.Second, atLeast(10))
	for _, e := range events {
		id := int(e.After["id"].(float64))
		extra, hasExtra := e.After["extra"]
		if id <= 5 {
			if hasExtra {
				t.Fatalf("pre-DDL event %d unexpectedly has extra=%v", id, extra)
			}
		} else {
			if !hasExtra || extra != "filled" {
				t.Fatalf("post-DDL event %d: extra=%v hasExtra=%v, want \"filled\"", id, extra, hasExtra)
			}
		}
	}
}
