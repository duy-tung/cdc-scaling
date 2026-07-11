//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
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
	return set
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
	t.Helper()
	// Pre-seed the state with the current position if none is saved yet.
	if p, err := store.Load(context.Background(), id); err != nil {
		t.Fatal(err)
	} else if p.IsZero() {
		conn := mysqlConn(t)
		seed := state.Position{GTIDSet: executedGTIDSet(t, conn)}
		if err := store.Save(context.Background(), id, seed); err != nil {
			t.Fatal(err)
		}
	}

	src := mysqlsource.New(mysqlsource.Config{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     mysqlUser(),
		Password: mysqlPassword(),
		ServerID: serverID,
		GTID:     true,
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
