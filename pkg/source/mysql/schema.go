package mysql

import (
	"fmt"
	"sync"

	"github.com/go-mysql-org/go-mysql/client"
)

// TableSchema holds the column layout of one table, used by the
// EventMapper to map positional binlog row values to named columns.
type TableSchema struct {
	Columns []string // in ordinal position order
	PK      []string // primary key column names, in key order
}

// schemaRegistry lazily loads table schemas from information_schema over a
// regular client connection and caches them. The whole cache is invalidated
// whenever a DDL statement is observed on the binlog stream, so schema
// changes replicate through the same ordered log as the data they affect.
type schemaRegistry struct {
	mu    sync.Mutex
	conn  *client.Conn
	cache map[string]*TableSchema

	addr, user, password string
}

func newSchemaRegistry(addr, user, password string) (*schemaRegistry, error) {
	r := &schemaRegistry{
		cache: make(map[string]*TableSchema),
		addr:  addr, user: user, password: password,
	}
	if err := r.reconnect(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *schemaRegistry) reconnect() error {
	if r.conn != nil {
		_ = r.conn.Close()
	}
	conn, err := client.Connect(r.addr, r.user, r.password, "information_schema")
	if err != nil {
		return fmt.Errorf("schema registry: connect %s: %w", r.addr, err)
	}
	r.conn = conn
	return nil
}

// Get returns the schema for db.table, loading it on first use.
func (r *schemaRegistry) Get(db, table string) (*TableSchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := db + "." + table
	if s, ok := r.cache[key]; ok {
		return s, nil
	}
	s, err := r.load(db, table)
	if err != nil {
		return nil, err
	}
	r.cache[key] = s
	return s, nil
}

// Refresh drops the cached entry for db.table and reloads it.
func (r *schemaRegistry) Refresh(db, table string) (*TableSchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.cache, db+"."+table)
	s, err := r.load(db, table)
	if err != nil {
		return nil, err
	}
	r.cache[db+"."+table] = s
	return s, nil
}

// InvalidateAll clears the cache (called when DDL is seen on the stream).
func (r *schemaRegistry) InvalidateAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache = make(map[string]*TableSchema)
}

func (r *schemaRegistry) load(db, table string) (*TableSchema, error) {
	res, err := r.query(
		`SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, db, table)
	if err != nil {
		return nil, fmt.Errorf("schema registry: load %s.%s: %w", db, table, err)
	}
	defer res.Close()
	s := &TableSchema{}
	for i := 0; i < res.RowNumber(); i++ {
		name, err := res.GetString(i, 0)
		if err != nil {
			return nil, err
		}
		key, _ := res.GetString(i, 1)
		s.Columns = append(s.Columns, name)
		if key == "PRI" {
			s.PK = append(s.PK, name)
		}
	}
	if len(s.Columns) == 0 {
		return nil, fmt.Errorf("schema registry: table %s.%s not found", db, table)
	}
	return s, nil
}

func (r *schemaRegistry) query(q string, args ...any) (*mysqlResult, error) {
	res, err := r.conn.Execute(q, args...)
	if err != nil {
		// One transparent reconnect: the registry connection can be idle for
		// long periods between DDL events.
		if rerr := r.reconnect(); rerr != nil {
			return nil, err
		}
		res, err = r.conn.Execute(q, args...)
		if err != nil {
			return nil, err
		}
	}
	return &mysqlResult{res}, nil
}

func (r *schemaRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
