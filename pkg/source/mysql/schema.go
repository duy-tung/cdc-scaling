package mysql

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
)

// ColumnKind classifies how a column's []byte binlog value must be
// interpreted. The binlog does not distinguish TEXT from BLOB (both are
// MYSQL_TYPE_BLOB), so information_schema's DATA_TYPE is the only way to
// map values type-faithfully.
type ColumnKind uint8

const (
	KindText   ColumnKind = iota // character data: emit as string
	KindBinary                   // raw bytes: emit as base64
	KindJSON                     // JSON document: emit as nested JSON
)

// TableSchema holds the column layout of one table, used by the
// EventMapper to map positional binlog row values to named columns.
type TableSchema struct {
	Columns []string     // in ordinal position order
	Kinds   []ColumnKind // parallel to Columns
	PK      []string     // primary key column names, in key order
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

// registryTimeout bounds schema lookups: a hung information_schema query
// would otherwise stall the source (and transitively the whole pipeline)
// with no context to cancel it.
const registryTimeout = 10 * time.Second

func (r *schemaRegistry) reconnect() error {
	if r.conn != nil {
		_ = r.conn.Close()
	}
	conn, err := client.ConnectWithTimeout(r.addr, r.user, r.password, "information_schema", registryTimeout,
		func(c *client.Conn) error {
			c.ReadTimeout = registryTimeout
			c.WriteTimeout = registryTimeout
			return nil
		})
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
		`SELECT COLUMN_NAME, COLUMN_KEY, DATA_TYPE FROM information_schema.COLUMNS
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
		// Clone: the cached schema outlives Result.Close, and GetString
		// returns zero-copy views into pooled buffers. key/dataType are
		// only inspected inside this loop and need no clone.
		name = strings.Clone(name)
		key, _ := res.GetString(i, 1)
		dataType, _ := res.GetString(i, 2)
		s.Columns = append(s.Columns, name)
		s.Kinds = append(s.Kinds, kindOf(dataType))
		if key == "PRI" {
			s.PK = append(s.PK, name)
		}
	}
	if len(s.Columns) == 0 {
		return nil, fmt.Errorf("schema registry: table %s.%s not found", db, table)
	}
	return s, nil
}

func kindOf(dataType string) ColumnKind {
	switch dataType {
	case "json":
		return KindJSON
	case "blob", "tinyblob", "mediumblob", "longblob",
		"binary", "varbinary", "bit",
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geomcollection", "geometrycollection", "vector":
		return KindBinary
	default:
		return KindText
	}
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
