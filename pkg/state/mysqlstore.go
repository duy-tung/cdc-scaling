package state

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
)

// MySQLStore persists positions in a MySQL table. This is the default
// production store: it needs no extra infrastructure beyond the database
// fleet Nomios already talks to.
type MySQLStore struct {
	mu   sync.Mutex
	conn *client.Conn

	addr, user, password, database string
}

const mysqlStateDDL = `CREATE TABLE IF NOT EXISTS nomios_state (
  hyperloop_id VARCHAR(128) NOT NULL PRIMARY KEY,
  gtid_set     TEXT,
  binlog_file  VARCHAR(255) NOT NULL DEFAULT '',
  binlog_pos   BIGINT UNSIGNED NOT NULL DEFAULT 0,
  seq_no       BIGINT UNSIGNED NOT NULL DEFAULT 0,
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)`

// NewMySQLStore connects to addr ("host:port") and ensures the state table
// exists in the given database.
func NewMySQLStore(addr, user, password, database string) (*MySQLStore, error) {
	s := &MySQLStore{addr: addr, user: user, password: password, database: database}
	if err := s.reconnect(); err != nil {
		return nil, err
	}
	if _, err := s.conn.Execute(mysqlStateDDL); err != nil {
		return nil, fmt.Errorf("state: create table: %w", err)
	}
	return s, nil
}

// ioTimeout bounds every state-store network operation. The Store API
// takes contexts, but go-mysql's client has no context-aware Execute —
// socket deadlines are what actually guarantee a stuck connection cannot
// hang checkpoint commits or shutdown indefinitely.
const ioTimeout = 5 * time.Second

func (s *MySQLStore) reconnect() error {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	conn, err := client.ConnectWithTimeout(s.addr, s.user, s.password, s.database, ioTimeout,
		func(c *client.Conn) error {
			c.ReadTimeout = ioTimeout
			c.WriteTimeout = ioTimeout
			return nil
		})
	if err != nil {
		return fmt.Errorf("state: connect %s: %w", s.addr, err)
	}
	s.conn = conn
	return nil
}

func (s *MySQLStore) Load(_ context.Context, id string) (Position, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.execute(
		"SELECT gtid_set, binlog_file, binlog_pos, seq_no FROM nomios_state WHERE hyperloop_id = ?", id)
	if err != nil {
		return Position{}, fmt.Errorf("state: load: %w", err)
	}
	defer r.Close()
	if r.RowNumber() == 0 {
		return Position{}, nil
	}
	gtid, _ := r.GetString(0, 0)
	file, _ := r.GetString(0, 1)
	pos, _ := r.GetUint(0, 2)
	seq, _ := r.GetUint(0, 3)
	return Position{GTIDSet: gtid, File: file, Offset: uint32(pos), SeqNo: seq}, nil
}

func (s *MySQLStore) Save(_ context.Context, id string, p Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.execute(
		`REPLACE INTO nomios_state (hyperloop_id, gtid_set, binlog_file, binlog_pos, seq_no)
		 VALUES (?, ?, ?, ?, ?)`,
		id, p.GTIDSet, p.File, int64(p.Offset), int64(p.SeqNo))
	if err != nil {
		return fmt.Errorf("state: save: %w", err)
	}
	return nil
}

// execute runs a query, transparently reconnecting once on a broken
// connection (state commits are long-lived and MySQL may time idle
// connections out between commits).
func (s *MySQLStore) execute(q string, args ...any) (*mysqlResult, error) {
	r, err := s.conn.Execute(q, args...)
	if err != nil {
		if rerr := s.reconnect(); rerr != nil {
			return nil, err
		}
		r, err = s.conn.Execute(q, args...)
		if err != nil {
			return nil, err
		}
	}
	return &mysqlResult{r}, nil
}

func (s *MySQLStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
