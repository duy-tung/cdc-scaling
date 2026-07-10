package state

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store persists the last committed position of a hyperloop so a restart can
// resume the stream without gaps.
type Store interface {
	// Load returns the saved position for the hyperloop, or a zero Position
	// (and nil error) when no state has been saved yet.
	Load(ctx context.Context, hyperloopID string) (Position, error)
	// Save persists the position for the hyperloop.
	Save(ctx context.Context, hyperloopID string, p Position) error
}

// FileStore persists positions as JSON files in a directory — one file per
// hyperloop. Intended for development and tests.
type FileStore struct {
	dir string
	mu  sync.Mutex
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("state: create dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(id string) string {
	return filepath.Join(s.dir, id+".json")
}

func (s *FileStore) Load(_ context.Context, id string) (Position, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.path(id))
	if os.IsNotExist(err) {
		return Position{}, nil
	}
	if err != nil {
		return Position{}, fmt.Errorf("state: read: %w", err)
	}
	var p Position
	if err := json.Unmarshal(b, &p); err != nil {
		return Position{}, fmt.Errorf("state: decode: %w", err)
	}
	return p, nil
}

func (s *FileStore) Save(_ context.Context, id string, p Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	tmp := s.path(id) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("state: write: %w", err)
	}
	return os.Rename(tmp, s.path(id))
}
