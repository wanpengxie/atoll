package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	errAdapterStateKeyRequired      = errors.New("store: adapter state key required")
	errAdapterCredentialKeyRequired = errors.New("store: adapter credential key required")
)

// AdapterStateStore is the channel-sqlite implementation of the adapter
// framework StateStore contract. It intentionally does not import
// adapters/framework; cmd/daemon wires it by structural typing.
type AdapterStateStore struct {
	db    *sql.DB
	nowFn func() int64
}

// NewAdapterStateStore returns a persistent key/value store backed by the
// channel-local adapter_state table.
func NewAdapterStateStore(db *sql.DB, nowFn func() int64) *AdapterStateStore {
	if nowFn == nil {
		nowFn = func() int64 { return time.Now().UnixMilli() }
	}
	return &AdapterStateStore{db: db, nowFn: nowFn}
}

func (s *AdapterStateStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var value []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM adapter_state WHERE key = ?`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("store: adapter_state get: %w", err)
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out, true, nil
}

func (s *AdapterStateStore) Put(ctx context.Context, key string, value []byte) error {
	if key == "" {
		return errAdapterStateKeyRequired
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO adapter_state (key, value, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET
		   value = excluded.value,
		   updated_at = excluded.updated_at`,
		key, cp, s.nowFn(),
	)
	if err != nil {
		return fmt.Errorf("store: adapter_state put: %w", err)
	}
	return nil
}

func (s *AdapterStateStore) Delete(ctx context.Context, key string) error {
	if key == "" {
		return errAdapterStateKeyRequired
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM adapter_state WHERE key = ?`, key); err != nil {
		return fmt.Errorf("store: adapter_state delete: %w", err)
	}
	return nil
}

func (s *AdapterStateStore) List(ctx context.Context, prefix string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT key FROM adapter_state
		  WHERE substr(key, 1, ?) = ?
		  ORDER BY key`,
		len(prefix), prefix,
	)
	if err != nil {
		return nil, fmt.Errorf("store: adapter_state list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("store: adapter_state scan: %w", err)
		}
		out = append(out, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: adapter_state rows: %w", err)
	}
	return out, nil
}

// AdapterCredentialStore persists adapter credentials in channel sqlite.
// It satisfies adapters/framework.CredentialStore by structural typing.
type AdapterCredentialStore struct {
	db    *sql.DB
	nowFn func() int64
}

// NewAdapterCredentialStore returns a persistent credential key/value store.
func NewAdapterCredentialStore(db *sql.DB, nowFn func() int64) *AdapterCredentialStore {
	if nowFn == nil {
		nowFn = func() int64 { return time.Now().UnixMilli() }
	}
	return &AdapterCredentialStore{db: db, nowFn: nowFn}
}

func (s *AdapterCredentialStore) Get(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM adapter_credentials WHERE key = ?`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("store: adapter_credentials get: %w", err)
	}
	return value, true, nil
}

func (s *AdapterCredentialStore) Put(ctx context.Context, key, value string) error {
	if key == "" {
		return errAdapterCredentialKeyRequired
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO adapter_credentials (key, value, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET
		   value = excluded.value,
		   updated_at = excluded.updated_at`,
		key, value, s.nowFn(),
	)
	if err != nil {
		return fmt.Errorf("store: adapter_credentials put: %w", err)
	}
	return nil
}

func (s *AdapterCredentialStore) Delete(ctx context.Context, key string) error {
	if key == "" {
		return errAdapterCredentialKeyRequired
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM adapter_credentials WHERE key = ?`, key); err != nil {
		return fmt.Errorf("store: adapter_credentials delete: %w", err)
	}
	return nil
}
