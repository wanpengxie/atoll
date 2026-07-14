package app

import (
	"database/sql"
	"testing"
)

func openTestAppDB(t *testing.T, path string) (*sql.DB, error) {
	t.Helper()
	p, err := OpenProcessDB(path, true)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = p.Close() })
	return p.DB, nil
}
