package app_test

import (
	"database/sql"
	"testing"

	"github.com/wanpengxie/atoll/app"
)

func openTestAppDB(t *testing.T, path string) (*sql.DB, error) {
	t.Helper()
	p, err := app.OpenProcessDB(path, true)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = p.Close() })
	return p.DB, nil
}
