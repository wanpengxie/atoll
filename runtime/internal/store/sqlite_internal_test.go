package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSqlite_EmptyPath(t *testing.T) {
	if _, err := openSqlite(context.Background(), "", OpenOptions{}, ""); err == nil {
		t.Error("openSqlite with empty dbPath must error")
	}
}

func TestOpenSqlite_MkdirAllFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	blocker := filepath.Join(dir, "iam-a-file")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if _, err := openSqlite(ctx, filepath.Join(blocker, "sub", "ch.sqlite"), OpenOptions{}, ChannelLocalDDL); err == nil {
		t.Error("openSqlite must error when the parent contains a regular file")
	}
}

func TestOpenSqlite_BadDDLErrors(t *testing.T) {
	if _, err := openSqlite(context.Background(), filepath.Join(t.TempDir(), "ch.sqlite"), OpenOptions{}, "THIS IS NOT VALID SQL;"); err == nil {
		t.Error("openSqlite must error on invalid DDL")
	}
}
