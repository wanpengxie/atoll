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

func TestVerifyChannelLocalSchemaRejectsExtraObject(t *testing.T) {
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `CREATE TABLE retired_shadow (x INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if err := verifyChannelLocalSchema(ctx, db); err == nil {
		t.Fatal("strict verifier accepted an extra table")
	}
}

func TestExpectedChannelSchemaCoversCatalogExactly(t *testing.T) {
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	want, err := expectedChannelSchema()
	if err != nil {
		t.Fatal(err)
	}
	got, err := readSchemaObjects(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("catalog object count=%d want=%d", len(got), len(want))
	}
	for name, expected := range want {
		actual, ok := got[name]
		if !ok || actual != expected {
			t.Fatalf("catalog object %q=%+v present=%v want=%+v", name, actual, ok, expected)
		}
	}
}
