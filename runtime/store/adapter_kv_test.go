package store_test

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/runtime/store"
)

func TestAdapterStateStoreRoundTripListAndDelete(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()
	s := store.NewAdapterStateStore(db, func() int64 { return 123 })

	if err := s.Put(ctx, "adapter:x:a", []byte("one")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := s.Put(ctx, "adapter:x:b", []byte("two")); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := s.Put(ctx, "adapter:y:a", []byte("other")); err != nil {
		t.Fatalf("Put other: %v", err)
	}

	got, ok, err := s.Get(ctx, "adapter:x:a")
	if err != nil || !ok || !bytes.Equal(got, []byte("one")) {
		t.Fatalf("Get = %q ok=%v err=%v", got, ok, err)
	}
	got[0] = 'X'
	again, _, _ := s.Get(ctx, "adapter:x:a")
	if string(again) != "one" {
		t.Fatalf("Get returned mutable backing bytes: %q", again)
	}
	keys, err := s.List(ctx, "adapter:x:")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if want := []string{"adapter:x:a", "adapter:x:b"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("List = %v want %v", keys, want)
	}
	if err := s.Delete(ctx, "adapter:x:a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := s.Get(ctx, "adapter:x:a"); err != nil || ok {
		t.Fatalf("Get deleted ok=%v err=%v", ok, err)
	}
}

func TestAdapterCredentialStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()
	s := newTestAdapterCredentialStore(t, db, func() int64 { return 456 })

	if err := s.Put(ctx, "feishu.app_secret", "secret-v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(ctx, "feishu.app_secret", "secret-v2"); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	got, ok, err := s.Get(ctx, "feishu.app_secret")
	if err != nil || !ok || got != "secret-v2" {
		t.Fatalf("Get = %q ok=%v err=%v", got, ok, err)
	}
	if err := s.Delete(ctx, "feishu.app_secret"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := s.Get(ctx, "feishu.app_secret"); err != nil || ok {
		t.Fatalf("Get deleted ok=%v err=%v", ok, err)
	}
}

func TestAdapterCredentialStoreEncryptsSQLiteValue(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()
	s := newTestAdapterCredentialStore(t, db, func() int64 { return 789 })

	const (
		key    = "adapter:feishu:actor:tool:feishu-adapter:feishu.app_secret"
		secret = "SECRET-storage-regression-zxcvbn0987654321"
	)
	if err := s.Put(ctx, key, secret); err != nil {
		t.Fatalf("Put: %v", err)
	}
	var stored string
	if err := db.QueryRowContext(ctx, `SELECT value FROM adapter_credentials WHERE key = ?`, key).Scan(&stored); err != nil {
		t.Fatalf("select raw value: %v", err)
	}
	if !strings.HasPrefix(stored, "v1:") {
		t.Fatalf("stored envelope version = %q, want v1 prefix", stored)
	}
	if strings.Contains(stored, secret) {
		t.Fatalf("stored credential contains plaintext secret: %q", stored)
	}
	got, ok, err := s.Get(ctx, key)
	if err != nil || !ok || got != secret {
		t.Fatalf("Get = %q ok=%v err=%v", got, ok, err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := s.Get(ctx, key); err != nil || ok {
		t.Fatalf("Get deleted ok=%v err=%v", ok, err)
	}
}

func newTestAdapterCredentialStore(t *testing.T, db *sql.DB, nowFn func() int64) *store.AdapterCredentialStore {
	t.Helper()
	box, err := store.NewAESGCMSecretBox(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatalf("NewAESGCMSecretBox: %v", err)
	}
	s, err := store.NewAdapterCredentialStore(db, nowFn, box)
	if err != nil {
		t.Fatalf("NewAdapterCredentialStore: %v", err)
	}
	return s
}
