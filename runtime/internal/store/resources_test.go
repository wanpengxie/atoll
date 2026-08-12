package store_test

import (
	"bytes"
	"testing"

	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

func TestResourceRegistryIsKVOnly(t *testing.T) {
	cs := openTestChannel(t)
	ctx := t.Context()
	reg := cs.Resources
	if err := reg.Create(ctx, "kv:a", resourcespec.KindKV, "agent:a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := reg.Create(ctx, "kv:a", resourcespec.KindKV, "agent:a", []byte("two")); err != resourcespec.ErrAlreadyExists {
		t.Fatalf("duplicate=%v", err)
	}
	meta, ok, err := reg.Resolve(ctx, "kv:a")
	if err != nil || !ok || meta.Kind != resourcespec.KindKV || meta.CreatedBy != "agent:a" {
		t.Fatalf("resolve=%+v,%v,%v", meta, ok, err)
	}
	value, found, err := cs.KVDriver.Read(ctx, "kv:a")
	if err != nil || !found || !bytes.Equal(value, []byte("one")) {
		t.Fatalf("read=%q,%v,%v", value, found, err)
	}
	if err := cs.KVDriver.Write(ctx, "kv:a", []byte("three")); err != nil {
		t.Fatal(err)
	}
	rows, _, err := reg.List(ctx, "kv:", 10, "")
	if err != nil || len(rows) != 1 || rows[0].ID != "kv:a" {
		t.Fatalf("list=%+v,%v", rows, err)
	}
	if err := reg.Delete(ctx, "kv:a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := reg.Resolve(ctx, "kv:a"); err != nil || ok {
		t.Fatalf("after delete ok=%v err=%v", ok, err)
	}
}
