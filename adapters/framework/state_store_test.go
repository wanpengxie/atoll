package framework

import (
	"context"
	"sort"
	"testing"
)

func TestMemoryStateStoreRoundTrip(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()
	if err := s.Put(ctx, "k1", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, ok, err := s.Get(ctx, "k1")
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get: %s ok=%v err=%v", v, ok, err)
	}
	if err := s.Put(ctx, "k2", []byte("v2")); err != nil {
		t.Fatalf("Put k2: %v", err)
	}
	keys, err := s.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(keys)
	want := []string{"k1", "k2"}
	if len(keys) != 2 || keys[0] != want[0] || keys[1] != want[1] {
		t.Fatalf("List got %v want %v", keys, want)
	}
}

func TestMemoryStateStoreCopiesOnGet(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()
	orig := []byte("hello")
	if err := s.Put(ctx, "k", orig); err != nil {
		t.Fatalf("Put: %v", err)
	}
	orig[0] = 'X' // mutate caller's slice — must not affect stored value
	v, _, _ := s.Get(ctx, "k")
	if string(v) != "hello" {
		t.Fatalf("Get returned mutated value: %s", v)
	}
}

func TestMemoryStateStoreEmptyKey(t *testing.T) {
	s := NewMemoryStateStore()
	if err := s.Put(context.Background(), "", []byte("v")); err == nil {
		t.Fatalf("Put empty: expected error")
	}
	if err := s.Delete(context.Background(), ""); err == nil {
		t.Fatalf("Delete empty: expected error")
	}
}

func TestNamespacedStateStoreScopesKeys(t *testing.T) {
	inner := NewMemoryStateStore()
	ns := NewNamespacedStateStore(inner, "feishu")
	ctx := context.Background()
	if err := ns.Put(ctx, "cred", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	v, ok, _ := inner.Get(ctx, "feishu:cred")
	if !ok || string(v) != "v" {
		t.Fatalf("inner.Get scoped: %s ok=%v", v, ok)
	}

	keys, _ := ns.List(ctx, "")
	if len(keys) != 1 || keys[0] != "cred" {
		t.Fatalf("ns.List returned %v, want [cred]", keys)
	}
}

func TestMemoryStateStoreListPrefix(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()
	_ = s.Put(ctx, "ad:1", []byte("v"))
	_ = s.Put(ctx, "ad:2", []byte("v"))
	_ = s.Put(ctx, "xb:3", []byte("v"))
	keys, err := s.List(ctx, "ad:")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "ad:1" || keys[1] != "ad:2" {
		t.Fatalf("List prefix=ad: got %v", keys)
	}
}
