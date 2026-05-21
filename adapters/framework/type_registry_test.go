package framework

import (
	"context"
	"reflect"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

func TestInMemoryTypeRegistryUpsertAndLookup(t *testing.T) {
	r := NewInMemoryTypeRegistry()
	ctx := context.Background()
	row := TypeRow{
		Type:           "feishu.chat.send",
		HandlerActorID: "tool:feishu-adapter",
		HandlerBinding: actor.BindingRuntimeOutbound,
		MaxPendingMs:   30_000,
	}
	got, err := r.Upsert(ctx, row)
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if !reflect.DeepEqual(got, row) {
		t.Fatalf("Upsert returned %+v want %+v", got, row)
	}
	loaded, ok, err := r.Lookup(ctx, "feishu.chat.send")
	if err != nil || !ok {
		t.Fatalf("Lookup: ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(loaded, row) {
		t.Fatalf("Lookup returned %+v want %+v", loaded, row)
	}
}

func TestTypeRegistryRejectsBadRows(t *testing.T) {
	r := NewInMemoryTypeRegistry()
	ctx := context.Background()
	cases := []struct {
		name string
		row  TypeRow
	}{
		{"missing-type", TypeRow{HandlerActorID: "a", HandlerBinding: actor.BindingEmbedded, MaxPendingMs: 1}},
		{"missing-actor", TypeRow{Type: "x", HandlerBinding: actor.BindingEmbedded, MaxPendingMs: 1}},
		{"bad-binding", TypeRow{Type: "x", HandlerActorID: "a", HandlerBinding: "wat", MaxPendingMs: 1}},
		{"zero-timeout", TypeRow{Type: "x", HandlerActorID: "a", HandlerBinding: actor.BindingEmbedded}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := r.Upsert(ctx, tc.row); err == nil {
				t.Fatalf("Upsert %s: expected error", tc.name)
			}
		})
	}
}

func TestInMemoryTypeRegistryListSorted(t *testing.T) {
	r := NewInMemoryTypeRegistry()
	ctx := context.Background()
	for _, t := range []string{"b.x", "a.y", "c.z"} {
		_, _ = r.Upsert(ctx, TypeRow{
			Type:           t,
			HandlerActorID: "tool:a",
			HandlerBinding: actor.BindingEmbedded,
			MaxPendingMs:   1,
		})
	}
	got, err := r.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 || got[0].Type != "a.y" || got[1].Type != "b.x" || got[2].Type != "c.z" {
		t.Fatalf("List unsorted: %v", got)
	}
}

func TestRegistryRejectsLegacyBinding(t *testing.T) {
	r := NewInMemoryTypeRegistry()
	ctx := context.Background()
	row := TypeRow{
		Type:           "old.type",
		HandlerActorID: "tool:a",
		HandlerBinding: actor.Binding("daemon_rpc"),
		MaxPendingMs:   1,
	}
	if _, err := r.Upsert(ctx, row); err == nil {
		t.Fatal("Upsert legacy binding: expected error")
	}
}
