package store_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// openChannelDB is the helper used by every TypeRegistry test.
func openChannelDB(t *testing.T) *store.TypeRegistry {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return store.NewTypeRegistry(db, func() int64 { return 1000 })
}

// TestTypeRegistry_UpsertLookupRoundTrip exercises the happy path:
// Upsert → Lookup returns equal row; HarnessView observes the same.
//
// Level A (proto-layer0 §1.4.1 / proto-layer1 §1.3): type_registry
// stores NO payload schema fields, so no SchemasByKind /
// FallbackResponseSchema assertions appear here.
func TestTypeRegistry_UpsertLookupRoundTrip(t *testing.T) {
	ctx := context.Background()
	reg := openChannelDB(t)

	in := adapter.TypeRow{
		Type:               "xhs.publish",
		HandlerActorID:     "tool:xhs",
		HandlerBinding:     actor.BindingEmbedded,
		MaxPendingMs:       60_000,
		AllowedKinds:       []message.Kind{message.KindRequest, message.KindResponse},
		TerminalConvention: adapter.TerminalPayloadStatus,
	}

	persisted, err := reg.Upsert(ctx, in)
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if persisted.Type != in.Type || persisted.HandlerActorID != in.HandlerActorID {
		t.Errorf("persisted=%+v want type=%s handler=%s",
			persisted, in.Type, in.HandlerActorID)
	}
	if persisted.MaxPendingMs != 60_000 {
		t.Errorf("max_pending_ms=%d want 60000", persisted.MaxPendingMs)
	}
	if persisted.TerminalConvention != adapter.TerminalPayloadStatus {
		t.Errorf("terminal_convention=%q", persisted.TerminalConvention)
	}
	if !reflect.DeepEqual(persisted.AllowedKinds, in.AllowedKinds) {
		t.Errorf("allowed_kinds=%v want %v", persisted.AllowedKinds, in.AllowedKinds)
	}

	got, ok, err := reg.Lookup(ctx, "xhs.publish")
	if err != nil || !ok {
		t.Fatalf("Lookup ok=%v err=%v", ok, err)
	}
	if got.Type != "xhs.publish" {
		t.Errorf("lookup type=%q", got.Type)
	}

	view, ok, err := reg.HarnessView().Lookup(ctx, "xhs.publish")
	if err != nil || !ok {
		t.Fatalf("HarnessView ok=%v err=%v", ok, err)
	}
	if view.HandlerActorID != "tool:xhs" {
		t.Errorf("harness view handler_actor_id=%q", view.HandlerActorID)
	}
	if view.TerminalConvention != string(adapter.TerminalPayloadStatus) {
		t.Errorf("harness view terminal=%q", view.TerminalConvention)
	}
	if view.MaxPendingMs != 60_000 {
		t.Errorf("harness view max_pending_ms=%d want 60000", view.MaxPendingMs)
	}
	if !reflect.DeepEqual(view.AllowedKinds, in.AllowedKinds) {
		t.Errorf("harness view allowed_kinds=%v", view.AllowedKinds)
	}
}

// TestTypeRegistry_UpsertReplaces verifies a second Upsert overwrites
// the first row (sqlite ON CONFLICT path).
func TestTypeRegistry_UpsertReplaces(t *testing.T) {
	ctx := context.Background()
	reg := openChannelDB(t)

	if _, err := reg.Upsert(ctx, adapter.TypeRow{
		Type:           "xhs.publish",
		HandlerActorID: "tool:xhs",
		HandlerBinding: actor.BindingEmbedded,
		MaxPendingMs:   60_000,
		AllowedKinds:   []message.Kind{message.KindRequest},
	}); err != nil {
		t.Fatalf("Upsert 1: %v", err)
	}
	// Overwrite with different binding + max_pending_ms.
	if _, err := reg.Upsert(ctx, adapter.TypeRow{
		Type:           "xhs.publish",
		HandlerActorID: "tool:xhs",
		HandlerBinding: actor.BindingRuntimeOutbound,
		MaxPendingMs:   90_000,
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
	}); err != nil {
		t.Fatalf("Upsert 2: %v", err)
	}
	got, ok, _ := reg.Lookup(ctx, "xhs.publish")
	if !ok {
		t.Fatal("Lookup missing after replace")
	}
	if got.HandlerBinding != actor.BindingRuntimeOutbound || got.MaxPendingMs != 90_000 {
		t.Errorf("after replace: binding=%q max_pending=%d", got.HandlerBinding, got.MaxPendingMs)
	}
	if len(got.AllowedKinds) != 2 {
		t.Errorf("after replace allowed_kinds=%v", got.AllowedKinds)
	}
}

// TestTypeRegistry_List sorts rows deterministically.
func TestTypeRegistry_List(t *testing.T) {
	ctx := context.Background()
	reg := openChannelDB(t)

	types := []string{"xhs.search", "xhs.publish", "xhs.cookie.sync"}
	for _, typ := range types {
		if _, err := reg.Upsert(ctx, adapter.TypeRow{
			Type:           typ,
			HandlerActorID: "tool:xhs",
			HandlerBinding: actor.BindingEmbedded,
			MaxPendingMs:   60_000,
			AllowedKinds:   []message.Kind{message.KindRequest},
		}); err != nil {
			t.Fatalf("Upsert %s: %v", typ, err)
		}
	}
	rows, err := reg.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("List len=%d want 3", len(rows))
	}
	want := []string{"xhs.cookie.sync", "xhs.publish", "xhs.search"}
	for i, r := range rows {
		if r.Type != want[i] {
			t.Errorf("rows[%d]=%q want %q", i, r.Type, want[i])
		}
	}
}

// TestTypeRegistry_LookupMissing returns ok=false on absent type.
func TestTypeRegistry_LookupMissing(t *testing.T) {
	ctx := context.Background()
	reg := openChannelDB(t)
	_, ok, err := reg.Lookup(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if ok {
		t.Error("expected ok=false")
	}
	_, ok, err = reg.HarnessView().Lookup(ctx, "nonexistent")
	if err != nil || ok {
		t.Errorf("HarnessView Lookup ok=%v err=%v", ok, err)
	}
}

// TestTypeRegistry_UpsertRejectsInvalid covers the row.Validate gate.
func TestTypeRegistry_UpsertRejectsInvalid(t *testing.T) {
	ctx := context.Background()
	reg := openChannelDB(t)

	cases := []struct {
		name string
		row  adapter.TypeRow
	}{
		{"missing type", adapter.TypeRow{HandlerActorID: "tool:x", HandlerBinding: actor.BindingEmbedded, MaxPendingMs: 100}},
		{"missing handler actor", adapter.TypeRow{Type: "t", HandlerBinding: actor.BindingEmbedded, MaxPendingMs: 100}},
		{"invalid binding", adapter.TypeRow{Type: "t", HandlerActorID: "tool:x", HandlerBinding: "bogus", MaxPendingMs: 100}},
		{"missing max_pending_ms", adapter.TypeRow{Type: "t", HandlerActorID: "tool:x", HandlerBinding: actor.BindingEmbedded}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := reg.Upsert(ctx, tc.row); err == nil {
				t.Errorf("Upsert should reject %s", tc.name)
			}
		})
	}
}

func TestTypeRegistry_UpsertRejectsReservedNamespace(t *testing.T) {
	ctx := context.Background()
	reg := openChannelDB(t)
	for _, typ := range []string{"system.foo", "actor.custom"} {
		if _, err := reg.Upsert(ctx, adapter.TypeRow{
			Type:           typ,
			HandlerActorID: "tool:x",
			HandlerBinding: actor.BindingEmbedded,
			MaxPendingMs:   100,
			AllowedKinds:   []message.Kind{message.KindEvent},
		}); err == nil {
			t.Fatalf("Upsert %s: expected error", typ)
		}
		if _, ok, err := reg.Lookup(ctx, typ); err != nil || ok {
			t.Fatalf("reserved row %s written ok=%v err=%v", typ, ok, err)
		}
	}
}

// TestTypeRegistry_DefaultTerminalConvention ensures empty
// terminal_convention persists as payload_status (sqlite DEFAULT).
func TestTypeRegistry_DefaultTerminalConvention(t *testing.T) {
	ctx := context.Background()
	reg := openChannelDB(t)
	if _, err := reg.Upsert(ctx, adapter.TypeRow{
		Type:           "xhs.publish",
		HandlerActorID: "tool:xhs",
		HandlerBinding: actor.BindingEmbedded,
		MaxPendingMs:   60_000,
		AllowedKinds:   []message.Kind{message.KindRequest},
		// TerminalConvention deliberately left empty.
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	row, _, _ := reg.Lookup(ctx, "xhs.publish")
	if row.TerminalConvention != adapter.TerminalPayloadStatus {
		t.Errorf("empty terminal_convention persisted as %q want payload_status",
			row.TerminalConvention)
	}
}
