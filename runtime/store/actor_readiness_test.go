package store_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/runtime/store"
)

func TestActorRegistry_UpdateReadinessTracksTransitions(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	reg := store.NewActorRegistry(db)
	if err := reg.Insert(ctx, actorreg.Record{
		ID:        "tool:xhs-adapter",
		Kind:      actor.KindTool,
		Binding:   actor.BindingRuntimeInboundViaRelay,
		CreatedAt: 1000,
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	tr, err := reg.UpdateReadiness(ctx, "tool:xhs-adapter", actorreg.ReadinessUpdate{
		State:     actorreg.ReadinessReady,
		Reason:    "ok",
		Detail:    json.RawMessage(`{"device_state":"online"}`),
		CheckedAt: 2000,
	})
	if err != nil {
		t.Fatalf("UpdateReadiness ready: %v", err)
	}
	if !tr.Changed || tr.Previous.State != actorreg.ReadinessUnknown || tr.Current.State != actorreg.ReadinessReady {
		t.Fatalf("ready transition=%+v", tr)
	}
	if tr.Current.LastReadyAt != 2000 || tr.Current.LastStateChangeAt != 2000 {
		t.Fatalf("ready timestamps=%+v", tr.Current)
	}

	tr, err = reg.UpdateReadiness(ctx, "tool:xhs-adapter", actorreg.ReadinessUpdate{
		State:     actorreg.ReadinessReady,
		Reason:    "ok",
		Detail:    json.RawMessage(`{"device_state":"still_online"}`),
		CheckedAt: 2500,
	})
	if err != nil {
		t.Fatalf("UpdateReadiness steady ready: %v", err)
	}
	if tr.Changed || tr.Current.LastReadyAt != 2500 || tr.Current.LastStateChangeAt != 2000 {
		t.Fatalf("steady ready transition=%+v", tr)
	}

	tr, err = reg.UpdateReadiness(ctx, "tool:xhs-adapter", actorreg.ReadinessUpdate{
		State:     actorreg.ReadinessNotReady,
		Reason:    "device_offline",
		Detail:    json.RawMessage(`{"device_state":"offline"}`),
		CheckedAt: 3000,
	})
	if err != nil {
		t.Fatalf("UpdateReadiness not ready: %v", err)
	}
	if !tr.Changed || tr.Current.LastReadyAt != 2500 || tr.Current.LastStateChangeAt != 3000 {
		t.Fatalf("not-ready transition=%+v", tr)
	}

	rec, ok, err := reg.Lookup(ctx, "tool:xhs-adapter")
	if err != nil || !ok {
		t.Fatalf("Lookup ok=%v err=%v", ok, err)
	}
	if rec.Readiness.State != actorreg.ReadinessNotReady || rec.Readiness.Reason != "device_offline" {
		t.Fatalf("lookup readiness=%+v", rec.Readiness)
	}
	if string(rec.Readiness.Detail) != `{"device_state":"offline"}` {
		t.Fatalf("lookup detail=%s", string(rec.Readiness.Detail))
	}
}
