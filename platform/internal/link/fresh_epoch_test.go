package link

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type freshComposition struct{ row storespec.CompositionRecord }

func (f freshComposition) LookupComposition(context.Context, actor.ActorID) (storespec.CompositionRecord, bool, error) {
	return f.row, true, nil
}
func (freshComposition) LookupCompositionPrincipal(context.Context, string) (storespec.CompositionRecord, bool, error) {
	return storespec.CompositionRecord{}, false, nil
}
func (freshComposition) ListComposition(context.Context) ([]storespec.CompositionRecord, error) {
	return nil, nil
}
func (freshComposition) DefaultComposition(context.Context) (actor.ActorID, bool, error) {
	return "", false, nil
}

type freshRegistry struct{ rec storespec.Record }

func (f freshRegistry) Lookup(context.Context, actor.ActorID) (storespec.Record, bool, error) {
	return f.rec, true, nil
}
func (freshRegistry) Exists(context.Context, actor.ActorID) (bool, error)    { return true, nil }
func (freshRegistry) ListActive(context.Context) ([]storespec.Record, error) { return nil, nil }

func TestFreshHandshakeRequiresCompositionDeclarationAndWireEpochEquality(t *testing.T) {
	id := actor.ActorID("tool:a")
	row := storespec.CompositionRecord{InstanceID: id, Placement: storespec.PlacementDaemon, DesiredHost: "daemon-a", Epoch: 4}
	rec := storespec.Record{ID: id, Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay, Host: "daemon-a"}
	a := &Acceptor{composition: freshComposition{row: row}, registry: freshRegistry{rec: rec}}
	meta := declarationSnapshotEntry{Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay, Epoch: 4, DaemonID: "daemon-a"}
	if got, err := a.resolveFreshHandshake(context.Background(), ipc.HandshakePayload{LeaseID: string(id), Epoch: 4}, meta); err != nil || got != id {
		t.Fatalf("matching triple rejected: id=%q err=%v", got, err)
	}
	for name, mutate := range map[string]func(*ipc.HandshakePayload, *declarationSnapshotEntry, *storespec.CompositionRecord, *storespec.Record){
		"wire epoch": func(h *ipc.HandshakePayload, _ *declarationSnapshotEntry, _ *storespec.CompositionRecord, _ *storespec.Record) {
			h.Epoch++
		},
		"declaration epoch": func(_ *ipc.HandshakePayload, m *declarationSnapshotEntry, _ *storespec.CompositionRecord, _ *storespec.Record) {
			m.Epoch++
		},
		"composition epoch": func(_ *ipc.HandshakePayload, _ *declarationSnapshotEntry, r *storespec.CompositionRecord, _ *storespec.Record) {
			r.Epoch++
		},
		"desired host": func(_ *ipc.HandshakePayload, _ *declarationSnapshotEntry, r *storespec.CompositionRecord, _ *storespec.Record) {
			r.DesiredHost = "daemon-b"
		},
		"registry host": func(_ *ipc.HandshakePayload, _ *declarationSnapshotEntry, _ *storespec.CompositionRecord, r *storespec.Record) {
			r.Host = "daemon-b"
		},
	} {
		t.Run(name, func(t *testing.T) {
			hp, m, cr, rr := ipc.HandshakePayload{LeaseID: string(id), Epoch: 4}, meta, row, rec
			mutate(&hp, &m, &cr, &rr)
			acc := &Acceptor{composition: freshComposition{row: cr}, registry: freshRegistry{rec: rr}}
			if _, err := acc.resolveFreshHandshake(context.Background(), hp, m); err == nil {
				t.Fatal("stale tuple accepted")
			}
		})
	}
}
