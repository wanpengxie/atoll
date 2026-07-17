package link

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type freshAuthority struct{ row storespec.ActorControlRow }

func (f freshAuthority) LookupActive(context.Context, actor.ActorID) (storespec.ActorControlRow, bool, error) {
	return f.row, true, nil
}
func (freshAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (freshAuthority) WorldOf(context.Context, actor.ActorID) (storespec.ActorWorld, bool, error) {
	return storespec.WorldDurable, true, nil
}
func (f freshAuthority) CheckAuthor(context.Context, storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	return storespec.AuthorOK, nil
}

func TestFreshHandshakeRequiresAuthorityAndWireVersionEquality(t *testing.T) {
	id := actor.ActorID("tool:a")
	row := storespec.ActorControlRow{
		ID: id, Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay,
		CurrentDeclVersion: 4,
		Placement:          storespec.Placement{Kind: storespec.PlacementDaemon, Host: "daemon-a"},
	}
	meta := declarationSnapshotEntry{Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay, Version: 4, DaemonID: "daemon-a"}
	a := &Acceptor{authority: freshAuthority{row: row}}
	if got, err := a.resolveFreshHandshake(context.Background(), ipc.HandshakePayload{LeaseID: string(id), Version: 4}, meta); err != nil || got != id {
		t.Fatalf("matching tuple rejected: id=%q err=%v", got, err)
	}
	for name, mutate := range map[string]func(*ipc.HandshakePayload, *declarationSnapshotEntry, *storespec.ActorControlRow){
		"wire version": func(h *ipc.HandshakePayload, _ *declarationSnapshotEntry, _ *storespec.ActorControlRow) {
			h.Version++
		},
		"declaration version": func(_ *ipc.HandshakePayload, m *declarationSnapshotEntry, _ *storespec.ActorControlRow) {
			m.Version++
		},
		"authority version": func(_ *ipc.HandshakePayload, _ *declarationSnapshotEntry, r *storespec.ActorControlRow) {
			r.CurrentDeclVersion++
		},
		"authority host": func(_ *ipc.HandshakePayload, _ *declarationSnapshotEntry, r *storespec.ActorControlRow) {
			r.Placement.Host = "daemon-b"
		},
		"authority binding": func(_ *ipc.HandshakePayload, _ *declarationSnapshotEntry, r *storespec.ActorControlRow) {
			r.Binding = actor.BindingRuntimeOutbound
		},
	} {
		t.Run(name, func(t *testing.T) {
			hp, m, control := ipc.HandshakePayload{LeaseID: string(id), Version: 4}, meta, row
			mutate(&hp, &m, &control)
			acc := &Acceptor{authority: freshAuthority{row: control}}
			if _, err := acc.resolveFreshHandshake(context.Background(), hp, m); err == nil {
				t.Fatal("stale tuple accepted")
			}
		})
	}
}
