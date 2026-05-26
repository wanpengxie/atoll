package viewcache

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
)

func TestActorSnapshotProjectsTypesAndReadiness(t *testing.T) {
	svc := newR7Service(t)
	ctx := context.Background()
	chID := channel.ID("ch-actors")

	frames := []viewsync.PushFrame{
		actorSnapshotFrame(chID, 1, "system.actor.registered", map[string]any{
			"actor_id":      "tool:xhs-adapter",
			"actor_kind":    "tool",
			"actor_binding": "runtime_inbound_via_relay",
			"display_name":  "xhs",
		}),
		actorSnapshotFrame(chID, 2, "system.type.installed", map[string]any{
			"type":             "xhs.publish",
			"allowed_kinds":    []string{"request", "response"},
			"handler_actor_id": "tool:xhs-adapter",
			"handler_binding":  "runtime_inbound_via_relay",
			"max_pending_ms":   30_000,
		}),
		actorSnapshotFrame(chID, 3, "actor.readiness.changed", map[string]any{
			"actor_id":   "tool:xhs-adapter",
			"changed_at": 3000,
			"current": map[string]any{
				"ready":                false,
				"reason":               "device_offline",
				"detail":               map[string]any{"device_state": "offline"},
				"last_ready_at":        2000,
				"last_state_change_at": 3000,
			},
		}),
	}
	for _, frame := range frames {
		if _, err := svc.Apply(ctx, frame); err != nil {
			t.Fatalf("Apply seq=%d: %v", frame.Seq, err)
		}
	}
	if _, err := svc.db.ExecContext(ctx, `
		INSERT INTO daemons
		  (id, key_hash, channel_id, owner_id, name, api_key, api_key_prefix, status, created_at)
		VALUES ('daemon-proxy', '', ?, 'user-1', 'Laptop', 'dk_test', 'dk_test...', 'online', 1000)`,
		string(chID),
	); err != nil {
		t.Fatalf("insert daemon: %v", err)
	}
	if _, err := svc.db.ExecContext(ctx, `
		INSERT INTO daemon_active_actors
		  (channel_id, actor_id, daemon_id, registered_at, last_seen_at)
		VALUES (?, 'tool:xhs-adapter', 'daemon-proxy', 1000, 2000)`,
		string(chID),
	); err != nil {
		t.Fatalf("insert active actor: %v", err)
	}

	got, err := svc.ActorSnapshot(ctx, chID)
	if err != nil {
		t.Fatalf("ActorSnapshot: %v", err)
	}
	if got.ChannelID != string(chID) || len(got.Actors) != 1 {
		t.Fatalf("snapshot=%+v", got)
	}
	a := got.Actors[0]
	if a.ActorID != "tool:xhs-adapter" || a.Kind != "tool" || a.Binding != "runtime_inbound_via_relay" {
		t.Fatalf("actor row=%+v", a)
	}
	if a.Ready || a.ReadyReason != "device_offline" || a.LastReadyAt != 2000 || a.LastStateChangeAt != 3000 {
		t.Fatalf("readiness row=%+v", a)
	}
	var detail map[string]string
	if err := json.Unmarshal(a.ReadyDetail, &detail); err != nil {
		t.Fatalf("ready detail JSON: %v", err)
	}
	if detail["device_state"] != "offline" {
		t.Fatalf("ready detail=%+v", detail)
	}
	if len(a.Types) != 1 || a.Types[0].Type != "xhs.publish" || a.Types[0].MaxPendingMs != 30_000 {
		t.Fatalf("types=%+v", a.Types)
	}
	if a.DaemonID != "daemon-proxy" || a.DaemonName != "Laptop" {
		t.Fatalf("daemon projection=%+v", a)
	}
}

func actorSnapshotFrame(chID channel.ID, seq viewsync.Seq, typ string, payload map[string]any) viewsync.PushFrame {
	raw, _ := json.Marshal(payload)
	id := message.ID("actor-snapshot-" + typ + "-" + r7Itoa(int64(seq)))
	return viewsync.PushFrame{
		ChannelID: chID,
		Seq:       seq,
		MessageID: id,
		Envelope: message.Envelope{
			ID:         id,
			TS:         int64(seq) * 1000,
			ChannelID:  chID,
			Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
			Kind:       message.KindEvent,
			Type:       typ,
			Payload:    raw,
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{actor.SystemActorID},
			Seq:        int64(seq),
		},
	}
}
