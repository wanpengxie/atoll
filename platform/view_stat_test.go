package platform

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// statTestActor is a minimal in-proc cell for driving View.Stat's tri-state
// (pre-spawn / live / post-despawn) — it does nothing on Receive.
type statTestActor struct{}

func (statTestActor) Receive(context.Context, *message.Envelope) error { return nil }

// TestView_Stat_TriState (DoD §7.3): Stat reports the authoritative embodiment
// presence, not the device/L3 advisory axis — false before spawn, true+StartedAt
// once live, false again after despawn (kill -0 semantics, not a soft/advisory read).
func TestView_Stat_TriState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h, err := Open(HomeConfig{ChannelID: channel.ID("test-view-stat"), DBPath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	id := actor.ActorID("agent:stat-probe")
	ctx := context.Background()

	if _, live := h.View().Stat(id); live {
		t.Fatalf("Stat before spawn: live = true, want false")
	}

	if err := h.Spawn(ctx, id, actor.KindAgent, func(actorcaps.Caps) actorrt.Actor {
		return statTestActor{}
	}); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	startedAt, live := h.View().Stat(id)
	if !live {
		t.Fatalf("Stat after spawn: live = false, want true")
	}
	if startedAt.IsZero() {
		t.Errorf("Stat after spawn: StartedAt is zero, want a bind instant")
	}

	if !h.channel.Cells().DespawnID(id) {
		t.Fatalf("DespawnID: no live embodiment found to kill")
	}

	if _, live := h.View().Stat(id); live {
		t.Fatalf("Stat after despawn: live = true, want false")
	}
}
