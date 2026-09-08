package sysactor

import (
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

func TestChannelMemberPresenceRequiresLiveBodyTestimony(t *testing.T) {
	s := New(Deps{Clock: func() time.Time { return time.UnixMilli(200) }})
	base := presence.Snapshot{L1Present: true, L1StartedAt: time.UnixMilli(100)}
	if present, _ := s.liveness(actor.KindChannel, base); present {
		t.Fatal("channel seat without body testimony reported present")
	}
	base.L3 = map[actorrt.ObsKind]presence.Testimony{
		actorrt.ObsKind(introspect.ObsDevicePresence): {Val: introspect.MarshalDevicePresence(true), ReceivedAt: 150},
	}
	if present, uptime := s.liveness(actor.KindChannel, base); !present || uptime != 100 {
		t.Fatalf("live body present=%v uptime=%d", present, uptime)
	}
	row := base.L3[actorrt.ObsKind(introspect.ObsDevicePresence)]
	row.StaleFromPriorLife = true
	base.L3[actorrt.ObsKind(introspect.ObsDevicePresence)] = row
	if present, _ := s.liveness(actor.KindChannel, base); present {
		t.Fatal("stale body testimony reported present")
	}
	if present, _ := s.liveness(actor.KindAgent, presence.Snapshot{L1Present: true}); !present {
		t.Fatal("ordinary actor liveness was made dependent on body testimony")
	}
}
