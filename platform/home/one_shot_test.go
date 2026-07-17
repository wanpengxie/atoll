package home

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type oneShotStat struct {
	births        int
	attempts      int
	recoveredDone int
}

type oneShotResolver struct {
	mu    sync.Mutex
	stats map[actor.ActorID]oneShotStat
}

func (r *oneShotResolver) mutate(id actor.ActorID, fn func(*oneShotStat)) oneShotStat {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats[id]
	fn(&s)
	r.stats[id] = s
	return s
}

func (r *oneShotResolver) snapshot(id actor.ActorID) oneShotStat {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats[id]
}

func (r *oneShotResolver) BuildClass(_ channel.ID, id actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	switch class {
	case "one.normal", "one.crash-after-terminal", "one.crash-before-terminal":
	default:
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		r.mutate(id, func(s *oneShotStat) { s.births++ })
		return func(sys actorbase.Sys) error {
			done, err := sys.State().Get(resource.ResourceID("completed"))
			if err != nil {
				return err
			}
			if done.Accepted() && done.Found {
				r.mutate(id, func(s *oneShotStat) { s.recoveredDone++ })
				return sys.End()
			}
			msg, err := sys.Recv()
			if err != nil {
				return err
			}
			stat := r.mutate(id, func(s *oneShotStat) { s.attempts++ })
			if class == "one.crash-before-terminal" && stat.attempts == 1 {
				panic("one-shot crash before terminal")
			}
			if out, err := sys.State().Put(resource.ResourceID("completed"), []byte("yes")); err != nil || !out.Accepted() {
				return fmt.Errorf("state completion: outcome=%+v err=%w", out, err)
			}
			if _, err := sys.Reply(msg, map[string]any{"attempt": stat.attempts}); err != nil {
				return err
			}
			if class == "one.crash-after-terminal" && stat.attempts == 1 {
				panic("one-shot crash after terminal")
			}
			return sys.End()
		}, nil
	}}}, true
}

func TestOneShotCompletionStateTerminalAndEndSurviveBothCrashCuts(t *testing.T) {
	ctx := context.Background()
	resolver := &oneShotResolver{stats: map[actor.ActorID]oneShotStat{}}
	h, err := Open(Config{
		ChannelID: "one-shot-cuts", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: resolver, DaemonAuthority: allowTestDaemonAuthority{},
		ReconcileInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	parent, err := h.Admit(ctx, actor.KindHuman, "one-shot-parent")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		class         string
		wantAttempts  int
		wantRecovered int
	}{
		{"one.normal", 1, 0},
		{"one.crash-after-terminal", 1, 1},
		{"one.crash-before-terminal", 2, 0},
	}
	for i, tc := range cases {
		child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{
			Kind: actor.KindAgent, Class: tc.class, NameHint: fmt.Sprintf("case-%d", i),
		}, fmt.Sprintf("one-shot-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().UnixMilli()
		expires := time.Now().Add(time.Minute).UnixMilli()
		res, err := h.systemPen.Write(ctx, &message.Envelope{
			ID: message.ID(fmt.Sprintf("one-shot-request-%d", i)), Kind: message.KindRequest,
			Type: "one.run", Audience: message.Audience{child}, Visibility: message.VisibilitySystem,
			TS: now, TSReceived: now, ExpiresAt: &expires,
		})
		if err != nil || !res.Accepted() {
			t.Fatalf("write %s=(%+v,%v)", tc.class, res, err)
		}
		waitHomeCondition(t, func() bool {
			_, active, _ := h.controlIndex.LookupActive(ctx, child)
			stat := resolver.snapshot(child)
			open, qerr := h.cs.Query.OpenRequestsForActor(ctx, child)
			return qerr == nil && !active && len(open) == 0 &&
				stat.attempts == tc.wantAttempts && stat.recoveredDone == tc.wantRecovered
		})
		stat := resolver.snapshot(child)
		minimumBirths := 1
		if tc.class != "one.normal" {
			minimumBirths = 2
		}
		if stat.births < minimumBirths {
			t.Fatalf("%s births=%d, want >=%d", tc.class, stat.births, minimumBirths)
		}
	}
}
