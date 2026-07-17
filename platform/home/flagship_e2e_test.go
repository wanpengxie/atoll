package home

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type flagshipRound struct {
	targets []actor.ActorID
	authors []actor.ActorID
}

type flagshipResolver struct {
	mu           sync.Mutex
	rounds       map[int]flagshipRound
	masterBirths int
	roundDone    chan int
}

func (r *flagshipResolver) recordRound(round int, targets, authors []actor.ActorID) {
	r.mu.Lock()
	if _, exists := r.rounds[round]; !exists {
		r.rounds[round] = flagshipRound{
			targets: append([]actor.ActorID(nil), targets...),
			authors: append([]actor.ActorID(nil), authors...),
		}
	}
	r.mu.Unlock()
	select {
	case r.roundDone <- round:
	default:
	}
}

func (r *flagshipResolver) snapshot(round int) (flagshipRound, bool, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	got, ok := r.rounds[round]
	got.targets = append([]actor.ActorID(nil), got.targets...)
	got.authors = append([]actor.ActorID(nil), got.authors...)
	return got, ok, r.masterBirths
}

func (r *flagshipResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	switch class {
	case "flagship.worker":
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error {
				msg, err := sys.Recv()
				if err != nil {
					return err
				}
				if _, err := sys.Reply(msg, map[string]any{"worker": sys.Self()}); err != nil {
					return err
				}
				return sys.End()
			}, nil
		}}}, true
	case "flagship.master":
		return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			r.mu.Lock()
			r.masterBirths++
			r.mu.Unlock()
			return func(sys actorbase.Sys) error {
				for {
					msg, err := sys.Recv()
					if err != nil {
						return err
					}
					if msg.Type != "flagship.start" && msg.Type != "flagship.next" {
						continue
					}
					round, err := flagshipCurrentRound(sys)
					if err != nil {
						return err
					}
					round++
					targets := make([]actor.ActorID, 0, 2)
					authors := make([]actor.ActorID, 0, 2)
					for worker := 0; worker < 2; worker++ {
						child, err := sys.Fork(actorrt.ForkSpec{
							Kind: actor.KindAgent, Class: "flagship.worker",
							NameHint: fmt.Sprintf("round-%d-worker-%d", round, worker),
						})
						if err != nil {
							return err
						}
						targets = append(targets, child)
						pending, err := sys.Call(child, "flagship.work", map[string]any{"round": round, "worker": worker})
						if err != nil {
							return err
						}
						term, err := pending.Wait(sys.Life(), 3*time.Second)
						if err != nil {
							return err
						}
						if term.ID == "" {
							return fmt.Errorf("worker %s produced no terminal", child)
						}
						authors = append(authors, term.Sender.ID)
					}
					if out, err := sys.State().Put(resource.ResourceID("round"), []byte(strconv.Itoa(round))); err != nil || !out.Accepted() {
						return fmt.Errorf("persist round %d: outcome=%+v err=%v", round, out, err)
					}
					r.recordRound(round, targets, authors)
					if round == 1 {
						if _, err := sys.AfterIdentity(800*time.Millisecond, "flagship.next", json.RawMessage(`{"round":2}`)); err != nil {
							return err
						}
					}
					if msg.Kind == message.KindRequest {
						if _, err := sys.Reply(msg, map[string]any{"round": round}); err != nil {
							return err
						}
					}
				}
			}, nil
		}}}, true
	default:
		return platform.ActorFactory{}, false
	}
}

func flagshipCurrentRound(sys actorbase.Sys) (int, error) {
	out, err := sys.State().Get(resource.ResourceID("round"))
	if err != nil {
		return 0, err
	}
	if !out.Accepted() {
		return 0, nil
	}
	round, err := strconv.Atoi(string(out.Value))
	if err != nil {
		return 0, fmt.Errorf("decode flagship round: %w", err)
	}
	return round, nil
}

func waitFlagshipRound(t *testing.T, r *flagshipResolver, round int) flagshipRound {
	t.Helper()
	deadline := time.NewTimer(6 * time.Second)
	defer deadline.Stop()
	for {
		if got, ok, _ := r.snapshot(round); ok {
			return got
		}
		select {
		case <-r.roundDone:
		case <-deadline.C:
			t.Fatalf("flagship round %d did not finish", round)
		}
	}
}

func assertFlagshipAttribution(t *testing.T, round int, got flagshipRound) {
	t.Helper()
	if len(got.targets) != 2 || len(got.authors) != 2 || got.targets[0] == got.targets[1] || got.authors[0] == got.authors[1] {
		t.Fatalf("round %d attribution=%+v", round, got)
	}
	targets := append([]actor.ActorID(nil), got.targets...)
	authors := append([]actor.ActorID(nil), got.authors...)
	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	sort.Slice(authors, func(i, j int) bool { return authors[i] < authors[j] })
	for i := range targets {
		if targets[i] != authors[i] {
			t.Fatalf("round %d target/terminal author mismatch: targets=%v authors=%v", round, targets, authors)
		}
	}
}

func TestFlagshipMasterForkCallStateTimerAndHomeRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	resolver := &flagshipResolver{rounds: map[int]flagshipRound{}, roundDone: make(chan int, 8)}
	h1 := openAcceptanceHome(t, dbPath, "flagship-workflow", resolver, 5*time.Millisecond)
	master, err := h1.Declare(ctx, DeclareRequest{
		SourceDeclID: "decl:flagship-master", Principal: "flagship-master",
		Kind: actor.KindAgent, Class: "flagship.master", Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h1.channel.Cells().CurrentIncarnation(master.Row.ID)
		return live
	})
	now := time.Now().UnixMilli()
	expires := time.Now().Add(time.Minute).UnixMilli()
	res, err := h1.systemPen.Write(ctx, &message.Envelope{
		ID: "flagship-start", Kind: message.KindRequest, Type: "flagship.start",
		Audience: message.Audience{master.Row.ID}, Visibility: message.VisibilitySystem,
		TS: now, TSReceived: now, ExpiresAt: &expires,
	})
	if err != nil || !res.Accepted() {
		t.Fatalf("start write=(%+v,%v)", res, err)
	}
	round1 := waitFlagshipRound(t, resolver, 1)
	assertFlagshipAttribution(t, 1, round1)
	if err := h1.Close(); err != nil {
		t.Fatal(err)
	}

	// No request is injected after restart. The durable identity-bound timer is
	// the sole driver of the second fork+Call round.
	h2 := openAcceptanceHome(t, dbPath, "flagship-workflow", resolver, 5*time.Millisecond)
	t.Cleanup(func() { _ = h2.Close() })
	round2 := waitFlagshipRound(t, resolver, 2)
	assertFlagshipAttribution(t, 2, round2)
	_, _, masterBirths := resolver.snapshot(2)
	if masterBirths < 2 {
		t.Fatalf("master births=%d, want one on each Home session", masterBirths)
	}
	state, err := h2.stateHandles.Resolve(ctx, master.Row.ID)
	if err != nil {
		t.Fatal(err)
	}
	read, err := state.Invoke(ctx, "read", resource.ResourceID("round"), nil, nil)
	if err != nil || string(read.Value) != "2" {
		t.Fatalf("durable master state=(%+v,%v)", read, err)
	}
	waitHomeCondition(t, func() bool {
		page, err := h2.cs.FiredTimers.ListFired(ctx, runtime.FiredTimerCursor{}, 16)
		return err == nil && len(page.Rows) == 0
	})
}
