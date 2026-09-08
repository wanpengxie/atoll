package channelmember

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

type stateStub struct {
	kv map[resource.ResourceID][]byte
}

func (s *stateStub) Get(id resource.ResourceID) (accessdoor.Outcome, error) {
	v, ok := s.kv[id]
	return accessdoor.Outcome{Value: v, Found: ok}, nil
}
func (s *stateStub) Put(id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	s.kv[id] = append([]byte(nil), args...)
	return accessdoor.Outcome{}, nil
}
func (s *stateStub) Del(id resource.ResourceID) (accessdoor.Outcome, error) {
	delete(s.kv, id)
	return accessdoor.Outcome{}, nil
}

var _ actorbase.StateHandle = (*stateStub)(nil)

// bodyStub is a body roster: live members keyed by full id, resolution by
// declaration and by kind:seed, counting declaration lookups.
type bodyStub struct {
	live     map[actor.ActorID]string // id → source decl
	declHits int
}

func (b *bodyStub) MemberOfDeclaration(decl string) (actor.ActorID, error) {
	b.declHits++
	var matches []actor.ActorID
	for id, d := range b.live {
		if d == decl {
			matches = append(matches, id)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return "", &actorbase.TargetResolveError{Code: "not_found", Target: decl}
	default:
		return "", &actorbase.TargetResolveError{Code: "actor_ambiguous", Target: decl}
	}
}
func (b *bodyStub) ResolveTarget(target string) (actor.ActorID, error) {
	for id := range b.live {
		if string(persistentID(id)) == target || string(id) == target {
			return id, nil
		}
	}
	return "", &actorbase.TargetResolveError{Code: "not_found", Target: target}
}
func (b *bodyStub) ActorFacts(_ context.Context, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	decl, ok := b.live[id]
	return channelspec.ActorFacts{Active: ok, SourceDeclID: decl}, ok, nil
}

func TestWordTargetIsPinnedInStateWithoutIncarnation(t *testing.T) {
	state := &stateStub{kv: map[resource.ResourceID][]byte{}}
	body := &bodyStub{live: map[actor.ActorID]string{"agent:native:100": "native-agent"}}
	ctx := context.Background()

	got, err := resolveWordTarget(ctx, state, body, "agent.ask", "native-agent")
	if err != nil || got != "agent:native:100" {
		t.Fatalf("first resolve=%s err=%v", got, err)
	}
	if string(state.kv[bindingKey("agent.ask")]) != `{"actor":"agent:native","target":"native-agent"}` {
		t.Fatalf("pin must store the actor id without incarnation: %s", state.kv[bindingKey("agent.ask")])
	}

	// The target restarts under a new incarnation: the pin still answers, no
	// declaration lookup happens.
	body.live = map[actor.ActorID]string{"agent:native:101": "native-agent"}
	got, err = resolveWordTarget(ctx, state, body, "agent.ask", "native-agent")
	if err != nil || got != "agent:native:101" || body.declHits != 1 {
		t.Fatalf("after restart resolve=%s err=%v declHits=%d", got, err, body.declHits)
	}

	// The member is gone and a differently-seeded instance of the same
	// declaration took over: the stale pin is dropped and re-resolved.
	body.live = map[actor.ActorID]string{"agent:native-b:7": "native-agent"}
	got, err = resolveWordTarget(ctx, state, body, "agent.ask", "native-agent")
	if err != nil || got != "agent:native-b:7" || body.declHits != 2 {
		t.Fatalf("after replacement resolve=%s err=%v declHits=%d", got, err, body.declHits)
	}

	// The manager retargets the word to another declaration: the pin made for
	// the old target does not apply.
	body.live["tool:router:1"] = "dispatcher"
	got, err = resolveWordTarget(ctx, state, body, "agent.ask", "dispatcher")
	if err != nil || got != "tool:router:1" {
		t.Fatalf("after retarget resolve=%s err=%v", got, err)
	}
}

func TestWordTargetReportsAbsentAndAmbiguousDeclarations(t *testing.T) {
	state := &stateStub{kv: map[resource.ResourceID][]byte{}}
	body := &bodyStub{live: map[actor.ActorID]string{}}
	_, err := resolveWordTarget(context.Background(), state, body, "agent.ask", "native-agent")
	var target *actorbase.TargetResolveError
	if !errors.As(err, &target) || target.Code != "not_found" {
		t.Fatalf("absent target err=%v", err)
	}
	body.live = map[actor.ActorID]string{"agent:a:1": "native-agent", "agent:b:1": "native-agent"}
	_, err = resolveWordTarget(context.Background(), state, body, "agent.ask", "native-agent")
	if !errors.As(err, &target) || target.Code != "actor_ambiguous" {
		t.Fatalf("ambiguous target err=%v", err)
	}
	if _, pinned := state.kv[bindingKey("agent.ask")]; pinned {
		t.Fatal("a failed resolution must not pin")
	}
}

// Without a ResolveTarget face the pin is unusable and every request falls
// back to the declaration; still correct, just not sticky.
func TestWordTargetWithoutResolverAlwaysConsultsDeclaration(t *testing.T) {
	state := &stateStub{kv: map[resource.ResourceID][]byte{}}
	body := &bodyStub{live: map[actor.ActorID]string{"agent:native:100": "native-agent"}}
	plain := struct{ Members }{body}
	for i := 0; i < 2; i++ {
		if got, err := resolveWordTarget(context.Background(), state, plain, "agent.ask", "native-agent"); err != nil || got != "agent:native:100" {
			t.Fatalf("resolve=%s err=%v", got, err)
		}
	}
	if body.declHits != 2 {
		t.Fatalf("declHits=%d", body.declHits)
	}
}
