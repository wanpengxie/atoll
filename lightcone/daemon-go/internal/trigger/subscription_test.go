package trigger

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// ---------------------------------------------------------------------------
// SubscriptionFilter.matches — protocol baseline 3-tuple conjunction.
// ---------------------------------------------------------------------------

func TestSubscriptionFilter_Matches(t *testing.T) {
	base := v4types.Envelope{
		Sender:     v4types.Sender{Kind: v4types.SenderSystem, ID: "system"},
		Kind:       v4types.KindEvent,
		Type:       "system.event",
		Visibility: v4types.VisibilitySystem,
	}

	cases := []struct {
		name   string
		filter SubscriptionFilter
		want   bool
	}{
		{"type only matches", SubscriptionFilter{Type: "system.event"}, true},
		{"type miss", SubscriptionFilter{Type: "human.text"}, false},
		{"type + kind match", SubscriptionFilter{Type: "system.event", Kind: v4types.KindEvent}, true},
		{"type + kind miss", SubscriptionFilter{Type: "system.event", Kind: v4types.KindRequest}, false},
		{"type + visibility match", SubscriptionFilter{Type: "system.event", Visibility: v4types.VisibilitySystem}, true},
		{"type + visibility miss", SubscriptionFilter{Type: "system.event", Visibility: v4types.VisibilityPublic}, false},
		{
			"full triple match",
			SubscriptionFilter{Type: "system.event", Kind: v4types.KindEvent, Visibility: v4types.VisibilitySystem},
			true,
		},
		{
			"full triple miss on kind",
			SubscriptionFilter{Type: "system.event", Kind: v4types.KindResponse, Visibility: v4types.VisibilitySystem},
			false,
		},
		{"empty type filter is invalid", SubscriptionFilter{}, false},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := c.filter.matches(&base)
			if got != c.want {
				t.Errorf("matches() = %v, want %v", got, c.want)
			}
		})
	}

	// nil envelope guard.
	if (SubscriptionFilter{Type: "x"}).matches(nil) {
		t.Errorf("matches(nil) should be false")
	}
}

// ---------------------------------------------------------------------------
// Registry.Register validation — invalid inputs silently ignored.
// ---------------------------------------------------------------------------

func TestRegistry_Register_RejectsInvalid(t *testing.T) {
	r := NewRegistry()

	r.Register("", SubscriptionFilter{Type: "system.event"})           // blank actor
	r.Register("   ", SubscriptionFilter{Type: "system.event"})        // whitespace actor
	r.Register("agent:a", SubscriptionFilter{Type: ""})                // empty type
	r.Register("agent:a", SubscriptionFilter{Type: "", Kind: "event"}) // empty type w/ kind

	if size := r.Size(); size != 0 {
		t.Errorf("Size() = %d after invalid registers, want 0", size)
	}
}

func TestRegistry_Register_TrimsWhitespace(t *testing.T) {
	r := NewRegistry()
	r.Register("  agent:monitor  ", SubscriptionFilter{Type: "system.event"})

	env := v4types.Envelope{Type: "system.event"}
	got := r.Match(&env)
	if want := []string{"agent:monitor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Match() = %v, want %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// Match — order preservation, dedup, and ListActive-independent
// behaviour (no actor liveness checks).
// ---------------------------------------------------------------------------

func TestRegistry_Match_OrderPreservedAndDedup(t *testing.T) {
	r := NewRegistry()
	r.Register("agent:a", SubscriptionFilter{Type: "system.event"})
	r.Register("agent:b", SubscriptionFilter{Type: "system.event"})
	// Same actor + different filter that also matches → dedupe in Match.
	r.Register("agent:a", SubscriptionFilter{Type: "system.event", Kind: v4types.KindEvent})

	env := v4types.Envelope{Type: "system.event", Kind: v4types.KindEvent}
	got := r.Match(&env)
	if want := []string{"agent:a", "agent:b"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Match() = %v, want %v (registration-order with dedup)", got, want)
	}
}

func TestRegistry_Match_FilterMissesYieldEmpty(t *testing.T) {
	r := NewRegistry()
	r.Register("agent:monitor", SubscriptionFilter{
		Type: "system.event", Kind: v4types.KindEvent, Visibility: v4types.VisibilitySystem,
	})

	// Visibility miss.
	env := v4types.Envelope{Type: "system.event", Kind: v4types.KindEvent, Visibility: v4types.VisibilityPublic}
	if got := r.Match(&env); len(got) != 0 {
		t.Errorf("expected no match on visibility miss, got %v", got)
	}

	// Type miss.
	env2 := v4types.Envelope{Type: "human.text", Kind: v4types.KindEvent, Visibility: v4types.VisibilitySystem}
	if got := r.Match(&env2); len(got) != 0 {
		t.Errorf("expected no match on type miss, got %v", got)
	}
}

func TestRegistry_Match_NilEnvelopeReturnsEmpty(t *testing.T) {
	r := NewRegistry()
	r.Register("agent:a", SubscriptionFilter{Type: "system.event"})
	if got := r.Match(nil); len(got) != 0 {
		t.Errorf("Match(nil) = %v, want empty", got)
	}
}

func TestRegistry_Match_EmptyRegistry(t *testing.T) {
	r := NewRegistry()
	env := v4types.Envelope{Type: "system.event"}
	if got := r.Match(&env); len(got) != 0 {
		t.Errorf("empty registry should yield empty match, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// Concurrent access — RWMutex usage check. Production daemon may have
// adapter framework F6 race with gateway dispatch on boot; this test
// ensures Register + Match don't trip the race detector.
// ---------------------------------------------------------------------------

func TestRegistry_Concurrent_NoRace(t *testing.T) {
	r := NewRegistry()
	env := v4types.Envelope{Type: "system.event", Kind: v4types.KindEvent}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Register("agent:writer", SubscriptionFilter{Type: "system.event"})
			_ = r.Match(&env)
			_ = r.Size()
			_ = i
		}()
	}
	wg.Wait()
	// 8 inserts of the same actor → Size() == 8 (entries kept; dedup
	// happens at Match time).
	if got := r.Size(); got != 8 {
		t.Errorf("Size() = %d, want 8", got)
	}
	if got := r.Match(&env); !reflect.DeepEqual(got, []string{"agent:writer"}) {
		t.Errorf("Match() = %v, want single agent:writer entry", got)
	}
}

// ---------------------------------------------------------------------------
// Registry implements SubscriptionMatcher — wire-up regression test.
// If someone refactors the Match signature this will fail to compile.
// ---------------------------------------------------------------------------

func TestRegistry_SatisfiesSubscriptionMatcher(t *testing.T) {
	var _ SubscriptionMatcher = (*Registry)(nil) // compile-time assertion

	r := NewRegistry()
	r.Register("agent:m", SubscriptionFilter{Type: "system.event"})
	g, err := NewGateway(&stubActorLookup{active: []string{"agent:m"}}, r)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	env := v4types.Envelope{
		Sender:     v4types.Sender{Kind: v4types.SenderSystem, ID: "system"},
		Kind:       v4types.KindEvent,
		Type:       "system.event",
		Visibility: v4types.VisibilitySystem,
		Audience:   []string{"*"},
	}
	got, err := g.Dispatch(context.Background(), &env, "system")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if want := []string{"agent:m"}; !reflect.DeepEqual(got, want) {
		t.Errorf("registry-backed Dispatch = %v, want %v", got, want)
	}
}
