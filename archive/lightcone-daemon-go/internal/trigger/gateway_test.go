package trigger

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// stubActorLookup returns a canned list of active actor ids — fixtures
// for `audience=['*']` expansion in §5.2 example cases. The optional
// err field lets tests verify the gateway surfaces backing-store
// failures unchanged.
type stubActorLookup struct {
	active []string
	err    error
}

func (s *stubActorLookup) ListActive(ctx context.Context) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := make([]string, len(s.active))
	copy(out, s.active)
	return out, nil
}

// stubSubscriptionMatcher returns a fixed slice — used for §5.4
// subscription path verification. Nil-value receiver implements the
// "no subscriptions" branch directly without explicit construction.
type stubSubscriptionMatcher struct {
	subs []string
}

func (s *stubSubscriptionMatcher) Match(_ *v4types.Envelope) []string {
	if s == nil {
		return nil
	}
	return s.subs
}

// mustGateway wires a Gateway with stubbed deps; failing here is a
// programmer error (table-driven cases assume New succeeds).
func mustGateway(t *testing.T, actors ActorLookup, subs SubscriptionMatcher) *Gateway {
	t.Helper()
	g, err := NewGateway(actors, subs)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	return g
}

// sortedCopy returns a sorted shallow copy — table assertions compare
// the return set, not the order, except where the test explicitly
// pins encounter order.
func sortedCopy(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// L1 §5.2 decision matrix — the 8 cases from the spec + the 5 acceptance
// vectors from the ticket. Driven as a single table so cross-impact
// regressions are visible in one place.
//
// Each row specifies:
//
//   - the envelope shape (sender, type, kind, visibility, audience)
//   - the channel's active actor set (for `audience=['*']`)
//   - upstream (defaults to sender.id — the dispatch-path semantics of
//     a direct harness write)
//   - the expected trigger result, sorted alphabetically
//
// Comments tie each row back to the spec sentence it enforces.
// ---------------------------------------------------------------------------

func TestGatewayDispatch_DecisionMatrix(t *testing.T) {
	type tableRow struct {
		name     string
		env      v4types.Envelope
		active   []string // ListActive result
		upstream string   // "" → use env.Sender.ID (direct write)
		want     []string
	}

	channelMembers := []string{"agent:a", "agent:b", "agent:c", "human:u1", "tool:xhs-adapter"}

	rows := []tableRow{
		{
			// §5.2 row 1: human broadcasts → every agent (and other
			// channel members) reacts; sender (human) is filtered as
			// dispatch-path upstream.
			name: "human_broadcast_event_public_triggers_all_except_self",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderHuman, ID: "human:u1"},
				Kind:       v4types.KindEvent,
				Type:       "human.text",
				Visibility: v4types.VisibilityPublic,
				Audience:   []string{"*"},
			},
			active: channelMembers,
			want:   []string{"agent:a", "agent:b", "agent:c", "tool:xhs-adapter"},
		},
		{
			// §5.2 row 2: directed request — explicit audience ignores
			// the `*` expansion path entirely.
			name: "human_request_directed_triggers_only_target",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderHuman, ID: "human:u1"},
				Kind:       v4types.KindRequest,
				Type:       "human.text",
				Visibility: v4types.VisibilityPublic,
				Audience:   []string{"agent:a"},
			},
			active: channelMembers,
			want:   []string{"agent:a"},
		},
		{
			// §5.2 row 3: agent_A broadcasts → everyone except A; the
			// self-trigger filter is what removes the sender.
			name: "agent_broadcast_event_public_excludes_self",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "agent:a"},
				Kind:       v4types.KindEvent,
				Type:       "agent.text",
				Visibility: v4types.VisibilityPublic,
				Audience:   []string{"*"},
			},
			active: channelMembers,
			want:   []string{"agent:b", "agent:c", "human:u1", "tool:xhs-adapter"},
		},
		{
			// §5.2 row 4: agent.text + visibility=system is the
			// "intermediate output" drop rule — no one reacts.
			name: "agent_text_system_visibility_drops_all",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "agent:a"},
				Kind:       v4types.KindEvent,
				Type:       "agent.text",
				Visibility: v4types.VisibilitySystem,
				Audience:   []string{"*"},
			},
			active: channelMembers,
			want:   nil,
		},
		{
			// §5.2 row 5: visibility=private → candidates collapse to
			// [sender.id], then self-trigger filter removes them. Result
			// is empty.
			name: "private_visibility_self_trigger_drops_everything",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "agent:a"},
				Kind:       v4types.KindEvent,
				Type:       "agent.text",
				Visibility: v4types.VisibilityPrivate,
				Audience:   []string{"*"},
			},
			active: channelMembers,
			want:   nil,
		},
		{
			// §5.2 row 6: system.heartbeat is hardcoded to drop everyone
			// regardless of visibility / audience.
			name: "system_heartbeat_drops_all",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderSystem, ID: "system"},
				Kind:       v4types.KindEvent,
				Type:       "system.heartbeat",
				Visibility: v4types.VisibilitySystem,
				Audience:   []string{"*"},
			},
			active: channelMembers,
			want:   nil,
		},
		{
			// §5.2 row 7: system.event @ visibility=system with no
			// subscriptions → no trigger. (The subscription-augmented
			// case lives in TestGatewayDispatch_SubscriptionAugmentation.)
			name: "system_event_no_subscriptions_drops_all",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderSystem, ID: "system"},
				Kind:       v4types.KindEvent,
				Type:       "system.event",
				Visibility: v4types.VisibilitySystem,
				Audience:   []string{"*"},
			},
			active: channelMembers,
			want:   nil,
		},
		{
			// §5.2 row 8: tool-emitted response → directed at agent_A.
			name: "tool_response_directed_triggers_target",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderTool, ID: "tool:xhs-adapter"},
				Kind:       v4types.KindResponse,
				Type:       "xhs.publish",
				Visibility: v4types.VisibilityPublic,
				Audience:   []string{"agent:a"},
			},
			active: channelMembers,
			want:   []string{"agent:a"},
		},
		{
			// §5.2 row 9: agent → tool request — `audience=[tool]`
			// expressing the receiver. Sender is filtered as upstream.
			name: "agent_request_to_tool_triggers_tool",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "agent:a"},
				Kind:       v4types.KindRequest,
				Type:       "xhs.publish",
				Visibility: v4types.VisibilityPublic,
				Audience:   []string{"tool:xhs-adapter"},
			},
			active: channelMembers,
			want:   []string{"tool:xhs-adapter"},
		},
		{
			// Directed audience that includes the sender — self-trigger
			// filter still removes the sender even on explicit listing
			// ("audience=[A, B]" with sender=A → only B reacts).
			name: "directed_audience_with_sender_still_filters_self",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "agent:a"},
				Kind:       v4types.KindEvent,
				Type:       "agent.text",
				Visibility: v4types.VisibilityPublic,
				Audience:   []string{"agent:a", "agent:b"},
			},
			active: channelMembers,
			want:   []string{"agent:b"},
		},
		{
			// Broadcast to a channel with zero active actors — empty
			// list propagates through (no error). This matches the
			// "freshly-bootstrapped channel before any agents register"
			// edge case.
			name: "broadcast_with_no_active_actors_yields_empty",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderHuman, ID: "human:u1"},
				Kind:       v4types.KindEvent,
				Type:       "human.text",
				Visibility: v4types.VisibilityPublic,
				Audience:   []string{"*"},
			},
			active: nil,
			want:   nil,
		},
		{
			// Duplicate audience entries collapse — dedupe is encounter
			// ordered, so the first occurrence wins.
			name: "duplicate_audience_ids_dedupe",
			env: v4types.Envelope{
				Sender:     v4types.Sender{Kind: v4types.SenderHuman, ID: "human:u1"},
				Kind:       v4types.KindEvent,
				Type:       "human.text",
				Visibility: v4types.VisibilityPublic,
				Audience:   []string{"agent:a", "agent:a", "agent:b"},
			},
			active: channelMembers,
			want:   []string{"agent:a", "agent:b"},
		},
	}

	for _, row := range rows {
		row := row
		t.Run(row.name, func(t *testing.T) {
			g := mustGateway(t, &stubActorLookup{active: row.active}, nil)

			upstream := row.upstream
			if upstream == "" {
				upstream = row.env.Sender.ID
			}

			got, err := g.Dispatch(context.Background(), &row.env, upstream)
			if err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			if !reflect.DeepEqual(sortedCopy(got), sortedCopy(row.want)) {
				t.Errorf("Dispatch result = %v, want %v", sortedCopy(got), sortedCopy(row.want))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Acceptance vector — agent_A emits audience=['*'] and MUST NOT trigger
// itself. Pinning this as a named test makes the regression signal
// obvious in CI output.
// ---------------------------------------------------------------------------

func TestGatewayDispatch_AgentBroadcast_DoesNotSelfTrigger(t *testing.T) {
	g := mustGateway(t, &stubActorLookup{
		active: []string{"agent:a", "agent:b", "agent:c"},
	}, nil)

	env := v4types.Envelope{
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "agent:a"},
		Kind:       v4types.KindEvent,
		Type:       "agent.text",
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{"*"},
	}

	got, err := g.Dispatch(context.Background(), &env, env.Sender.ID)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	for _, id := range got {
		if id == "agent:a" {
			t.Fatalf("self-trigger filter failed: agent:a appears in result %v", got)
		}
	}
	if want := []string{"agent:b", "agent:c"}; !reflect.DeepEqual(sortedCopy(got), want) {
		t.Errorf("Dispatch result = %v, want %v", sortedCopy(got), want)
	}
}

// ---------------------------------------------------------------------------
// L1 §5.3 dispatch-path semantics — scheduler upstream MUST NOT filter
// the original sender. This is the future-message / long-pending
// fallback path. Same envelope, same channel, different upstream →
// different result.
// ---------------------------------------------------------------------------

func TestGatewayDispatch_DispatchPathSelfTrigger_SchedulerBypassesFilter(t *testing.T) {
	active := []string{"agent:a", "agent:b"}
	g := mustGateway(t, &stubActorLookup{active: active}, nil)

	env := v4types.Envelope{
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "agent:a"},
		Kind:       v4types.KindEvent,
		Type:       "agent.text",
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{"*"},
	}

	t.Run("direct_write_upstream_equals_sender_filters_self", func(t *testing.T) {
		got, err := g.Dispatch(context.Background(), &env, env.Sender.ID)
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if want := []string{"agent:b"}; !reflect.DeepEqual(sortedCopy(got), want) {
			t.Errorf("direct write result = %v, want %v", got, want)
		}
	})

	t.Run("scheduler_upstream_empty_keeps_sender", func(t *testing.T) {
		got, err := g.Dispatch(context.Background(), &env, "")
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		// Sender IS included because upstream="" → self-filter disabled.
		if want := []string{"agent:a", "agent:b"}; !reflect.DeepEqual(sortedCopy(got), want) {
			t.Errorf("scheduler result = %v, want %v", sortedCopy(got), want)
		}
	})

	t.Run("scheduler_upstream_system_keeps_sender", func(t *testing.T) {
		// Alternative upstream sentinel: "system" actor that long-pending
		// fallback uses. Behaviour matches "" because "system" is not the
		// sender.
		got, err := g.Dispatch(context.Background(), &env, "system")
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if want := []string{"agent:a", "agent:b"}; !reflect.DeepEqual(sortedCopy(got), want) {
			t.Errorf("scheduler-system result = %v, want %v", sortedCopy(got), want)
		}
	})
}

// ---------------------------------------------------------------------------
// L1 §5.4 subscription — visibility=system events default to no trigger,
// but a registered subscriber lights up.
// ---------------------------------------------------------------------------

func TestGatewayDispatch_SubscriptionAugmentation(t *testing.T) {
	t.Run("system_event_with_subscriber_triggers_subscriber", func(t *testing.T) {
		g := mustGateway(t,
			&stubActorLookup{active: []string{"agent:a", "agent:monitor"}},
			&stubSubscriptionMatcher{subs: []string{"agent:monitor"}},
		)

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
		if want := []string{"agent:monitor"}; !reflect.DeepEqual(sortedCopy(got), want) {
			t.Errorf("Dispatch result = %v, want %v", got, want)
		}
	})

	t.Run("subscriber_equal_to_upstream_still_filtered", func(t *testing.T) {
		// Subscribers still pass through step 4 filter — including
		// self-trigger. If the subscribing actor happens to be the
		// dispatch-path upstream it MUST NOT receive its own event.
		g := mustGateway(t,
			&stubActorLookup{active: []string{"agent:a"}},
			&stubSubscriptionMatcher{subs: []string{"agent:a"}},
		)
		env := v4types.Envelope{
			Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "agent:a"},
			Kind:       v4types.KindEvent,
			Type:       "system.event",
			Visibility: v4types.VisibilitySystem,
			Audience:   []string{"*"},
		}
		got, err := g.Dispatch(context.Background(), &env, "agent:a")
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected no triggers (subscriber == upstream), got %v", got)
		}
	})

	t.Run("subscriber_on_heartbeat_still_dropped_by_step4_rule", func(t *testing.T) {
		// The system.heartbeat drop-all rule overrides subscriptions —
		// scheduler liveness pings stay off-channel even for monitors.
		g := mustGateway(t,
			&stubActorLookup{active: []string{"agent:a", "agent:monitor"}},
			&stubSubscriptionMatcher{subs: []string{"agent:monitor"}},
		)
		env := v4types.Envelope{
			Sender:     v4types.Sender{Kind: v4types.SenderSystem, ID: "system"},
			Kind:       v4types.KindEvent,
			Type:       "system.heartbeat",
			Visibility: v4types.VisibilitySystem,
			Audience:   []string{"*"},
		}
		got, err := g.Dispatch(context.Background(), &env, "system")
		if err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("system.heartbeat should drop all, got %v", got)
		}
	})
}

// ---------------------------------------------------------------------------
// Error / programmer-error paths.
// ---------------------------------------------------------------------------

func TestNewGateway_NilActorLookup(t *testing.T) {
	if _, err := NewGateway(nil, nil); err == nil {
		t.Errorf("expected error for nil ActorLookup")
	}
}

func TestGatewayDispatch_NilEnvelope(t *testing.T) {
	g := mustGateway(t, &stubActorLookup{}, nil)
	_, err := g.Dispatch(context.Background(), nil, "")
	if !errors.Is(err, ErrNilEnvelope) {
		t.Errorf("expected ErrNilEnvelope, got %v", err)
	}
}

func TestGatewayDispatch_InvalidVisibility(t *testing.T) {
	g := mustGateway(t, &stubActorLookup{}, nil)
	env := v4types.Envelope{
		Sender:     v4types.Sender{Kind: v4types.SenderHuman, ID: "human:u1"},
		Kind:       v4types.KindEvent,
		Type:       "human.text",
		Visibility: v4types.Visibility("bogus"),
		Audience:   []string{"*"},
	}
	_, err := g.Dispatch(context.Background(), &env, "human:u1")
	if !errors.Is(err, ErrInvalidVisibility) {
		t.Errorf("expected ErrInvalidVisibility, got %v", err)
	}
}

func TestGatewayDispatch_ActorLookupError(t *testing.T) {
	bang := errors.New("boom")
	g := mustGateway(t, &stubActorLookup{err: bang}, nil)
	env := v4types.Envelope{
		Sender:     v4types.Sender{Kind: v4types.SenderHuman, ID: "human:u1"},
		Kind:       v4types.KindEvent,
		Type:       "human.text",
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{"*"},
	}
	_, err := g.Dispatch(context.Background(), &env, "human:u1")
	if !errors.Is(err, bang) {
		t.Errorf("expected ActorLookup error to propagate, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Order preservation — explicit audience order is preserved through
// dedupe.
// ---------------------------------------------------------------------------

func TestGatewayDispatch_AudienceOrderPreserved(t *testing.T) {
	g := mustGateway(t, &stubActorLookup{active: []string{"agent:b", "agent:a", "agent:c"}}, nil)
	env := v4types.Envelope{
		Sender:     v4types.Sender{Kind: v4types.SenderHuman, ID: "human:u1"},
		Kind:       v4types.KindEvent,
		Type:       "human.text",
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{"agent:c", "agent:a", "agent:b"},
	}
	got, err := g.Dispatch(context.Background(), &env, "human:u1")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	want := []string{"agent:c", "agent:a", "agent:b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}
