package actor_test

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Kind closed set {human, agent, system, tool}.
//
// Contract under test (kind.go): ParseKind is the ENFORCED closed-set gate.
// In-set canonical strings parse to the matching ADT value; everything else —
// empty, casing variants, near-misses, plurals, leading/trailing space — is
// rejected so an out-of-set value can never enter the Kind ADT. The backing
// enumeration is private; the public contract is the predicate.

func TestParseKindAcceptsClosedSet(t *testing.T) {
	cases := []struct {
		raw  string
		want actor.Kind
	}{
		{"human", actor.KindHuman},
		{"agent", actor.KindAgent},
		{"system", actor.KindSystem},
		{"tool", actor.KindTool},
	}
	for _, c := range cases {
		got, ok := actor.ParseKind(c.raw)
		if !ok {
			t.Errorf("ParseKind(%q) ok=false; want in-set hit", c.raw)
			continue
		}
		if got != c.want {
			t.Errorf("ParseKind(%q)=%v want %v", c.raw, got, c.want)
		}
		// round-trip: the parsed value's wire form is the canonical string.
		if got.String() != c.raw {
			t.Errorf("ParseKind(%q).String()=%q want %q", c.raw, got.String(), c.raw)
		}
	}
}

func TestParseKindRejectsOutOfSet(t *testing.T) {
	for _, raw := range []string{
		"",         // empty is not a member
		"Human",    // casing is significant
		"HUMAN",    //
		"agents",   // plural near-miss
		"bot",      // plausible synonym, not a member
		"service",  // plausible synonym, not a member
		"daemon",   // retired-era vocabulary
		"worker",   // retired-era vocabulary
		" human",   // surrounding whitespace not trimmed
		"human ",   //
		"human\n",  //
		"tool:foo", // an ActorID, not a Kind
	} {
		got, ok := actor.ParseKind(raw)
		if ok {
			t.Errorf("ParseKind(%q)=%v ok=true; want rejected", raw, got)
		}
		// rejected parse must yield the zero value, never a partial.
		if got != "" {
			t.Errorf("ParseKind(%q) rejected but returned non-zero %q", raw, got)
		}
	}
}

// Binding closed set {embedded, runtime_outbound, runtime_inbound_via_relay}.
//
// Contract under test (binding.go): same closed-set gate semantics as Kind.
// The canonical strings are the wire/SQL forms; ParseBinding accepts exactly
// those and rejects everything else, including any retired transport-axis
// names.

func TestParseBindingAcceptsClosedSet(t *testing.T) {
	cases := []struct {
		raw  string
		want actor.Binding
	}{
		{"embedded", actor.BindingEmbedded},
		{"runtime_outbound", actor.BindingRuntimeOutbound},
		{"runtime_inbound_via_relay", actor.BindingRuntimeInboundViaRelay},
	}
	for _, c := range cases {
		got, ok := actor.ParseBinding(c.raw)
		if !ok {
			t.Errorf("ParseBinding(%q) ok=false; want in-set hit", c.raw)
			continue
		}
		if got != c.want {
			t.Errorf("ParseBinding(%q)=%v want %v", c.raw, got, c.want)
		}
		if got.String() != c.raw {
			t.Errorf("ParseBinding(%q).String()=%q want %q", c.raw, got.String(), c.raw)
		}
	}
}

func TestParseBindingRejectsOutOfSet(t *testing.T) {
	for _, raw := range []string{
		"",                           // empty is not a member
		"daemon_rpc",                 // retired transport-axis vocabulary
		"in_worker_bus",              //
		"federated_actor",            //
		"worker",                     //
		"workerhost",                 //
		"in_process_x",               // plausible-but-fake
		"Embedded",                   // casing is significant
		"RUNTIME_OUTBOUND",           //
		"runtime_inbound",            // truncated near-miss of via_relay member
		"runtime_inbound_via_relay ", // surrounding whitespace not trimmed
	} {
		got, ok := actor.ParseBinding(raw)
		if ok {
			t.Errorf("ParseBinding(%q)=%v ok=true; want rejected", raw, got)
		}
		if got != "" {
			t.Errorf("ParseBinding(%q) rejected but returned non-zero %q", raw, got)
		}
	}
}

// ActorID identity (identity.go): the well-known channel-local system actor
// has the fixed wire id "system", and ActorID.String round-trips its wire form.
// This pins A1 (addressing) at the identity layer: SystemActorID is the stable
// address every channel seeds at genesis.

func TestSystemActorID(t *testing.T) {
	if actor.SystemActorID.String() != "system" {
		t.Errorf("SystemActorID=%q want %q", actor.SystemActorID, "system")
	}
}

func TestActorIDStringRoundTrip(t *testing.T) {
	for _, raw := range []string{"system", "user:abc", "agent:planner", "tool:search", ""} {
		if got := actor.ActorID(raw).String(); got != raw {
			t.Errorf("ActorID(%q).String()=%q want %q", raw, got, raw)
		}
	}
}

// Reserved-type closed sets (reserved.go): protocol-frozen envelope.type names
// the substrate enforces. Pinning their exact wire strings makes any silent
// rename a test failure. Only the system.* control-plane mirror set lives in
// kernel now — the actor.* introspection vocabulary is a stdlib convention
// (lib/introspect), not a substrate-enforced reserved set.

func TestReservedSystemEventTypeValues(t *testing.T) {
	cases := map[string]string{
		actor.ReservedSystemChannelCreated:    "system.channel.created",
		actor.ReservedSystemActorRegistered:   "system.actor.registered",
		actor.ReservedSystemActorDeregistered: "system.actor.deregistered",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("reserved system event type = %q want %q", got, want)
		}
	}
}
