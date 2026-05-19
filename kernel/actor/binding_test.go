package actor_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

func TestAllBindings(t *testing.T) {
	want := []actor.Binding{
		actor.BindingInProcess,
		actor.BindingOutboundHTTP,
		actor.BindingViaServerTransit,
	}
	if len(actor.AllBindings) != len(want) {
		t.Fatalf("AllBindings len=%d want=%d", len(actor.AllBindings), len(want))
	}
	seen := map[actor.Binding]bool{}
	for _, b := range actor.AllBindings {
		if seen[b] {
			t.Errorf("duplicate binding %s", b)
		}
		seen[b] = true
	}
	for _, w := range want {
		if !seen[w] {
			t.Errorf("missing binding %s", w)
		}
	}
}

func TestBindingStringRoundTrip(t *testing.T) {
	cases := map[actor.Binding]string{
		actor.BindingInProcess:        "in_process",
		actor.BindingOutboundHTTP:     "outbound_http",
		actor.BindingViaServerTransit: "via_server_transit",
	}
	for b, want := range cases {
		if got := b.String(); got != want {
			t.Errorf("%v.String()=%q want %q", b, got, want)
		}
	}
}

func TestParseBindingCanonicalOnly(t *testing.T) {
	for _, b := range actor.AllBindings {
		got, ok := actor.ParseBinding(string(b))
		if !ok {
			t.Errorf("ParseBinding(%q) ok=false; want canonical hit", b)
		}
		if got != b {
			t.Errorf("ParseBinding(%q)=%v want %v", b, got, b)
		}
	}

	for _, in := range []string{"", "daemon_rpc", "in_worker_bus", "federated_actor", "in_process_x", "DAEMON_RPC"} {
		got, ok := actor.ParseBinding(in)
		if ok {
			t.Errorf("ParseBinding(%q)=%v ok=true; want ok=false", in, got)
		}
	}
}
