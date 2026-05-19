package adapter_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
)

// TestAllBindingKinds enforces the L1 §11.7 closed set: three M1.5
// values, no extras (federation candidate stays commented-out per
// m1.5-tickets §T10).
func TestAllBindingKinds(t *testing.T) {
	want := []adapter.BindingKind{
		adapter.BindingInProcess,
		adapter.BindingOutboundHTTP,
		adapter.BindingViaServerTransit,
	}
	if len(adapter.AllBindingKinds) != len(want) {
		t.Fatalf("AllBindingKinds len=%d want=%d (M1.5 closed set)",
			len(adapter.AllBindingKinds), len(want))
	}
	seen := map[adapter.BindingKind]bool{}
	for _, b := range adapter.AllBindingKinds {
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

// TestBindingKindStringRoundTrip — wire form must equal canonical value.
func TestBindingKindStringRoundTrip(t *testing.T) {
	cases := map[adapter.BindingKind]string{
		adapter.BindingInProcess:        "in_process",
		adapter.BindingOutboundHTTP:     "outbound_http",
		adapter.BindingViaServerTransit: "via_server_transit",
	}
	for b, want := range cases {
		if got := b.String(); got != want {
			t.Errorf("%v.String()=%q want %q", b, got, want)
		}
	}
}

// TestNormalizeBindingCanonical — canonical wire-form strings round-trip.
func TestNormalizeBindingCanonical(t *testing.T) {
	for _, b := range adapter.AllBindingKinds {
		got, ok := adapter.NormalizeBinding(string(b))
		if !ok {
			t.Errorf("NormalizeBinding(%q) ok=false; want canonical hit", b)
		}
		if got != b {
			t.Errorf("NormalizeBinding(%q)=%v want %v", b, got, b)
		}
	}
}

// TestNormalizeBindingLegacy — both legacy names map to in_process per
// L1 §11.7 LegacyBindingMap.
func TestNormalizeBindingLegacy(t *testing.T) {
	cases := []string{"daemon_rpc", "in_worker_bus"}
	for _, in := range cases {
		got, ok := adapter.NormalizeBinding(in)
		if !ok {
			t.Errorf("NormalizeBinding(%q) ok=false; want legacy hit", in)
		}
		if got != adapter.BindingInProcess {
			t.Errorf("NormalizeBinding(%q)=%v want BindingInProcess", in, got)
		}
	}
}

// TestNormalizeBindingUnknown — unknown values return ok=false.
func TestNormalizeBindingUnknown(t *testing.T) {
	cases := []string{"", "federated_actor", "in_process_x", "DAEMON_RPC"}
	for _, in := range cases {
		got, ok := adapter.NormalizeBinding(in)
		if ok {
			t.Errorf("NormalizeBinding(%q)=%v ok=true; want ok=false", in, got)
		}
	}
}

// TestMatchesActorBinding — handler_binding must equal actor_binding
// 1:1 per L1 §11.7 + L2 §1.4.2.
func TestMatchesActorBinding(t *testing.T) {
	cases := []struct {
		handler adapter.BindingKind
		actor   actorreg.Binding
		want    bool
	}{
		{adapter.BindingInProcess, actorreg.BindingInProcess, true},
		{adapter.BindingOutboundHTTP, actorreg.BindingOutboundHTTP, true},
		{adapter.BindingViaServerTransit, actorreg.BindingViaServerTransit, true},
		{adapter.BindingInProcess, actorreg.BindingOutboundHTTP, false},
		{adapter.BindingOutboundHTTP, "", false},
		// Cross-domain canonical strings happen to match — verifies the
		// kernel/actor + kernel/adapter Binding values stay in sync.
		{adapter.BindingViaServerTransit, actorreg.Binding("via_server_transit"), true},
	}
	for _, c := range cases {
		if got := c.handler.MatchesActorBinding(c.actor); got != c.want {
			t.Errorf("%v.MatchesActorBinding(%v)=%v want %v",
				c.handler, c.actor, got, c.want)
		}
	}
}
