// FIX-T8 phase-1 unit tests for the gateway-local default-kind table.
// These guard the early-validation contract spelled out in L1 §1.1
// (core type → default kind, override flag) without booting the full
// handler stack — the table is consulted before daemonbus / placements
// / actor_registry are involved.

package gateway

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestResolveKind_DefaultsForCoreType(t *testing.T) {
	t.Parallel()
	cases := map[string]message.Kind{
		"human.text":       message.KindEvent,
		"agent.text":       message.KindEvent,
		"system.event":     message.KindEvent,
		"system.heartbeat": message.KindEvent,
		"file.created":     message.KindEvent,
		"file.updated":     message.KindEvent,
	}
	for typeName, want := range cases {
		got, ok := resolveKind(typeName, "")
		if !ok {
			t.Errorf("%s: ok=false", typeName)
			continue
		}
		if got != want {
			t.Errorf("%s: kind=%q want %q", typeName, got, want)
		}
	}
}

func TestResolveKind_CallerOverride_AllowedCoreType(t *testing.T) {
	t.Parallel()
	// human.text + agent.text allow caller-supplied override to request /
	// response per L1 §1.1.
	for _, typ := range []string{"human.text", "agent.text"} {
		for _, k := range []message.Kind{message.KindEvent, message.KindRequest, message.KindResponse} {
			got, ok := resolveKind(typ, k)
			if !ok || got != k {
				t.Errorf("%s/%s: got=%q ok=%v", typ, k, got, ok)
			}
		}
	}
}

func TestResolveKind_CallerOverride_LockedCoreType(t *testing.T) {
	t.Parallel()
	// system.event / system.heartbeat / file.* lock kind to default; an
	// explicit non-default value MUST be rejected.
	locked := []string{"system.event", "system.heartbeat", "file.created", "file.updated"}
	for _, typ := range locked {
		// Default-equal override → accepted.
		if _, ok := resolveKind(typ, message.KindEvent); !ok {
			t.Errorf("%s: default-equal override should be accepted", typ)
		}
		// Non-default override → rejected.
		if _, ok := resolveKind(typ, message.KindRequest); ok {
			t.Errorf("%s: KindRequest override should be rejected", typ)
		}
	}
}

func TestResolveKind_BusinessType_OmittedKind(t *testing.T) {
	t.Parallel()
	// Unknown types are business types; caller MUST supply kind. server
	// returns ("", true) — the empty kind propagates and the daemon
	// harness reports kind_not_allowed.
	got, ok := resolveKind("xhs.publish", "")
	if !ok {
		t.Fatalf("ok=false for business type")
	}
	if got != "" {
		t.Errorf("business type w/o kind: got=%q want empty", got)
	}
}

func TestResolveKind_BusinessType_WithKind(t *testing.T) {
	t.Parallel()
	// gateway does not know business-type allowed_kinds (that's daemon-
	// side registry); it just forwards the caller-supplied kind.
	got, ok := resolveKind("xhs.publish", message.KindRequest)
	if !ok || got != message.KindRequest {
		t.Errorf("business + KindRequest: got=%q ok=%v", got, ok)
	}
}
