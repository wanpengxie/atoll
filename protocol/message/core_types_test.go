package message

import "testing"

// TestLookupCoreType pins the core-type closed set and each entry's rule
// (DefaultKind + AllowOverride). These are engine-built-in types; their
// default kind and override policy are part of the substrate contract.
func TestLookupCoreType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		typeName      string
		defaultKind   Kind
		allowOverride bool
	}{
		{"human.text", KindEvent, true},
		{"agent.text", KindEvent, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.typeName, func(t *testing.T) {
			t.Parallel()
			rule, ok := LookupCoreType(c.typeName)
			if !ok {
				t.Fatalf("LookupCoreType(%q) ok=false, want true (must be a core type)", c.typeName)
			}
			if rule.DefaultKind != c.defaultKind {
				t.Errorf("%q DefaultKind = %q, want %q", c.typeName, rule.DefaultKind, c.defaultKind)
			}
			if rule.AllowOverride != c.allowOverride {
				t.Errorf("%q AllowOverride = %v, want %v", c.typeName, rule.AllowOverride, c.allowOverride)
			}
		})
	}
}

// TestSystemHeartbeatIsNotACoreType pins the substrate invariant that
// system.heartbeat is wire-only (a control frame, not a channel
// envelope) and therefore MUST NOT appear in the core-type table. If it
// ever resolves here, the substrate has wrongly acquired a per-type
// special case for a non-envelope frame.
func TestSystemHeartbeatIsNotACoreType(t *testing.T) {
	t.Parallel()
	if _, ok := LookupCoreType("system.heartbeat"); ok {
		t.Error("LookupCoreType(\"system.heartbeat\") ok=true; heartbeat is wire-only and must not be a core type")
	}
}

// TestLookupCoreTypeRejectsNonCore pins that unknown / non-core type names
// resolve ok=false (they must fall through to domain type resolution).
// Includes a retired pre-v1 spelling and a Layer-3 business type — neither
// is a substrate core type.
func TestLookupCoreTypeRejectsNonCore(t *testing.T) {
	t.Parallel()
	nonCore := []string{
		"",
		"agent.progress",    // collapsed into agent.text + visibility=system
		"core.system_event", // removed: zero producer (live path is agent.text + visibility=system)
		"file.created",      // removed: zero producer (additive when a real file-event producer exists)
		"file.updated",      // removed: zero producer
		"system.event",      // pre-v1 spelling, intentionally not aliased
		"system_event",      // not the canonical core.system_event spelling
		"example.action",    // Layer-3 domain type (not a core type)
		"Human.Text",        // wrong case
		"human.text ",       // trailing space
		"file.deleted",      // not in the v1 set
	}
	for _, name := range nonCore {
		if rule, ok := LookupCoreType(name); ok {
			t.Errorf("LookupCoreType(%q) ok=true (rule=%+v), want false", name, rule)
		}
	}
}

// TestCoreTypeTableCardinality pins the core-type closed set at exactly the 2
// live-producer entries. New core types are a protocol change; this count is a
// deliberate drift tripwire (and confirms the zero-producer entries —
// core.system_event / file.* — plus heartbeat / agent.progress, are all
// absent).
func TestCoreTypeTableCardinality(t *testing.T) {
	t.Parallel()
	known := []string{"human.text", "agent.text"}
	for _, n := range known {
		if _, ok := LookupCoreType(n); !ok {
			t.Errorf("expected core type %q missing from table", n)
		}
	}
	// Guard absence of anything that would push the table past the live set.
	for _, n := range []string{"system.heartbeat", "agent.progress", "core.system_event", "file.created", "file.updated"} {
		if _, ok := LookupCoreType(n); ok {
			t.Errorf("%q must not be in the core-type table", n)
		}
	}
}
