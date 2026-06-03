package message

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// TestAudienceStrings pins that Strings() returns a faithful copy of the
// wire string values in order — addressing (A1) is by explicit ActorID
// list. The returned slice must be an independent copy: mutating it must
// not corrupt the source Audience.
func TestAudienceStrings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   Audience
		want []string
	}{
		{"empty", Audience{}, []string{}},
		{"single", Audience{"agent:channel-agent"}, []string{"agent:channel-agent"}},
		{"multi preserves order", Audience{"tool:xhs", "agent:bob", "human:alice"},
			[]string{"tool:xhs", "agent:bob", "human:alice"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := c.in.Strings()
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("Strings() = %v, want %v", got, c.want)
			}
			if len(got) > 0 {
				orig := c.in[0]
				got[0] = "MUTATED"
				if c.in[0] != orig {
					t.Errorf("Strings() leaked aliasing: mutating result changed source Audience")
				}
			}
		})
	}
}

// TestAudienceRoundTrip pins the wire form of Audience: a JSON array of
// bare actor-id strings, preserving order and arity. Self-scheduling is
// expressed by including the sender's own id (not a special marker), and
// broadcast is an explicit enumeration (no "*" wildcard in the set).
func TestAudienceRoundTrip(t *testing.T) {
	t.Parallel()
	src := Audience{"agent:a", "agent:a", "tool:xhs"} // duplicates allowed; arity preserved
	raw, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(raw) != `["agent:a","agent:a","tool:xhs"]` {
		t.Errorf("Audience wire form = %s, want a bare actor-id string array", raw)
	}
	var got Audience
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, src) {
		t.Errorf("Audience round-trip mismatch: got %v, want %v", got, src)
	}
}

// TestIDString pins the ID wire form as a transparent string alias.
func TestIDString(t *testing.T) {
	t.Parallel()
	id := ID("msg-42")
	if id.String() != "msg-42" {
		t.Errorf("ID.String() = %q, want %q", id.String(), "msg-42")
	}
	raw, err := json.Marshal(id)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(raw) != `"msg-42"` {
		t.Errorf("ID wire form = %s, want a bare string", raw)
	}
}

// guard: Audience element type stays actor.ActorID (addressing currency).
var _ = func() bool {
	var a Audience = []actor.ActorID{"x"}
	_ = a
	return true
}()
