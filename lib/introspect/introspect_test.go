package introspect

import (
	"encoding/json"
	"reflect"
	"testing"
)

func sampleDescribe() Describe {
	return Describe{
		ActorID:     "device:laptop",
		Description: "one-liner",
		SkillDoc:    "# doc",
		Types: map[string]TypeMeta{
			"device.exec": {
				Description:  "run bash",
				AllowedKinds: []string{"request"},
				MaxPendingMs: 120_000,
			},
		},
	}
}

func TestAnswerDescribe_Full(t *testing.T) {
	d := sampleDescribe()
	got, ok := AnswerDescribe(d, DescribeRequest{})
	if !ok {
		t.Fatal("full answer: ok=false")
	}
	if !reflect.DeepEqual(got, d) {
		t.Fatalf("full answer = %+v; want the Describe itself", got)
	}
}

func TestAnswerDescribe_TypeSelector(t *testing.T) {
	d := sampleDescribe()
	got, ok := AnswerDescribe(d, DescribeRequest{Type: "device.exec"})
	if !ok {
		t.Fatal("type answer: ok=false")
	}
	dt, isDT := got.(DescribeType)
	if !isDT {
		t.Fatalf("type answer is %T; want DescribeType", got)
	}
	if dt.ActorID != "device:laptop" || dt.Type != "device.exec" || dt.MaxPendingMs != 120_000 {
		t.Fatalf("type answer = %+v", dt)
	}
}

func TestAnswerDescribe_UnknownType(t *testing.T) {
	if _, ok := AnswerDescribe(sampleDescribe(), DescribeRequest{Type: "nope"}); ok {
		t.Fatal("unknown type: ok=true; want false")
	}
}

func TestParseDescribeRequest(t *testing.T) {
	if req, err := ParseDescribeRequest(nil); err != nil || req.Type != "" {
		t.Fatalf("nil payload: req=%+v err=%v", req, err)
	}
	req, err := ParseDescribeRequest([]byte(`{"type":"x.y"}`))
	if err != nil || req.Type != "x.y" {
		t.Fatalf("selector payload: req=%+v err=%v", req, err)
	}
	if _, err := ParseDescribeRequest([]byte(`{`)); err == nil {
		t.Fatal("malformed payload: want error")
	}
}

// Guard the frozen convention constants so an accidental rename trips a test.
func TestReservedQueryNames(t *testing.T) {
	if QueryDescribe != "actor.describe" {
		t.Fatalf("QueryDescribe drifted: %q", QueryDescribe)
	}
	if QueryList != "actor.list" {
		t.Fatalf("QueryList drifted: %q", QueryList)
	}
	if QueryStatus != "actor.status" {
		t.Fatalf("QueryStatus drifted: %q", QueryStatus)
	}
}

func TestParseStatusRequest(t *testing.T) {
	req, err := ParseStatusRequest([]byte(`{"actor_id":"agent:a"}`))
	if err != nil || req.ActorID != "agent:a" {
		t.Fatalf("req=%+v err=%v", req, err)
	}
	for _, raw := range [][]byte{nil, []byte(`{}`), []byte(`{`)} {
		if _, err := ParseStatusRequest(raw); err == nil {
			t.Fatalf("ParseStatusRequest(%q) accepted invalid input", raw)
		}
	}
}

func TestParseDevicePresenceRequiresBooleanOnlineField(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		want *bool
	}{
		{name: "online", raw: `{"online":true}`, want: boolPointer(true)},
		{name: "offline", raw: `{"online":false}`, want: boolPointer(false)},
		{name: "null document", raw: `null`},
		{name: "empty object", raw: `{}`},
		{name: "null field", raw: `{"online":null}`},
		{name: "wrong field type", raw: `{"online":"false"}`},
		{name: "other field", raw: `{"other":1}`},
		{name: "non object", raw: `false`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := ParseDevicePresence([]byte(test.raw))
			if test.want == nil {
				if ok {
					t.Fatalf("ParseDevicePresence(%s)=(%+v,true), want unreadable", test.raw, got)
				}
				return
			}
			if !ok || got.Online != *test.want {
				t.Fatalf("ParseDevicePresence(%s)=(%+v,%v), want online=%v", test.raw, got, ok, *test.want)
			}
		})
	}
}

func boolPointer(value bool) *bool { return &value }

// TestWireFieldNames pins the JSON contract — the exact keys the LLM-facing
// tools (describe_actor / describe_type) and every actor self-answer rely on.
// Changing a key here is a protocol-level convention revision.
func TestWireFieldNames(t *testing.T) {
	full, _ := json.Marshal(sampleDescribe())
	var fullKeys map[string]json.RawMessage
	_ = json.Unmarshal(full, &fullKeys)
	for _, k := range []string{"actor_id", "description", "skill_doc", "types"} {
		if _, ok := fullKeys[k]; !ok {
			t.Fatalf("Describe wire shape missing %q: %s", k, full)
		}
	}

	dt, _ := AnswerDescribe(sampleDescribe(), DescribeRequest{Type: "device.exec"})
	single, _ := json.Marshal(dt)
	var singleKeys map[string]json.RawMessage
	_ = json.Unmarshal(single, &singleKeys)
	for _, k := range []string{"actor_id", "type", "description", "allowed_kinds", "max_pending_ms"} {
		if _, ok := singleKeys[k]; !ok {
			t.Fatalf("DescribeType wire shape missing %q: %s", k, single)
		}
	}
}

func TestCatalogRoundTrip(t *testing.T) {
	c := Catalog{Actors: []CatalogEntry{
		{ID: "a1", Kind: "agent", Present: true, UptimeMs: 1500},
	}}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	var round Catalog
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal catalog: %v", err)
	}
	if !reflect.DeepEqual(round, c) {
		t.Fatalf("catalog round-trip mismatch: got %+v want %+v", round, c)
	}
}
