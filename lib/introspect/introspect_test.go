package introspect

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func sampleDescribe() Describe {
	return Describe{
		Class:        "device",
		Interfaces:   []string{"tool"},
		Capabilities: map[string]bool{},
		Words: map[string]WordSpec{
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
	selected, isDescribe := got.(Describe)
	if !isDescribe {
		t.Fatalf("type answer is %T; want Describe", got)
	}
	if len(selected.Words) != 1 || selected.Words["device.exec"].MaxPendingMs != 120_000 {
		t.Fatalf("type answer = %+v", selected)
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

// Guard the remaining standard query word.
func TestReservedQueryNames(t *testing.T) {
	if QueryDescribe != "actor.describe" {
		t.Fatalf("QueryDescribe drifted: %q", QueryDescribe)
	}
}

func TestManifestReservationAppliesToWordsNotClassNames(t *testing.T) {
	for _, class := range []string{"device", "svcactor"} {
		manifest := Manifest{Class: class, Interfaces: []string{"actor"}, Words: map[string]WordSpec{"device.read": {}}}
		if class == "svcactor" {
			manifest.Interfaces = []string{"actor", "svcactor"}
			manifest.Words = map[string]WordSpec{"svcactor.get": {}}
		}
		if err := ValidateManifest(manifest); err != nil {
			t.Fatalf("class %q rejected: %v", class, err)
		}
	}
	if err := ValidateManifest(Manifest{Class: "device", Interfaces: []string{"actor"}, Words: map[string]WordSpec{"system.channel.list": {}}}); err == nil {
		t.Fatal("reserved word prefix accepted for a non-owner")
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
	for _, k := range []string{"class", "interfaces", "capabilities", "words"} {
		if _, ok := fullKeys[k]; !ok {
			t.Fatalf("Describe wire shape missing %q: %s", k, full)
		}
	}

	dt, _ := AnswerDescribe(sampleDescribe(), DescribeRequest{Type: "device.exec"})
	single, _ := json.Marshal(dt)
	var singleKeys map[string]json.RawMessage
	_ = json.Unmarshal(single, &singleKeys)
	for _, k := range []string{"class", "interfaces", "capabilities", "words"} {
		if _, ok := singleKeys[k]; !ok {
			t.Fatalf("selector wire shape missing %q: %s", k, single)
		}
	}
}

func TestCatalogRoundTrip(t *testing.T) {
	c := Catalog{Actors: []CatalogEntry{
		{ID: "a1", Kind: "agent", Name: "Planner", Description: "Plans work.", Present: true, UptimeMs: 1500},
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

func TestSchemaFieldsAreAdditiveAndOmittedForExistingActors(t *testing.T) {
	legacy := TypeMeta{Description: "legacy", PayloadFields: []FieldDoc{{Name: "value", Description: "value"}}}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "input_schema") || strings.Contains(string(raw), "output_schema") {
		t.Fatalf("empty additive schema fields changed legacy wire shape: %s", raw)
	}
	withSchemas := TypeMeta{
		Description:  "mcp",
		InputSchema:  json.RawMessage(`{"type":"object","$defs":{"X":{"type":"string"}}}`),
		OutputSchema: json.RawMessage(`{"type":"array","items":{"type":"number"}}`),
	}
	raw, err = json.Marshal(withSchemas)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"input_schema"`) || !strings.Contains(string(raw), `"output_schema"`) {
		t.Fatalf("schema fields missing: %s", raw)
	}
}
