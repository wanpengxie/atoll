package access

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/resource"
)

// mkInvocation builds a baseline object-op Invocation with the always-present
// fields populated; tests override what they exercise.
func mkInvocation() Invocation {
	return Invocation{
		Caller:    actor.ActorID("agent:alice"),
		Resource:  resource.ResourceID("file:report"),
		Operation: OpRead,
		Args:      []byte("payload"),
	}
}

// TestInvocationRoundTripMinimal exercises the minimal object-op invocation:
// only the always-present (non-omitempty) fields. It pins the wire contract that
// a bare Invocation survives marshal/unmarshal without the substrate inventing
// or dropping fields, and with Grant absent (nil) when not op=set.
func TestInvocationRoundTripMinimal(t *testing.T) {
	t.Parallel()
	src := mkInvocation()
	got := roundTrip(t, src)
	if !reflect.DeepEqual(got, src) {
		t.Errorf("minimal invocation round-trip mismatch\n got:  %#v\nwant: %#v", got, src)
	}
}

// TestInvocationRoundTripSetShape exercises the LEGAL op=set invocation: the
// operand is the typed Grant and Args is nil (§3.4 — set/delete carry no Args;
// a set with non-nil Args is malformed and rejected at the runtime door, so it
// is deliberately NOT modeled here as a benign round-trip). Asserts struct
// fidelity for Caller/Resource/Operation/Grant after a JSON round trip; Args
// round-trip fidelity is covered by TestArgsTriState on an object op.
func TestInvocationRoundTripSetShape(t *testing.T) {
	t.Parallel()
	src := Invocation{
		Caller:    actor.ActorID("agent:alice"),
		Resource:  resource.ResourceID("file:report"),
		Operation: OpSet,
		Grant: &Grant{
			Grantee: actor.ActorID("agent:bob"),
			Ops:     []Operation{OpRead, OpWrite},
		},
	}
	got := roundTrip(t, src)
	if !reflect.DeepEqual(got, src) {
		t.Errorf("set-shape invocation round-trip mismatch\n got:  %#v\nwant: %#v", got, src)
	}
}

// TestInvocationZeroValue pins the zero-value wire form: a bare Invocation{}
// marshals with args:null (Args nil, no omitempty) and NO grant key (Grant nil,
// omitempty), with the string fields empty — and survives a round trip. This
// guards the empty/zero contract independently of any populated fixture.
func TestInvocationZeroValue(t *testing.T) {
	t.Parallel()
	raw, err := json.Marshal(Invocation{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal map: %v", err)
	}
	if _, ok := m["grant"]; ok {
		t.Errorf("zero-value Invocation must omit grant, got: %s", raw)
	}
	if got := string(m["args"]); got != "null" {
		t.Errorf("zero-value args = %s, want null", got)
	}
	for _, k := range []string{"caller", "resource", "operation"} {
		if got := string(m[k]); got != `""` {
			t.Errorf("zero-value %s = %s, want empty string", k, got)
		}
	}
	if got := roundTrip(t, Invocation{}); !reflect.DeepEqual(got, Invocation{}) {
		t.Errorf("zero-value round-trip mismatch: got %#v", got)
	}
}

// TestArgsTriState pins that Args distinguishes nil (no operand → null on wire)
// from []byte{} (present-but-empty → "" on wire). reflect.DeepEqual treats a nil
// slice and an empty non-nil slice as unequal, so nil-ness must survive the
// round trip. A real byte payload must also survive (base64 round trip).
func TestArgsTriState(t *testing.T) {
	t.Parallel()
	cases := map[string][]byte{
		"nil (no operand)":      nil,
		"empty (present-empty)": {},
		"real bytes":            []byte("\x00\x01binary\xff"),
	}
	for name, args := range cases {
		args := args
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			src := mkInvocation()
			src.Args = args
			got := roundTrip(t, src)
			if !reflect.DeepEqual(got.Args, args) {
				t.Errorf("Args round-trip mismatch: got %#v (nil=%v), want %#v (nil=%v)",
					got.Args, got.Args == nil, args, args == nil)
			}
		})
	}
}

// TestArgsWireForm pins the raw JSON encoding of Args (not left to implicit
// behavior): nil → null, []byte{} → "", real bytes → a base64 JSON string. This
// is Go encoding/json's []byte behavior, deliberately pinned because Args carries
// opaque binary (a secret/file), not JSON.
func TestArgsWireForm(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		args []byte
		want string // substring that must appear in the raw JSON
	}{
		{"nil → null", nil, `"args":null`},
		{"empty → \"\"", []byte{}, `"args":""`},
		{"bytes → base64", []byte("hi"), `"args":"aGk="`}, // base64("hi") == "aGk="
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			src := mkInvocation()
			src.Args = c.args
			raw, err := json.Marshal(src)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if !strings.Contains(string(raw), c.want) {
				t.Errorf("Args wire form: raw %s does not contain %q", raw, c.want)
			}
		})
	}
}

// TestGrantPresenceTriState pins the Grant pointer's absent/present signal: nil
// Grant (the non-set ops) is OMITTED from the wire entirely (omitempty), so no
// "grant" key appears; a non-nil Grant survives the round trip faithfully.
func TestGrantPresenceTriState(t *testing.T) {
	t.Parallel()

	// nil Grant → no grant key on the wire (check the actual key set, not a
	// loose substring — "grant" could appear inside a value otherwise).
	nilGrant := mkInvocation()
	raw, err := json.Marshal(nilGrant)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal map: %v", err)
	}
	if _, ok := m["grant"]; ok {
		t.Errorf("nil Grant should be omitted from wire, got: %s", raw)
	}

	// non-nil Grant → round-trips.
	withGrant := mkInvocation()
	withGrant.Operation = OpSet
	withGrant.Grant = &Grant{Grantee: actor.ActorID("agent:bob"), Ops: []Operation{OpRead}}
	got := roundTrip(t, withGrant)
	if got.Grant == nil {
		t.Fatalf("non-nil Grant dropped on round trip")
	}
	if !reflect.DeepEqual(got.Grant, withGrant.Grant) {
		t.Errorf("Grant round-trip mismatch: got %#v, want %#v", got.Grant, withGrant.Grant)
	}
}

// TestInvocationFieldSet1To1WithContentFields is the normative drift guard. It
// reflects over the Invocation STRUCT TYPE (not a marshal of a fixture) and
// compares every exported field's json-tag name 1:1 with contentFields. Reading
// the type — not a populated value — is what makes this hard: a NEW field added
// with omitempty would be omitted from a zero-value marshal and slip past a
// marshal-of-fixture check, but it cannot hide from reflect.TypeOf. Drift on
// either side (struct grows a field the list lacks, or list names a dropped
// field) trips this test. Unlike envelope's flattened sender.*, grant stays a
// single top-level key.
func TestInvocationFieldSet1To1WithContentFields(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(Invocation{})
	structKeys := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("json")
		if tag == "" || tag == "-" {
			t.Fatalf("Invocation field %s has no json tag (every wire field must be tagged)", f.Name)
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			t.Fatalf("Invocation field %s has an empty json name", f.Name)
		}
		structKeys = append(structKeys, name)
	}

	wantKeys := append([]string{}, contentFields...)
	sort.Strings(structKeys)
	sort.Strings(wantKeys)
	if !reflect.DeepEqual(structKeys, wantKeys) {
		t.Errorf("Invocation struct fields drifted from contentFields:\n  extra in struct: %v\n  missing from struct: %v\n  struct: %v\n  contentFields: %v",
			diff(structKeys, wantKeys), diff(wantKeys, structKeys), structKeys, wantKeys)
	}
}

// TestInvocationWireKeys pins the omitempty WIRING (complementary to the
// reflection drift guard above, which pins the struct↔contentFields 1:1): a
// populated Invocation (non-nil Grant) emits exactly contentFields on the wire,
// while a minimal one omits the omitempty grant key but keeps the other four.
func TestInvocationWireKeys(t *testing.T) {
	t.Parallel()

	wireKeys := func(inv Invocation) map[string]struct{} {
		raw, err := json.Marshal(inv)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unmarshal map: %v", err)
		}
		set := make(map[string]struct{}, len(m))
		for k := range m {
			set[k] = struct{}{}
		}
		return set
	}

	// Populated (Grant present) → exactly contentFields.
	full := Invocation{
		Caller:    actor.ActorID("agent:alice"),
		Resource:  resource.ResourceID("file:report"),
		Operation: OpSet,
		Grant:     &Grant{Grantee: actor.ActorID("agent:bob"), Ops: []Operation{OpRead}},
	}
	got := wireKeys(full)
	if len(got) != len(contentFields) {
		t.Errorf("populated Invocation wire keys = %v, want exactly contentFields %v", got, contentFields)
	}
	for _, k := range contentFields {
		if _, ok := got[k]; !ok {
			t.Errorf("populated Invocation wire form missing key %q (keys=%v)", k, got)
		}
	}

	// Minimal (nil Grant) → grant omitted, the other four present.
	min := wireKeys(mkInvocation())
	if _, ok := min["grant"]; ok {
		t.Errorf("minimal Invocation must omit grant, got keys %v", min)
	}
	for _, k := range []string{"caller", "resource", "operation", "args"} {
		if _, ok := min[k]; !ok {
			t.Errorf("minimal Invocation wire form missing key %q (keys=%v)", k, min)
		}
	}
}

// TestContentFieldsShape guards contentFields' own internal shape: non-empty and
// every entry is a bare top-level key (Invocation has no dotted/nested wire keys;
// grant is opaque, not flattened — unlike envelope's sender.*).
func TestContentFieldsShape(t *testing.T) {
	t.Parallel()
	if len(contentFields) == 0 {
		t.Fatal("contentFields is empty")
	}
	for _, f := range contentFields {
		if strings.Contains(f, ".") {
			t.Errorf("contentFields entry %q has unexpected dotted shape (Invocation has no nested wire keys)", f)
		}
	}
}

// ---------------------------------------------------------------------------
// helpers (shared with grant_test.go)
// ---------------------------------------------------------------------------

func roundTrip[T any](t *testing.T, src T) T {
	t.Helper()
	raw, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got T
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v\nraw: %s", err, raw)
	}
	return got
}

// diff returns elements in a not in b.
func diff(a, b []string) []string {
	seen := make(map[string]struct{}, len(b))
	for _, s := range b {
		seen[s] = struct{}{}
	}
	var out []string
	for _, s := range a {
		if _, ok := seen[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}
