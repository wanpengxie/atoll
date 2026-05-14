package registry

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// templates_test.go covers FIX-5 (T105): the XHSCreatorTypes() request
// schemas MUST accept the exact payload shapes RealProvider emits in
// real mode, so harness Step 6 cannot reject them.
//
// Helpers walk the SchemasByKind json.RawMessage on the relevant row,
// compile the per-kind request schema with the same jsonschema/v5
// compiler the harness uses, then assert validate() on a sample payload.

// findRow looks up the XHSCreatorTypes() row whose Type matches name.
func findRow(t *testing.T, name string) TypeRow {
	t.Helper()
	for _, r := range XHSCreatorTypes() {
		if r.Type == name {
			return r
		}
	}
	t.Fatalf("XHSCreatorTypes(): no row with Type=%q", name)
	return TypeRow{}
}

// compileXHSRequestSchema compiles the "request" branch of a TypeRow's
// SchemasByKind under the same jsonschema/v5 compiler config the
// install / harness Step 6 path uses.
func compileXHSRequestSchema(t *testing.T, row TypeRow) *jsonschema.Schema {
	t.Helper()
	var schemas map[string]json.RawMessage
	if err := json.Unmarshal(row.SchemasByKind, &schemas); err != nil {
		t.Fatalf("decode schemas_by_kind for %s: %v", row.Type, err)
	}
	raw, ok := schemas["request"]
	if !ok {
		t.Fatalf("%s: no request schema", row.Type)
	}
	c := jsonschema.NewCompiler()
	url := "type://" + row.Type + "/request"
	if err := c.AddResource(url, strings.NewReader(string(raw))); err != nil {
		t.Fatalf("AddResource %s: %v", row.Type, err)
	}
	s, err := c.Compile(url)
	if err != nil {
		t.Fatalf("compile %s: %v", row.Type, err)
	}
	return s
}

// validatePayload helper: unmarshal raw payload into any and validate.
func validatePayload(t *testing.T, s *jsonschema.Schema, raw string) error {
	t.Helper()
	var probe any
	if err := json.Unmarshal([]byte(raw), &probe); err != nil {
		t.Fatalf("decode probe %q: %v", raw, err)
	}
	return s.Validate(probe)
}

// TestXHSPublishRequestSchema_AcceptsRealProviderPayloads covers
// the four legal RealProvider Publish shapes:
//
//   1. content_path only (T102 FIX-2 baseline, no inline content)
//   2. content_path + tags
//   3. content_path + images=object[] (normalized base64 data refs)
//   4. content only (legacy / mock-compat fallback per RealProvider.go)
//
// All four MUST pass.
func TestXHSPublishRequestSchema_AcceptsRealProviderPayloads(t *testing.T) {
	s := compileXHSRequestSchema(t, findRow(t, "xhs.publish"))

	cases := []struct {
		name    string
		payload string
	}{
		{
			"content_path_only",
			`{"title":"hi","content_path":"/abs/path/note.md"}`,
		},
		{
			"content_path_with_tags",
			`{"title":"hi","content_path":"/abs/note.md","tags":["a","b"]}`,
		},
		{
			"content_path_with_image_objects",
			`{"title":"hi","content_path":"/abs/note.md","images":[{"type":"data","value":"data:image/png;base64,XYZ","fileName":"x.png"}]}`,
		},
		{
			"content_only_legacy_fallback",
			`{"title":"hi","content":"body"}`,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := validatePayload(t, s, tc.payload); err != nil {
				t.Fatalf("payload should pass: %v", err)
			}
		})
	}
}

// TestXHSPublishRequestSchema_RejectsInvalid asserts the schema still
// catches actually-broken payloads:
//   - neither content nor content_path → reject
//   - additional property → reject
//   - title missing → reject
func TestXHSPublishRequestSchema_RejectsInvalid(t *testing.T) {
	s := compileXHSRequestSchema(t, findRow(t, "xhs.publish"))

	cases := []struct {
		name    string
		payload string
	}{
		{"title_missing", `{"content":"x"}`},
		{"no_content_or_content_path", `{"title":"hi"}`},
		{"unknown_field", `{"title":"hi","content":"x","secret":"leak"}`},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := validatePayload(t, s, tc.payload); err == nil {
				t.Fatalf("payload should fail but passed: %s", tc.payload)
			}
		})
	}
}

// TestXHSNoteFetchRequestSchema_AcceptsRealProviderPayloads covers the
// three legal RealProvider GetNote shapes (matching xhs-cli/note.go's
// `--url OR (--note-id && --xsec-token)` rule):
//
//   1. url only
//   2. note_id + xsec_token
//   3. all three (url + note_id + xsec_token)
//
// Plus the legacy mock shape (note_id only) — still legal per
// `--url OR note_id` anyOf.
func TestXHSNoteFetchRequestSchema_AcceptsRealProviderPayloads(t *testing.T) {
	s := compileXHSRequestSchema(t, findRow(t, "xhs.note.fetch"))

	cases := []struct {
		name    string
		payload string
	}{
		{"url_only", `{"url":"https://www.xiaohongshu.com/explore/abc?xsec_token=tk"}`},
		{"note_id_plus_xsec_token", `{"note_id":"n1","xsec_token":"tk"}`},
		{"all_three", `{"note_id":"n1","url":"https://x/n1","xsec_token":"tk"}`},
		{"note_id_only_legacy", `{"note_id":"n1"}`},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := validatePayload(t, s, tc.payload); err != nil {
				t.Fatalf("payload should pass: %v", err)
			}
		})
	}
}

// TestXHSNoteFetchRequestSchema_RejectsInvalid asserts the schema still
// catches actually-broken payloads:
//   - xsec_token alone (no note_id / url) → reject (dead-end per
//     xhs-cli/note.go R4-T1)
//   - additional property → reject
func TestXHSNoteFetchRequestSchema_RejectsInvalid(t *testing.T) {
	s := compileXHSRequestSchema(t, findRow(t, "xhs.note.fetch"))

	cases := []struct {
		name    string
		payload string
	}{
		{"xsec_token_alone", `{"xsec_token":"tk"}`},
		{"empty_payload", `{}`},
		{"unknown_field", `{"note_id":"n1","leak":"bad"}`},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := validatePayload(t, s, tc.payload); err == nil {
				t.Fatalf("payload should fail but passed: %s", tc.payload)
			}
		})
	}
}
