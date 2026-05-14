package coagent

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

func TestParseAudience_TrimsAndSkipsEmpty(t *testing.T) {
	got := parseAudience(" bob ,, alice ,")
	want := []string{"bob", "alice"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseAudience = %v, want %v", got, want)
	}
}

func TestParseAudience_EmptyReturnsNil(t *testing.T) {
	if got := parseAudience("   "); got != nil {
		t.Fatalf("expected nil for empty input, got %v", got)
	}
}

func TestParseDocRefs_NilPointerWhenEmpty(t *testing.T) {
	got, err := parseDocRefs("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil pointer, got %v", *got)
	}
}

func TestParseDocRefs_TrimsAndDeduplicatesEmpty(t *testing.T) {
	got, err := parseDocRefs("work/x.md, work/y.md ,, ,")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatalf("expected non-nil pointer")
	}
	want := []string{"work/x.md", "work/y.md"}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("got %v, want %v", *got, want)
	}
}

func TestParsePayload_RejectsNonObject(t *testing.T) {
	if _, err := parsePayload(`["a"]`); err == nil {
		t.Fatalf("expected error on array payload")
	}
	if _, err := parsePayload(`"plain"`); err == nil {
		t.Fatalf("expected error on string payload")
	}
}

func TestParsePayload_AcceptsObject(t *testing.T) {
	raw, err := parsePayload(`{"text":"hi"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(raw) != `{"text":"hi"}` {
		t.Fatalf("payload not preserved: %s", raw)
	}
}

// TestResolvePayloadSource_InlineWins covers the "only --payload" path:
// returns the inline string verbatim, no filesystem touch.
func TestResolvePayloadSource_InlineOnly(t *testing.T) {
	got, err := resolvePayloadSource(`{"a":1}`, "")
	if err != nil || got != `{"a":1}` {
		t.Fatalf("got (%q, %v); want (%q, nil)", got, err, `{"a":1}`)
	}
}

// TestResolvePayloadSource_FileReadsContents writes a JSON file and
// asserts resolvePayloadSource returns its body. Exercises the
// T102 FIX-2 happy path (xhs-cli funnels large payloads through here).
func TestResolvePayloadSource_FileReadsContents(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/payload.json"
	body := `{"large":"` + strings.Repeat("x", 200_000) + `"}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	got, err := resolvePayloadSource("", path)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != body {
		t.Fatalf("body mismatch: len got=%d want=%d", len(got), len(body))
	}
}

// TestResolvePayloadSource_MutuallyExclusive — passing both flags is
// always an error so caller intent is unambiguous.
func TestResolvePayloadSource_MutuallyExclusive(t *testing.T) {
	_, err := resolvePayloadSource(`{"a":1}`, "/tmp/foo.json")
	if err == nil {
		t.Fatal("expected mutual-exclusion error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("err = %q", err)
	}
}

// TestResolvePayloadSource_MissingFile surfaces a useful error when
// the path doesn't exist — caller logs the failure rather than
// receiving an empty payload silently.
func TestResolvePayloadSource_MissingFile(t *testing.T) {
	_, err := resolvePayloadSource("", "/nope/no-such-file.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

// TestResolvePayloadSource_RejectsOversizeFile guards against the
// caller staging a 100 MiB payload accidentally — the daemon-rpc layer
// caps at 1 MiB anyway, so refuse early with a clear message.
func TestResolvePayloadSource_RejectsOversizeFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/oversize.json"
	if err := os.WriteFile(path, make([]byte, 10*1024*1024), 0o600); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	// Stub osStat so the test does not depend on real filesystem
	// allocator behaviour (10 MiB takes < 1s on tmpfs but slow disks).
	origStat := osStat
	osStat = func(name string) (osFileInfo, error) {
		return fakeFileInfo{size: 9 * 1024 * 1024}, nil
	}
	defer func() { osStat = origStat }()
	_, err := resolvePayloadSource("", path)
	if err == nil {
		t.Fatal("expected size-cap error")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %q", err)
	}
}

type fakeFileInfo struct{ size int64 }

func (f fakeFileInfo) Size() int64 { return f.size }

func TestParseTime_AbsoluteMS(t *testing.T) {
	got, err := parseTime("1700000000000", time.UnixMilli(0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != 1700000000000 {
		t.Fatalf("got %v, want 1700000000000", got)
	}
}

func TestParseTime_PositiveDuration(t *testing.T) {
	now := time.UnixMilli(1700000000_000)
	got, err := parseTime("+30m", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != 1700000000_000+30*60*1000 {
		t.Fatalf("got %v, want now+30m", got)
	}
}

func TestParseTime_NegativeDuration(t *testing.T) {
	now := time.UnixMilli(1700000000_000)
	got, err := parseTime("-5s", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != 1700000000_000-5_000 {
		t.Fatalf("got %v, want now-5s", got)
	}
}

func TestParseTime_RejectsGarbage(t *testing.T) {
	if _, err := parseTime("not-a-time", time.Now()); err == nil {
		t.Fatalf("expected error on garbage input")
	}
}

func TestResolveVisibility_PrivateShorthand(t *testing.T) {
	v, err := resolveVisibility(&commonFlags{Private: true})
	if err != nil || v != v4types.VisibilityPrivate {
		t.Fatalf("got (%q, %v); want (private, nil)", v, err)
	}
}

func TestResolveVisibility_SystemShorthand(t *testing.T) {
	v, err := resolveVisibility(&commonFlags{System: true})
	if err != nil || v != v4types.VisibilitySystem {
		t.Fatalf("got (%q, %v); want (system, nil)", v, err)
	}
}

func TestResolveVisibility_MutualExclusion(t *testing.T) {
	if _, err := resolveVisibility(&commonFlags{Private: true, System: true}); err == nil {
		t.Fatalf("expected mutual-exclusion error")
	}
	if _, err := resolveVisibility(&commonFlags{Private: true, Visibility: "public"}); err == nil {
		t.Fatalf("expected mutual-exclusion error")
	}
}

func TestResolveVisibility_RejectsUnknown(t *testing.T) {
	if _, err := resolveVisibility(&commonFlags{Visibility: "weird"}); err == nil {
		t.Fatalf("expected error on unknown visibility")
	}
}

func TestResolveVisibility_DefaultEmpty(t *testing.T) {
	v, err := resolveVisibility(&commonFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "" {
		t.Fatalf("expected zero value when nothing supplied, got %q", v)
	}
}
