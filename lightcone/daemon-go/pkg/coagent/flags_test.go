package coagent

import (
	"reflect"
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
