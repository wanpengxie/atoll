package base

import "testing"

func TestRuntimeErrorCodeGoldenIsExactlyNine(t *testing.T) {
	want := map[string]struct{}{
		"cancelled": {}, "cas_mismatch": {}, "interrupted": {}, "overloaded": {},
		"provider_timeout": {}, "provider_crash": {}, "provider_failed": {},
		"input_too_large": {}, "empty_input": {},
	}
	got := map[string]struct{}{}
	for _, code := range runtimeErrorCodes {
		got[code] = struct{}{}
	}
	if len(got) != len(want) {
		t.Fatalf("codes=%v", got)
	}
	for code := range want {
		if _, ok := got[code]; !ok {
			t.Fatalf("missing code %q in %v", code, got)
		}
	}
}
