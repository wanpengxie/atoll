package echo

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// The config mechanism under test: ONE parser (parseConfig) feeds both the
// acceptance gate (ClassDecl.ValidateConfig) and the build (construct), the
// parsed value rides Def's closure into run, and run enforces it at the
// countdown.start boundary.

func TestParseConfig_EmptyYieldsDefaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parseConfig(nil) = %v, want ok", err)
	}
	if cfg.MaxSeconds != DefaultMaxSeconds {
		t.Fatalf("MaxSeconds = %d, want default %d", cfg.MaxSeconds, DefaultMaxSeconds)
	}
}

func TestParseConfig_ZeroKnobMeansDefault(t *testing.T) {
	cfg, err := parseConfig(json.RawMessage(`{"max_seconds":0}`))
	if err != nil {
		t.Fatalf("parseConfig = %v, want ok", err)
	}
	if cfg.MaxSeconds != DefaultMaxSeconds {
		t.Fatalf("MaxSeconds = %d, want default %d", cfg.MaxSeconds, DefaultMaxSeconds)
	}
}

func TestParseConfig_RejectsNegativeAndUnknownFields(t *testing.T) {
	for _, raw := range []string{
		`{"max_seconds":-1}`,
		`{"max_secondz":10}`, // typo'd knob must fail loud, not mean "default"
	} {
		if _, err := parseConfig(json.RawMessage(raw)); err == nil {
			t.Fatalf("parseConfig(%s) = ok, want error", raw)
		}
	}
}

// construct is fail-closed: a config that cannot parse fails THIS body's
// build; it never half-builds on silent defaults.
func TestConstruct_FailsClosedOnBadConfig(t *testing.T) {
	_, err := construct(registry.InstanceSpec{Config: json.RawMessage(`{"max_seconds":-5}`)}, registry.Deps{})
	if err == nil {
		t.Fatal("construct with bad config = ok, want error")
	}
}

func TestConstruct_CapturesConfigAndDefaultsID(t *testing.T) {
	decl, err := construct(registry.InstanceSpec{Config: json.RawMessage(`{"max_seconds":10}`)}, registry.Deps{})
	if err != nil {
		t.Fatalf("construct = %v, want ok", err)
	}
	if decl.ID != actor.ActorID("echo") {
		t.Fatalf("blank spec id → %q, want class default \"echo\"", decl.ID)
	}
	proc, err := decl.Factory.Proc.New()
	if err != nil || proc == nil {
		t.Fatalf("Def.New() = (%v, %v), want a live Proc", proc, err)
	}
}

// Runtime enforcement: the knob parsed at build time caps countdown.start.
// Distinct failure code from payload_invalid — the payload is well-formed,
// it is over THIS instance's configured limit.
func TestRun_CountdownRejectsSecondsBeyondConfiguredMax(t *testing.T) {
	over := requestMsg("req-over", TypeCountdownStart, startPayload{Seconds: 11, Note: "too long"})
	sys := &fakeSys{queue: []actorbase.Msg{over}}

	if err := run(sys, Config{MaxSeconds: 10}); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop", err)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "limit_exceeded" {
		t.Fatalf("fails = %+v, want one limit_exceeded", sys.fails)
	}
	if !strings.Contains(sys.fails[0].detail, "max_seconds 10") {
		t.Fatalf("fail detail = %q, want it to name the configured cap", sys.fails[0].detail)
	}
	if len(sys.timers) != 0 || len(sys.progresses) != 0 {
		t.Fatalf("timers/progresses = %+v/%+v, want none armed on a refused start", sys.timers, sys.progresses)
	}
}
