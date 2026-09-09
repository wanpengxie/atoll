package native

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestDefaultConfigKeepsRecoveryCollectionsBounded(t *testing.T) {
	cfg, err := ParseConfig(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxOpenWorks != 32 || cfg.MaxAssignmentsPerLooper != 32 || cfg.MaxInputsPerWork != 128 || cfg.MaxOperationKeys != 256 || cfg.MaxTurns != 12 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestToolsOmittedAndExplicitlyEmptyRemainDistinct(t *testing.T) {
	omitted, err := ParseConfig(DefaultConfig())
	if err != nil || omitted.ToolsConfigured {
		t.Fatalf("omitted tools config=%+v err=%v", omitted, err)
	}
	empty, err := ParseConfig(json.RawMessage(`{"loopers":["l"],"llm_actor":"m","max_open_works":1,"max_assignments_per_looper":1,"max_inputs_per_work":1,"max_operation_keys":1,"max_turns":1,"tools":[]}`))
	if err != nil || !empty.ToolsConfigured || len(empty.Tools) != 0 {
		t.Fatalf("empty tools config=%+v err=%v", empty, err)
	}
}

func TestToolAllowlistValidation(t *testing.T) {
	base := `{"loopers":["l"],"llm_actor":"m","max_open_works":1,"max_assignments_per_looper":1,"max_inputs_per_work":1,"max_operation_keys":1,"max_turns":1,"tools":%s}`
	for _, tools := range []string{
		`null`,
		`[{"name":"bad name","actor":"a","word":"x.run"}]`,
		`[{"name":"x","actor":"a","word":"x.run"},{"name":"x","actor":"b","word":"y.run"}]`,
		`[{"name":"x","actor":" ","word":"x.run"}]`,
	} {
		if _, err := ParseConfig(json.RawMessage(fmt.Sprintf(base, tools))); err == nil {
			t.Fatalf("invalid tools accepted: %s", tools)
		}
	}
}

func TestConfigRejectsUnboundedWorkCollections(t *testing.T) {
	base := map[string]any{
		"loopers":                    []string{"loop"},
		"llm_actor":                  "llm",
		"max_open_works":             1,
		"max_assignments_per_looper": 1,
		"max_inputs_per_work":        1,
		"max_operation_keys":         1,
		"max_turns":                  1,
	}
	for _, field := range []string{"max_assignments_per_looper", "max_inputs_per_work", "max_operation_keys"} {
		copy := make(map[string]any, len(base))
		for key, value := range base {
			copy[key] = value
		}
		copy[field] = 0
		raw, err := json.Marshal(copy)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseConfig(raw); err == nil {
			t.Fatalf("zero %s was accepted", field)
		}
	}
}

func TestConfigRejectsDuplicateLooperLane(t *testing.T) {
	raw := json.RawMessage(`{
		"loopers":["loop","loop"],

		"llm_actor":"llm",
		"max_open_works":1,
		"max_assignments_per_looper":1,
		"max_inputs_per_work":1,
		"max_operation_keys":1,
		"max_turns":1
	}`)
	if _, err := ParseConfig(raw); err == nil {
		t.Fatal("duplicate Looper lane was accepted")
	}
}

func TestConfigAcceptsBodySideHostHandleAndRejectsWhitespace(t *testing.T) {
	raw := json.RawMessage(`{
		"loopers":["loop"],

		"llm_actor":"llm",
		"host_actor":"host",
		"max_open_works":1,
		"max_assignments_per_looper":1,
		"max_inputs_per_work":1,
		"max_operation_keys":1,
		"max_turns":1
	}`)
	cfg, err := ParseConfig(raw)
	if err != nil || cfg.HostActor != "host" {
		t.Fatalf("host handle config=%+v err=%v", cfg, err)
	}
	raw = json.RawMessage(`{
		"loopers":["loop"],

		"llm_actor":"llm",
		"host_actor":" host ",
		"max_open_works":1,
		"max_assignments_per_looper":1,
		"max_inputs_per_work":1,
		"max_operation_keys":1,
		"max_turns":1
	}`)
	if _, err := ParseConfig(raw); err == nil {
		t.Fatal("whitespace-padded host handle was accepted")
	}
}
