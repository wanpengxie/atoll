package native

import (
	"encoding/json"
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

func TestControllerDoesNotAcceptModelToolOrPromptConfiguration(t *testing.T) {
	for _, field := range []string{`"tools":[]`, `"workspace_actor":"workspace"`, `"host_actor":"host"`, `"prompt":"mutable"`} {
		raw := json.RawMessage(`{"loopers":["l"],"llm_actor":"m","max_open_works":1,"max_assignments_per_looper":1,"max_inputs_per_work":1,"max_operation_keys":1,"max_turns":1,` + field + `}`)
		if _, err := ParseConfig(raw); err == nil {
			t.Fatalf("removed Controller field accepted: %s", field)
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
