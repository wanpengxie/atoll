package api

import (
	"encoding/json"
	"testing"
)

func TestGenerateRequestPreservesPrebuiltToolsJSONByteForByte(t *testing.T) {
	want := `[ {"name":"z","parameters":{"type":"object"}}, {"name":"a","parameters":{"type":"object"}} ]`
	raw, err := json.Marshal(GenerateRequest{ToolsJSON: want})
	if err != nil {
		t.Fatal(err)
	}
	var got GenerateRequest
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.ToolsJSON != want {
		t.Fatalf("tools string changed:\nwant %q\n got %q", want, got.ToolsJSON)
	}
}
