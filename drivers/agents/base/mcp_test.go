package base

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/lib/metatool"
)

func TestBuildMCPCatalogShape(t *testing.T) {
	exec := func(context.Context, metatool.MetaTool, json.RawMessage) metatool.ResultValue {
		return metatool.ResultValue{}
	}
	tools := BuildMCPCatalog(exec)

	catalog := metatool.MetaTools()
	if len(tools) != len(catalog) {
		t.Fatalf("catalog size = %d, want %d (the 7 meta tools)", len(tools), len(catalog))
	}
	for i, mt := range catalog {
		if tools[i].Name != mt.Spec.Name {
			t.Fatalf("tool[%d] name = %q, want %q", i, tools[i].Name, mt.Spec.Name)
		}
		if tools[i].Description != mt.Spec.Description {
			t.Fatalf("tool[%d] description mismatch", i)
		}
		if len(mt.Spec.Schema) > 0 && len(tools[i].Schema) == 0 {
			t.Fatalf("tool[%d] %q dropped its schema", i, mt.Spec.Name)
		}
		if tools[i].Handler == nil {
			t.Fatalf("tool[%d] %q has nil handler", i, mt.Spec.Name)
		}
	}
}

func TestHandlerRoutesToExecutor(t *testing.T) {
	var gotName string
	var gotParams string
	exec := func(_ context.Context, mt metatool.MetaTool, params json.RawMessage) metatool.ResultValue {
		gotName = mt.Spec.Name
		gotParams = string(params)
		return metatool.ResultValue{
			Name:    mt.Spec.Name,
			Value:   map[string]any{"ok": true, "echo": "v"},
			IsError: false,
		}
	}
	tools := BuildMCPCatalog(exec)

	// Drive the first tool's handler and assert exec was reached + rendered.
	res := tools[0].Handler(context.Background(), json.RawMessage(`{"a":1}`))
	if gotName != tools[0].Name {
		t.Fatalf("exec got mt %q, want %q", gotName, tools[0].Name)
	}
	if gotParams != `{"a":1}` {
		t.Fatalf("exec got params %q", gotParams)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(res.Text), &decoded); err != nil {
		t.Fatalf("result text not JSON: %q (%v)", res.Text, err)
	}
	if decoded["ok"] != true || decoded["echo"] != "v" {
		t.Fatalf("rendered value wrong: %v", decoded)
	}
	if res.IsError {
		t.Fatalf("IsError should be false")
	}
}

func TestRenderMCPResultCarriesErrorFlag(t *testing.T) {
	rv := metatool.NewError("call_actor", metatool.InternalError, "boom", "retry", nil)
	res := RenderMCPResult(rv)
	if !res.IsError {
		t.Fatalf("error ResultValue must render IsError=true")
	}
	if res.Text == "" {
		t.Fatalf("error result should carry rendered text")
	}
}
