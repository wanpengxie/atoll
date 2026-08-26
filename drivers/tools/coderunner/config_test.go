package coderunner

import (
	"encoding/json"
	"testing"
)

func TestParseConfigStrict(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		ok   bool
	}{
		{name: "empty bytes", raw: "", ok: true},
		{name: "empty object", raw: `{}`, ok: true},
		{name: "fixed", raw: `{"program":"export function run(){}","requires":["echo","mcp:github","system"],"node":"/usr/bin/node"}`, ok: true},
		{name: "runtime", raw: `{"runtime":{"command":"python3","args":["/opt/atoll_runtime.py"],"suffix":".py"}}`, ok: true},
		{name: "runtime blank command", raw: `{"runtime":{"command":" "}}`, ok: false},
		{name: "runtime unknown field", raw: `{"runtime":{"command":"python3","shell":true}}`, ok: false},
		{name: "unknown", raw: `{"extra":true}`},
		{name: "blank program", raw: `{"program":"  "}`},
		{name: "null program", raw: `{"program":null}`},
		{name: "null requires", raw: `{"requires":null}`},
		{name: "bad require", raw: `{"requires":["echo.say"]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseConfig(json.RawMessage(test.raw))
			if (err == nil) != test.ok {
				t.Fatalf("parseConfig(%s) error=%v, want ok=%v", test.raw, err, test.ok)
			}
		})
	}
}

func TestDecodeRunModes(t *testing.T) {
	modeOne := &coderunnerActor{cfg: Config{Node: defaultNode}}
	modeTwo := &coderunnerActor{cfg: Config{Program: "export async function run(){}", Requires: []string{"echo"}, Node: defaultNode}}
	tests := []struct {
		name  string
		actor *coderunnerActor
		raw   string
		ok    bool
	}{
		{name: "mode one program", actor: modeOne, raw: `{"program":"export async function run(){}","requires":["echo"],"args":{"x":1}}`, ok: true},
		{name: "mode one missing", actor: modeOne, raw: `{}`},
		{name: "mode two args", actor: modeTwo, raw: `{"args":1}`, ok: true},
		{name: "mode two program forbidden", actor: modeTwo, raw: `{"program":"x"}`},
		{name: "mode two requires forbidden", actor: modeTwo, raw: `{"requires":[]}`},
		{name: "unknown field", actor: modeOne, raw: `{"program":"x","extra":1}`},
		{name: "requires null", actor: modeOne, raw: `{"program":"x","requires":null}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.actor.decodeRun(json.RawMessage(test.raw))
			if (err == nil) != test.ok {
				t.Fatalf("decodeRun(%s) error=%v, want ok=%v", test.raw, err, test.ok)
			}
		})
	}
}
