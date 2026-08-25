//go:build unix

package coderunner

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"os/exec"
	"testing"
	"time"
)

func TestRunnerTerminals(t *testing.T) {
	tests := []struct {
		name     string
		program  string
		wantOp   string
		wantKind string
	}{
		{name: "missing run", program: `export const x = 1`, wantOp: "error", wantKind: "invalid_output"},
		{name: "syntax", program: `export async function run( {`, wantOp: "error", wantKind: "syntax"},
		{name: "exception", program: `export async function run(){ throw new Error("x") }`, wantOp: "error", wantKind: "exception"},
		{name: "invalid output", program: `export async function run(){ return 1n }`, wantOp: "error", wantKind: "invalid_output"},
		{name: "result", program: `export async function run(){ console.log("hello", {x:1}); return {ok:true} }`, wantOp: "result"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frames := runProgram(t, test.program, false)
			var terminal nodeFrame
			for _, frame := range frames {
				if frame.Op == "result" || frame.Op == "error" {
					terminal = frame
				}
			}
			if terminal.Op != test.wantOp || terminal.Kind != test.wantKind {
				t.Fatalf("terminal=%+v frames=%+v", terminal, frames)
			}
			if test.name == "result" {
				if len(frames) < 2 || frames[0].Op != "log" || frames[0].Stream != "stdout" {
					t.Fatalf("console.log was not redirected: %+v", frames)
				}
			}
		})
	}
}

func TestRunnerCancelAbortsSignal(t *testing.T) {
	program := `export async function run({atoll}) { if (atoll.signal.aborted) return "aborted"; return await new Promise(resolve => atoll.signal.addEventListener("abort", () => resolve("aborted"), {once:true})) }`
	frames := runProgram(t, program, true)
	for _, frame := range frames {
		if frame.Op == "result" && string(frame.Value) == `"aborted"` {
			return
		}
	}
	t.Fatalf("cancel did not abort signal: %+v", frames)
}

func runProgram(t *testing.T, program string, cancel bool) []nodeFrame {
	t.Helper()
	cmd := exec.Command(defaultNode, "--input-type=module", "-e", runnerSource)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Skipf("node unavailable: %v", err)
	}
	start := startFrame{Op: "start", Program: "data:text/javascript;base64," + base64.StdEncoding.EncodeToString([]byte(program)), Args: json.RawMessage("null"), Actors: map[string]string{}, Self: "tool:runner:1", Channel: "c", RequestID: "r"}
	if err := json.NewEncoder(stdin).Encode(start); err != nil {
		t.Fatal(err)
	}
	if cancel {
		time.Sleep(25 * time.Millisecond)
		if err := json.NewEncoder(stdin).Encode(map[string]string{"op": "cancel"}); err != nil {
			t.Fatal(err)
		}
	}
	var frames []nodeFrame
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		var frame nodeFrame
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			t.Fatalf("decode %q: %v", scanner.Text(), err)
		}
		frames = append(frames, frame)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	return frames
}
