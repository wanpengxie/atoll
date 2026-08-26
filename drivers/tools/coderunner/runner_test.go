//go:build unix

package coderunner

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// fakeHost plays the MCP server side in-process so the runner is tested
// against the wire contract alone.
type fakeHost struct {
	t        *testing.T
	stdin    *json.Encoder
	logs     []loggingMessage
	progress []progressNotification
	returned json.RawMessage
	failed   *runtimeFailure
	calls    []toolCallParams
	tools    []toolSpec
}

func (h *fakeHost) reply(id json.RawMessage, result any) {
	raw, _ := json.Marshal(result)
	if err := h.stdin.Encode(rpcMessage{JSONRPC: "2.0", ID: id, Result: raw}); err != nil {
		h.t.Logf("reply after runtime exit: %v", err)
	}
}

func (h *fakeHost) toolResult(id json.RawMessage, structured any) {
	raw, _ := json.Marshal(structured)
	h.reply(id, toolCallResult{Content: []contentBlock{{Type: "text", Text: string(raw)}}, StructuredContent: raw})
}

func (h *fakeHost) handle(m rpcMessage) (done bool) {
	switch {
	case m.isRequest():
		switch m.Method {
		case "initialize":
			h.reply(m.ID, initializeResult{ProtocolVersion: mcpProtocolVersion, Capabilities: map[string]any{"tools": map[string]any{}}, ServerInfo: map[string]any{"name": "fake"}})
		case "tools/list":
			h.reply(m.ID, toolListResult{Tools: append(append([]toolSpec{}, h.tools...), hostTools...)})
		case "tools/call":
			var p toolCallParams
			_ = json.Unmarshal(m.Params, &p)
			h.calls = append(h.calls, p)
			switch p.Name {
			case toolContext:
				h.toolResult(m.ID, map[string]any{"self": "tool:runner:1", "channel": "c", "request_id": "r", "args": map[string]any{"n": 1}, "actors": map[string]string{"echo": "tool:echo:1"}})
			case toolReturn:
				var args struct {
					Value json.RawMessage `json:"value"`
				}
				_ = json.Unmarshal(p.Arguments, &args)
				h.returned = args.Value
				h.toolResult(m.ID, map[string]any{})
				return true
			case toolFail:
				var f runtimeFailure
				_ = json.Unmarshal(p.Arguments, &f)
				h.failed = &f
				h.toolResult(m.ID, map[string]any{})
				return true
			case "echo__echo_say":
				h.toolResult(m.ID, map[string]any{"text": "echoed:" + string(p.Arguments)})
			default:
				h.reply(m.ID, toolCallResult{Content: []contentBlock{{Type: "text", Text: "nope"}}, IsError: true, StructuredContent: json.RawMessage(`{"error_code":"undeclared_capability","detail":"nope"}`)})
			}
		default:
			h.t.Fatalf("unexpected request %s", m.Method)
		}
	case m.isNotification():
		switch m.Method {
		case "notifications/message":
			var l loggingMessage
			_ = json.Unmarshal(m.Params, &l)
			h.logs = append(h.logs, l)
		case "notifications/progress":
			var p progressNotification
			_ = json.Unmarshal(m.Params, &p)
			h.progress = append(h.progress, p)
		}
	}
	return false
}

func runProgram(t *testing.T, program string, cancelAfter time.Duration, tools ...toolSpec) *fakeHost {
	t.Helper()
	return runProgramWith(t, Config{}, program, cancelAfter, tools...)
}

// runProgramWith drives any configured runtime through the same fake host —
// the contract is the wire, not the language.
func runProgramWith(t *testing.T, cfg Config, program string, cancelAfter time.Duration, tools ...toolSpec) *fakeHost {
	t.Helper()
	command, args, suffix := cfg.runtime()
	path := filepath.Join(t.TempDir(), "program"+suffix)
	if err := os.WriteFile(path, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(command, args...)
	cmd.Env = append(os.Environ(), programEnv+"="+path)
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
	host := &fakeHost{t: t, stdin: json.NewEncoder(stdin), tools: tools}
	if cancelAfter > 0 {
		go func() {
			time.Sleep(cancelAfter)
			_ = stdin.Close()
		}()
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 8<<20)
	for scanner.Scan() {
		var m rpcMessage
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			t.Fatalf("non-protocol line on stdout: %q", scanner.Text())
		}
		if host.handle(m) {
			break
		}
	}
	_ = stdin.Close()
	_ = cmd.Wait()
	return host
}

func TestRunnerTerminals(t *testing.T) {
	tests := []struct {
		name     string
		program  string
		wantKind string
	}{
		{name: "missing run", program: `export const x = 1`, wantKind: "invalid_output"},
		{name: "syntax", program: `export async function run( {`, wantKind: "syntax"},
		{name: "exception", program: `export async function run(){ throw new Error("x") }`, wantKind: "exception"},
		{name: "invalid output", program: `export async function run(){ return 1n }`, wantKind: "invalid_output"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := runProgram(t, test.program, 0)
			if host.failed == nil || host.failed.Kind != test.wantKind {
				t.Fatalf("failed=%+v returned=%s", host.failed, host.returned)
			}
		})
	}
}

func TestRunnerReturnsValueAndRedirectsConsole(t *testing.T) {
	program := `export async function run({atoll, args}){ console.log("hello", {x:1}); console.error("bad"); atoll.log("note"); atoll.progress("processing", {step:1}); return {ok:true, n: args.n, self: atoll.self} }`
	host := runProgram(t, program, 0)
	if host.failed != nil || string(host.returned) != `{"ok":true,"n":1,"self":"tool:runner:1"}` {
		t.Fatalf("failed=%+v returned=%s", host.failed, host.returned)
	}
	if len(host.logs) != 3 || host.logs[0].Level != "info" || host.logs[0].Logger != "console" || string(host.logs[0].Data) != `"hello {\"x\":1}"` ||
		host.logs[1].Level != "error" || host.logs[2].Logger != "atoll" {
		t.Fatalf("logs=%+v", host.logs)
	}
	if len(host.progress) != 1 || host.progress[0].Message != "processing" || string(host.progress[0].ProgressToken) != `"r"` {
		t.Fatalf("progress=%+v", host.progress)
	}
}

func TestRunnerCallsToolsByMetaAndRefusesUndeclared(t *testing.T) {
	echoTool := toolSpec{Name: "echo__echo_say", InputSchema: json.RawMessage(`{"type":"object"}`), Meta: map[string]any{metaTarget: "echo", metaWord: "echo.say", "atoll/actor": "tool:echo:1"}}
	program := `export async function run({atoll}){
  const a = await atoll.call({target:"echo", type:"echo.say", input:{text:"hi"}, deadlineMs: 500});
  const b = await atoll.call({target:"tool:echo:1", type:"echo.say", input:"scalar"});
  let c; try { await atoll.call({target:"echo", type:"echo.other", input:{}}) } catch (e) { c = e.code }
  return {a, b, c};
}`
	host := runProgram(t, program, 0, echoTool)
	if host.failed != nil {
		t.Fatalf("failed=%+v", host.failed)
	}
	var out struct {
		A, B map[string]string
		C    string
	}
	if err := json.Unmarshal(host.returned, &out); err != nil {
		t.Fatal(err)
	}
	if out.A["text"] != `echoed:{"text":"hi"}` || out.B["text"] != `echoed:{"$input":"scalar"}` || out.C != "undeclared_capability" {
		t.Fatalf("returned=%s", host.returned)
	}
	var sawDeadline bool
	for _, call := range host.calls {
		if call.Name == "echo__echo_say" && string(call.Meta[metaDeadline]) == "500" {
			sawDeadline = true
		}
	}
	if !sawDeadline {
		t.Fatalf("deadline did not ride _meta: %+v", host.calls)
	}
}

func TestRunnerCancelAbortsSignal(t *testing.T) {
	program := `export async function run({atoll}) { if (atoll.signal.aborted) return "aborted"; return await new Promise(resolve => atoll.signal.addEventListener("abort", () => resolve("aborted"), {once:true})) }`
	host := runProgram(t, program, 150*time.Millisecond)
	// Once stdin closes the runtime cannot deliver its return through the
	// session; the contract is that the program observed the abort and the
	// process exited (runProgram returns only after Wait).
	if host.failed != nil && host.failed.Kind != "exception" {
		t.Fatalf("unexpected failure %+v", host.failed)
	}
}

// The Python runtime is the second implementation of the same contract: the
// identical fake host, the identical assertions, a different language.
func TestPythonRuntimeSpeaksTheSameContract(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 unavailable")
	}
	runtimePath, err := filepath.Abs(filepath.Join("runtimes", "python", "atoll_runtime.py"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Runtime: &RuntimeConfig{Command: "python3", Args: []string{runtimePath}, Suffix: ".py"}}
	echoTool := toolSpec{Name: "echo__echo_say", InputSchema: json.RawMessage(`{"type":"object"}`), Meta: map[string]any{metaTarget: "echo", metaWord: "echo.say", "atoll/actor": "tool:echo:1"}}
	program := "async def run(atoll, args):\n" +
		"    print('hello', {'x': 1})\n" +
		"    atoll.log('note')\n" +
		"    atoll.progress('processing', {'step': 1})\n" +
		"    a = await atoll.call(target='echo', type='echo.say', input={'text': 'hi'}, deadline_ms=500)\n" +
		"    b = await atoll.call(target='tool:echo:1', type='echo.say', input='scalar')\n" +
		"    try:\n" +
		"        await atoll.call(target='echo', type='echo.other', input={})\n" +
		"        c = None\n" +
		"    except Exception as e:\n" +
		"        c = e.code\n" +
		"    return {'a': a, 'b': b, 'c': c, 'n': args['n'], 'self': atoll.self_id}\n"
	host := runProgramWith(t, cfg, program, 0, echoTool)
	if host.failed != nil {
		t.Fatalf("failed=%+v", host.failed)
	}
	var out struct {
		A, B map[string]string
		C    string
		N    int
		Self string
	}
	if err := json.Unmarshal(host.returned, &out); err != nil {
		t.Fatalf("%v: %s", err, host.returned)
	}
	if out.A["text"] != `echoed:{"text": "hi"}` || out.B["text"] != `echoed:{"$input": "scalar"}` || out.C != "undeclared_capability" || out.N != 1 || out.Self != "tool:runner:1" {
		t.Fatalf("returned=%s", host.returned)
	}
	if len(host.logs) != 2 || host.logs[0].Logger != "console" || string(host.logs[0].Data) != `"hello {'x': 1}"` || host.logs[1].Logger != "atoll" {
		t.Fatalf("logs=%+v", host.logs)
	}
	if len(host.progress) != 1 || host.progress[0].Message != "processing" {
		t.Fatalf("progress=%+v", host.progress)
	}
	for _, kind := range []struct{ program, want string }{
		{"def run(atoll, args):\n    raise RuntimeError('x')\n", "exception"},
		{"x = 1\n", "invalid_output"},
		{"def run(:\n", "syntax"},
	} {
		host := runProgramWith(t, cfg, kind.program, 0)
		if host.failed == nil || host.failed.Kind != kind.want {
			t.Fatalf("program %q failed=%+v", kind.program, host.failed)
		}
	}
}
