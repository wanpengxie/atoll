package pibridge

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEmbeddedBundleDigestAndNodeFloor(t *testing.T) {
	if got := fmt.Sprintf("%x", sha256.Sum256(source)); got != BundleSHA256 {
		t.Fatalf("bundle digest=%s", got)
	}
	if !supportedNode("v22.19.0") || !supportedNode("v23.0.0") || supportedNode("v22.18.9") || supportedNode("garbage") {
		t.Fatal("Node version floor is not enforced")
	}
}

func TestConcurrentActorsMaterializeOneCompleteBridgeAsset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.mjs")
	content := []byte("complete asset")
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- materialize(path, content)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(content) {
		t.Fatalf("asset=%q err=%v", got, err)
	}
}

func TestSlowProgressConsumerDoesNotBlockAnotherCallTerminal(t *testing.T) {
	slow := &call{frames: make(chan frame, 1), done: make(chan struct{})}
	fast := &call{frames: make(chan frame, 1), done: make(chan struct{})}
	b := &Bridge{cmd: &exec.Cmd{}, pending: map[string]*call{"slow": slow, "fast": fast}, done: make(chan struct{}), logger: slog.Default()}
	var stream strings.Builder
	stream.WriteString("{\"kind\":\"ready\",\"protocol\":1}\n")
	for range 64 {
		stream.WriteString("{\"id\":\"slow\",\"kind\":\"progress\",\"event\":{}}\n")
	}
	stream.WriteString("{\"id\":\"fast\",\"kind\":\"result\",\"value\":{\"ok\":true}}\n")
	ready := make(chan frame, 1)
	b.read(strings.NewReader(stream.String()), ready)
	select {
	case terminal := <-fast.frames:
		if terminal.Kind != "result" {
			t.Fatalf("terminal=%+v", terminal)
		}
	case <-time.After(time.Second):
		t.Fatal("fast terminal was head-of-line blocked by slow progress")
	}
	close(slow.done)
	close(fast.done)
}

func TestEnvironmentPolicyKeepsProviderKeysOutOfWorkspaceChild(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-secret")
	t.Setenv("HTTPS_PROXY", "http://proxy.test")
	has := func(env []string, entry string) bool {
		for _, value := range env {
			if value == entry {
				return true
			}
		}
		return false
	}
	if has(selectedEnvironment(RuntimeEnvironment), "OPENAI_API_KEY=test-secret") {
		t.Fatal("workspace runtime inherited an LLM provider key")
	}
	if !has(selectedEnvironment(ProviderEnvironment), "OPENAI_API_KEY=test-secret") || !has(selectedEnvironment(RuntimeEnvironment), "HTTPS_PROXY=http://proxy.test") {
		t.Fatal("provider key or common network configuration was omitted")
	}
}

func startTestBridge(t *testing.T) (*Bridge, string) {
	t.Helper()
	cwd := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	b, err := Start(ctx, "node", cwd, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close(); cancel() })
	return b, cwd
}

func TestEmbeddedPiBridgeModelsAndFauxGeneration(t *testing.T) {
	b, _ := startTestBridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	models, err := b.Call(ctx, "llm.models", map[string]any{"provider": "faux"}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(models), `"faux-1"`) {
		t.Fatalf("faux catalog missing: %s", models)
	}
	generated, err := b.Call(ctx, "llm.generate", map[string]any{"provider": "faux", "model": "faux-1", "messages": []any{map[string]any{"role": "user", "content": "hi", "timestamp": 1}}, "options": map[string]any{"faux_response": "hello from Pi"}}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(generated), "hello from Pi") {
		t.Fatalf("generation=%s", generated)
	}
}

func TestEmbeddedPiBridgeCanScriptARealToolCallForLoopIntegration(t *testing.T) {
	b, _ := startTestBridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	generated, err := b.Call(ctx, "llm.generate", map[string]any{
		"provider": "faux", "model": "faux-1",
		"messages": []any{map[string]any{"role": "user", "content": "write it", "timestamp": 1}},
		"tools":    []any{map[string]any{"name": "write", "description": "write", "parameters": map[string]any{"type": "object"}}},
		"options":  map[string]any{"faux_tool_call": map[string]any{"id": "tc-1", "name": "write", "arguments": map[string]any{"path": "result.txt", "content": "from loop\n"}}},
	}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"type":"toolCall"`, `"id":"tc-1"`, `"name":"write"`, `"path":"result.txt"`} {
		if !strings.Contains(string(generated), want) {
			t.Fatalf("generation missing %s: %s", want, generated)
		}
	}
}

func TestEmbeddedPiWorkspaceParitySlice(t *testing.T) {
	b, cwd := startTestBridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	call := func(op string, args any) json.RawMessage {
		t.Helper()
		raw, err := b.Call(ctx, op, args, cwd, nil)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return raw
	}
	call("workspace.write", map[string]any{"path": "sample.txt", "content": "alpha\nbeta\n"})
	call("workspace.edit", map[string]any{"path": "sample.txt", "edits": []any{map[string]any{"oldText": "beta", "newText": "gamma"}}})
	read := call("workspace.read", map[string]any{"path": "sample.txt"})
	if !strings.Contains(string(read), "gamma") {
		t.Fatalf("read=%s", read)
	}
	bash := call("workspace.bash", map[string]any{"command": "pwd && wc -l sample.txt"})
	if !strings.Contains(string(bash), filepath.Base(cwd)) || !strings.Contains(string(bash), "2 sample.txt") {
		t.Fatalf("bash=%s", bash)
	}
	onDisk, err := os.ReadFile(filepath.Join(cwd, "sample.txt"))
	if err != nil || string(onDisk) != "alpha\ngamma\n" {
		t.Fatalf("file=%q err=%v", onDisk, err)
	}
}

func TestEmbeddedPiWorkspaceUsesPerCallCWDAndEnvironment(t *testing.T) {
	b, _ := startTestBridge(t)
	cwd := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := b.CallWithEnvironment(ctx, "workspace.bash", map[string]any{
		"command": `printf '%s|%s|%s' "$BRANCH_VALUE" "$PWD" "${HOME-unset}"`,
	}, cwd, map[string]string{"BRANCH_VALUE": "branch-specific"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "branch-specific|"+cwd+"|unset") {
		t.Fatalf("branch execution snapshot was not applied: %s", raw)
	}
}

func TestEmbeddedPiSearchDoesNotFallBackToBridgePATH(t *testing.T) {
	b, _ := startTestBridge(t)
	cwd := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := b.CallWithEnvironment(ctx, "workspace.grep", map[string]any{"pattern": "anything", "path": "."}, cwd, map[string]string{"PATH": ""}, nil)
	var bridgeErr *Error
	if !errors.As(err, &bridgeErr) || bridgeErr.Code != "tool_failed" || !strings.Contains(bridgeErr.Detail, "not available") {
		t.Fatalf("grep used Bridge PATH instead of branch snapshot: %v", err)
	}
}

func TestEmbeddedPiWorkspaceUsesPiNativeArgumentValidation(t *testing.T) {
	b, cwd := startTestBridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := b.Call(ctx, "workspace.write", map[string]any{
		"path": "sample.txt",
	}, cwd, nil)
	var bridgeErr *Error
	if !errors.As(err, &bridgeErr) || bridgeErr.Code != "invalid_args" || !strings.Contains(bridgeErr.Detail, "Validation failed") {
		t.Fatalf("validation error=%v", err)
	}
}

func TestEmbeddedPiReadPaginationAndContinuation(t *testing.T) {
	b, cwd := startTestBridge(t)
	content := "one\ntwo\nthree\nfour\n"
	if err := os.WriteFile(filepath.Join(cwd, "large file.txt"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	page, err := b.Call(ctx, "workspace.read", map[string]any{"path": "large file.txt", "offset": 2, "limit": 2}, cwd, nil)
	var pageResult struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	_ = json.Unmarshal(page, &pageResult)
	if err != nil || len(pageResult.Content) != 1 || !strings.Contains(pageResult.Content[0].Text, "two\nthree") || !strings.Contains(pageResult.Content[0].Text, "offset=4") {
		t.Fatalf("page=%s err=%v", page, err)
	}
	last, err := b.Call(ctx, "workspace.read", map[string]any{"path": "large file.txt", "offset": 4, "limit": 10}, cwd, nil)
	if err != nil || !strings.Contains(string(last), "four") {
		t.Fatalf("last=%s err=%v", last, err)
	}
	if _, err := b.Call(ctx, "workspace.read", map[string]any{"path": "large file.txt", "offset": 99}, cwd, nil); err == nil {
		t.Fatal("out-of-range offset was accepted")
	}
	for _, args := range []map[string]any{{"path": "large file.txt", "offset": 0}, {"path": "large file.txt", "limit": 0}} {
		if _, err := b.Call(ctx, "workspace.read", args, cwd, nil); err == nil {
			t.Fatalf("invalid pagination accepted: %+v", args)
		}
	}
}

func TestEmbeddedPiSearchAndListingTools(t *testing.T) {
	b, cwd := startTestBridge(t)
	if err := os.MkdirAll(filepath.Join(cwd, "dir with spaces"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cwd, "dir with spaces", "alpha.txt"), []byte("needle\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, tc := range []struct {
		op   string
		args any
		want string
	}{
		{"workspace.grep", map[string]any{"pattern": "needle", "path": "dir with spaces"}, "alpha.txt:1"},
		{"workspace.grep", map[string]any{"pattern": "absent", "path": "dir with spaces"}, "No matches found"},
		{"workspace.find", map[string]any{"pattern": "*.txt", "path": "dir with spaces"}, "alpha.txt"},
		{"workspace.find", map[string]any{"pattern": "*.go", "path": "dir with spaces"}, "No files found"},
		{"workspace.ls", map[string]any{"path": "dir with spaces"}, "alpha.txt"},
	} {
		raw, err := b.Call(ctx, tc.op, tc.args, cwd, nil)
		if err != nil || !strings.Contains(string(raw), tc.want) {
			t.Fatalf("%s=%s err=%v, want %q", tc.op, raw, err, tc.want)
		}
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := b.Call(cancelled, "workspace.grep", map[string]any{"pattern": "needle", "path": "."}, cwd, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled grep error=%v", err)
	}
}

func TestEmbeddedPiImageToolResultCanReachFauxProvider(t *testing.T) {
	b, cwd := startTestBridge(t)
	png, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cwd, "pixel.png"), png, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	read, err := b.Call(ctx, "workspace.read", map[string]any{"path": "pixel.png"}, cwd, nil)
	if err != nil || !strings.Contains(string(read), `"type":"image"`) || !strings.Contains(string(read), `"mimeType":"image/png"`) {
		t.Fatalf("image read=%s err=%v", read, err)
	}
	var result struct {
		Content []json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(read, &result); err != nil {
		t.Fatal(err)
	}
	generated, err := b.Call(ctx, "llm.generate", map[string]any{
		"provider": "faux", "model": "faux-1",
		"messages": []any{
			map[string]any{"role": "user", "content": "inspect", "timestamp": 1},
			map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "toolCall", "id": "tc-image", "name": "read", "arguments": map[string]any{"path": "pixel.png"}}}, "timestamp": 2},
			map[string]any{"role": "toolResult", "toolCallId": "tc-image", "toolName": "read", "content": result.Content, "isError": false, "timestamp": 3},
		},
		"options": map[string]any{"faux_response": "image received"},
	}, cwd, nil)
	if err != nil || !strings.Contains(string(generated), "image received") {
		t.Fatalf("generation=%s err=%v", generated, err)
	}
}

func TestEmbeddedPiRejectsImageForTextOnlyModel(t *testing.T) {
	b, cwd := startTestBridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := b.Call(ctx, "llm.generate", map[string]any{
		"provider": "faux", "model": "faux-1",
		"messages": []any{map[string]any{"role": "user", "content": []any{
			map[string]any{"type": "text", "text": "image"},
			map[string]any{"type": "image", "data": base64.StdEncoding.EncodeToString([]byte("png")), "mimeType": "image/png"},
		}, "timestamp": 1}},
		"options": map[string]any{"faux_supports_images": false},
	}, cwd, nil)
	var bridgeErr *Error
	if !errors.As(err, &bridgeErr) || !strings.Contains(bridgeErr.Detail, "does not support image") {
		t.Fatalf("unsupported image error=%v", err)
	}
}
