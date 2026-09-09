package pillm

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/tools/pibridge"
)

// The preload owns every fetch in this child: no fixture can reach a real
// endpoint. Counts cover Pi + SDK + bridge + pi-llm, not just Go mock attempts.
func TestRetryCountsActualBridgeHTTPAttempts(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node unavailable")
	}
	dir := t.TempDir()
	countFile := filepath.Join(dir, "requests")
	preload := filepath.Join(dir, "mock-fetch.mjs")
	source := `import { appendFileSync } from "node:fs";
let calls=0;
globalThis.fetch = async (_url, init) => {
  calls++;
  appendFileSync(` + strconv.Quote(countFile) + `, "request\n");
  const body=JSON.parse(init.body);
  if (body.messages[0].content === "delay") return new Response('{"error":{"code":"rate_limit"}}',{status:429,headers:{"content-type":"application/json","retry-after":"60"}});
  if (body.messages[0].content === "broken-stream") {
    const encoder = new TextEncoder(); let first=true;
    return new Response(new ReadableStream({pull(controller){if(first){first=false;controller.enqueue(encoder.encode('data: {"id":"fixture","choices":[{"index":0,"delta":{"content":"partial-must-not-escape"},"finish_reason":null}]}\n\n'));}else{controller.error(new Error("fixture transport cut"));}}}),{status:200,headers:{"content-type":"text/event-stream"}});
  }
  if (body.messages[0].content === "permanent") return new Response('{"error":{"message":"fixture-secret","type":"invalid_request_error"}}',{status:401,headers:{"content-type":"application/json"}});
  if(calls<3) return new Response('{"error":{"message":"fixture-secret","type":"rate_limit_error"}}',{status:429,headers:{"content-type":"application/json","retry-after":"0"}});
  const chunks=[{id:"fixture",object:"chat.completion.chunk",created:1,model:"deepseek-v4-pro",choices:[{index:0,delta:{role:"assistant",content:"ok"},finish_reason:null}]},{id:"fixture",object:"chat.completion.chunk",created:1,model:"deepseek-v4-pro",choices:[{index:0,delta:{},finish_reason:"stop"}]}];
  return new Response(chunks.map(x=>"data: "+JSON.stringify(x)+"\n\n").join("")+"data: [DONE]\n\n",{status:200,headers:{"content-type":"text/event-stream"}});
};`
	if err := os.WriteFile(preload, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(dir, "node-fixture")
	if err := os.WriteFile(wrapper, []byte("#!/bin/sh\nexec node --import "+strconv.Quote(preload)+" \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := pibridge.Start(ctx, wrapper, dir, nil, pibridge.ProviderEnvironment)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	args := map[string]any{"provider": "deepseek", "model": "deepseek-v4-pro", "api_key": "fixture", "messages": []any{map[string]any{"role": "user", "content": "retry", "timestamp": 1}}, "options": map[string]any{"maxRetries": 99}}
	raw, n, err := retryGenerate(ctx, retryTestConfig(), func(ctx context.Context) (json.RawMessage, error) { return b.Call(ctx, "llm.generate", args, dir, nil) }, noProgress)
	if err != nil || n != 3 || !strings.Contains(string(raw), `"text":"ok"`) {
		t.Fatalf("attempts=%d result=%s error=%v", n, raw, err)
	}
	counts, _ := os.ReadFile(countFile)
	if strings.Count(string(counts), "request\n") != 3 {
		t.Fatalf("actual HTTP requests=%s", counts)
	}
	args["messages"] = []any{map[string]any{"role": "user", "content": "permanent", "timestamp": 1}}
	_, n, err = retryGenerate(ctx, retryTestConfig(), func(ctx context.Context) (json.RawMessage, error) { return b.Call(ctx, "llm.generate", args, dir, nil) }, noProgress)
	if n != 1 || err == nil || strings.Contains(err.Error(), "fixture-secret") {
		t.Fatalf("attempts=%d error=%v", n, err)
	}
	var failure *providerFailure
	if !errors.As(err, &failure) || failure.Code != "auth" || failure.Status != 401 || failure.ProviderCode != "invalid_request_error" {
		t.Fatalf("structured error lost: %#v", err)
	}
	counts, _ = os.ReadFile(countFile)
	if strings.Count(string(counts), "request\n") != 4 {
		t.Fatalf("permanent error retried: %s", counts)
	}
	args["messages"] = []any{map[string]any{"role": "user", "content": "delay", "timestamp": 1}}
	_, n, err = retryGenerate(ctx, retryTestConfig(), func(ctx context.Context) (json.RawMessage, error) { return b.Call(ctx, "llm.generate", args, dir, nil) }, noProgress)
	if !errors.As(err, &failure) || failure.Code != "deadline_exceeded" || failure.RetryAfter != time.Minute || n != 1 {
		t.Fatalf("retry-after not preserved/bounded: n=%d err=%#v", n, err)
	}
	args["messages"] = []any{map[string]any{"role": "user", "content": "broken-stream", "timestamp": 1}}
	raw, n, err = retryGenerate(ctx, retryTestConfig(), func(ctx context.Context) (json.RawMessage, error) { return b.Call(ctx, "llm.generate", args, dir, nil) }, noProgress)
	if err == nil || raw != nil || n != 1 {
		t.Fatalf("partial stream escaped/retried blindly: raw=%s n=%d err=%v", raw, n, err)
	}
	counts, _ = os.ReadFile(countFile)
	if strings.Count(string(counts), "request\n") != 6 {
		t.Fatalf("actual HTTP requests=%s", counts)
	}
}
