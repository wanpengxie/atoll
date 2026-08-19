package mcphttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/mcpcodec"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/toolsurface"
)

func TestGenerationScopedHTTPMCPAuthCatalogAndRetirement(t *testing.T) {
	life, retire := context.WithCancel(context.Background())
	surface, err := toolsurface.Assemble([]driverproto.ToolSpec{{Name: "call_actor", Description: "call", Schema: json.RawMessage(`{"type":"object"}`)}}, toolsurface.Claude, driverproto.Situation{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := Start(life, surface, func() mcpcodec.InvokeFunc {
		return func(context.Context, driverproto.ToolInvocation) driverproto.ToolResult {
			return driverproto.ToolResult{Text: `{"ok":true}`}
		}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Servers map[string]struct {
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(server.Config(), &cfg); err != nil {
		t.Fatal(err)
	}
	atoll := cfg.Servers[toolsurface.ClaudeServer]
	requestBody := []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	request, _ := http.NewRequest(http.MethodPost, atoll.URL, bytes.NewReader(requestBody))
	request.Header.Set("Authorization", atoll.Headers["Authorization"])
	request.Header.Set("Origin", "https://attacker.example")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign origin status=%d", response.StatusCode)
	}
	request, _ = http.NewRequest(http.MethodPost, atoll.URL, bytes.NewReader(requestBody))
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", response.StatusCode)
	}
	request, _ = http.NewRequest(http.MethodPost, atoll.URL, bytes.NewReader(requestBody))
	request.Header.Set("Authorization", atoll.Headers["Authorization"])
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"call_actor"`) {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
	retire()
	select {
	case <-server.Done():
	case <-time.After(time.Second):
		t.Fatal("generation retirement left listener alive")
	}
}

func TestCallbackSaturationRejectsOneCallWithoutRetiringGeneration(t *testing.T) {
	life, retire := context.WithCancel(context.Background())
	defer retire()
	surface, err := toolsurface.Assemble([]driverproto.ToolSpec{{Name: "call_actor", Description: "call", Schema: json.RawMessage(`{"type":"object"}`)}}, toolsurface.Claude, driverproto.Situation{})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	var calls atomic.Int32
	server, err := Start(life, surface, func() mcpcodec.InvokeFunc {
		return func(context.Context, driverproto.ToolInvocation) driverproto.ToolResult {
			calls.Add(1)
			entered <- struct{}{}
			<-release
			return driverproto.ToolResult{Text: `{"ok":true}`}
		}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	var cfg struct {
		Servers map[string]struct {
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(server.Config(), &cfg); err != nil {
		t.Fatal(err)
	}
	atoll := cfg.Servers[toolsurface.ClaudeServer]
	client := &http.Client{Timeout: time.Second}
	call := func(id int) string {
		body := []byte(`{"jsonrpc":"2.0","id":` + fmt.Sprint(id) + `,"method":"tools/call","params":{"name":"call_actor","arguments":{}}}`)
		request, _ := http.NewRequest(http.MethodPost, atoll.URL, bytes.NewReader(body))
		request.Header.Set("Authorization", atoll.Headers["Authorization"])
		response, err := client.Do(request)
		if err != nil {
			return err.Error()
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return string(raw)
	}
	var group sync.WaitGroup
	group.Add(16)
	for id := 1; id <= 16; id++ {
		go func(id int) { defer group.Done(); _ = call(id) }(id)
	}
	for i := 0; i < 16; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("did not fill callback admission slots")
		}
	}
	overloaded := call(17)
	close(release)
	group.Wait()
	if calls.Load() != 16 || !strings.Contains(overloaded, "concurrency limit reached") {
		t.Fatalf("calls=%d overloaded=%s", calls.Load(), overloaded)
	}
	if ping := callRPC(t, client, atoll.URL, atoll.Headers["Authorization"], `{"jsonrpc":"2.0","id":18,"method":"ping","params":{}}`); !strings.Contains(ping, `"id":18`) {
		t.Fatalf("generation did not survive saturation: %s", ping)
	}
}

func callRPC(t *testing.T, client *http.Client, url, authorization, body string) string {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	request.Header.Set("Authorization", authorization)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return string(raw)
}
