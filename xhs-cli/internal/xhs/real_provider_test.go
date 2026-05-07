package xhs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeDaemon 用于测试 RealProvider 与 daemon /rpc endpoint 的契约。
type fakeDaemon struct {
	server         *httptest.Server
	lastMethod     string
	lastParams     map[string]any
	lastAuthHeader string
	respondErr     bool
	respondMissing bool // ok=true 但 result 缺 correlation_id
	respondNonOK   bool // ok=false 带 error
	emptyBody      bool
}

func newFakeDaemon() *fakeDaemon {
	d := &fakeDaemon{}
	d.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		d.lastAuthHeader = r.Header.Get("Authorization")

		body, _ := io.ReadAll(r.Body)
		var req struct {
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.Unmarshal(body, &req)
		d.lastMethod = req.Method
		d.lastParams = req.Params

		w.Header().Set("Content-Type", "application/json")
		switch {
		case d.emptyBody:
			w.WriteHeader(http.StatusOK)
			// write nothing
		case d.respondNonOK:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": false,
				"error": map[string]any{
					"code":    "rpc_handler_not_found",
					"message": "no handler for device.command.send",
				},
			})
		case d.respondMissing:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":     true,
				"result": map[string]any{},
			})
		case d.respondErr:
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok": true,
				"result": map[string]any{
					"correlation_id": "01HCORRELATION",
					"self_check": map[string]any{
						"due_at": "2026-05-08T06:00:00Z",
					},
				},
			})
		}
	}))
	return d
}

func newRealProviderForTest(d *fakeDaemon) *RealProvider {
	return NewRealProvider(RealConfig{
		DaemonHTTP: d.server.URL,
		Token:      "test-token",
		ChannelID:  "ch-test",
	})
}

func TestRealProvider_Publish_Dispatch(t *testing.T) {
	d := newFakeDaemon()
	defer d.server.Close()
	p := newRealProviderForTest(d)

	out, err := p.Publish(context.Background(), PublishArgs{
		Title:   "T",
		Content: "body",
		Tags:    []string{"a", "b"},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	ack := out.(DispatchAck)
	if ack.CorrelationID != "01HCORRELATION" {
		t.Fatalf("correlation_id mismatch: %q", ack.CorrelationID)
	}
	if ack.Status != "dispatched" {
		t.Fatalf("status mismatch: %q", ack.Status)
	}
	if ack.SelfCheck["due_at"] != "2026-05-08T06:00:00Z" {
		t.Fatalf("self_check mismatch: %v", ack.SelfCheck)
	}
	if d.lastMethod != "device.command.send" {
		t.Fatalf("method mismatch: %q", d.lastMethod)
	}
	if d.lastAuthHeader != "Bearer test-token" {
		t.Fatalf("auth header mismatch: %q", d.lastAuthHeader)
	}
	if d.lastParams["channel_id"] != "ch-test" {
		t.Fatalf("channel_id mismatch: %v", d.lastParams["channel_id"])
	}
	if d.lastParams["type"] != "xhs.publish" {
		t.Fatalf("type mismatch: %v", d.lastParams["type"])
	}
	innerParams, ok := d.lastParams["params"].(map[string]any)
	if !ok {
		t.Fatalf("params inner not object: %T", d.lastParams["params"])
	}
	if innerParams["title"] != "T" {
		t.Fatalf("title mismatch: %v", innerParams["title"])
	}
}

func TestRealProvider_AllCommandTypes(t *testing.T) {
	d := newFakeDaemon()
	defer d.server.Close()
	p := newRealProviderForTest(d)

	cases := []struct {
		name     string
		fn       func(context.Context, *RealProvider) (any, error)
		wantType string
	}{
		{"publish", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.Publish(ctx, PublishArgs{Title: "x", Content: "y"})
		}, "xhs.publish"},
		{"search", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.Search(ctx, SearchArgs{Keyword: "k", Limit: 5})
		}, "xhs.search"},
		{"get-my-recent", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.GetMyRecent(ctx, GetMyRecentArgs{Limit: 5})
		}, "xhs.get-my-recent"},
		{"get-note", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.GetNote(ctx, GetNoteArgs{NoteID: "n1"})
		}, "xhs.get-note"},
		{"publish-status", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.PublishStatus(ctx, PublishStatusArgs{NoteID: "n1"})
		}, "xhs.publish-status"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := tc.fn(context.Background(), p)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			ack := out.(DispatchAck)
			if ack.CorrelationID == "" {
				t.Fatal("missing correlation_id")
			}
			if d.lastParams["type"] != tc.wantType {
				t.Fatalf("type mismatch: want %q, got %v", tc.wantType, d.lastParams["type"])
			}
		})
	}
}

func TestRealProvider_DaemonUnreachable(t *testing.T) {
	// 用一个立即关掉的 server，模拟连接失败。
	d := newFakeDaemon()
	d.server.Close()

	p := newRealProviderForTest(d)
	_, err := p.Publish(context.Background(), PublishArgs{Title: "T", Content: "x"})
	if err == nil {
		t.Fatal("expected error for unreachable daemon")
	}
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T %v", err, err)
	}
	if ce.Code != "daemon_unreachable" {
		t.Fatalf("expected daemon_unreachable, got %q", ce.Code)
	}
	if !strings.Contains(ce.Msg, "failed to reach daemon") {
		t.Fatalf("message should mention daemon: %q", ce.Msg)
	}
}

func TestRealProvider_DaemonReturnsError(t *testing.T) {
	d := newFakeDaemon()
	d.respondNonOK = true
	defer d.server.Close()

	p := newRealProviderForTest(d)
	_, err := p.Publish(context.Background(), PublishArgs{Title: "T", Content: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T", err)
	}
	if ce.Code != "rpc_handler_not_found" {
		t.Fatalf("expected rpc_handler_not_found, got %q", ce.Code)
	}
}

func TestRealProvider_DaemonResultMissingCorrelation(t *testing.T) {
	d := newFakeDaemon()
	d.respondMissing = true
	defer d.server.Close()

	p := newRealProviderForTest(d)
	_, err := p.Publish(context.Background(), PublishArgs{Title: "T", Content: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T", err)
	}
	if ce.Code != "invalid_daemon_response" {
		t.Fatalf("expected invalid_daemon_response, got %q", ce.Code)
	}
}

func TestRealProvider_EmptyBody(t *testing.T) {
	d := newFakeDaemon()
	d.emptyBody = true
	defer d.server.Close()

	p := newRealProviderForTest(d)
	_, err := p.Publish(context.Background(), PublishArgs{Title: "T", Content: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T", err)
	}
	if ce.Code != "invalid_daemon_response" {
		t.Fatalf("expected invalid_daemon_response, got %q", ce.Code)
	}
}

func TestRealProvider_HTTPError(t *testing.T) {
	d := newFakeDaemon()
	d.respondErr = true
	defer d.server.Close()

	p := newRealProviderForTest(d)
	_, err := p.Publish(context.Background(), PublishArgs{Title: "T", Content: "x"})
	if err == nil {
		t.Fatal("expected error")
	}
	// http.Error 写的是 plain text → JSON decode 失败 → invalid_daemon_response
	var ce *CodeError
	if !errors.As(err, &ce) || ce.Code != "invalid_daemon_response" {
		t.Fatalf("expected invalid_daemon_response, got %v", err)
	}
}

func TestRealProvider_Name(t *testing.T) {
	d := newFakeDaemon()
	defer d.server.Close()
	if newRealProviderForTest(d).Name() != "real" {
		t.Fatal("real provider name mismatch")
	}
}

func TestLoadRealConfigFromEnv_RequiresAllVars(t *testing.T) {
	t.Setenv(EnvDaemonHTTP, "")
	t.Setenv(EnvDaemonToken, "")
	t.Setenv(EnvChannelID, "")

	if _, err := LoadRealConfigFromEnv(); err == nil {
		t.Fatal("expected error when all envs missing")
	}

	t.Setenv(EnvDaemonHTTP, "http://localhost:7070")
	if _, err := LoadRealConfigFromEnv(); err == nil {
		t.Fatal("expected error when token missing")
	}

	t.Setenv(EnvDaemonToken, "tok")
	if _, err := LoadRealConfigFromEnv(); err == nil {
		t.Fatal("expected error when channel id missing")
	}

	t.Setenv(EnvChannelID, "ch-1")
	cfg, err := LoadRealConfigFromEnv()
	if err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	if cfg.DaemonHTTP != "http://localhost:7070" || cfg.Token != "tok" || cfg.ChannelID != "ch-1" {
		t.Fatalf("config mismatch: %+v", cfg)
	}
}
