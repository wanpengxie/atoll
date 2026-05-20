package xhs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
)

// fakeRunner is a stub CoagentRunner that captures every Run call and
// returns a pre-configured result. Tests use it to verify the argv +
// payload assembly without spawning real subprocesses.
//
// The runner also reads any `--payload-file <path>` value off argv at
// call time and stashes the file bytes alongside the argv so test
// assertions can examine the payload without race-ing against the
// dispatch tempfile-cleanup defer (T102 FIX-2).
type fakeRunner struct {
	mu      sync.Mutex
	calls   []fakeRunnerCall
	result  CoagentResult
	err     error
	customs map[string]CoagentResult // override per type, optional
}

type fakeRunnerCall struct {
	Argv        []string
	Env         []string
	PayloadFile string // path captured from --payload-file flag
	PayloadBody []byte // body read from PayloadFile while still present
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		result: CoagentResult{
			ID:            "msg-FAKE-1",
			CorrelationID: "msg-FAKE-1",
			Kind:          "request",
		},
	}
}

// SetError configures Run to return err on the next call (and all
// subsequent calls until SetError(nil) clears it).
func (f *fakeRunner) SetError(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

// SetResultForType lets a test configure the per-type result so each
// invocation returns a distinct correlation_id.
func (f *fakeRunner) SetResultForType(typeName string, res CoagentResult) {
	f.mu.Lock()
	if f.customs == nil {
		f.customs = map[string]CoagentResult{}
	}
	f.customs[typeName] = res
	f.mu.Unlock()
}

func (f *fakeRunner) Run(_ context.Context, cfg RealConfig, argv []string) (CoagentResult, error) {
	f.mu.Lock()
	// Record the env the production runner would actually pass to the
	// child process (cfg.Env + the three Daemon overrides). This keeps
	// env-propagation assertions tied to the real shape callers see.
	call := fakeRunnerCall{
		Argv: append([]string{}, argv...),
		Env:  buildEnv(cfg),
	}
	// Capture payload-file contents BEFORE returning so the dispatch
	// caller's defer cleanup() does not unlink the file mid-assertion.
	for i := 0; i < len(argv); i++ {
		if argv[i] == "--payload-file" && i+1 < len(argv) {
			call.PayloadFile = argv[i+1]
			if body, rerr := os.ReadFile(argv[i+1]); rerr == nil {
				call.PayloadBody = body
			}
			break
		}
	}
	f.calls = append(f.calls, call)
	err := f.err
	res := f.result
	if f.customs != nil {
		// argv has the shape: ["ask", "--type", "<TYPE>", ...].
		typeName := argvType(argv)
		if v, ok := f.customs[typeName]; ok {
			res = v
		}
	}
	f.mu.Unlock()
	return res, err
}

// argvType returns the value of --type in argv, or "" when absent.
func argvType(argv []string) string {
	for i := 0; i < len(argv); i++ {
		if argv[i] == "--type" && i+1 < len(argv) {
			return argv[i+1]
		}
	}
	return ""
}

// lastCall returns the most recent recorded invocation.
func (f *fakeRunner) lastCall(t *testing.T) fakeRunnerCall {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		t.Fatal("expected at least one runner call")
	}
	return f.calls[len(f.calls)-1]
}

// extractPayload decodes the JSON value passed via --payload-file. The
// T102 FIX-2 contract is that RealProvider.dispatch NEVER writes the
// raw payload onto argv (cookie / image base64 must not appear in
// /proc/<pid>/cmdline); the fakeRunner captures the file body before the
// dispatch caller's deferred cleanup unlinks it.
func extractPayload(t *testing.T, c fakeRunnerCall) map[string]any {
	t.Helper()
	if c.PayloadFile == "" {
		t.Fatal("--payload-file missing from argv (T102 FIX-2: payload must not be inline on argv)")
	}
	if len(c.PayloadBody) == 0 {
		t.Fatal("payload file captured but body is empty")
	}
	// Defense-in-depth: make sure the legacy inline --payload flag
	// is NOT present.
	for i := 0; i < len(c.Argv); i++ {
		if c.Argv[i] == "--payload" {
			t.Fatal("--payload should not appear on argv after T102 FIX-2")
		}
	}
	var got map[string]any
	if err := json.Unmarshal(c.PayloadBody, &got); err != nil {
		t.Fatalf("decode payload (%d bytes): %v", len(c.PayloadBody), err)
	}
	return got
}

// extractAudience returns the --audience value.
func extractAudience(t *testing.T, c fakeRunnerCall) string {
	t.Helper()
	for i := 0; i < len(c.Argv); i++ {
		if c.Argv[i] == "--audience" && i+1 < len(c.Argv) {
			return c.Argv[i+1]
		}
	}
	t.Fatal("--audience missing from argv")
	return ""
}

// newTestProvider builds a RealProvider wired to a fakeRunner with a
// deterministic env so tests can assert env propagation.
func newTestProvider() (*RealProvider, *fakeRunner) {
	fr := newFakeRunner()
	cfg := RealConfig{
		DaemonHTTP: "http://daemon.test:7070",
		Token:      "test-token",
		ChannelID:  "ch-test",
		Env:        []string{"PATH=/usr/bin", "ALREADY=present"},
	}
	p := NewRealProvider(cfg).WithRunner(fr)
	return p, fr
}

func TestRealProvider_Publish_Dispatch(t *testing.T) {
	p, fr := newTestProvider()
	fr.SetResultForType(cmdTypePublish, CoagentResult{
		ID: "msg-publish", CorrelationID: "msg-publish", Kind: "request",
	})
	out, err := p.Publish(context.Background(), PublishArgs{
		Title:       "T",
		ContentPath: "/abs/path/to/note.md",
		Tags:        []string{"a", "b"},
		ImageData:   []map[string]any{{"type": "data", "value": "data:..."}},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	ack := out.(DispatchAck)
	if ack.CorrelationID != "msg-publish" {
		t.Fatalf("correlation_id = %q; want msg-publish", ack.CorrelationID)
	}
	if ack.Status != "dispatched" {
		t.Fatalf("status = %q; want dispatched", ack.Status)
	}

	call := fr.lastCall(t)
	if call.Argv[0] != "ask" {
		t.Fatalf("first argv = %q; want \"ask\"", call.Argv[0])
	}
	if extractAudience(t, call) != AdapterActor {
		t.Fatalf("audience = %q; want %q", extractAudience(t, call), AdapterActor)
	}
	if argvType(call.Argv) != cmdTypePublish {
		t.Fatalf("type = %q; want %q", argvType(call.Argv), cmdTypePublish)
	}
	payload := extractPayload(t, call)
	if payload["title"] != "T" {
		t.Fatalf("title = %v; want T", payload["title"])
	}
	if payload["content_path"] != "/abs/path/to/note.md" {
		t.Fatalf("content_path missing/wrong: %v", payload["content_path"])
	}
	if _, has := payload["content"]; has {
		t.Fatalf("inline content should NOT be forwarded; got %v", payload["content"])
	}

	// env propagation
	envMap := envToMap(call.Env)
	if envMap[EnvDaemonHTTP] != "http://daemon.test:7070" {
		t.Fatalf("env %s = %q; want http://daemon.test:7070", EnvDaemonHTTP, envMap[EnvDaemonHTTP])
	}
	if envMap[EnvDaemonToken] != "test-token" {
		t.Fatalf("env %s = %q; want test-token", EnvDaemonToken, envMap[EnvDaemonToken])
	}
	if envMap[EnvChannelID] != "ch-test" {
		t.Fatalf("env %s = %q; want ch-test", EnvChannelID, envMap[EnvChannelID])
	}
	if envMap["ALREADY"] != "present" {
		t.Fatalf("expected pass-through env to survive; got %q", envMap["ALREADY"])
	}
}

func TestRealProvider_AllCommandTypes(t *testing.T) {
	cases := []struct {
		name     string
		fn       func(context.Context, *RealProvider) (any, error)
		wantType string
	}{
		{"publish", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.Publish(ctx, PublishArgs{Title: "x", ContentPath: "/x.md"})
		}, cmdTypePublish},
		{"search", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.Search(ctx, SearchArgs{Keyword: "k", Limit: 5})
		}, cmdTypeSearch},
		{"recent", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.GetMyRecent(ctx, GetMyRecentArgs{Limit: 5})
		}, cmdTypeRecentFetch},
		{"get-note", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.GetNote(ctx, GetNoteArgs{NoteID: "n1", XsecToken: "tk"})
		}, cmdTypeNoteFetch},
		{"sync-cookie", func(ctx context.Context, p *RealProvider) (any, error) {
			return p.SyncCookie(ctx, SyncCookieArgs{})
		}, cmdTypeCookieSync},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, fr := newTestProvider()
			fr.SetResultForType(tc.wantType, CoagentResult{
				ID: "msg-" + tc.name, CorrelationID: "msg-" + tc.name, Kind: "request",
			})
			out, err := tc.fn(context.Background(), p)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			ack := out.(DispatchAck)
			if ack.CorrelationID == "" {
				t.Fatalf("missing correlation_id")
			}
			call := fr.lastCall(t)
			if argvType(call.Argv) != tc.wantType {
				t.Fatalf("type = %q; want %q", argvType(call.Argv), tc.wantType)
			}
			if extractAudience(t, call) != AdapterActor {
				t.Fatalf("audience = %q; want %q", extractAudience(t, call), AdapterActor)
			}
		})
	}
}

// TestRealProvider_GetNote_DispatchShape carries forward the legacy
// contract: url-only / all-three / note-id+token must all pass through
// to the daemon. xsec-token-alone is rejected at the CLI layer
// (note_test.go) so we do not exercise it here.
func TestRealProvider_GetNote_DispatchShape(t *testing.T) {
	cases := []struct {
		name        string
		args        GetNoteArgs
		wantNoteID  any
		wantURL     any
		wantXsecTok any
	}{
		{
			name:        "url-only",
			args:        GetNoteArgs{URL: "https://www.xiaohongshu.com/explore/abc?xsec_token=tk1"},
			wantNoteID:  nil,
			wantURL:     "https://www.xiaohongshu.com/explore/abc?xsec_token=tk1",
			wantXsecTok: nil,
		},
		{
			name: "all-three-given",
			args: GetNoteArgs{
				NoteID: "n1", URL: "https://x.com/n1?xsec_token=tk3", XsecToken: "tk3",
			},
			wantNoteID:  "n1",
			wantURL:     "https://x.com/n1?xsec_token=tk3",
			wantXsecTok: "tk3",
		},
		{
			name:        "note-id-and-token",
			args:        GetNoteArgs{NoteID: "n2", XsecToken: "tk4"},
			wantNoteID:  "n2",
			wantURL:     nil,
			wantXsecTok: "tk4",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, fr := newTestProvider()
			if _, err := p.GetNote(context.Background(), tc.args); err != nil {
				t.Fatalf("get-note: %v", err)
			}
			call := fr.lastCall(t)
			if argvType(call.Argv) != cmdTypeNoteFetch {
				t.Fatalf("type = %q; want %q", argvType(call.Argv), cmdTypeNoteFetch)
			}
			payload := extractPayload(t, call)
			assertOptional := func(field string, want any) {
				got, has := payload[field]
				if want == nil {
					if has {
						t.Fatalf("%s should be absent; got %v", field, got)
					}
					return
				}
				if got != want {
					t.Fatalf("%s mismatch: want %v, got %v", field, want, got)
				}
			}
			assertOptional("note_id", tc.wantNoteID)
			assertOptional("url", tc.wantURL)
			assertOptional("xsec_token", tc.wantXsecTok)
		})
	}
}

// TestPublishRealMode_NoContentInline guards the legacy contract:
// real mode must NOT carry inline `content` (only `content_path`).
func TestPublishRealMode_NoContentInline(t *testing.T) {
	p, fr := newTestProvider()
	_, err := p.Publish(context.Background(), PublishArgs{
		Title:       "T",
		ContentPath: "/abs/path/to/note.md",
		Content:     "INLINE-SHOULD-NOT-BE-SENT",
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	payload := extractPayload(t, fr.lastCall(t))
	if v, has := payload["content"]; has {
		t.Fatalf("real RPC payload must NOT contain inline 'content'; got %v", v)
	}
	if payload["content_path"] != "/abs/path/to/note.md" {
		t.Fatalf("content_path missing/wrong: %v", payload["content_path"])
	}
}

// TestRealProvider_CoagentReject — coagent exit=3 with reject JSON
// surfaces as CodeError{Code=<reason>}.
func TestRealProvider_CoagentReject(t *testing.T) {
	p, fr := newTestProvider()
	fr.SetError(&CodeError{Code: "harness_request_audience_invalid", Msg: "type has no handler"})
	_, err := p.Publish(context.Background(), PublishArgs{Title: "T", ContentPath: "/x.md"})
	if err == nil {
		t.Fatal("expected error")
	}
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T %v", err, err)
	}
	if ce.Code != "harness_request_audience_invalid" {
		t.Fatalf("code = %q; want harness_request_audience_invalid", ce.Code)
	}
}

// TestRejectFromStderr_FlatStructure — FIX-6 §9 (codex t94b): coagent
// CLI writes a flat {"error":"reject","reason":"...","detail":"..."}
// trailing JSON line; xhs RealProvider must extract the reason
// verbatim instead of degrading to coagent_reject.
func TestRejectFromStderr_FlatStructure(t *testing.T) {
	stderr := "coagent: ask rejected: schema_violation: payload.title required\n" +
		`{"error":"reject","reason":"schema_violation","detail":"payload.title required"}` + "\n"
	err := rejectFromStderr(stderr)
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T %v", err, err)
	}
	if ce.Code != "schema_violation" {
		t.Errorf("code = %q, want schema_violation", ce.Code)
	}
	if ce.Msg != "payload.title required" {
		t.Errorf("msg = %q, want payload.title required", ce.Msg)
	}
}

// TestRejectFromStderr_NestedFallback — legacy coagent versions emit
// a nested {"error":{"reason":...}} body. Parser must still pull the
// reason for backward compat.
func TestRejectFromStderr_NestedFallback(t *testing.T) {
	stderr := `{"error":{"reason":"actor_not_registered","detail":"unknown actor"}}`
	err := rejectFromStderr(stderr)
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T", err)
	}
	if ce.Code != "actor_not_registered" {
		t.Errorf("code = %q, want actor_not_registered (nested form)", ce.Code)
	}
}

// TestRejectFromStderr_NoJSON_GenericFallback — when stderr has no
// JSON at all the parser falls back to the generic code so callers
// at least see *something*.
func TestRejectFromStderr_NoJSON_GenericFallback(t *testing.T) {
	err := rejectFromStderr("coagent: ask rejected: <opaque text without JSON>")
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T", err)
	}
	if ce.Code != "coagent_reject" {
		t.Errorf("code = %q, want coagent_reject (generic fallback)", ce.Code)
	}
}

// TestRealProvider_RejectReasonPassthrough — e2e: stub the runner so
// it returns exit=3 + the canonical coagent stderr shape and verify
// the RealProvider surfaces the harness reason as the CodeError code.
// This anchors the entire xhs → coagent reject pipeline.
func TestRealProvider_RejectReasonPassthrough(t *testing.T) {
	p, fr := newTestProvider()
	fr.SetError(rejectFromStderr(
		"coagent: ask rejected: handler_actor_binding_mismatch: tool:xhs not bound to daemon_rpc\n" +
			`{"error":"reject","reason":"handler_actor_binding_mismatch","detail":"tool:xhs not bound to daemon_rpc"}` + "\n",
	))
	_, err := p.Publish(context.Background(), PublishArgs{Title: "T", ContentPath: "/x.md"})
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T %v", err, err)
	}
	if ce.Code != "handler_actor_binding_mismatch" {
		t.Errorf("code = %q, want handler_actor_binding_mismatch (passthrough)", ce.Code)
	}
	if !strings.Contains(ce.Msg, "tool:xhs not bound to daemon_rpc") {
		t.Errorf("msg = %q, want detail to flow through", ce.Msg)
	}
}

// TestRealProvider_MissingCorrelationID — runner returns success but
// without correlation_id → invalid_daemon_response.
func TestRealProvider_MissingCorrelationID(t *testing.T) {
	p, fr := newTestProvider()
	fr.SetResultForType(cmdTypePublish, CoagentResult{ID: "msg-id", Kind: "request"})
	_, err := p.Publish(context.Background(), PublishArgs{Title: "T", ContentPath: "/x.md"})
	if err == nil {
		t.Fatal("expected error")
	}
	var ce *CodeError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CodeError, got %T", err)
	}
	if ce.Code != "invalid_daemon_response" {
		t.Fatalf("code = %q; want invalid_daemon_response", ce.Code)
	}
}

// TestRealProvider_Name sanity.
func TestRealProvider_Name(t *testing.T) {
	p, _ := newTestProvider()
	if p.Name() != "real" {
		t.Fatalf("name = %q; want real", p.Name())
	}
}

// TestRealProvider_LargePayload_NotInArgv exercises the T102 FIX-2
// claude 98-3 major fix: a 200 KiB image payload — which would crash
// the legacy `--payload <json>` path with E2BIG on most kernels (Linux
// ARG_MAX is typically 128 KiB) — must succeed because the payload now
// travels via --payload-file. The test asserts (1) the request reaches
// the runner with --payload-file in argv, (2) the legacy --payload
// flag is absent, (3) the file body decodes to the original params, and
// (4) the file is unlinked after dispatch returns.
func TestRealProvider_LargePayload_NotInArgv(t *testing.T) {
	p, fr := newTestProvider()

	// Build a ~200 KiB base64-ish blob. Larger than typical ARG_MAX so
	// the legacy code path would have failed on real spawn.
	const largeSize = 200 * 1024
	bigBlob := strings.Repeat("A", largeSize)

	_, err := p.Publish(context.Background(), PublishArgs{
		Title:       "big-publish",
		ContentPath: "/abs/path/to/note.md",
		ImageData:   []map[string]any{{"type": "data", "value": bigBlob}},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	call := fr.lastCall(t)

	// 1) argv contains --payload-file, NOT --payload.
	var payloadFile string
	for i := 0; i < len(call.Argv); i++ {
		if call.Argv[i] == "--payload" {
			t.Fatalf("argv must not contain --payload after T102 FIX-2 (got %v)", call.Argv)
		}
		if call.Argv[i] == "--payload-file" && i+1 < len(call.Argv) {
			payloadFile = call.Argv[i+1]
		}
	}
	if payloadFile == "" {
		t.Fatalf("--payload-file missing from argv: %v", call.Argv)
	}

	// 2) Argv must not contain the giant blob verbatim (defense in
	// depth — the previous attempt at writing a payload-file flag could
	// regress by accidentally also appending --payload).
	for _, arg := range call.Argv {
		if strings.Contains(arg, bigBlob[:1024]) {
			t.Fatal("argv element contains the giant blob — payload leaked back onto argv")
		}
	}

	// 3) The captured file body decodes back to the original params.
	if len(call.PayloadBody) < largeSize {
		t.Fatalf("captured payload body length %d < blob %d", len(call.PayloadBody), largeSize)
	}
	var decoded map[string]any
	if jerr := json.Unmarshal(call.PayloadBody, &decoded); jerr != nil {
		t.Fatalf("decode payload body: %v", jerr)
	}
	imgs, ok := decoded["images"].([]any)
	if !ok || len(imgs) != 1 {
		t.Fatalf("images shape unexpected: %T %v", decoded["images"], decoded["images"])
	}

	// 4) The tempfile is removed before Publish returns.
	if _, statErr := os.Stat(payloadFile); !os.IsNotExist(statErr) {
		t.Fatalf("payload tempfile %q should be removed after dispatch; stat err = %v",
			payloadFile, statErr)
	}
}

// TestRealProvider_PayloadFile_Permissions ensures the staged payload
// file is owner-read/write only — cookies must not be world-readable
// even for the brief tempfile lifetime.
func TestRealProvider_PayloadFile_Permissions(t *testing.T) {
	// We hook into writePayloadTempFile directly because the dispatch
	// path removes the file before Publish returns. The function is
	// the same one dispatch uses; this is a focused permission check.
	path, cleanup, err := writePayloadTempFile([]byte(`{"cookie":"secret"}`))
	if err != nil {
		t.Fatalf("writePayloadTempFile: %v", err)
	}
	defer cleanup()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("perm = %v, want 0600", perm)
	}
}

// TestLoadRealConfigFromEnv_RequiresAllVars covers env validation.
func TestLoadRealConfigFromEnv_RequiresAllVars(t *testing.T) {
	t.Setenv(EnvDaemonHTTP, "")
	t.Setenv(EnvDaemonHTTPAlt, "")
	t.Setenv(EnvDaemonToken, "")
	t.Setenv(EnvDaemonTokenAlt, "")
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

// TestLoadRealConfigFromEnv_HonoursLegacyEnvAliases — the rename from
// COAGENT_DAEMON_HTTP → DAEMON_URL must keep old deployments working.
func TestLoadRealConfigFromEnv_HonoursLegacyEnvAliases(t *testing.T) {
	t.Setenv(EnvDaemonHTTP, "")
	t.Setenv(EnvDaemonToken, "")
	t.Setenv(EnvDaemonHTTPAlt, "http://legacy:7070")
	t.Setenv(EnvDaemonTokenAlt, "legacy-tok")
	t.Setenv(EnvChannelID, "ch-legacy")

	cfg, err := LoadRealConfigFromEnv()
	if err != nil {
		t.Fatalf("legacy env load: %v", err)
	}
	if cfg.DaemonHTTP != "http://legacy:7070" {
		t.Fatalf("legacy DaemonHTTP = %q", cfg.DaemonHTTP)
	}
	if cfg.Token != "legacy-tok" {
		t.Fatalf("legacy Token = %q", cfg.Token)
	}
}

// TestLoadRealConfigFromEnv_RequiresAbsoluteURL guards bad URLs.
func TestLoadRealConfigFromEnv_RequiresAbsoluteURL(t *testing.T) {
	t.Setenv(EnvDaemonToken, "tok")
	t.Setenv(EnvDaemonTokenAlt, "")
	t.Setenv(EnvChannelID, "ch-1")
	t.Setenv(EnvDaemonHTTPAlt, "")

	bad := []string{"not-a-url", "127.0.0.1:7070", "//127.0.0.1:7070", "/relative/path"}
	for _, raw := range bad {
		t.Setenv(EnvDaemonHTTP, raw)
		_, err := LoadRealConfigFromEnv()
		if err == nil {
			t.Fatalf("expected error for non-absolute %q", raw)
		}
		var ce *CodeError
		if !errors.As(err, &ce) || ce.Code != "config_invalid" {
			t.Fatalf("expected config_invalid for %q, got %v", raw, err)
		}
	}
	t.Setenv(EnvDaemonHTTP, "http://127.0.0.1:7070")
	if _, err := LoadRealConfigFromEnv(); err != nil {
		t.Fatalf("expected ok for absolute URL, got %v", err)
	}
}

// envToMap parses an env slice (KEY=VAL) into a map for assertions.
func envToMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, kv := range env {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		out[kv[:eq]] = kv[eq+1:]
	}
	return out
}
