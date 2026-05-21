package feishu_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/adapters/feishu"
	"github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ----------------------------------------------------------------------
// Shared test scaffolding (Feishu mock server + framework wiring).
// ----------------------------------------------------------------------

type fakeFeishu struct {
	mu        sync.Mutex
	tokenHit  int
	sendHit   int
	createHit int

	// raw payloads captured for assertion
	lastSendBody   []byte
	lastCreateBody []byte

	// configurable failure modes
	sendCode   int // override envelope code
	sendMsg    string
	tokenCode  int
	failOnSend bool   // simulate transport failure
	tokenValue string // emitted token (default "TOKEN-abcdef1234567890")

	// request headers captured
	lastAuth string
}

func newFakeFeishu() *fakeFeishu {
	return &fakeFeishu{tokenValue: "TOKEN-abcdef1234567890"}
}

func (f *fakeFeishu) serve(t *testing.T) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v3/tenant_access_token/internal",
		func(w http.ResponseWriter, r *http.Request) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.tokenHit++
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), "app_id") {
				http.Error(w, "missing app_id", http.StatusBadRequest)
				return
			}
			resp := map[string]any{
				"code":                f.tokenCode,
				"msg":                 "ok",
				"tenant_access_token": f.tokenValue,
				"expire":              7200,
			}
			_ = json.NewEncoder(w).Encode(resp)
		})
	mux.HandleFunc("/im/v1/messages",
		func(w http.ResponseWriter, r *http.Request) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.sendHit++
			f.lastAuth = r.Header.Get("Authorization")
			body, _ := io.ReadAll(r.Body)
			f.lastSendBody = body
			if f.failOnSend {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			code := f.sendCode
			msg := f.sendMsg
			if msg == "" {
				msg = "ok"
			}
			resp := map[string]any{
				"code": code,
				"msg":  msg,
				"data": map[string]any{
					"message_id": "om_abc123",
					"chat_id":    "oc_chat_001",
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
		})
	mux.HandleFunc("/im/v1/chats",
		func(w http.ResponseWriter, r *http.Request) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.createHit++
			body, _ := io.ReadAll(r.Body)
			f.lastCreateBody = body
			resp := map[string]any{
				"code": 0,
				"msg":  "ok",
				"data": map[string]any{"chat_id": "oc_new_chat"},
			}
			_ = json.NewEncoder(w).Encode(resp)
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeFeishu) sendCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sendHit
}

func (f *fakeFeishu) tokenCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tokenHit
}

func (f *fakeFeishu) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createHit
}

type fakeChain struct {
	mu      sync.Mutex
	written []*message.Envelope
	results []harness.WriteResult
}

func (c *fakeChain) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *env
	c.written = append(c.written, &cp)
	if len(c.results) > 0 {
		res := c.results[0]
		c.results = c.results[1:]
		if res.MessageID == "" {
			res.MessageID = env.ID
		}
		return res, nil
	}
	return harness.WriteResult{MessageID: env.ID, Seq: int64(len(c.written))}, nil
}

func (c *fakeChain) snapshot() []*message.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*message.Envelope, len(c.written))
	copy(out, c.written)
	return out
}

type memoryActorRegistry struct {
	rows map[actor.ActorID]actorreg.Record
}

func newMemoryActorRegistry() *memoryActorRegistry {
	return &memoryActorRegistry{rows: map[actor.ActorID]actorreg.Record{}}
}
func (r *memoryActorRegistry) Insert(_ context.Context, rec actorreg.Record) error {
	r.rows[rec.ID] = rec
	return nil
}
func (r *memoryActorRegistry) Lookup(_ context.Context, id actor.ActorID) (actorreg.Record, bool, error) {
	rec, ok := r.rows[id]
	return rec, ok, nil
}
func (r *memoryActorRegistry) Exists(_ context.Context, id actor.ActorID) (bool, error) {
	_, ok := r.rows[id]
	return ok, nil
}
func (r *memoryActorRegistry) ListActive(_ context.Context) ([]actorreg.Record, error) {
	out := make([]actorreg.Record, 0, len(r.rows))
	for _, rec := range r.rows {
		out = append(out, rec)
	}
	return out, nil
}
func (r *memoryActorRegistry) Deregister(_ context.Context, id actor.ActorID, at int64) error {
	rec := r.rows[id]
	rec.DeregisteredAt = at
	r.rows[id] = rec
	return nil
}

type recordingLogger struct {
	mu    sync.Mutex
	lines []string
	args  []any
}

func (l *recordingLogger) record(level, msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, level+":"+msg)
	l.args = append(l.args, args...)
}
func (l *recordingLogger) Debug(msg string, args ...any) { l.record("debug", msg, args) }
func (l *recordingLogger) Info(msg string, args ...any)  { l.record("info", msg, args) }
func (l *recordingLogger) Warn(msg string, args ...any)  { l.record("warn", msg, args) }
func (l *recordingLogger) Error(msg string, args ...any) { l.record("error", msg, args) }

func (l *recordingLogger) dump() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := strings.Join(l.lines, "\n")
	for _, a := range l.args {
		if v, ok := a.(string); ok {
			out += " " + v
		}
	}
	return out
}

// ----------------------------------------------------------------------
// Setup helper: build Manager + feishu Module pointed at fakeFeishu.
// ----------------------------------------------------------------------

type setupResult struct {
	mgr    *framework.Manager
	chain  *fakeChain
	lookup *framework.MemoryRequestLookup
	fake   *fakeFeishu
	logger *recordingLogger
	creds  framework.CredentialStore
	tregs  *framework.InMemoryTypeRegistry
}

func setup(t *testing.T, mods ...func(*feishu.Module)) *setupResult {
	t.Helper()
	fake := newFakeFeishu()
	srv := fake.serve(t)

	logger := &recordingLogger{}
	credStore := framework.NewMemoryCredentialStore()

	mod := feishu.New(
		feishu.WithBaseURL(srv.URL),
		feishu.WithDeps(framework.Deps{
			Logger:  logger,
			Metrics: framework.NoopMetrics{},
			Clock:   time.Now,
		}),
		feishu.WithMaxPendingMs(2_000),
	)
	scopedCreds := framework.NewScopedCredentialStoreForDeclaration(credStore, mod.Declares())
	_ = scopedCreds.Put(context.Background(), feishu.CredKeyAppID, "cli_app_001")
	_ = scopedCreds.Put(context.Background(), feishu.CredKeyAppSecret, "SECRET-zxcvbn0987654321")
	for _, m := range mods {
		m(mod)
	}

	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actorreg.Record{
		ID:      "tool:feishu-adapter",
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
	})

	chain := &fakeChain{}
	lookup := framework.NewMemoryRequestLookup(nil)
	tregs := framework.NewInMemoryTypeRegistry()

	mgr, err := framework.NewManager(framework.ManagerConfig{
		ChannelID:       "channel:test",
		ActorRegistry:   registry,
		TypeRegistry:    tregs,
		HarnessChain:    chain,
		RequestLookup:   lookup,
		Logger:          logger,
		CredentialStore: credStore,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background(), []adapter.Module{mod}); err != nil {
		t.Fatalf("Install: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	return &setupResult{
		mgr: mgr, chain: chain, lookup: lookup, fake: fake,
		logger: logger, creds: scopedCreds, tregs: tregs,
	}
}

func newRequest(typ, id, payload string) *message.Envelope {
	return &message.Envelope{
		ID:         message.ID(id),
		TS:         time.Now().UnixMilli(),
		ChannelID:  "channel:test",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:author"},
		Kind:       message.KindRequest,
		Type:       typ,
		Payload:    json.RawMessage(payload),
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{"tool:feishu-adapter"},
	}
}

// ----------------------------------------------------------------------
// Acceptance tests
// ----------------------------------------------------------------------

func TestInstallRegistersTypesInTypeRegistry(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	for _, want := range []string{feishu.TypeChatSend, feishu.TypeChatCreate} {
		row, ok, _ := s.tregs.Lookup(ctx, want)
		if !ok {
			t.Fatalf("type_registry missing %s; rows=%+v", want, s.tregs)
		}
		if row.HandlerActorID != "tool:feishu-adapter" {
			t.Fatalf("type=%s handler=%s want tool:feishu-adapter", want, row.HandlerActorID)
		}
		if row.HandlerBinding != actor.BindingRuntimeOutbound {
			t.Fatalf("type=%s binding=%s want runtime_outbound", want, row.HandlerBinding)
		}
	}
}

func TestInstallRejectsMissingCredentials(t *testing.T) {
	logger := &recordingLogger{}
	credStore := framework.NewMemoryCredentialStore()

	mod := feishu.New(
		feishu.WithDeps(framework.Deps{
			Logger: logger,
		}),
	)
	// only app_id, no app_secret
	scopedCreds := framework.NewScopedCredentialStoreForDeclaration(credStore, mod.Declares())
	_ = scopedCreds.Put(context.Background(), feishu.CredKeyAppID, "cli_app_001")
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actorreg.Record{
		ID: "tool:feishu-adapter", Kind: actor.KindTool, Binding: actor.BindingRuntimeOutbound,
	})
	mgr, _ := framework.NewManager(framework.ManagerConfig{
		ChannelID:       "channel:test",
		ActorRegistry:   registry,
		HarnessChain:    &fakeChain{},
		RequestLookup:   framework.NewMemoryRequestLookup(nil),
		CredentialStore: credStore,
		Logger:          logger,
	})
	err := mgr.Install(context.Background(), []adapter.Module{mod})
	if err == nil {
		t.Fatalf("expected install error for missing secret")
	}
	if !errors.Is(err, framework.ErrCredentialMissing) {
		t.Fatalf("expected ErrCredentialMissing, got %v", err)
	}
}

func TestChatSendEndToEnd(t *testing.T) {
	s := setup(t)
	req := newRequest(feishu.TypeChatSend, "req-send-1",
		`{"chat_id":"oc_001","text":"hi from agent"}`)
	s.lookup.Put(req)
	if err := s.mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	written := s.chain.snapshot()
	if len(written) != 1 {
		t.Fatalf("expected 1 written envelope, got %d", len(written))
	}
	resp := written[0]
	if resp.Kind != message.KindResponse {
		t.Fatalf("kind=%s want response", resp.Kind)
	}
	if resp.Sender.ID != "tool:feishu-adapter" {
		t.Fatalf("sender.id=%s want tool:feishu-adapter", resp.Sender.ID)
	}
	var payload map[string]any
	_ = json.Unmarshal(resp.Payload, &payload)
	if payload["status"] != "completed" {
		t.Fatalf("payload.status=%v want completed; payload=%s", payload["status"], string(resp.Payload))
	}
	if payload["message_id"] != "om_abc123" {
		t.Fatalf("payload.message_id=%v want om_abc123", payload["message_id"])
	}
	if s.fake.sendCount() != 1 {
		t.Fatalf("fake feishu send hits=%d want 1", s.fake.sendCount())
	}
	// First call should have fetched a token.
	if s.fake.tokenCount() != 1 {
		t.Fatalf("token fetch=%d want 1", s.fake.tokenCount())
	}
	// Authorization header must include Bearer with the token.
	if !strings.HasPrefix(s.fake.lastAuth, "Bearer ") {
		t.Fatalf("missing bearer header: %q", s.fake.lastAuth)
	}
}

func TestChatSendReusesCachedToken(t *testing.T) {
	s := setup(t)
	for i := 0; i < 3; i++ {
		req := newRequest(feishu.TypeChatSend, "req-"+string(rune('A'+i)),
			`{"chat_id":"oc_001","text":"hi"}`)
		s.lookup.Put(req)
		if err := s.mgr.Dispatch(context.Background(), req); err != nil {
			t.Fatalf("Dispatch %d: %v", i, err)
		}
	}
	if s.fake.tokenCount() != 1 {
		t.Fatalf("expected 1 token fetch (cached), got %d", s.fake.tokenCount())
	}
	if s.fake.sendCount() != 3 {
		t.Fatalf("expected 3 send hits, got %d", s.fake.sendCount())
	}
}

func TestChatCreateEndToEnd(t *testing.T) {
	s := setup(t)
	req := newRequest(feishu.TypeChatCreate, "req-create",
		`{"name":"engineering-team","user_ids":["ou_001"]}`)
	s.lookup.Put(req)
	if err := s.mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if s.fake.createCount() != 1 {
		t.Fatalf("create hits=%d want 1", s.fake.createCount())
	}
	resp := s.chain.snapshot()[0]
	var payload map[string]any
	_ = json.Unmarshal(resp.Payload, &payload)
	if payload["chat_id"] != "oc_new_chat" {
		t.Fatalf("payload.chat_id=%v want oc_new_chat", payload["chat_id"])
	}
}

func TestChatSendFeishuAPIFailureProducesTerminalFailure(t *testing.T) {
	s := setup(t)
	s.fake.sendCode = 99991663 // mimic real feishu error code
	s.fake.sendMsg = "permission denied"

	req := newRequest(feishu.TypeChatSend, "req-fail",
		`{"chat_id":"oc_001","text":"hi"}`)
	s.lookup.Put(req)
	if err := s.mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	resp := s.chain.snapshot()[0]
	var payload map[string]any
	_ = json.Unmarshal(resp.Payload, &payload)
	if payload["status"] != "failed" {
		t.Fatalf("expected failed status, got %v; payload=%s",
			payload["status"], string(resp.Payload))
	}
	if payload["reason"] != string(message.TerminalReceiverInternalError) {
		t.Fatalf("reason=%v want receiver_internal_error", payload["reason"])
	}
	if code, _ := payload["error_code"].(string); !strings.HasPrefix(code, "feishu_code_") {
		t.Fatalf("error_code=%v want feishu_code_*", payload["error_code"])
	}
}

func TestChatSendTransportErrorProducesTerminalFailure(t *testing.T) {
	s := setup(t)
	s.fake.failOnSend = true
	req := newRequest(feishu.TypeChatSend, "req-net",
		`{"chat_id":"oc_001","text":"hi"}`)
	s.lookup.Put(req)
	if err := s.mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	resp := s.chain.snapshot()[0]
	var payload map[string]any
	_ = json.Unmarshal(resp.Payload, &payload)
	if payload["status"] != "failed" {
		t.Fatalf("expected failed status, got %v", payload["status"])
	}
}

func TestPayloadValidationFailsCleanTerminal(t *testing.T) {
	s := setup(t)
	req := newRequest(feishu.TypeChatSend, "req-bad",
		`{"chat_id":"oc_001"}`) // missing text
	s.lookup.Put(req)
	if err := s.mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if s.fake.sendCount() != 0 {
		t.Fatalf("send hit on invalid payload: %d", s.fake.sendCount())
	}
	resp := s.chain.snapshot()[0]
	var payload map[string]any
	_ = json.Unmarshal(resp.Payload, &payload)
	if payload["status"] != "failed" {
		t.Fatalf("expected failed status, got %v", payload["status"])
	}
	if payload["reason"] != string(message.TerminalReceiverInternalError) {
		t.Fatalf("reason=%v want receiver_internal_error", payload["reason"])
	}
	if payload["error_code"] != "payload_invalid" {
		t.Fatalf("error_code=%v want payload_invalid", payload["error_code"])
	}
}

// TestCredentialNeverAppearsInLogs is the §T4 credential safety acceptance.
// The recording logger captures every line + every arg. We exercise the
// normal happy path + an explicit error path and assert neither the
// app_secret nor the raw access_token appears anywhere.
func TestCredentialNeverAppearsInLogs(t *testing.T) {
	s := setup(t)

	// Happy path first.
	req1 := newRequest(feishu.TypeChatSend, "req-cred-1",
		`{"chat_id":"oc_001","text":"hi"}`)
	s.lookup.Put(req1)
	_ = s.mgr.Dispatch(context.Background(), req1)

	// Error path.
	s.fake.sendCode = 12345
	s.fake.sendMsg = "secret leak SECRET-zxcvbn0987654321 token TOKEN-abcdef1234567890"
	req2 := newRequest(feishu.TypeChatSend, "req-cred-2",
		`{"chat_id":"oc_001","text":"x"}`)
	s.lookup.Put(req2)
	_ = s.mgr.Dispatch(context.Background(), req2)

	dump := s.logger.dump()
	if strings.Contains(dump, "SECRET-zxcvbn0987654321") {
		t.Fatalf("app_secret leaked in logs: %s", dump)
	}
	if strings.Contains(dump, "TOKEN-abcdef1234567890") {
		t.Fatalf("access_token leaked in logs: %s", dump)
	}

	// Also assert the response payload's detail string is redacted.
	for _, resp := range s.chain.snapshot() {
		if strings.Contains(string(resp.Payload), "SECRET-zxcvbn0987654321") {
			t.Fatalf("app_secret leaked in response payload: %s", string(resp.Payload))
		}
		if strings.Contains(string(resp.Payload), "TOKEN-abcdef1234567890") {
			t.Fatalf("access_token leaked in response payload: %s", string(resp.Payload))
		}
	}
}

func TestUnknownTypeProducesTerminalFailure(t *testing.T) {
	s := setup(t)
	// Build a request whose type IS declared by the adapter but
	// dispatch through the Module directly skipping the manager's
	// type guard so we exercise the Module's own fallback.
	mod := feishu.New(
		feishu.WithBaseURL("http://localhost"),
		feishu.WithDeps(framework.Deps{
			CredentialStore: s.creds,
			Logger:          s.logger,
			Metrics:         framework.NoopMetrics{},
		}),
	)
	if err := mod.Init(context.Background(), &adapter.ModuleContext{
		AdapterName:    "feishu",
		AdapterActorID: "tool:feishu-adapter",
		ChannelID:      "channel:test",
		Respond: func(_ context.Context, _ adapter.CorrelationKey, payload json.RawMessage, opts adapter.RespondOptions) (adapter.RespondResult, error) {
			if opts.Status != "failed" {
				t.Fatalf("expected failed status, got %q", opts.Status)
			}
			if opts.Reason != string(message.TerminalReceiverInternalError) {
				t.Fatalf("expected reason=receiver_internal_error, got %q", opts.Reason)
			}
			var body map[string]any
			if err := json.Unmarshal(payload, &body); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			if body["error_code"] != "type_unsupported" {
				t.Fatalf("expected error_code=type_unsupported, got %v", body["error_code"])
			}
			return adapter.RespondResult{MessageID: "x"}, nil
		},
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	env := newRequest("feishu.does.not.exist", "r1", `{}`)
	if err := mod.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}
