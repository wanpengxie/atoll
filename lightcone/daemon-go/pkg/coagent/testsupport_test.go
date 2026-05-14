package coagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// fixedTime is the deterministic wall-clock tests share so envelope.ts
// is predictable. 2023-11-14T22:13:20Z in unix ms.
const fixedTimeMS int64 = 1700000000_000

func fixedClock() time.Time { return time.UnixMilli(fixedTimeMS) }

// fakeIDGen returns deterministic ids: "id-1", "id-2", ... so tests
// can assert exact envelope ids without dragging UUID randomness in.
func fakeIDGen() func() string {
	var (
		mu sync.Mutex
		n  int
	)
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		n++
		return idStr(n)
	}
}

func idStr(n int) string { return "id-" + intStr(n) }

func intStr(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// ---------------------------------------------------------------------------
// In-memory harness mocks (copies of the patterns from pkg/harness tests so
// pkg/coagent tests stay independent of the harness internals).
// ---------------------------------------------------------------------------

type memStore struct {
	mu        sync.Mutex
	byID      map[string]*v4types.Envelope
	terminals map[string]*v4types.Envelope
}

func newMemStore() *memStore {
	return &memStore{
		byID:      map[string]*v4types.Envelope{},
		terminals: map[string]*v4types.Envelope{},
	}
}

func (m *memStore) FindByID(_ context.Context, id string) (*v4types.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.byID[id]
	if !ok {
		return nil, nil
	}
	cp := *v
	return &cp, nil
}

func (m *memStore) FindParent(ctx context.Context, id string) (*v4types.Envelope, error) {
	return m.FindByID(ctx, id)
}

func (m *memStore) FindTerminalResponse(_ context.Context, parentID string) (*v4types.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.terminals[parentID]
	if !ok {
		return nil, nil
	}
	cp := *v
	return &cp, nil
}

func (m *memStore) InsertMessage(_ context.Context, env *v4types.Envelope, tsReceived int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.byID[env.ID]; exists {
		return pkgharness.ErrUniqueViolation
	}
	if env.Kind == v4types.KindResponse && env.IsTerminal {
		if _, exists := m.terminals[env.ParentID]; exists {
			return pkgharness.ErrUniqueViolation
		}
	}
	env.TSReceived = tsReceived
	cp := *env
	m.byID[env.ID] = &cp
	if env.Kind == v4types.KindResponse && env.IsTerminal {
		m.terminals[env.ParentID] = &cp
	}
	return nil
}

func (m *memStore) WithTerminalTx(ctx context.Context, body func(tx pkgharness.Store) error) error {
	return body(m)
}

// seedRequest inserts a pre-existing request envelope for `coagent answer`
// tests. The envelope is written directly, bypassing the harness — we
// only need it to be findable via FindByID.
func (m *memStore) seedRequest(env *v4types.Envelope) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *env
	m.byID[env.ID] = &cp
}

type memActors struct {
	mu   sync.Mutex
	byID map[string]*pkgharness.ActorMeta
}

func newMemActors() *memActors {
	return &memActors{byID: map[string]*pkgharness.ActorMeta{}}
}

func (m *memActors) Get(_ context.Context, actorID string) (*pkgharness.ActorMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.byID[actorID]
	if !ok {
		return nil, nil
	}
	cp := *v
	return &cp, nil
}

func (m *memActors) seed(id string, kind v4types.SenderKind, binding string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byID[id] = &pkgharness.ActorMeta{ActorID: id, Kind: kind, Binding: binding}
}

func (m *memActors) deregister(id string, at int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.byID[id]; ok {
		e.DeregisteredAt = &at
	}
}

type memTypes struct {
	mu    sync.Mutex
	types map[string]*pkgharness.TypeInfo
}

func newMemTypes() *memTypes { return &memTypes{types: map[string]*pkgharness.TypeInfo{}} }

func (m *memTypes) Get(t string) (*pkgharness.TypeInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	info, ok := m.types[t]
	return info, ok
}

func (m *memTypes) put(info *pkgharness.TypeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.types[info.Type] = info
}

// ---------------------------------------------------------------------------
// Worker-side fixture builders
// ---------------------------------------------------------------------------

// busFixture bundles a fully wired in_worker_bus binding for the
// "alice" caller. Tests can mutate fixture.actors / fixture.types
// freely between calls.
type busFixture struct {
	store   *memStore
	actors  *memActors
	types   *memTypes
	deps    pkgharness.Deps
	binding Binding
}

func newBusFixture(t *testing.T) *busFixture {
	t.Helper()
	store := newMemStore()
	actors := newMemActors()
	types := newMemTypes()

	// Seed canonical actors used across CLI tests. alice = agent caller,
	// bob = agent receiver, tool:xhs = tool receiver, system = system.
	actors.seed("alice", v4types.SenderAgent, "in_worker_bus")
	actors.seed("bob", v4types.SenderAgent, "in_worker_bus")
	actors.seed("tool:xhs", v4types.SenderTool, "daemon_rpc")
	actors.seed("system", v4types.SenderSystem, "")

	deps := pkgharness.Deps{
		Store:      store,
		Actors:     actors,
		Types:      types,
		Dispatcher: pkgharness.NoopDispatcher{},
		Clock:      func() int64 { return fixedTimeMS },
		ChannelID:  "ch-1",
	}
	binding := NewInWorkerBusBinding(InWorkerBusOptions{
		Deps: deps,
		CallerCtx: pkgharness.CallerCtx{
			Authenticated: true,
			ActorID:       "alice",
		},
	})
	return &busFixture{
		store:   store,
		actors:  actors,
		types:   types,
		deps:    deps,
		binding: binding,
	}
}

// installBizType registers a business type with the provided handler
// actor id binding. Pass "" for handlerActorID when testing the open
// audience path; pass "tool:xhs" when testing handler_match.
func (f *busFixture) installBizType(t *testing.T, typeName string, allowedKinds []v4types.Kind, handlerActorID string) {
	t.Helper()
	// minimal "accept any object" schema for each kind so harness Step 6
	// validates. compileNoopSchema panics on bug — fail t instead.
	schemas := map[v4types.Kind]*jsonschema.Schema{}
	for _, k := range allowedKinds {
		schemas[k] = compileNoopSchema(t)
	}
	info := &pkgharness.TypeInfo{
		Type:           typeName,
		AllowedKinds:   allowedKinds,
		HandlerBinding: "in_worker_bus",
		HandlerActorID: handlerActorID,
		Schemas:        schemas,
	}
	if handlerActorID == "" {
		// open: any concrete audience is acceptable
	}
	f.types.put(info)
}

// compileNoopSchema compiles {"type":"object"} so harness Step 6
// accepts any JSON object payload. Tests use this when they don't
// care about per-kind shape.
func compileNoopSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	url := "type://noop"
	if err := c.AddResource(url, bytes.NewReader([]byte(`{"type":"object"}`))); err != nil {
		t.Fatalf("compile noop schema: %v", err)
	}
	s, err := c.Compile(url)
	if err != nil {
		t.Fatalf("compile noop schema: %v", err)
	}
	return s
}

// runCLI exercises Run with the binding from the bus fixture (or any
// custom binding when override != nil). Returns the exit code, stdout
// bytes, and stderr bytes. envOverrides override individual env keys
// — un-set keys default to the canonical test environment.
func (f *busFixture) runCLI(args []string, envOverrides map[string]string) (int, string, string) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	env := defaultTestEnv()
	for k, v := range envOverrides {
		env[k] = v
	}
	exit := Run(context.Background(), Config{
		Args:    args,
		Env:     EnvFromMap(env),
		Stdout:  stdout,
		Stderr:  stderr,
		Clock:   fixedClock,
		NewID:   fakeIDGen(),
		Binding: f.binding,
	})
	return exit, stdout.String(), stderr.String()
}

func defaultTestEnv() map[string]string {
	return map[string]string{
		"COAGENT_CHANNEL_ID":             "ch-1",
		"COAGENT_SELF_ID":                "alice",
		"COAGENT_TRIGGER_CORRELATION_ID": "trig-1",
		"COAGENT_AUTH_TOKEN":             "ignored-by-in-worker-bus",
	}
}

// decodeSuccess parses the writeSuccess JSON shape. Returns the parsed
// fields plus a fatal on shape mismatch.
type successOut struct {
	ID            string `json:"id"`
	CorrelationID string `json:"correlation_id"`
	Kind          string `json:"kind"`
	Dedupe        bool   `json:"dedupe"`
}

func decodeSuccess(t *testing.T, raw string) successOut {
	t.Helper()
	var s successOut
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("decode success output %q: %v", raw, err)
	}
	return s
}

// ---------------------------------------------------------------------------
// daemon_rpc mock — a hand-rolled httptest handler tests can use to
// observe the CLI binding's HTTP behaviour without dragging in the
// real daemon harness or channel sqlite.
// ---------------------------------------------------------------------------

// mockDaemon is a thin httptest fixture that records every request
// the CLI sends and replays a canned response for each call. Tests
// use it to assert the daemon_rpc binding's wire shape (headers,
// path, body fields) without spinning up the real internal/harness
// handler.
type mockDaemon struct {
	srv      *httptest.Server
	mu       sync.Mutex
	requests []recordedRequest
	// responses[i] is replayed for the i-th request (FIFO). When
	// fewer responses than requests are configured, the last
	// response is reused.
	responses []mockResponse
}

type recordedRequest struct {
	Path string
	Auth string
	Body messageSendRequest
}

type mockResponse struct {
	Status int
	Body   any
}

func newMockDaemon(t *testing.T, responses []mockResponse) *mockDaemon {
	t.Helper()
	m := &mockDaemon{responses: responses}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/rpc/message.send", m.handle)
	m.srv = httptest.NewServer(mux)
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mockDaemon) handle(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()
	rec := recordedRequest{
		Path: r.URL.Path,
		Auth: r.Header.Get("Authorization"),
	}
	if err := json.NewDecoder(r.Body).Decode(&rec.Body); err != nil && !errors.Is(err, http.ErrBodyReadAfterClose) {
		// Record but still respond — tests assert on body separately.
		rec.Body = messageSendRequest{}
	}

	m.mu.Lock()
	m.requests = append(m.requests, rec)
	idx := len(m.requests) - 1
	if idx >= len(m.responses) {
		idx = len(m.responses) - 1
	}
	resp := m.responses[idx]
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.Status)
	if resp.Body != nil {
		_ = json.NewEncoder(w).Encode(resp.Body)
	}
}

func (m *mockDaemon) URL() string { return m.srv.URL }

func (m *mockDaemon) lastRequest(t *testing.T) recordedRequest {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.requests) == 0 {
		t.Fatalf("expected at least one recorded request, got 0")
	}
	return m.requests[len(m.requests)-1]
}

// ---------------------------------------------------------------------------
// stubBinding — minimal Binding implementation tests can plug in to
// exercise specific failure shapes (e.g. "binding has no LookupRequest").
// ---------------------------------------------------------------------------

type stubBinding struct {
	// send is invoked on each Send call. When nil, Send returns a
	// canned success with empty fields.
	send func(deps pkgharness.Deps) (*SendResult, error)
	// captureEnv (optional) is invoked with the envelope passed to
	// Send so tests can assert on the constructed envelope's fields.
	captureEnv func(env *v4types.Envelope)
	// lookupOK / lookupEnv control LookupRequest. Default returns
	// (nil, false, nil).
	lookupOK  bool
	lookupEnv *v4types.Envelope
	lookupErr error
	// handlerActor / handlerOK / handlerErr control
	// ResolveHandlerActorID. Default returns ("", false, nil).
	handlerActor string
	handlerOK    bool
	handlerErr   error
}

func (s *stubBinding) Send(_ context.Context, env *v4types.Envelope, _ SendOptions) (*SendResult, error) {
	if s.captureEnv != nil {
		s.captureEnv(env)
	}
	if s.send == nil {
		return &SendResult{}, nil
	}
	return s.send(pkgharness.Deps{})
}

func (s *stubBinding) LookupRequest(_ context.Context, _ string) (*v4types.Envelope, bool, error) {
	return s.lookupEnv, s.lookupOK, s.lookupErr
}

func (s *stubBinding) ResolveHandlerActorID(_ context.Context, _ string) (string, bool, error) {
	return s.handlerActor, s.handlerOK, s.handlerErr
}

// runWithBinding is a small helper for tests that need a custom
// Binding rather than the canonical bus fixture. It builds a Config
// with deterministic clock + id generator + default test env so
// callers focus on the args + binding behaviour.
func runWithBinding(args []string, b Binding) (int, string, string) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exit := Run(context.Background(), Config{
		Args:    args,
		Env:     EnvFromMap(defaultTestEnv()),
		Stdout:  stdout,
		Stderr:  stderr,
		Clock:   fixedClock,
		NewID:   fakeIDGen(),
		Binding: b,
	})
	return exit, stdout.String(), stderr.String()
}
