package harness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// ---------------------------------------------------------------------------
// In-memory mocks
// ---------------------------------------------------------------------------

type memStore struct {
	mu        sync.Mutex
	byID      map[string]*v4types.Envelope
	terminals map[string]*v4types.Envelope // parent_id -> terminal response
	insertErr error                        // optional injected error on InsertMessage
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
	if m.insertErr != nil {
		return m.insertErr
	}
	if _, exists := m.byID[env.ID]; exists {
		return ErrUniqueViolation
	}
	if env.Kind == v4types.KindResponse && env.IsTerminal {
		if _, exists := m.terminals[env.ParentID]; exists {
			return ErrUniqueViolation
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

func (m *memStore) WithTerminalTx(ctx context.Context, body func(tx Store) error) error {
	// In-memory store has no real tx; the body sees the same map under
	// mutex (good enough for unit tests).
	return body(m)
}

type memActors struct {
	byID map[string]*ActorMeta
}

func newMemActors() *memActors {
	return &memActors{byID: map[string]*ActorMeta{}}
}

func (m *memActors) Get(_ context.Context, actorID string) (*ActorMeta, error) {
	v, ok := m.byID[actorID]
	if !ok {
		return nil, nil
	}
	cp := *v
	return &cp, nil
}

type memTypes struct {
	byType map[string]*TypeInfo
}

func newMemTypes() *memTypes {
	return &memTypes{byType: map[string]*TypeInfo{}}
}

func (m *memTypes) Get(t string) (*TypeInfo, bool) {
	v, ok := m.byType[t]
	return v, ok
}

type memWorkerLocks struct {
	active map[string]int64 // agent_id -> active fencing_token
}

func newMemWorkerLocks() *memWorkerLocks { return &memWorkerLocks{active: map[string]int64{}} }

func (m *memWorkerLocks) IsActive(_ context.Context, agentID string, token int64) (bool, error) {
	v, ok := m.active[agentID]
	return ok && v == token, nil
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

type fixture struct {
	store       *memStore
	actors      *memActors
	types       *memTypes
	workerLocks *memWorkerLocks
	deps        Deps
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	store := newMemStore()
	actors := newMemActors()
	types := newMemTypes()
	wl := newMemWorkerLocks()

	// Seed two actors used across tests.
	actors.byID["alice"] = &ActorMeta{ActorID: "alice", Kind: v4types.SenderAgent, Binding: "in_worker_bus"}
	actors.byID["bob"] = &ActorMeta{ActorID: "bob", Kind: v4types.SenderAgent, Binding: "in_worker_bus"}
	actors.byID["system"] = &ActorMeta{ActorID: "system", Kind: v4types.SenderSystem}
	actors.byID["tool:xhs"] = &ActorMeta{ActorID: "tool:xhs", Kind: v4types.SenderTool, Binding: "daemon_rpc"}

	deps := Deps{
		Store:       store,
		Actors:      actors,
		Types:       types,
		WorkerLocks: wl,
		Dispatcher:  NoopDispatcher{},
		Clock:       func() int64 { return 1700000000_000 },
		ChannelID:   "ch-1",
	}
	return &fixture{store: store, actors: actors, types: types, workerLocks: wl, deps: deps}
}

func validEvent() *v4types.Envelope {
	return &v4types.Envelope{
		ID:         "msg-1",
		TS:         1700000000_000,
		ChannelID:  "ch-1",
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "alice"},
		Kind:       v4types.KindEvent,
		Type:       "agent.text",
		Payload:    json.RawMessage(`{"text":"hello"}`),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{"*"},
	}
}

func validCallerCtx() CallerCtx {
	return CallerCtx{
		Authenticated: true,
		ActorID:       "alice",
	}
}

func mustReject(t *testing.T, err error, expected v4types.HarnessRejectReason) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected reject %q, got nil", expected)
	}
	var rerr *RejectError
	if !errors.As(err, &rerr) {
		t.Fatalf("expected RejectError, got %T: %v", err, err)
	}
	if rerr.Reason != expected {
		t.Fatalf("expected reason %q, got %q (detail=%s)", expected, rerr.Reason, rerr.Detail)
	}
}

// ---------------------------------------------------------------------------
// Step 1 — auth
// ---------------------------------------------------------------------------

func TestWrite_Step1_AuthFailed(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	ctx := CallerCtx{Authenticated: false, ActorID: "alice"}
	_, err := Write(context.Background(), f.deps, env, ctx)
	mustReject(t, err, v4types.HarnessAuthFailed)
}

// ---------------------------------------------------------------------------
// Step 2 — required fields + ADT enum + One Law pairing
// ---------------------------------------------------------------------------

func TestWrite_Step2_MissingID(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.ID = ""
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessMissingRequiredField)
}

func TestWrite_Step2_MissingTS(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.TS = 0
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessMissingRequiredField)
}

func TestWrite_Step2_KindInvalid_Empty(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.Type = "biz.foo" // non-core, no kind default
	env.Kind = ""
	// Register the business type so Step 4 passes.
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:         "biz.foo",
		AllowedKinds: []v4types.Kind{v4types.KindEvent},
		Schemas:      map[v4types.Kind]*jsonschema.Schema{},
	}
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessKindInvalid)
}

func TestWrite_Step2_KindInvalid_Bogus(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.Kind = v4types.Kind("bogus")
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessKindInvalid)
}

func TestWrite_Step2_ResponseMissingParentID(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.Kind = v4types.KindResponse
	env.ParentID = ""
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessResponseMissingParentID)
}

// TestWrite_Step2_ChannelMismatch covers the FIX-3 R1 requirement
// (T103 / codex t91 critical): a binding bound to channel A MUST
// reject an envelope addressed to channel B with `channel_mismatch`,
// not silently accept it as registry/audience miss in subsequent
// steps. Deps.ChannelID is the authoritative channel; envelope is
// caller-supplied untrusted input.
func TestWrite_Step2_ChannelMismatch(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.ChannelID = "ch-other"
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessChannelMismatch)
}

// ---------------------------------------------------------------------------
// Step 3 — sender × caller + actor_registry + fencing
// ---------------------------------------------------------------------------

func TestWrite_Step3_SenderMismatch(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.Sender.ID = "bob"
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessSenderMismatch)
}

func TestWrite_Step3_SenderDeregistered_Missing(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.Sender.ID = "unknown"
	_, err := Write(context.Background(), f.deps, env, CallerCtx{Authenticated: true, ActorID: "unknown"})
	mustReject(t, err, v4types.HarnessSenderDeregistered)
}

func TestWrite_Step3_SenderDeregistered_SoftDelete(t *testing.T) {
	f := newFixture(t)
	dereg := int64(1500000000)
	f.actors.byID["alice"].DeregisteredAt = &dereg
	env := validEvent()
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessSenderDeregistered)
}

func TestWrite_Step3_SenderKindMismatch(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	ctx := validCallerCtx()
	ctx.DeclaredSenderKind = v4types.SenderHuman // registry says agent
	_, err := Write(context.Background(), f.deps, env, ctx)
	mustReject(t, err, v4types.HarnessSenderKindMismatch)
}

func TestWrite_Step3_FencingStale(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	ctx := validCallerCtx()
	ctx.FencingToken = 5
	// worker_locks has no entry for alice → IsActive returns false
	_, err := Write(context.Background(), f.deps, env, ctx)
	mustReject(t, err, v4types.HarnessWorkerFencingStale)
}

func TestWrite_Step3_FencingActive_OK(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	f.workerLocks.active["alice"] = 7
	ctx := validCallerCtx()
	ctx.FencingToken = 7
	_, err := Write(context.Background(), f.deps, env, ctx)
	if err != nil {
		t.Fatalf("expected success with matching fencing token, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Step 4 — type whitelist
// ---------------------------------------------------------------------------

func TestWrite_Step4_UnknownType(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.Type = "biz.unknown"
	env.Kind = v4types.KindEvent
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessUnknownType)
}

// ---------------------------------------------------------------------------
// Step 5 — kind × type + audience narrow
// ---------------------------------------------------------------------------

func TestWrite_Step5_KindNotAllowed_CoreNoOverride(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.Type = "system.event"
	env.Sender.ID = "system"
	env.Kind = v4types.KindRequest // not allowed for system.event (no override)
	_, err := Write(context.Background(), f.deps, env, CallerCtx{Authenticated: true, ActorID: "system"})
	mustReject(t, err, v4types.HarnessKindNotAllowed)
}

func TestWrite_Step5_KindNotAllowed_Business(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:         "biz.foo",
		AllowedKinds: []v4types.Kind{v4types.KindEvent},
		Schemas:      map[v4types.Kind]*jsonschema.Schema{},
	}
	env := validEvent()
	env.Type = "biz.foo"
	env.Kind = v4types.KindRequest
	env.Audience = []string{"bob"}
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessKindNotAllowed)
}

func TestWrite_Step5_RequestAudienceInvalid_Star(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:         "biz.foo",
		AllowedKinds: []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		Schemas:      buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
	}
	env := validEvent()
	env.Type = "biz.foo"
	env.Kind = v4types.KindRequest
	env.Audience = []string{"*"}
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessRequestAudienceInvalid)
}

func TestWrite_Step5_RequestAudienceInvalid_Multi(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:         "biz.foo",
		AllowedKinds: []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		Schemas:      buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
	}
	env := validEvent()
	env.Type = "biz.foo"
	env.Kind = v4types.KindRequest
	env.Audience = []string{"bob", "tool:xhs"}
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessRequestAudienceInvalid)
}

func TestWrite_Step5_AudienceActorNotRegistered(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:         "biz.foo",
		AllowedKinds: []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		Schemas:      buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
	}
	env := validEvent()
	env.Type = "biz.foo"
	env.Kind = v4types.KindRequest
	env.Audience = []string{"ghost"}
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessAudienceActorNotRegistered)
}

func TestWrite_Step5_AudienceHandlerMismatch(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:           "biz.foo",
		AllowedKinds:   []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		HandlerActorID: "tool:xhs",
		Schemas:        buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
	}
	env := validEvent()
	env.Type = "biz.foo"
	env.Kind = v4types.KindRequest
	env.Audience = []string{"bob"} // not the declared handler
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessAudienceHandlerMismatch)
}

// ---------------------------------------------------------------------------
// Step 6 — payload schema
// ---------------------------------------------------------------------------

func TestWrite_Step6_PayloadSchemaViolation_Business(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:         "biz.foo",
		AllowedKinds: []v4types.Kind{v4types.KindEvent},
		Schemas:      buildSchemas(t, `{"type":"object","required":["x"]}`, ""),
	}
	env := validEvent()
	env.Type = "biz.foo"
	env.Kind = v4types.KindEvent
	env.Payload = json.RawMessage(`{}`)
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessPayloadSchemaViolation)
}

func TestWrite_Step6_PayloadSchemaViolation_Core_NonObject(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.Payload = json.RawMessage(`"a string is not an object"`)
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessPayloadSchemaViolation)
}

// ---------------------------------------------------------------------------
// Step 7 — doc_refs
// ---------------------------------------------------------------------------

func TestWrite_Step7_DocRefsAbsolute(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	refs := []string{"/etc/passwd"}
	env.DocRefs = &refs
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessDocRefsInvalid)
}

func TestWrite_Step7_DocRefsTraversal(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	refs := []string{"a/../../b"}
	env.DocRefs = &refs
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessDocRefsInvalid)
}

func TestWrite_Step7_DocRefsCrossChannel(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	refs := []string{"channels/other-channel/notes.md"}
	env.DocRefs = &refs
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessDocRefsInvalid)
}

func TestWrite_Step7_DocRefsSameChannel_OK(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	refs := []string{"channels/ch-1/notes.md", "local/note.md"}
	env.DocRefs = &refs
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Step 8 — The One Law (response uniqueness + parent invalid + idempotent dedupe)
// ---------------------------------------------------------------------------

func TestWrite_Step8_ResponseParentInvalid_Missing(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:               "biz.foo",
		AllowedKinds:       []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		Schemas:            buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
		TerminalConvention: "single-response",
	}
	env := validEvent()
	env.ID = "resp-1"
	env.Type = "biz.foo"
	env.Kind = v4types.KindResponse
	env.ParentID = "no-such-req"
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessResponseParentInvalid)
}

func TestWrite_Step8_ResponseParentInvalid_NotRequest(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:               "biz.foo",
		AllowedKinds:       []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		Schemas:            buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
		TerminalConvention: "single-response",
	}
	// Seed an event in the store so parent_id lookup returns a non-request row.
	f.store.byID["evt-1"] = &v4types.Envelope{ID: "evt-1", Kind: v4types.KindEvent}
	env := validEvent()
	env.ID = "resp-2"
	env.Type = "biz.foo"
	env.Kind = v4types.KindResponse
	env.ParentID = "evt-1"
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	mustReject(t, err, v4types.HarnessResponseParentInvalid)
}

func TestWrite_Step8_TerminalDuplicate(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:               "biz.foo",
		AllowedKinds:       []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		Schemas:            buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
		TerminalConvention: "single-response",
	}
	// Seed a request + existing terminal response.
	f.store.byID["req-1"] = &v4types.Envelope{ID: "req-1", Kind: v4types.KindRequest}
	prior := &v4types.Envelope{
		ID:       "winner",
		Kind:     v4types.KindResponse,
		ParentID: "req-1",
	}
	f.store.byID["winner"] = prior
	f.store.terminals["req-1"] = prior

	env := validEvent()
	env.ID = "loser"
	env.Type = "biz.foo"
	env.Kind = v4types.KindResponse
	env.ParentID = "req-1"
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	var rerr *RejectError
	if !errors.As(err, &rerr) {
		t.Fatalf("expected RejectError, got %v", err)
	}
	if rerr.Reason != v4types.HarnessTerminalDuplicate {
		t.Fatalf("expected terminal_duplicate, got %q", rerr.Reason)
	}
	if rerr.DedupeResponseID != "winner" {
		t.Fatalf("expected dedupe_response_id=winner, got %q", rerr.DedupeResponseID)
	}
}

func TestWrite_Step8_TerminalSameIDDedupe(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:               "biz.foo",
		AllowedKinds:       []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		Schemas:            buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
		TerminalConvention: "single-response",
	}
	f.store.byID["req-1"] = &v4types.Envelope{ID: "req-1", Kind: v4types.KindRequest}
	// Use an envelope we'll write twice — second write triggers Step 0.5 dedupe.
	env := validEvent()
	env.ID = "resp-1"
	env.Type = "biz.foo"
	env.Kind = v4types.KindResponse
	env.ParentID = "req-1"
	env.Payload = json.RawMessage(`{"ok":true}`)

	r1, err := Write(context.Background(), f.deps, env, validCallerCtx())
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	if r1.Dedupe {
		t.Fatalf("first write should not be dedupe")
	}

	env2 := *env
	r2, err := Write(context.Background(), f.deps, &env2, validCallerCtx())
	if err != nil {
		t.Fatalf("second write: %v", err)
	}
	if !r2.Dedupe {
		t.Fatalf("second write should be dedupe")
	}
	if r2.ID != "resp-1" {
		t.Fatalf("expected dedupe id resp-1, got %q", r2.ID)
	}
}

func TestWrite_Step8_PayloadStatusTerminal(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:               "biz.foo",
		AllowedKinds:       []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		Schemas:            buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
		TerminalConvention: "payload_status",
	}
	f.store.byID["req-1"] = &v4types.Envelope{ID: "req-1", Kind: v4types.KindRequest}

	// Non-terminal response (status=progress) — should insert, not lock terminal slot.
	env := validEvent()
	env.ID = "prog-1"
	env.Type = "biz.foo"
	env.Kind = v4types.KindResponse
	env.ParentID = "req-1"
	env.Payload = json.RawMessage(`{"status":"progress"}`)
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	if err != nil {
		t.Fatalf("non-terminal response failed: %v", err)
	}

	// Terminal response — should succeed and mark is_terminal.
	env2 := validEvent()
	env2.ID = "done-1"
	env2.Type = "biz.foo"
	env2.Kind = v4types.KindResponse
	env2.ParentID = "req-1"
	env2.Payload = json.RawMessage(`{"status":"completed"}`)
	r, err := Write(context.Background(), f.deps, env2, validCallerCtx())
	if err != nil {
		t.Fatalf("terminal response failed: %v", err)
	}
	if r.ID != "done-1" {
		t.Fatalf("expected id done-1, got %q", r.ID)
	}
	if _, ok := f.store.terminals["req-1"]; !ok {
		t.Fatalf("expected terminal slot to be filled")
	}
}

// ---------------------------------------------------------------------------
// Step 0.5 — universal id dedupe (same-id retry / message_id_conflict)
// ---------------------------------------------------------------------------

func TestWrite_Step05_SameIDSameContent_Dedupe(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	if _, err := Write(context.Background(), f.deps, env, validCallerCtx()); err != nil {
		t.Fatalf("first write: %v", err)
	}
	env2 := *env
	r, err := Write(context.Background(), f.deps, &env2, validCallerCtx())
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !r.Dedupe {
		t.Fatalf("expected dedupe, got fresh result")
	}
}

func TestWrite_Step05_SameIDDifferentContent_Conflict(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	if _, err := Write(context.Background(), f.deps, env, validCallerCtx()); err != nil {
		t.Fatalf("first write: %v", err)
	}
	env2 := validEvent() // same id but different payload
	env2.Payload = json.RawMessage(`{"text":"different"}`)
	_, err := Write(context.Background(), f.deps, env2, validCallerCtx())
	mustReject(t, err, v4types.HarnessMessageIDConflict)
}

// ---------------------------------------------------------------------------
// Normalize / dispatch / kind override semantics
// ---------------------------------------------------------------------------

func TestWrite_Normalize_AudienceVisibilityKind(t *testing.T) {
	f := newFixture(t)
	env := &v4types.Envelope{
		ID:        "msg-norm",
		TS:        1,
		ChannelID: "ch-1",
		Sender:    v4types.Sender{Kind: v4types.SenderAgent, ID: "alice"},
		Type:      "agent.text",
		Payload:   json.RawMessage(`{"text":"hi"}`),
		// Kind / Audience / Visibility / CorrelationID intentionally left empty.
	}
	_, err := Write(context.Background(), f.deps, env, validCallerCtx())
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if env.Kind != v4types.KindEvent {
		t.Fatalf("expected default kind=event, got %q", env.Kind)
	}
	if env.Visibility != v4types.VisibilityPublic {
		t.Fatalf("expected default visibility=public, got %q", env.Visibility)
	}
	if len(env.Audience) != 1 || env.Audience[0] != "*" {
		t.Fatalf("expected default audience=['*'], got %v", env.Audience)
	}
	if env.CorrelationID != env.ID {
		t.Fatalf("expected self-rooted correlation_id, got %q", env.CorrelationID)
	}
}

func TestWrite_Normalize_CorrelationFromTrigger(t *testing.T) {
	f := newFixture(t)
	env := validEvent()
	env.CorrelationID = ""
	ctx := validCallerCtx()
	ctx.Trigger = &TriggerCtx{CorrelationID: "trig-1"}
	_, err := Write(context.Background(), f.deps, env, ctx)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if env.CorrelationID != "trig-1" {
		t.Fatalf("expected propagated correlation_id=trig-1, got %q", env.CorrelationID)
	}
}

// dispatchSpy counts the dispatch invocations to assert the harness
// always dispatches on success and never on reject.
type dispatchSpy struct {
	count int
}

func (d *dispatchSpy) Dispatch(_ context.Context, _ *v4types.Envelope) error {
	d.count++
	return nil
}

func TestWrite_DispatchInvokedOnSuccessOnly(t *testing.T) {
	f := newFixture(t)
	spy := &dispatchSpy{}
	f.deps.Dispatcher = spy

	// Success path
	if _, err := Write(context.Background(), f.deps, validEvent(), validCallerCtx()); err != nil {
		t.Fatalf("success path: %v", err)
	}
	if spy.count != 1 {
		t.Fatalf("expected dispatch count 1 after success, got %d", spy.count)
	}
	// Reject path
	env := validEvent()
	env.ID = "msg-reject"
	env.Sender.ID = "bob" // mismatch
	if _, err := Write(context.Background(), f.deps, env, validCallerCtx()); err == nil {
		t.Fatalf("expected reject")
	}
	if spy.count != 1 {
		t.Fatalf("dispatch should not fire on reject, got %d", spy.count)
	}
}

// ---------------------------------------------------------------------------
// Concurrency — same id race + The One Law uniqueness race
// ---------------------------------------------------------------------------

func TestWrite_Concurrent_SameID_OneWinsRestDedupe(t *testing.T) {
	f := newFixture(t)
	const N = 100

	var wg sync.WaitGroup
	results := make([]*Result, N)
	errs := make([]error, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			env := validEvent()
			env.ID = "shared"
			env.CorrelationID = "shared" // pre-normalize to keep canonical hash equal
			r, err := Write(context.Background(), f.deps, env, validCallerCtx())
			results[idx] = r
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	successes := 0
	dedupes := 0
	conflicts := 0
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			var rerr *RejectError
			if errors.As(errs[i], &rerr) && rerr.Reason == v4types.HarnessMessageIDConflict {
				conflicts++
				continue
			}
			t.Fatalf("unexpected error: %v", errs[i])
		}
		if results[i].Dedupe {
			dedupes++
		} else {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 fresh success, got %d", successes)
	}
	if conflicts != 0 {
		t.Fatalf("same-content concurrent writes should never produce conflict, got %d", conflicts)
	}
	if dedupes != N-1 {
		t.Fatalf("expected %d dedupes, got %d", N-1, dedupes)
	}
}

func TestWrite_Concurrent_TerminalUniqueness(t *testing.T) {
	f := newFixture(t)
	f.types.byType["biz.foo"] = &TypeInfo{
		Type:               "biz.foo",
		AllowedKinds:       []v4types.Kind{v4types.KindRequest, v4types.KindResponse},
		Schemas:            buildSchemas(t, `{"type":"object"}`, `{"type":"object"}`),
		TerminalConvention: "single-response",
	}
	f.store.byID["req-1"] = &v4types.Envelope{ID: "req-1", Kind: v4types.KindRequest}

	const N = 100
	var wg sync.WaitGroup
	errs := make([]error, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			env := validEvent()
			env.ID = sprintf("resp-%d", idx)
			env.Type = "biz.foo"
			env.Kind = v4types.KindResponse
			env.ParentID = "req-1"
			env.CorrelationID = "req-1"
			_, err := Write(context.Background(), f.deps, env, validCallerCtx())
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	successes := 0
	terminalDupes := 0
	for i := 0; i < N; i++ {
		if errs[i] == nil {
			successes++
			continue
		}
		var rerr *RejectError
		if !errors.As(errs[i], &rerr) {
			t.Fatalf("unexpected error: %v", errs[i])
		}
		if rerr.Reason == v4types.HarnessTerminalDuplicate {
			terminalDupes++
			if rerr.DedupeResponseID == "" {
				t.Fatalf("terminal_duplicate must carry dedupe_response_id")
			}
		} else {
			t.Fatalf("unexpected reject %q", rerr.Reason)
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 terminal success, got %d", successes)
	}
	if terminalDupes != N-1 {
		t.Fatalf("expected %d terminal_duplicate, got %d", N-1, terminalDupes)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func buildSchemas(t *testing.T, reqSchema, respSchema string) map[v4types.Kind]*jsonschema.Schema {
	t.Helper()
	out := make(map[v4types.Kind]*jsonschema.Schema)
	if reqSchema != "" {
		out[v4types.KindRequest] = compileTestSchema(t, "req", reqSchema)
		// For business types where event uses same schema, reuse the request schema.
		out[v4types.KindEvent] = compileTestSchema(t, "ev", reqSchema)
	}
	if respSchema != "" {
		out[v4types.KindResponse] = compileTestSchema(t, "resp", respSchema)
	}
	return out
}

func compileTestSchema(t *testing.T, name, src string) *jsonschema.Schema {
	t.Helper()
	url := "test://" + name
	c := jsonschema.NewCompiler()
	if err := c.AddResource(url, strings.NewReader(src)); err != nil {
		t.Fatalf("schema %s add: %v", name, err)
	}
	s, err := c.Compile(url)
	if err != nil {
		t.Fatalf("schema %s compile: %v", name, err)
	}
	return s
}
