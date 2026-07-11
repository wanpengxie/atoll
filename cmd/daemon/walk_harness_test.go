package main

// walk_harness_test.go is 期11 spec §6's shared platform-level walk rig: a
// REAL platform.Home (real sqlite channel truth) wired to one or more REAL
// daemons — real cmd/daemon/internal/storagehost.Host (real os.Root-confined
// filesystem) driven by real platform.RunCompute, connected over a real
// httptest+websocket link (Home.ServeAttach / link.Dial), with NO app-layer
// HTTP hop (ServeAttach IS the daemon-attach endpoint; app's HTTP plan-pull
// layer is a separate concern this rig never touches — this file lives
// under cmd/daemon/ specifically so it CAN import
// cmd/daemon/internal/storagehost, which sits outside every other package's
// Go-enforced internal/ visibility). Every walk test in this package drives
// resource operations through lib/actorbase Procs — real actor vocabulary
// (sys.Resource()), never a direct accessdoor.door call — matching §6's own
// "真实 actor 词表非直调门" requirement.
//
// Deviation flagged (see cmd/daemon/walk_workspace_test.go's own doc for
// detail): §6 item①'s literal text says "fork 子代" for the workspace walk's
// child. actorbase.Sys.Fork's child is architecturally CONFINED to a
// home-hosted incarnation (platform/internal/link/livearms.go: "Spawn is
// deliberately left zero — the fork/despawn arm does not cross the wire this
// period, 期6 拍") and a fork child is furthermore NOT a channel member
// (fork_census_test.go's own 户籍轴断言②) — but Open/FileOpener is ONLY
// implemented on the daemon-hosted wire proxy (S5's own explicit, flagged
// scope narrowing: "boundHandle/liveResourceAccess do NOT implement
// FileOpener"), AND the daemon's own attach膜律 requires an ACTIVE
// membership row before it will even open a stream for an id (accept.go's
// "问① 膜律"). Under the architecture S1–S5 actually shipped, no combination
// of (Fork, daemon-hosted child) can exist — Fork's child is home-hosted by
// construction, home-hosted has no Open. This rig therefore models the
// walk's "子代" as a SECOND real, Admit'd, daemon-attached actor (a sibling
// member the parent ShareActor-grants direct rights to, exactly the
// non-member-grantee shape §2's decay-law tests already exercise for a fork
// child), not a literal Sys.Fork call — flagged for review rather than
// silently reinterpreted.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/cmd/daemon/internal/storagehost"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func testLogger() *slog.Logger {
	if os.Getenv("WALK_DEBUG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- Home -------------------------------------------------------------

// newWalkHome opens a real platform.Home over a temp sqlite file.
func newWalkHome(t *testing.T, chID channelpkg.ID) *platform.Home {
	t.Helper()
	h, err := platform.Open(platform.HomeConfig{ChannelID: chID, DBPath: walkDBPath(t)})
	if err != nil {
		t.Fatalf("platform.Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// walkDBPath mints a fresh sqlite path under t.TempDir().
func walkDBPath(t *testing.T) string {
	t.Helper()
	return t.TempDir() + "/walk.sqlite"
}

// newWalkHomeWithConfig opens a real platform.Home from an explicit
// HomeConfig (DBPath included) and does NOT register a t.Cleanup — the
// crash-recovery walk manages Home lifecycle itself (a "server crash"
// closes h1 and opens a fresh h2 against the SAME DBPath mid-test).
func newWalkHomeWithConfig(t *testing.T, cfg platform.HomeConfig) *platform.Home {
	t.Helper()
	h, err := platform.Open(cfg)
	if err != nil {
		t.Fatalf("platform.Open: %v", err)
	}
	return h
}

// newWalkDaemonServer wraps h.ServeAttach in an httptest server that always
// authenticates the attaching connection as daemonID (mirrors every other
// cross-wire test in this codebase, e.g. platform.TestHomeCancelRequest_CrossWire
// — a real WS round trip, no app-layer HTTP hop).
func newWalkDaemonServer(t *testing.T, h *platform.Home, daemonID string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeAttach(w, r, daemonID)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// newWalkDaemonServerSwappable is newWalkDaemonServer's crash-recovery
// variant (§6.3's "server crash + restart"): the httptest handler routes
// through an atomically-swappable *platform.Home pointer, so a test can
// simulate "the server process restarted" by opening a FRESH Home against
// the SAME sqlite path and swapping it in — the daemon's own ServerWS URL
// (and so its redial loop's target) never changes, exactly matching how a
// real daemon reconnects to the same address after a server restart.
func newWalkDaemonServerSwappable(t *testing.T, daemonID string, initial *platform.Home) (wsURL string, swap func(*platform.Home)) {
	t.Helper()
	var cur atomic.Pointer[platform.Home]
	cur.Store(initial)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur.Load().ServeAttach(w, r, daemonID)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), func(h *platform.Home) { cur.Store(h) }
}

// --- Daemon-side desired/builder (a minimal static reconcile source) ------

// staticDesired is a thread-safe actorrt.DesiredSource a walk test appends
// to as it admits new daemon-hosted actors — mirrors the daemon binary's own
// planSource.Members shape, without the HTTP plan-pull machinery this rig
// deliberately skips.
type staticDesired struct {
	mu      sync.Mutex
	members []actorrt.DesiredMember
}

func (d *staticDesired) Members(context.Context) ([]actorrt.DesiredMember, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]actorrt.DesiredMember(nil), d.members...), nil
}

func (d *staticDesired) add(m actorrt.DesiredMember) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.members = append(d.members, m)
}

// staticBuilder is a minimal platform.ComputeBuilder over a plain map.
type staticBuilder struct {
	mu   sync.Mutex
	byID map[actor.ActorID]platform.ActorFactory
}

func newStaticBuilder() *staticBuilder {
	return &staticBuilder{byID: map[actor.ActorID]platform.ActorFactory{}}
}

func (b *staticBuilder) Lookup(id actor.ActorID) (platform.ActorFactory, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f, ok := b.byID[id]
	return f, ok
}

func (b *staticBuilder) set(id actor.ActorID, f platform.ActorFactory) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.byID[id] = f
}

// --- Daemon lifecycle ---------------------------------------------------

// walkDaemon is one real daemon this rig runs in-process: a real
// storagehost.Host rooted at its own temp workspace, driven by a real
// platform.RunCompute goroutine dialed at a walk home's ServeAttach.
type walkDaemon struct {
	ComputeID string
	WSRoot    string
	Host      *storagehost.Host

	desired *staticDesired
	builder *staticBuilder
	cancel  context.CancelFunc
	done    chan struct{}
}

// walkDaemonConfig lets a walk override the storage-host reconcile cadence
// (default: fast, so a ReclaimAck/orphan-sweep walk does not wait on
// production's 60s backstop interval) and/or substitute a wrapped
// LocalFileOpener (the crash-recovery walk's own interception seam — see
// walk_crash_test.go). LocalFileOpener is a FACTORY (not a value) because
// the real storagehost.Host it should wrap is only opened INSIDE
// startWalkDaemon.
type walkDaemonConfig struct {
	ScrubberInterval time.Duration
	LocalFileOpener  func(sh *storagehost.Host) platform.LocalFileOpener // nil -> storageHostAdapter{host: sh}
}

// startWalkDaemon opens a fresh storagehost.Host (its own fresh temp
// workspace root) and runs platform.RunCompute against wsURL in the
// background, for the lifetime of t. Actors are added via addActor AFTER
// this call (RunCompute's ring reconciles whatever staticDesired/
// staticBuilder currently hold on every poll tick).
func startWalkDaemon(t *testing.T, wsURL, computeID, chID string, cfg walkDaemonConfig) *walkDaemon {
	t.Helper()
	return startWalkDaemonAt(t, wsURL, computeID, chID, t.TempDir(), cfg)
}

// startWalkDaemonAt is startWalkDaemon with an EXPLICIT workspace root —
// the daemon-crash walk's own seam: a second daemon instance (simulating a
// process restart) must reopen the SAME on-disk root a first, now-dead
// instance wrote real bytes into, to prove recovery relies on ReconcilePull
// alone (storagehost.Open holds no cross-process/cross-instance memory of
// its own — it just re-scans the filesystem).
func startWalkDaemonAt(t *testing.T, wsURL, computeID, chID, wsRoot string, cfg walkDaemonConfig) *walkDaemon {
	t.Helper()
	sh, err := storagehost.Open(wsRoot, chID, testLogger())
	if err != nil {
		t.Fatalf("storagehost.Open: %v", err)
	}

	scrubberInterval := cfg.ScrubberInterval
	if scrubberInterval <= 0 {
		scrubberInterval = 200 * time.Millisecond
	}
	var localOpener platform.LocalFileOpener = storageHostAdapter{host: sh}
	if cfg.LocalFileOpener != nil {
		localOpener = cfg.LocalFileOpener(sh)
	}

	desired := &staticDesired{}
	builder := newStaticBuilder()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = platform.RunCompute(ctx, platform.ComputeConfig{
			ServerWS:         wsURL,
			ComputeID:        computeID,
			Logger:           testLogger(),
			Desired:          desired,
			Builder:          builder,
			StorageHost:      storageHostAdapter{host: sh},
			LocalFileOpener:  localOpener,
			Poll:             50 * time.Millisecond,
			ScrubberInterval: scrubberInterval,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
		_ = sh.Close()
	})
	return &walkDaemon{ComputeID: computeID, WSRoot: wsRoot, Host: sh, desired: desired, builder: builder, cancel: cancel, done: done}
}

// addActor admits id into h's membership then declares it desired on this
// daemon, built from def — the daemon-hosted counterpart of spawnWithPen's
// home-hosted admission (both go through Admit first: accept.go's 膜律
// requires an ACTIVE membership row before a daemon's attach may even open
// that id's stream).
func (wd *walkDaemon) addActor(t *testing.T, h *platform.Home, id actor.ActorID, kind actor.Kind, def actorbase.Def) actor.ActorID {
	t.Helper()
	minted, err := h.Admit(context.Background(), kind, strings.ReplaceAll(string(id), ":", "-"))
	if err != nil {
		t.Fatalf("admit %s: %v", id, err)
	}
	wd.builder.set(minted, platform.ActorFactory{Proc: def})
	wd.desired.add(actorrt.DesiredMember{ID: minted, Kind: kind, Lifecycle: actorrt.LifecycleAlwaysOn})
	return minted
}

// --- Controller pen (test driver identity) -------------------------------

// controllerPen is a no-op home-hosted cell whose only purpose is to capture
// the welded harness.Pen the substrate mints for it at admission — the
// legitimate way a test writes truth AS some caller identity (mirrors
// platform_test's own spawnWithPen, re-implemented here since this package
// cannot import that unexported helper across the package boundary).
type controllerPen struct{ pen harness.Pen }

func (controllerPen) Receive(context.Context, *message.Envelope) error { return nil }

// newControllerPen admits+spawns id as a home-hosted pen-bearing cell and
// returns its welded Pen — the identity the walk test writes requests AS.
func newControllerPen(t *testing.T, h *platform.Home, id actor.ActorID, kind actor.Kind) harness.Pen {
	t.Helper()
	var pen harness.Pen
	_, err := platform.SpawnForTesting(h, kind, strings.ReplaceAll(string(id), ":", "-"), platform.ActorFactory{Legacy: func(p harness.Pen) actorrt.Actor {
		pen = p
		return controllerPen{pen: p}
	}})
	if err != nil {
		t.Fatalf("spawn controller %s: %v", id, err)
	}
	return pen
}

// --- Request/response driving --------------------------------------------

// sendAndAwait writes a kind=request envelope (as callerPen) addressed to
// target and blocks for its terminal response. It retries with a FRESH
// message id (never resending a terminalized one) on a receiver_unavailable
// terminal or on an outright timeout with no terminal at all — both are the
// same underlying race: the daemon-hosted target may not have finished
// attaching (RunCompute's ring reconciles on its own poll tick + real WS
// dial latency) by the time the FIRST attempt lands. overall bounds the
// whole retry loop.
func sendAndAwait(t *testing.T, h *platform.Home, callerPen harness.Pen, target actor.ActorID, reqType string, payload any, overall time.Duration) message.Envelope {
	t.Helper()
	term, err := trySendAndAwait(h, callerPen, target, reqType, payload, overall)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return term
}

// trySendAndAwait is sendAndAwait's non-fatal core: same retry discipline
// (fresh message id on receiver_unavailable or an outright no-terminal
// timeout, bounded by overall), but returns an error instead of calling
// t.Fatalf — the seam a POLLING caller (walk_crash_test.go's own
// pollUntilLanded, waiting for an EVENTUAL landing across a simulated
// crash+reconnect) needs: a single connectivity hiccup must not abort the
// whole test the way a bare sendAndAwait call would.
func trySendAndAwait(h *platform.Home, callerPen harness.Pen, target actor.ActorID, reqType string, payload any, overall time.Duration) (message.Envelope, error) {
	var raw json.RawMessage
	if payload == nil {
		raw = json.RawMessage(`{}`) // L0 §2.2: envelope.payload=null is not legal
	} else {
		var err error
		raw, err = json.Marshal(payload)
		if err != nil {
			return message.Envelope{}, fmt.Errorf("marshal payload: %w", err)
		}
	}
	deadline := time.Now().Add(overall)
	for {
		reqID := message.ID(uuid.NewString())
		env := &message.Envelope{
			ID:         reqID,
			TS:         time.Now().UnixMilli(),
			Kind:       message.KindRequest,
			Type:       reqType,
			Payload:    raw,
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{target},
		}
		res, werr := callerPen.Write(context.Background(), env)
		if werr != nil {
			return message.Envelope{}, fmt.Errorf("pen.Write(%s -> %s): %w", reqType, target, werr)
		}
		if !res.Accepted() {
			return message.Envelope{}, fmt.Errorf("pen.Write(%s -> %s) rejected: %s (%s)", reqType, target, res.RejectReason, res.RejectDetail)
		}

		term, ok := waitForTerminal(h, reqID, 2*time.Second)
		if ok {
			if isReceiverUnavailable(term) {
				if time.Now().After(deadline) {
					return message.Envelope{}, fmt.Errorf("%s -> %s: still receiver_unavailable after %v (daemon never attached in time)", reqType, target, overall)
				}
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return term, nil
		}
		if time.Now().After(deadline) {
			return message.Envelope{}, fmt.Errorf("%s -> %s: no terminal within %v", reqType, target, overall)
		}
	}
}

// pollUntilLanded polls op (a request type whose walkResult.OK reports
// landing status, e.g. "walk.stat") every 150ms until OK=true or overall
// elapses — the crash-recovery walks' own "wait for the daemon's Scrubber
// pass to notice + resend Committed" primitive. A connectivity hiccup
// (mid-redial) is tolerated and simply retried, unlike a bare sendAndAwait
// call, which would abort the whole test on the FIRST such hiccup.
func pollUntilLanded(t *testing.T, h *platform.Home, callerPen harness.Pen, target actor.ActorID, op string, overall time.Duration) walkResult {
	t.Helper()
	deadline := time.Now().Add(overall)
	var last walkResult
	var lastErr error
	for time.Now().Before(deadline) {
		term, err := trySendAndAwait(h, callerPen, target, op, nil, 3*time.Second)
		if err != nil {
			lastErr = err
			time.Sleep(150 * time.Millisecond)
			continue
		}
		var p struct {
			Status string `json:"status"`
		}
		_ = json.Unmarshal(term.Payload, &p)
		if p.Status != "completed" {
			lastErr = fmt.Errorf("%s -> %s: status=%q, want completed (raw=%s)", op, target, p.Status, term.Payload)
			time.Sleep(150 * time.Millisecond)
			continue
		}
		var wr walkResult
		decodeWalkPayload(t, term, &wr)
		last = wr
		if wr.OK {
			return wr
		}
		lastErr = nil
		time.Sleep(150 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("pollUntilLanded(%s -> %s) never landed within %v: last error=%v", op, target, overall, lastErr)
	}
	t.Fatalf("pollUntilLanded(%s -> %s) never landed within %v: last result=%+v", op, target, overall, last)
	return walkResult{}
}

// waitForTerminal polls the channel log for a kind=response with the given
// parentID, up to timeout.
func waitForTerminal(h *platform.Home, parentID message.ID, timeout time.Duration) (message.Envelope, bool) {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := h.View().ReadAfterSeq(ctx, 0, 10000)
		if err == nil {
			for _, row := range rows {
				if row.Envelope.Kind == message.KindResponse && row.Envelope.ParentID == parentID {
					return row.Envelope, true
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return message.Envelope{}, false
}

func isReceiverUnavailable(term message.Envelope) bool {
	var p struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(term.Payload, &p); err != nil {
		return false
	}
	return p.Status == "failed" && p.Reason == string(message.TerminalReceiverUnavailable)
}

// walkResult is the common completion-status envelope every walk actor's
// reply payload carries (merged with additional per-type fields by
// behavior.RespondJSON — see the Status field on the resulting terminal
// payload). ok=false + reason carries a caller-facing failure summary for a
// resource op that came back rejected (NOT a Go/sys.Fail error — the request
// itself completed, the access verdict was a reject, exactly the "Outcome
// carries the verdict" discipline the accessdoor package documents).
type walkResult struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

// decodeWalkPayload unmarshals a terminal envelope's payload into dst
// (a struct also embedding/declaring a Status field for completeness checks)
// — helper to keep each walk test's assertions terse.
func decodeWalkPayload(t *testing.T, term message.Envelope, dst any) {
	t.Helper()
	if err := json.Unmarshal(term.Payload, dst); err != nil {
		t.Fatalf("decode terminal payload %s: %v", term.Payload, err)
	}
}

// --- Generic walk actor -----------------------------------------------

// walkOpFunc handles one request type for a walk actor: it does whatever
// sys.Resource() work the step needs and returns the JSON-marshalable reply
// value (success) or a (code, detail) pair (routed through sys.Fail — a
// genuine Go/protocol-level error, distinct from a resource-op REJECT, which
// a walkOpFunc instead encodes as a successful reply carrying
// walkResult{OK:false, Reason: string(outcome.RejectReason)} — the request
// itself completed; the access verdict was a deny/not_found, exactly the
// "Outcome carries the verdict" split accessdoor's own doc draws).
type walkOpFunc func(sys actorbase.Sys, msg actorbase.Msg) (reply any, failCode, failDetail string)

// walkActorDef builds an actorbase.Def whose Proc dispatches msg.Type
// against ops — the one shared shape every walk test's actors use (§6's
// "真实 actor 词表非直调门": every op below calls sys.Resource(), never
// accessdoor directly), so each walk file only supplies its own verb
// closures.
func walkActorDef(ops map[string]walkOpFunc) actorbase.Def {
	return actorbase.Def{
		Doc: "walk test driver actor (期11 §6): dispatches msg.Type to a verb table calling sys.Resource().",
		New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error {
				for {
					msg, err := sys.Recv()
					if err != nil {
						return err
					}
					if msg.Kind != message.KindRequest {
						continue
					}
					op, ok := ops[msg.Type]
					if !ok {
						_, _ = sys.Fail(msg, "type_unsupported", "walk actor does not handle "+msg.Type)
						continue
					}
					reply, code, detail := op(sys, msg)
					if code != "" {
						_, _ = sys.Fail(msg, code, detail)
						continue
					}
					_, _ = sys.Reply(msg, reply)
				}
			}, nil
		},
	}
}

// requireCompleted fails the test with a readable diagnostic if term is not
// a status=completed terminal.
func requireCompleted(t *testing.T, label string, term message.Envelope) {
	t.Helper()
	var p struct {
		Status string `json:"status"`
		Code   string `json:"error_code"`
		Detail string `json:"detail"`
	}
	decodeWalkPayload(t, term, &p)
	if p.Status != "completed" {
		t.Fatalf("%s: status=%q (code=%q detail=%q), want completed (raw=%s)", label, p.Status, p.Code, p.Detail, term.Payload)
	}
}
