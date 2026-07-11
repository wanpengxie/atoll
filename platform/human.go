package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// human.go is the subjectgate door's LAW-FORM face, rebuilt per正典 (期12 —
// actor-base-model 三层律 v0.4 + humancell-design v0.5). Home.Human hands the
// app a HumanHandle — the verbs an off-process subject drives — while every
// capability stays welded INSIDE the wall on the subject's own LIVE CELL:
//
//   - 能力取用，不现铸 (P2): the pen/schedule/access all live on the cell's
//     Caps (minted only at buildCaps by the supply ring). The door takes the
//     cell's OccupantDriver per verb (identity-bound lazy — no per-handle
//     capability cache) and drives it; cell absent = honest refusal
//     (ErrCellUnavailable, a transient — the ring re-mints next tick),
//     NEVER a door-side mint.
//   - 活性执法 = cell 生死 (P3): the per-call gate is "the live cell's driver
//     answered" — membership reads happen ONLY for error classification
//     (not_member vs cell-unavailable), never as a liveness proxy.
//   - 权威源恒 from-log (P6/D1): Resolve/Cancel judge open/audience/sender
//     against the log/store; the engine's serve account is a per-incarnation
//     projection used only as its own double-answer guard.
//   - 门退纯层2 (P4): no construction, no入籍, no authentication re-do —
//     session→user is the membrane's (app) job; the door trusts the
//     authenticated id and enforces law.

// ErrClosed is every subjectgate verb's refusal once Home.Close has begun.
// Checked BEFORE any store read so a verb racing teardown never touches a
// closing store. The app maps it to 503.
var ErrClosed = errors.New("platform: channel home is closed")

// ErrNotMember is the door's真实拒绝 for a subject that is not an active
// channel member (看得见≠在里面 — the membrane law). Classification read,
// never enforcement (P3). The app maps it to 403.
var ErrNotMember = errors.New("platform: not an active channel member")

// ErrCellUnavailable is the door's honest transient: the subject IS an active
// member but its cell is not currently drivable (the supply ring's re-mint
// window, an engine still starting, or a just-replaced incarnation's membrane
// rejecting a stale capability). Retryable — the ring re-mints on the next
// tick/poke; the app maps it to a retryable code, never internal.
var ErrCellUnavailable = errors.New("platform: subject cell unavailable (transient — supply ring re-mints)")

// ErrRequestNotFound is returned by Resolve/Cancel when reqID has no request
// row in this channel's log.
var ErrRequestNotFound = errors.New("platform: request not found")

// ErrNotRequestSender is Cancel's执法: only the sender of a request may
// cancel it (帽①: 撤自己的). The app maps it to 403.
var ErrNotRequestSender = errors.New("platform: not the request's sender")

// ErrRequestClosed is Resolve/Cancel's "already终态". The app maps it to 409.
var ErrRequestClosed = errors.New("platform: request already closed")

// ErrNotInAudience is Resolve's guard: a subject may only resolve a request
// addressed to it (audience 含我). The app maps it to 403.
var ErrNotInAudience = errors.New("platform: not in the request's audience")

// ErrInvalidDecision is Resolve's入口 guard on the decision verb: only
// approved / rejected are valid human decisions (both close the request —
// they BOTH map to completed+payload.decision). The app maps it to 400.
var ErrInvalidDecision = errors.New("platform: resolve decision must be approved or rejected")

// maxJSONDepth bounds the container nesting a client-supplied JSON blob may
// carry before it is decoded into an UNSTRUCTURED map[string]any (Unmarshal
// recurses per nesting level — an over-deep blob is a fatal stack overflow,
// not a catchable error).
const maxJSONDepth = 64

// boundedJSONDepth scans raw's structural tokens WITHOUT materialising any
// value (json.Decoder.Token is iterative) and errors when container nesting
// exceeds maxJSONDepth. Malformed JSON returns nil here — the caller's own
// Unmarshal surfaces the parse error.
func boundedJSONDepth(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	depth := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return nil
		}
		switch tok {
		case json.Delim('{'), json.Delim('['):
			depth++
			if depth > maxJSONDepth {
				return fmt.Errorf("platform: json nesting exceeds %d levels", maxJSONDepth)
			}
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
	}
}

// WriteRejectedError wraps a substrate write reject so the app can surface it
// (422) without touching harness/actorbase types itself — the door re-wraps
// the engine's typed actorbase.WriteRejected carrier into this public form.
type WriteRejectedError struct {
	Reason string
	Detail string
}

func (e *WriteRejectedError) Error() string {
	return "platform: write rejected: " + e.Reason + " (" + e.Detail + ")"
}

// SubmitSpec is the content-only shape of one subject write: identity
// (sender/channel_id) is welded by the cell's own pen; audience/kind are the
// app's resolved routing decision (机制/政策劈分). ExpiresAt, when set, is
// the request's declared deadline (the expiry reaper's durable contract).
type SubmitSpec struct {
	ID         message.ID
	Type       string
	Kind       message.Kind
	Payload    json.RawMessage
	Audience   []actor.ActorID
	Visibility message.Visibility
	ParentID   message.ID
	ExpiresAt  *int64
}

// HumanHandle is the door's law-form面 for one subject in one channel —
// identity-bound and LAZY: it holds NO capability of its own (zero pen/sched
// fields — P2 能力取用不现铸). Every verb takes the live cell's driver at
// call time, so a handle that outlives a Remove or a cell replacement fails
// honestly per call instead of wielding a stale capability.
type HumanHandle struct {
	home   *Home
	userID actor.ActorID
}

// Human returns the door handle for subject id in this channel. Closed check
// first (ErrClosed before any store read — the old door's次序讲究 kept), then
// a membership CLASSIFICATION read (non-member → ErrNotMember; 分类非执法,
// P3). Zero construction, zero入籍 — connection/write is never a supply
// entry (三入口三原语之外无第四). A member whose cell is mid-re-mint still
// gets a handle: presence feeds and tail work, writes answer
// ErrCellUnavailable until the ring catches up (no permanent degradation for
// a connect landing in the absent window).
func (h *Home) Human(ctx context.Context, id actor.ActorID) (*HumanHandle, error) {
	if id == "" {
		return nil, fmt.Errorf("platform: Human id required")
	}
	if h.closed.Load() {
		return nil, ErrClosed
	}
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("platform: Human membership lookup: %w", err)
	}
	if !ok || !rec.IsActive() || rec.Kind != actor.KindHuman {
		return nil, ErrNotMember
	}
	return &HumanHandle{home: h, userID: id}, nil
}

// HumanPrincipal resolves the current human instance by the opaque principal
// column. Actor-id segments are diagnostic only and are never parsed.
func (h *Home) HumanPrincipal(ctx context.Context, principal string) (*HumanHandle, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	reg, ok := h.cs.Registry.(storespec.PrincipalRegistry)
	if !ok {
		return nil, errors.New("platform: principal registry unavailable")
	}
	rec, found, err := reg.LookupActivePrincipal(ctx, actor.KindHuman, principal)
	if err != nil {
		return nil, fmt.Errorf("platform: Human principal lookup: %w", err)
	}
	if !found {
		return nil, ErrNotMember
	}
	return h.Human(ctx, rec.ID)
}

// driverFor is every verb's first line: take the live cell's OccupantDriver
// NOW (per-verb, no cache). A miss is classified — not enforced — by one
// registry read: still an active member means the cell is in the ring's
// re-mint window (ErrCellUnavailable, transient); otherwise the subject was
// removed (ErrNotMember).
func (h *Home) driverFor(ctx context.Context, id actor.ActorID) (actorrt.OccupantDriver, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if d, ok := h.channel.Cells().Driver(id); ok {
		return d, nil
	}
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("platform: driver membership classify: %w", err)
	}
	if !ok || !rec.IsActive() {
		return nil, ErrNotMember
	}
	return nil, ErrCellUnavailable
}

// mapDriveErr folds the engine/membrane sentinels into the door's public
// error family: the occupant gate (engine not yet Running / draining) and
// the three live-membrane rejections (pen/schedule/access of a replaced or
// dead incarnation) are all the same honest transient — ErrCellUnavailable.
// The engine's typed reject carrier re-wraps as the public
// WriteRejectedError. Everything else passes through untouched.
func mapDriveErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, actorbase.ErrOccupantNotReady) ||
		errors.Is(err, link.ErrWriterNotLive) ||
		errors.Is(err, link.ErrScheduleNotLive) ||
		errors.Is(err, link.ErrAccessNotLive) {
		return ErrCellUnavailable
	}
	var wr *actorbase.WriteRejected
	if errors.As(err, &wr) {
		return &WriteRejectedError{Reason: wr.Reason, Detail: wr.Detail}
	}
	return err
}

// Submit commits one subject write through the live cell's own welded pen
// (DriveWrite) and returns the receipt (message id + seq) synchronously.
// ID defaults to a fresh uuid, kind to request (the old door's contract).
// NO caller ledger, NO author#2 arm: deadline closure is the substrate
// expiry reaper's obligation (义务归位 D3) — the harness stamps a default
// TTL when the app declares none.
func (h HumanHandle) Submit(ctx context.Context, spec SubmitSpec) (message.ID, int64, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return "", 0, err
	}
	id := spec.ID
	if id == "" {
		id = message.ID(uuid.NewString())
	}
	kind := spec.Kind
	if kind == "" {
		kind = message.KindRequest
	}
	msgID, seq, derr := drv.DriveWrite(actorrt.DriveWrite{
		ID:         id,
		Type:       spec.Type,
		Kind:       kind,
		Payload:    spec.Payload,
		Audience:   spec.Audience,
		Visibility: spec.Visibility,
		ParentID:   spec.ParentID,
		ExpiresAt:  spec.ExpiresAt,
	})
	if derr != nil {
		return "", 0, mapDriveErr(derr)
	}
	return msgID, seq, nil
}

// Resolve answers a deferred request addressed to this subject (帽①的应答).
// From-log five steps (D1 — the truth is in the log, not memory): recover
// the request by id → audience 含我 → still open (OpenRequestsForActor) →
// bounded-depth payload merge → completed+payload.decision through the live
// cell (DriveRespond; approve AND reject BOTH map to completed — 拒绝是把
// 问题答完了, failed is the machine terminal). Restart-safe: a request left
// open by a crashed incarnation resolves fine on the re-minted cell.
func (h HumanHandle) Resolve(ctx context.Context, reqID message.ID, decision string, payload json.RawMessage) error {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return err
	}
	// decision闭集 BEFORE any log work: payload.decision becomes permanent
	// channel truth, garbage must never land there.
	if decision != "approved" && decision != "rejected" {
		return ErrInvalidDecision
	}
	req, ok, err := h.home.cs.Requests.FindByID(ctx, reqID)
	if err != nil {
		return fmt.Errorf("platform: Resolve lookup: %w", err)
	}
	if !ok || req == nil {
		return ErrRequestNotFound
	}
	if !audienceContains(req.Audience, h.userID) {
		return ErrNotInAudience
	}
	open, err := h.home.isRequestOpen(ctx, h.userID, reqID)
	if err != nil {
		return fmt.Errorf("platform: Resolve open-check: %w", err)
	}
	if !open {
		return ErrRequestClosed
	}
	merged := map[string]any{}
	if len(payload) > 0 {
		if derr := boundedJSONDepth(payload); derr != nil {
			return &WriteRejectedError{Reason: "bad_payload", Detail: derr.Error()}
		}
		if uerr := json.Unmarshal(payload, &merged); uerr != nil {
			return &WriteRejectedError{Reason: "bad_payload", Detail: uerr.Error()}
		}
		// JSON `null` is legal JSON that unmarshals a map to nil — writing
		// merged["decision"] below would panic (修复批 P1-1: a ws client
		// could kill the connection handler with payload:null). A null
		// payload reads as "no payload".
		if merged == nil {
			merged = map[string]any{}
		}
	}
	merged["decision"] = decision
	raw, _ := json.Marshal(merged)
	if _, werr := drv.DriveRespond(req, actorrt.DriveRespond{
		Status:  message.StatusCompleted,
		Payload: raw,
	}); werr != nil {
		return mapDriveErr(werr)
	}
	return nil
}

// Cancel撤自己发出的 request (帽①, 五步 from-log): recover by id → sender==我
// (硬执法) → still open → write the cancel terminal through the live cell
// (caller self-close: failed+unanswered_timeout+cancelled:true, 逐字对齐
// callLedger.cancel; a concurrent real answer racing it is benign — terminal
// duplicate reads as「已完成」) → CancelRequest interrupt hint (best-effort;
// truth already closed). Cross-incarnation safe for the same reason Resolve
// is; the daemon上行 sibling of this geometry is handleCancelUpstream.
func (h HumanHandle) Cancel(ctx context.Context, reqID message.ID) error {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return err
	}
	req, ok, err := h.home.cs.Requests.FindByID(ctx, reqID)
	if err != nil {
		return fmt.Errorf("platform: Cancel lookup: %w", err)
	}
	if !ok || req == nil {
		return ErrRequestNotFound
	}
	if req.Sender.ID != h.userID {
		return ErrNotRequestSender
	}
	var receiver actor.ActorID
	if len(req.Audience) > 0 {
		receiver = req.Audience[0]
	}
	open, err := h.home.isRequestOpen(ctx, receiver, reqID)
	if err != nil {
		return fmt.Errorf("platform: Cancel open-check: %w", err)
	}
	if !open {
		return ErrRequestClosed
	}
	cancelPayload, _ := json.Marshal(map[string]any{
		"error_code": string(message.TerminalUnansweredTimeout),
		"detail":     "cancelled by sender",
		"cancelled":  true,
	})
	if _, werr := drv.DriveRespond(req, actorrt.DriveRespond{
		Status:  message.StatusFailed,
		Reason:  string(message.TerminalUnansweredTimeout),
		Payload: cancelPayload,
	}); werr != nil {
		return mapDriveErr(werr)
	}
	if receiver != "" {
		h.home.CancelRequest(receiver, reqID)
	}
	return nil
}

// isRequestOpen reports whether reqID is still an open request addressed to
// receiver — the truth-derived open-status check (from-log five steps' "仍
// open"). A closed (terminal-answered) or unknown request is not open.
func (h *Home) isRequestOpen(ctx context.Context, receiver actor.ActorID, reqID message.ID) (bool, error) {
	if receiver == "" {
		return false, nil
	}
	rows, err := h.cs.Query.OpenRequestsForActor(ctx, receiver)
	if err != nil {
		return false, err
	}
	for _, r := range rows {
		if r.Envelope.ID == reqID {
			return true, nil
		}
	}
	return false, nil
}

// After arms an identity-bound durable self-timer through the live cell
// (DriveAfter → schedule.BindIdentity: a subject's reminder is a promise
// that outlives incarnations and deploys — D7). The arming rides the cell's
// own liveSchedule membrane; a removed member's later fire is refused by the
// scheduler's own EnsureLive户籍 check (second line of defence — the dereg
// cascade already clears the rows).
func (h HumanHandle) After(ctx context.Context, d time.Duration, msgType string, payload []byte) (schedule.TimerID, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return "", err
	}
	id, derr := drv.DriveAfter(d, msgType, payload)
	if derr != nil {
		return "", mapDriveErr(derr)
	}
	return schedule.TimerID(id), nil
}

// CancelTimer cancels an identity-bound timer this subject armed (ack-less:
// a cancel racing an already-due fire may still see it ring).
func (h HumanHandle) CancelTimer(ctx context.Context, id schedule.TimerID) error {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return err
	}
	return mapDriveErr(drv.DriveCancelTimer(string(id)))
}

// --- Resource face (D10, day-1 = KV six + Share two) -----------------------
//
// The subject's resource face IS its cell's Caps.Access (Q-E 正形) — the door
// only forwards through the driver seam; enforcement is the cell's
// liveAccess membrane + the resource door's R, literally the same path a
// Proc's Sys.Resource takes (zero subject bypass). Open/CreateFile (file
// bytes) are not here: home-hosted byte redemption is deferred with the 债②
// file route.

func (h HumanHandle) ResourceCreate(ctx context.Context, id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	out, derr := drv.DriveResourceCreate(id, args)
	return out, mapDriveErr(derr)
}

func (h HumanHandle) ResourceRead(ctx context.Context, id resource.ResourceID) (accessdoor.Outcome, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	out, derr := drv.DriveResourceRead(id)
	return out, mapDriveErr(derr)
}

func (h HumanHandle) ResourceWrite(ctx context.Context, id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	out, derr := drv.DriveResourceWrite(id, args)
	return out, mapDriveErr(derr)
}

func (h HumanHandle) ResourceDelete(ctx context.Context, id resource.ResourceID) (accessdoor.Outcome, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	out, derr := drv.DriveResourceDelete(id)
	return out, mapDriveErr(derr)
}

func (h HumanHandle) ResourceStat(ctx context.Context, id resource.ResourceID) (accessdoor.StatResult, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return accessdoor.StatResult{}, err
	}
	out, derr := drv.DriveResourceStat(id)
	return out, mapDriveErr(derr)
}

func (h HumanHandle) ResourceList(ctx context.Context, q accessdoor.ListQuery) (accessdoor.ListPage, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return accessdoor.ListPage{}, err
	}
	out, derr := drv.DriveResourceList(q)
	return out, mapDriveErr(derr)
}

// ResourceShareActor grants target ops on id (set-sugar; the door's day-1
// ingress独立收窄 ops ⊆ {read,write} — 授权衰减律 rides the same Invoke).
// 产品面 (coral) wraps this as "给 T 设变量" — the subject never sees "share".
func (h HumanHandle) ResourceShareActor(ctx context.Context, id resource.ResourceID, target actor.ActorID, ops []access.Operation) (accessdoor.Outcome, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	out, derr := drv.DriveResourceShareActor(id, target, ops)
	return out, mapDriveErr(derr)
}

func (h HumanHandle) ResourceShareMembers(ctx context.Context, id resource.ResourceID, ops []access.Operation) (accessdoor.Outcome, error) {
	drv, err := h.home.driverFor(ctx, h.userID)
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	out, derr := drv.DriveResourceShareMembers(id, ops)
	return out, mapDriveErr(derr)
}

// --- L3 device presence (token form, 期12 S4) -------------------------------

// PresenceConnect feeds this subject's L3 device presence online for one
// gateway ws session and returns that session's own token (层3 obs 轴,
// advisory — 正交 actor 活性, 绝不互训). Token-set form: each ws connection
// holds its OWN token and a late disconnect can only remove ITSELF — the
// Remove→re-Admit straddle (an old tab's dying ws extinguishing a fresh
// session's online) is structurally gone, no generation counter needed.
// Straddle gate: membership + closed are re-checked INSIDE presenceMu so a
// stale handle held across a Remove cannot feed a removed id online (returns
// "" — a no-op token). Classification read, not enforcement.
func (h HumanHandle) PresenceConnect() string {
	return h.home.presenceConnect(h.userID)
}

// PresenceDisconnect drops one session token; the offline snapshot is fed
// only when the LAST token goes (显式 offline, 不靠 decay — 常驻 cell 不死).
// An empty or unknown token is a no-op.
func (h HumanHandle) PresenceDisconnect(token string) {
	h.home.presenceDisconnect(h.userID, token)
}

func (h *Home) presenceConnect(id actor.ActorID) string {
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	// Straddle gate (DoD-13): a handle minted before a Remove must not feed
	// a removed id online. Checked under presenceMu so it is ordered against
	// clearPresence's own lock hold.
	if h.closed.Load() {
		return ""
	}
	if rec, ok, err := h.cs.Registry.Lookup(context.Background(), id); err != nil || !ok || !rec.IsActive() {
		return ""
	}
	if h.presenceSessions == nil {
		h.presenceSessions = map[actor.ActorID]map[string]struct{}{}
	}
	set := h.presenceSessions[id]
	if set == nil {
		set = map[string]struct{}{}
		h.presenceSessions[id] = set
	}
	token := uuid.NewString()
	first := len(set) == 0
	set[token] = struct{}{}
	if first {
		h.feedDevicePresence(id, true)
	}
	return token
}

func (h *Home) presenceDisconnect(id actor.ActorID, token string) {
	if token == "" {
		return
	}
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	set := h.presenceSessions[id]
	if set == nil {
		return
	}
	if _, ok := set[token]; !ok {
		return
	}
	delete(set, token)
	if len(set) == 0 {
		delete(h.presenceSessions, id)
		h.feedDevicePresence(id, false)
	}
}

// clearPresence clears every session token for id and Forgets its fold
// snapshot — Remove's对称清账 (pending-leftovers #3): the ring's削 is a
// quiet teardown with no down edge, so the fold would otherwise keep serving
// a stale "online" forever. Forget decays the id to the honest unknown.
//
// Cross-generation guard (修复批 P1-2): both the token clear and the Forget
// run under presenceMu — the same lock presenceConnect feeds under — and the
// registry is re-read first: if id is ACTIVE again (a re-Admit already
// landed), this clear belongs to a dead generation and must not wipe the new
// session's account/snapshot. Residual honesty: membership writes are not
// serialized with presenceMu, so an exotic interleaving (re-Admit committing
// between this read and the clear) can still decay a fresh session to
// unknown — presence is ADVISORY three-state with unknown as the honest
// default, so the residual mis-state is a stale "unknown" until the next
// connect edge, never a false "online"; full linearization would couple the
// membership tx to the presence account, deliberately not built (pre-launch
// 不过度安全化).
func (h *Home) clearPresence(id actor.ActorID) {
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	if rec, ok, err := h.cs.Registry.Lookup(context.Background(), id); err == nil && ok && rec.IsActive() {
		return // a newer generation already re-admitted this id — not ours to clear
	}
	delete(h.presenceSessions, id)
	h.deviceFold.Forget(id)
}

// feedDevicePresence pushes an online/offline edge into the home
// device-presence fold through its ObsWatcher entry — the door is this
// subject's L3 producer.
func (h *Home) feedDevicePresence(id actor.ActorID, online bool) {
	h.deviceFold.OnObs(context.Background(), id,
		actorrt.Incarnation{},
		actorrt.ObsKind(introspect.ObsDevicePresence),
		actorrt.ObsValue(introspect.MarshalDevicePresence(online)))
}

// audienceContains reports whether id is in the audience list.
func audienceContains(aud message.Audience, id actor.ActorID) bool {
	for _, a := range aud {
		if a == id {
			return true
		}
	}
	return false
}
