package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// human.go is the subjectgate door's LAW-FORM face: Home.Human hands the app a
// HumanHandle — the four verbs a subject drives — while the write pen and the
// identity-bound schedule handle stay welded INSIDE the wall (the app never sees
// a harness.Pen or a Minter; red line 10 + archtest). The door is执法 + 通道:
// routing policy (app) and the统治面 (control plane) are显式 outside it (§2.7
// three-face law). It is kind-blind: the door neither knows nor needs to know
// whether the id behind a session is a person, a local agent (coral), or both —
// 法只认 id+凭据+门.

// ErrClosed is every subjectgate verb's refusal once Home.Close has begun (the
// home.closed flag is set). A ws/垫片 goroutine holding a HumanHandle is NOT joined
// by Close, so without this a post-Close Submit could mint a caller + Arm a死后
// timer against a closing store. The app maps it to 503 (retryable elsewhere / gone
// here). Zero side effect: a closed home admits and arms nothing.
var ErrClosed = errors.New("platform: channel home is closed")

// ErrNotMember is Home.Human's真实拒绝 for a subject that is not an active
// channel member (看得见≠在里面 — the membrane law). The app maps it to 403.
// Zero construction, zero入籍: Human() never admits, it only checks.
var ErrNotMember = errors.New("platform: not an active channel member")

// ErrRequestNotFound is returned by Resolve/Cancel when reqID has no request row
// in this channel's log (a bad id, or a cross-channel id the log refuses).
var ErrRequestNotFound = errors.New("platform: request not found")

// ErrNotRequestSender is Cancel's执法 (door step 8, kept hard): only the sender
// of a request may cancel it (帽①: 撤自己的). The app maps it to 403.
var ErrNotRequestSender = errors.New("platform: not the request's sender")

// ErrRequestClosed is Resolve/Cancel's "already终态" — the request已结束 (a
// terminal already exists). The app maps it to 409 (idempotent-friendly).
var ErrRequestClosed = errors.New("platform: request already closed")

// ErrNotInAudience is Resolve's guard: a subject may only resolve a request that
// is addressed to it (audience 含我). The app maps it to 403.
var ErrNotInAudience = errors.New("platform: not in the request's audience")

// WriteRejectedError wraps a substrate write reject (a non-Accepted WriteResult)
// so the app can surface it as 422 without ever touching harness types itself.
type WriteRejectedError struct {
	Reason string
	Detail string
}

func (e *WriteRejectedError) Error() string {
	return "platform: write rejected: " + e.Reason + " (" + e.Detail + ")"
}

// SubmitSpec is the content-only shape of one subject write: identity
// (sender/channel_id) is left for the pen to weld, and audience/kind are already
// resolved by the app's routing policy before Submit is called (机制/政策劈分,
// Desired/Builder 同款). ExpiresAt, when set, is the caller's deadline for a
// kind=request (drives the receiver's closure).
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

// HumanHandle is the door's law-form面 for one user in one channel — Submit /
// Resolve / Cancel / After / CancelTimer. It holds the user's welded pen and
// identity-bound schedule handle inside the wall. Every verb RE-CHECKS active
// membership before acting: the door executes a动词, it does not hand out a
// long-lived bare capability — a handle whose user was removed AFTER Home.Human
// returned rejects every write (防死后写回潮). Human() itself builds nothing
// durable: the pen/schedule are门现铸 (minter.Mint / schedMinter.Mint,
// id-scoped, emitSink 先例), independent of any cell being live.
type HumanHandle struct {
	home   *Home
	userID actor.ActorID
	pen    harness.Pen
	sched  schedule.ScheduleHandle
}

// Human returns the door handle for user id (a "user:<id>" actor id) in this
// channel after户籍校验 — a non-member gets ErrNotMember (真实拒绝, zero
// construction). Embodiment (the Receive cell) is the ring's via Admit, never
// Human's; connection/write is NOT a construction entry (三入口三原语之外无第四).
func (h *Home) Human(ctx context.Context, id actor.ActorID) (*HumanHandle, error) {
	if id == "" {
		return nil, fmt.Errorf("platform: Human id required")
	}
	if err := h.requireActiveMember(ctx, id); err != nil {
		return nil, err
	}
	return &HumanHandle{
		home:   h,
		userID: id,
		pen:    h.minter.Mint(id, actor.KindHuman, h.channelID),
		sched:  h.schedMinter.Mint(id),
	}, nil
}

// requireActiveMember is the per-verb membrane + liveness gate every subjectgate
// entry runs first: the home must be OPEN (else ErrClosed — a post-Close verb on an
// un-joined ws goroutine must not act) AND id must be an active registry member (else
// ErrNotMember). Order matters: closed is checked BEFORE the store lookup, so a verb
// racing teardown never touches a closing store.
func (h *Home) requireActiveMember(ctx context.Context, id actor.ActorID) error {
	if h.closed.Load() {
		return ErrClosed
	}
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil {
		return fmt.Errorf("platform: Human membership lookup: %w", err)
	}
	if !ok || !rec.IsActive() {
		return ErrNotMember
	}
	return nil
}

// clock adapts Home's ms clock to behavior's func() time.Time.
func (h HumanHandle) clock() func() time.Time {
	return func() time.Time { return time.UnixMilli(h.home.nowMs()) }
}

// humanCaller returns the home-scoped author#2 closure manager for a human
// subject, welded to that user's own pen (minter.Mint id-scoped — the same
// identity Human()'s handle writes under). Get-or-create under lock: one Caller
// per (channel, user), SHARED by Submit's Arm (gateway goroutine) and the human
// cell's Match (cell goroutine), and stable across cell crash/revive so a request
// armed before the crash still gets its unanswered_timeout after rebirth.
func (h *Home) humanCaller(id actor.ActorID) *behavior.Caller {
	h.humanCallersMu.Lock()
	defer h.humanCallersMu.Unlock()
	if h.humanCallers == nil {
		h.humanCallers = map[actor.ActorID]*behavior.Caller{}
	}
	if c, ok := h.humanCallers[id]; ok {
		return c
	}
	pen := h.minter.Mint(id, actor.KindHuman, h.channelID)
	clock := func() time.Time { return time.UnixMilli(h.nowMs()) }
	c := behavior.NewCaller(pen, clock, nil)
	// Home closing: never (re)mint a caller INTO the index after teardown began.
	// stopAllHumanCallers has cleared it, and re-storing would resurrect a caller
	// whose Arm could fire a死后 terminal through the pen into a closing store. The
	// verb entries are already ErrClosed-gated (requireActiveMember), so this is the
	// belt-and-suspenders half — a caller is still returned for any non-verb holder,
	// just left unindexed so the cleared index stays cleared.
	if h.closed.Load() {
		return c
	}
	h.humanCallers[id] = c
	return c
}

// stopHumanCaller stops and drops the shared per-user Caller for id: its pending
// timers are stopped (no fireTimeout fires after the subject is gone) and the
// by-id index entry is deleted. Called from Home.Remove — once id is no longer a
// member, an armed request must NOT still write a死后 unanswered_timeout through
// the裸 pen, and the index must not grow monotonically. A no-op if id never had a
// caller (e.g. a non-human removal). A cell crash/revive does NOT call this: the
// Caller is keyed by id (stable across incarnations), so a request armed before a
// crash still gets its terminal after rebirth (既有 by-id 稳定语义).
func (h *Home) stopHumanCaller(id actor.ActorID) {
	h.humanCallersMu.Lock()
	c := h.humanCallers[id]
	delete(h.humanCallers, id)
	h.humanCallersMu.Unlock()
	if c != nil {
		c.Stop()
	}
}

// stopAllHumanCallers stops every live per-user Caller at teardown and clears the
// index. CALL SITE (Home.Close): AFTER cells are stopped (no cell goroutine can
// Arm a fresh request into a caller we are about to drop) and BEFORE the stores
// close (a still-armed timer firing would write a terminal through the pen into an
// already-closing store).
func (h *Home) stopAllHumanCallers() {
	h.humanCallersMu.Lock()
	callers := make([]*behavior.Caller, 0, len(h.humanCallers))
	for _, c := range h.humanCallers {
		if c != nil {
			callers = append(callers, c)
		}
	}
	h.humanCallers = map[actor.ActorID]*behavior.Caller{}
	h.humanCallersMu.Unlock()
	for _, c := range callers {
		c.Stop()
	}
}

// Submit commits one subject write through the welded pen and returns the receipt
// (message id + seq) synchronously (POST 201 / ws ack same shape). Per-call户籍
// 校验 first: a removed member's stale handle is rejected. audience/kind are the
// app's resolved routing decision — Submit writes what it is told, identity is
// substrate-welded.
func (h HumanHandle) Submit(ctx context.Context, spec SubmitSpec) (message.ID, int64, error) {
	if err := h.home.requireActiveMember(ctx, h.userID); err != nil {
		return "", 0, err
	}
	now := h.home.nowMs()
	id := spec.ID
	if id == "" {
		id = message.ID(uuid.NewString())
	}
	kind := spec.Kind
	if kind == "" {
		kind = message.KindRequest
	}
	aud := make(message.Audience, 0, len(spec.Audience))
	aud = append(aud, spec.Audience...)
	// Sender / ChannelID left zero: the pen welds the subject's own identity at
	// write time (sealed-pen). The door holds the pen; the app never does.
	env := &message.Envelope{
		ID:         id,
		TS:         now,
		Kind:       kind,
		Type:       spec.Type,
		Audience:   aud,
		Payload:    spec.Payload,
		Visibility: spec.Visibility,
		ParentID:   spec.ParentID,
		ExpiresAt:  spec.ExpiresAt,
	}
	res, err := h.pen.Write(ctx, env)
	if err != nil {
		return "", 0, err
	}
	if !res.Accepted() {
		return "", 0, &WriteRejectedError{Reason: string(res.RejectReason), Detail: res.RejectDetail}
	}
	// author#2: a subject-authored request arms the shared per-user Caller so an
	// unanswered request gets its unanswered_timeout terminal (fired off the
	// gateway goroutine, closed by the cell goroutine's Match). A no-deadline
	// request registers in-flight without a timer (no closure guarantee owed).
	if kind == message.KindRequest {
		h.home.humanCaller(h.userID).Arm(env)
	}
	return res.MessageID, res.Seq, nil
}

// Resolve answers a deferred request addressed to this subject (帽①的应答). It is
// from-log: the original request is recovered by id (A-P12 消解 — no request read
// port), then校验 audience含我 + 仍 open. approve AND reject BOTH map to
// `completed` + payload.decision — 拒绝是把问题答完了,不是失败 (failed is the
// machine terminal, INVARIANT-10's三值闭集, never a human's rejection). Restart-
// safe: the truth is in the log, not memory.
func (h HumanHandle) Resolve(ctx context.Context, reqID message.ID, decision string, payload json.RawMessage) error {
	if err := h.home.requireActiveMember(ctx, h.userID); err != nil {
		return err
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
		if uerr := json.Unmarshal(payload, &merged); uerr != nil {
			return &WriteRejectedError{Reason: "bad_payload", Detail: uerr.Error()}
		}
	}
	merged["decision"] = decision
	raw, _ := json.Marshal(merged)
	if _, werr := behavior.Respond(ctx, h.pen, h.clock(), req, behavior.ResponseSpec{
		Status:  message.StatusCompleted,
		Payload: raw,
	}); werr != nil {
		return werr
	}
	return nil
}

// Cancel撤自己发出的 request (帽①, 五步): from-log → sender==我 (硬执法) → 已有
// 终态则「已结束」→ write the cancel terminal (撞 duplicate 呈现「已完成」) →
// 门内直调 Home.CancelRequest 发打断. Restart-safe.
func (h HumanHandle) Cancel(ctx context.Context, reqID message.ID) error {
	if err := h.home.requireActiveMember(ctx, h.userID); err != nil {
		return err
	}
	// 步1: from-log.
	req, ok, err := h.home.cs.Requests.FindByID(ctx, reqID)
	if err != nil {
		return fmt.Errorf("platform: Cancel lookup: %w", err)
	}
	if !ok || req == nil {
		return ErrRequestNotFound
	}
	// 步2: sender==我 (门第8步硬执法).
	if req.Sender.ID != h.userID {
		return ErrNotRequestSender
	}
	// 步3: 已有终态 → 已结束. A request is open iff its receiver still lists it as
	// an open request; sender==我 above guarantees a single-audience request has a
	// receiver to ask.
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
	// 步4: 写取消终态 (逐字对齐 callLedger.cancel: failed+unanswered_timeout+
	// cancelled:true). behavior.Respond treats a HarnessTerminalDuplicate as
	// success (nil err) — a concurrent real answer racing the cancel is benign
	// (呈现「已完成」).
	cancelPayload, _ := json.Marshal(map[string]any{
		"error_code": string(message.TerminalUnansweredTimeout),
		"detail":     "cancelled by sender",
		"cancelled":  true,
	})
	if _, werr := behavior.Respond(ctx, h.pen, h.clock(), req, behavior.ResponseSpec{
		Status:  message.StatusFailed,
		Reason:  string(message.TerminalUnansweredTimeout),
		Payload: cancelPayload,
	}); werr != nil {
		return werr
	}
	// 步5: 门内直调 Home.CancelRequest 发打断 (best-effort hint; the terminal above
	// already closed truth). target = the request's receiver.
	if receiver != "" {
		h.home.CancelRequest(receiver, reqID)
	}
	return nil
}

// isRequestOpen reports whether reqID is still an open request addressed to
// receiver (the truth-derived open-status check the door uses for Resolve's
// "仍 open" and Cancel's "已结束"). A closed (terminal-answered) or unknown
// request is not open.
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

// After arms an identity-bound self-timer (提醒过重启 — BindIdentity survives a
// restart; G19 消解, no app-held handle). 裸 identity-bound schedMinter.Mint,
// NOT wrapped in the liveSchedule membrane (that binds a live incarnation and
// cannot mint with zero cell — the door's schedule is id-scoped, self-heals
// through EnsureLive on fire).
func (h HumanHandle) After(ctx context.Context, d time.Duration, msgType string, payload []byte) (schedule.TimerID, error) {
	if err := h.home.requireActiveMember(ctx, h.userID); err != nil {
		return "", err
	}
	fireAt := h.home.nowMs() + d.Milliseconds()
	return h.sched.Schedule(ctx, schedule.ScheduleReq{
		Bind:    schedule.BindIdentity,
		FireAt:  fireAt,
		Type:    msgType,
		Payload: payload,
	})
}

// CancelTimer cancels an identity-bound timer this subject armed (ack-less: a
// cancel racing an already-due fire may still see it ring, schedule contract).
func (h HumanHandle) CancelTimer(ctx context.Context, id schedule.TimerID) error {
	if err := h.home.requireActiveMember(ctx, h.userID); err != nil {
		return err
	}
	return h.sched.Cancel(ctx, id)
}

// PresenceConnect feeds this subject's L3 device presence online for one gateway
// ws session (层3 obs 轴, advisory — 正交 actor 活性, 绝不互训). Refcounted per
// (channel, user): the online edge is fed only on the FIRST session; a later tab
// just increments. No户籍 re-check — presence is advisory and the handle was
// already gated by Home.Human's membership check; a removed member's stale feed is
// harmless (its cell is gone).
func (h HumanHandle) PresenceConnect() {
	h.home.presenceConnect(h.userID)
}

// PresenceDisconnect feeds this subject's L3 device presence offline when the LAST
// session drops (显式 offline snapshot, 不靠 decay — 常驻 cell 不死). A non-final
// disconnect only decrements; a stale disconnect past zero is a no-op.
func (h HumanHandle) PresenceDisconnect() {
	h.home.presenceDisconnect(h.userID)
}

// presenceConnect increments the subject's session refcount and feeds the online
// device-presence edge on the 0→1 edge, under presenceMu (edges totally ordered).
func (h *Home) presenceConnect(id actor.ActorID) {
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	if h.presenceSessions == nil {
		h.presenceSessions = map[actor.ActorID]int{}
	}
	was := h.presenceSessions[id]
	h.presenceSessions[id] = was + 1
	if was == 0 {
		h.feedDevicePresence(id, true)
	}
}

// presenceDisconnect decrements the subject's session refcount and feeds the
// offline edge only on the 1→0 edge (last session), under presenceMu.
func (h *Home) presenceDisconnect(id actor.ActorID) {
	h.presenceMu.Lock()
	defer h.presenceMu.Unlock()
	n := h.presenceSessions[id]
	if n <= 0 {
		return
	}
	n--
	if n == 0 {
		delete(h.presenceSessions, id)
		h.feedDevicePresence(id, false)
		return
	}
	h.presenceSessions[id] = n
}

// feedDevicePresence pushes an online/offline edge into the home device-presence
// fold through its existing ObsWatcher entry (the same fold a daemon adapter feeds
// via the obs axis) — the door is this subject's L3 producer.
func (h *Home) feedDevicePresence(id actor.ActorID, online bool) {
	h.deviceFold.OnObs(context.Background(), id,
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
