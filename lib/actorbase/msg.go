package actorbase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// Msg is the substrate's in-hand projection of one delivered envelope — pure,
// immutable data with zero-effect methods (spec §1.2: "Msg=纯不可变数据零效果
// 方法,泄漏 msg=泄漏只读数据;全系统唯一有效果的把手=sys"). It embeds the
// envelope VERBATIM — the occupant reads the same truth row the feed/tail
// read path already serves in full — plus the one thing an envelope itself
// does not carry: the ctx this particular delivery is scoped to.
//
// The embedding replaced two hand-copied 12-field projection tables (purity
// 手动档, owner 2026-07-13): the original NewMsg mirror silently dropped
// TSReceived (an unrecorded transcription miss, not curation — the field was
// always occupant-visible through the read path), and the reverse projection
// hand-assigned every field to route around the envelope-literal fence.
// Embedding makes the "1:1 projection" claim a structural fact: a future
// envelope field rides along automatically, in both directions.
//
// Two面 notes the embedding creates:
//   - TSReceived is now visible on Msg: it is the engine's truth-commit
//     stamp (overwritten unconditionally at append) — on any envelope an
//     occupant builds itself it is meaningless and stays zero.
//   - Msg is an IN-MEMORY handle, never a wire object — do not serialize it.
//     (The embedded envelope promotes Envelope's strict UnmarshalJSON onto
//     Msg; nothing may rely on that.)
//
// Ctx is unexported and reached only through the Ctx() accessor (mirrors
// http.Request.Context()) — a request-scoped struct is the one place Go
// tolerates ctx-as-a-field, and keeping it unexported stops a caller from
// constructing a Msg with a stray ctx that did not come from the engine's own
// ledger. For a MAILBOX-origin kind=request delivery, Ctx() is derived from the
// serve ledger's entry (deadline + cancel, §1.5); for event/timer/no-one-
// waiting deliveries (no closure obligation), Ctx() IS sys.Life() — one
// process-life ctx, one name, no second "background" ctx anywhere in this
// package. A LOG-origin Msg promises neither (see MsgOrigin).
type Msg struct {
	message.Envelope

	ctx    context.Context
	origin MsgOrigin
	app    harness.Context
}

// MsgOrigin names WHICH ledger authorises a write against this Msg. It is the
// substrate's answer to a structural fact, not a convenience flag: an
// off-process subject has no mailbox, so it never holds a delivery handle —
// its frames carry a bare request id and its authority can only come from the
// LOG. Making that the caller-visible fact (rather than a parallel set of
// Identity-suffixed verbs) keeps "trust the ledger or trust the log" a
// first-class distinction instead of one hidden inside a method name.
//
// The type and its constants are EXPORTED because NewMsg is called from
// outside this package; the Msg FIELD is unexported so no one can build a Msg
// whose origin did not come through NewMsg's check.
type MsgOrigin uint8

const (
	// OriginUnset is the ZERO value and is ILLEGAL. It is never a default and
	// never a fallback arm: NewMsg panics on it, and any write verb that meets
	// it answers ErrMsgOriginUnset. It exists only so a Msg that skipped NewMsg
	// (a zero-value discard leaking into a live path) fails loud instead of
	// silently taking the mailbox arm.
	OriginUnset MsgOrigin = iota

	// OriginMailbox is a delivery: the engine's Recv path (projectWork) and
	// pendingTicket.Wait construct it. The authority for a write against it is
	// the SERVE LEDGER — Reply/Fail/Progress all gate on isClosed. For a
	// kind=request delivery Ctx() IS the request scope (deadline + cancel).
	OriginMailbox

	// OriginLog is a request recovered through a log lookup by a holder that
	// may never have Recv'd it — the off-process subject's only handle shape.
	// Its contract (spec §3.1a), pinned by tests:
	//
	//  1. Ctx() does NOT promise request scope. It carries whatever ctx the
	//     constructor was handed (in practice sys.Life()), so a consumer must
	//     NOT thread it downstream as "this request's ctx" — work threaded on
	//     it would outrun the request's deadline/cancel without erroring.
	//  2. It NEVER enters a mailbox and is never produced by Recv.
	//  3. It is a TERMINAL-ONLY write handle: Reply and Fail only. Progress
	//     against it is misuse (ErrLogOriginTerminalOnly) — a provisional is
	//     not a terminal, and the whole point of this handle is "recover it,
	//     write the terminal, drop it".
	//
	// The write itself is not ledger-gated: the authority is the log, and the
	// backstop is the harness (terminal-uniqueness index + the four-arm
	// response authorization table). actorbase grows NO truth-query dependency
	// for it.
	OriginLog
)

// Ctx returns the ctx this delivery is scoped to. See the Sys godoc's
// provenance rule for which ctx a Proc body should thread onward from here —
// and MsgOrigin's OriginLog note for the one case where Ctx() promises
// nothing about the request's own scope.
func (m Msg) Ctx() context.Context { return m.ctx }

// Cause is what a Proc hands to a write verb when the thing it is about to
// write exists because of THIS message — the ordinary case, since an actor's
// outbound traffic is nearly always work it is doing on someone's behalf. The
// answering verbs (Reply/Fail/Progress) take the Msg itself and derive this
// internally; the originating verbs take it explicitly, because they are the
// ones that also have a legitimate other answer (message.Root()).
func (m Msg) Cause() message.Cause { return message.From(m.Envelope) }

func (m Msg) Caller() (harness.Caller, bool) {
	if m.app.Caller == nil {
		return harness.Caller{}, false
	}
	return *m.app.Caller, true
}

// Context returns the immutable application context carried by the ledger row.
func (m Msg) Context() harness.Context { return m.app.Clone() }

// WithSession returns a request value with an explicitly selected session.
// It changes neither the original message nor any actor-global state.
func (m Msg) WithSession(session string) (Msg, error) {
	if strings.TrimSpace(session) == "" || strings.TrimSpace(session) != session || session == "new" {
		return Msg{}, errors.New("actorbase: session must be a concrete non-blank id")
	}
	m.app = m.app.Clone()
	m.app.Session = session
	return m, nil
}

// EffectiveCaller is the only caller-attribution rule used by receivers.
func EffectiveCaller(m Msg) harness.Caller {
	if caller, ok := m.Caller(); ok {
		return caller
	}
	return harness.Caller{Channel: m.ChannelID, Actor: m.Sender.ID}
}

// NewMsg projects env into a Msg bound to ctx and to the ledger that
// authorises writes against it — the ONE constructor (the engine's Recv path,
// the frame interpreter's from-log recovery, and any test fixture all go
// through it; there is no second, partial way to build a populated Msg). env
// is copied by value, never retained: mutating the envelope after projection
// cannot reach back into an already-delivered Msg (Payload/Audience share
// backing arrays exactly as the old field-by-field copy did — both were
// shallow).
//
// origin is REQUIRED and its zero value panics. Go cannot forbid a zero enum
// by type, and a required parameter only defends against a forgotten argument,
// not a wrongly-passed one — so the check is here, at construction, in the
// same fail-loud register as the serve ledger's capacity assertion: a bad
// origin is an assembly-time wiring bug, never an author's intent, and there
// is no return path that could carry it honestly (pendingTicket.Wait and
// projectWork have no error outlet for "the engine miswired itself").
func NewMsg(origin MsgOrigin, ctx context.Context, env message.Envelope) Msg {
	if origin != OriginMailbox && origin != OriginLog {
		panic("actorbase: NewMsg origin must be OriginMailbox or OriginLog (the zero value is illegal)")
	}
	msg := Msg{Envelope: env, ctx: ctx, origin: origin}
	msg.Payload = nil
	app, body, err := harness.UnwrapPayload(env.Payload)
	if err != nil {
		panic("actorbase: invalid payload envelope: " + err.Error())
	}
	msg.app = app
	msg.Payload = body
	return msg
}

// NewBodyMsg is for adapters and tests that synthesize an actor-visible body
// without first writing a ledger envelope. Real ledger delivery uses NewMsg
// and remains strict about the canonical {_context,body} shape.
func NewBodyMsg(origin MsgOrigin, ctx context.Context, env message.Envelope) Msg {
	return NewBodyMsgContext(origin, ctx, harness.Context{}, env)
}

// NewBodyMsgContext is the context-carrying form used by adapters and tests
// which synthesize a delivered body without first appending it to the ledger.
func NewBodyMsgContext(origin MsgOrigin, ctx context.Context, app harness.Context, env message.Envelope) Msg {
	wrapped, err := harness.WrapPayload(app, env.Payload)
	if err != nil {
		panic("actorbase: invalid synthetic body: " + err.Error())
	}
	env.Payload = wrapped
	return NewMsg(origin, ctx, env)
}

// decodeRequestContext decodes the request payload's `_context` value into
// its one legal shape {caller:{channel,actor}}. Every level is checked for
// presence AND non-emptiness: `null` at any level, `{}`, and a caller missing
// channel or actor are all rejected — the engine (encodeRequestPayload) never
// writes those, so seeing one means the payload is not canonical.
func decodeRequestContext(raw json.RawMessage) (harness.Caller, error) {
	var outer struct {
		Caller json.RawMessage `json:"caller"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&outer); err != nil {
		return harness.Caller{}, errors.New("_context: " + err.Error())
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || len(outer.Caller) == 0 {
		return harness.Caller{}, errors.New("_context must be {caller:{channel,actor}}")
	}
	var callerRaw struct {
		Channel *string `json:"channel"`
		Actor   *string `json:"actor"`
	}
	dec = json.NewDecoder(bytes.NewReader(outer.Caller))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&callerRaw); err != nil {
		return harness.Caller{}, errors.New("_context.caller: " + err.Error())
	}
	if callerRaw.Channel == nil || *callerRaw.Channel == "" || callerRaw.Actor == nil || *callerRaw.Actor == "" {
		return harness.Caller{}, errors.New("_context.caller requires non-empty channel and actor")
	}
	return harness.Caller{Channel: channel.ID(*callerRaw.Channel), Actor: actor.ActorID(*callerRaw.Actor)}, nil
}
