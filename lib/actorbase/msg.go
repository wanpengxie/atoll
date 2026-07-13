package actorbase

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
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
// ledger. For a kind=request delivery, Ctx() is derived from the serve
// ledger's entry (deadline + cancel, §1.5); for event/timer/no-one-waiting
// deliveries (no closure obligation), Ctx() IS sys.Life() — one process-life
// ctx, one name, no second "background" ctx anywhere in this package.
type Msg struct {
	message.Envelope

	ctx context.Context
}

// Ctx returns the ctx this delivery is scoped to. See the Sys godoc's
// provenance rule for which ctx a Proc body should thread onward from here.
func (m Msg) Ctx() context.Context { return m.ctx }

// NewMsg projects env into a Msg bound to ctx — the ONE constructor (the
// engine's Recv path and any test fixture both go through it; there is no
// second, partial way to build a Msg). env is copied by value, never
// retained: mutating the envelope after projection cannot reach back into an
// already-delivered Msg (Payload/Audience share backing arrays exactly as
// the old field-by-field copy did — both were shallow).
func NewMsg(ctx context.Context, env message.Envelope) Msg {
	return Msg{Envelope: env, ctx: ctx}
}
