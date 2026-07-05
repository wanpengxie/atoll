package link

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// ErrWriterNotLive is the WHEN-validity rejection: a livePen whose welded
// incarnation is no longer the live embodiment (despawned / dead / replaced)
// refuses to write. It is the death-after-write fence — a capability captured by
// a goroutine that outlived its incarnation cannot author truth on its behalf.
var ErrWriterNotLive = errors.New("link: writer no longer the live incarnation")

// livePen is the liveCap (WHEN-validity membrane) over a raw
// harness.Pen: a thin wrapper that, per write, first checks the host that the
// welded incarnation is STILL live (by POINTER, ABA-safe; lock-free) and only
// then forwards to the raw pen. It is the platform-assembly half of the
// death-after-write fence — the substrate (actorrt) owns liveness, the harness owns
// the WHO weld + append, and this wrapper composes the two with no change to
// either (bi-layer: actorrt never imports harness; livePen lives here in link,
// beside emitSink/RemoteWriter, so the port path can construct it too).
//
// HONEST SCOPE: it fences "a leaked cap used long after death" and "ABA across an
// incarnation replacement". The sub-microsecond window between the IsLive check
// passing and raw.Write committing is the accepted in-flight seam (a current
// incarnation's best-effort last gasp); truth is append-only + by-identity, so a
// seam-leaked write is an honest "id authored" row, never a lost update. livePen
// is a lease, not strict fencing.
type livePen struct {
	raw  harness.Pen
	inc  actorrt.Incarnation
	host *actorrt.Runtime
}

// NewLivePen wraps raw in the WHEN-validity membrane welded to inc, gated on host.
// Participant cells (agent/tool/human) are born with this; the sole substrate
// anchor exempt from the incarnation gate is the system/sysactor pen, which
// deliberately uses the raw pen (no successor principal to impersonate; the
// closure reconciler must write even when no cell is live). Every relay sink on
// the daemon path is membrane-wrapped — there is no exempt "daemon relay pen".
func NewLivePen(raw harness.Pen, inc actorrt.Incarnation, host *actorrt.Runtime) harness.Pen {
	return livePen{raw: raw, inc: inc, host: host}
}

// Write implements harness.Pen: fence on the welded incarnation's liveness, then
// forward to the raw pen (which welds WHO + appends). The incarnation NEVER rides
// the envelope — Sender.ID is stamped by the raw pen exactly as before.
func (p livePen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	if !p.host.IsLive(p.inc) {
		return harness.WriteResult{}, ErrWriterNotLive
	}
	return p.raw.Write(ctx, env)
}
