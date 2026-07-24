package link

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// RemoteWriter is the OUT-OF-PROCESS end of the write contract: a relay-only
// PROXY PEN (harness.Pen) a daemon-hosted remote cell uses to emit upward over
// the port wire and observe the host's authoritative write verdict.
//
// It lives in the platform ASSEMBLY layer, NOT in the substrate. A remote pen is
// the COMPOSITION of two substrate mechanisms — the harness.Pen contract and the
// ipc wire protocol — and composing them into an end-to-end pen is an assembly
// concern, not a kernel-native one (unlike harness.boundPen, whose dependencies
// close entirely inside the truth engine, this one straddles harness + ipc). Its
// SYMMETRIC counterpart is the host-side emitSink (accept.go), which composes the
// same two mechanisms the other way — Mint a pen, write truth, mirror the verdict
// back onto the wire. Both halves of the emit/ack protocol therefore live
// together here in link, beside their consumers (dial.go opens the remote end,
// accept.go serves the host end). This is why moving it out of runtime/ipc lets
// ipc fall back to a protocol-only wire leaf and frees actorrt's compile closure
// of harness.
//
// MECHANISM: RemoteWriter is the EMIT DIALECT of relayCore (relaycore.go) — the
// shared FIFO no-id synchronous round-trip machine (six axioms there, in one
// place). This type only translates the emit dialect: EmitPayload out, an
// EmitAckPayload rebuilt into a harness.WriteResult back. It owns no FIFO / lock /
// head-dissolve logic of its own.
//
// INVARIANT: the proxy pen NEVER injects identity and NEVER fail-fasts. It only
// relays the envelope up the wire; the identity weld + fail-fast live on the
// HOST side, where the link's emitSink Mints a Pen for the connection's
// authenticated bound id. A daemon cell's behavior leaves Sender.ID/ChannelID
// empty (it has no Minter), the proxy pen relays that empty envelope, and the
// host pen welds the bound identity. So this proxy must stay a pure relay — see
// the platform emit-identity test.
//
// A remote cell has no local truth: its Respond/EmitEvent drive this pen,
// which sends a KindEmit and BLOCKS until the matching KindEmitAck returns. The
// returned harness.WriteResult is reconstructed from that ack, so a remote
// cell's Respond observes the EXACT outcome a local cell's Respond would — the
// write contract is not downgraded across the wire.
type RemoteWriter struct {
	core *relayCore[ackResult]
}

// ackResult carries one resolved KindEmitAck back to the blocked Write: the
// reconstructed harness verdict plus the host-side error decoded from the ack.
type ackResult struct {
	res harness.WriteResult
	err error
}

// errRemoteWriterClosed is returned to a blocked or new Write once the writer is
// torn down (the connection died with emits still in flight). It is the emit
// dialect's close sentinel (relayCore.closedErr) — its identity stays stable so
// consumers can still judge a teardown apart from a host verdict.
var errRemoteWriterClosed = errors.New("link: remote writer closed")

// NewRemoteWriter binds a remote writer to codec (the actor's port connection).
// The codec's write side is mutex-guarded, so emits may share it with the
// actor's other outbound frames.
func NewRemoteWriter(codec *ipc.Codec) *RemoteWriter {
	return &RemoteWriter{core: newRelayCore[ackResult](codec, ipc.KindEmit, errRemoteWriterClosed)}
}

// Write sends env upward as a KindEmit and blocks until the host returns the
// matching KindEmitAck (FIFO) or ctx is cancelled. It satisfies harness.Pen
// (relay-only): a remote cell's pen seam is this method, so its behavior.Respond
// / behavior.EmitEvent flow to the host harness (truth owner) and observe the
// authoritative verdict. It does NOT inject identity here — the host emitSink's
// Mint welds the bound id (see the type doc invariant).
//
// The FIFO round-trip mechanics live in relayCore.roundTrip; Write only maps the
// emit dialect onto core.roundTrip's outcome triple, and both the pre-send-cancel
// (definiteErr) and the teardown-in-flight (transportErr → errRemoteWriterClosed)
// arms collapse to the same Pen (WriteResult, error) contract: they surface as an
// error, never a fabricated verdict.
func (w *RemoteWriter) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	if env == nil {
		return harness.WriteResult{}, errors.New("link: remote writer nil envelope")
	}
	payload, err := json.Marshal(ipc.EmitPayload{Envelope: *env})
	if err != nil {
		return harness.WriteResult{}, err
	}
	ack, transportErr, definiteErr := w.core.roundTrip(ctx, payload)
	if definiteErr != nil {
		// Pre-send ctx cancel: the emit frame provably never left the wire (the host
		// never saw it), so we honestly report ctx.Err() as a non-execution — not a
		//普通取消 after the emit already reached the host.
		return harness.WriteResult{}, definiteErr
	}
	if transportErr != nil {
		// Wire-write failure, post-send cancel, or teardown-in-flight
		// (errRemoteWriterClosed). The Pen contract has no unknown-verdict slot, so an
		// unconfirmed emit is surfaced as an error — the identity/text of a teardown
		// stays errRemoteWriterClosed, so the consumer's behaviour is unchanged.
		return harness.WriteResult{}, transportErr
	}
	return ack.res, ack.err
}

// DeliverAck routes one inbound KindEmitAck into the FIFO head waiter via the
// core. The remote's single read loop calls this when it decodes a KindEmitAck
// frame. It reconstructs the harness.WriteResult verdict and the transport error
// from the ack payload before handing the emit-dialect Ack to the core.
func (w *RemoteWriter) DeliverAck(ack ipc.EmitAckPayload) {
	w.core.deliverAck(ackResult{
		res: harness.WriteResult{
			MessageID:    ack.MessageID,
			Seq:          ack.Seq,
			RejectReason: harness.HarnessRejectReason(ack.RejectReason),
			RejectDetail: ack.RejectDetail,
		},
		err: decodeAckError(ack.ErrorCode, ack.ErrorMessage),
	})
}

func (w *RemoteWriter) sendCancel(requestID message.ID) error {
	raw, err := json.Marshal(ipc.CancelPayload{RequestID: requestID})
	if err != nil {
		return err
	}
	return w.core.writeOneWay(ipc.KindCancelRequest, raw)
}

func (w *RemoteWriter) publishObs(kind string, value []byte) error {
	raw, err := json.Marshal(ipc.ObsPayload{Kind: kind, Value: append([]byte(nil), value...)})
	if err != nil {
		return err
	}
	return w.core.writeOneWay(ipc.KindObs, raw)
}

// Close fails every pending waiter with errRemoteWriterClosed and rejects
// subsequent Writes. The connection died with emits in flight: those cells must
// see a transport error, not block forever.
func (w *RemoteWriter) Close() { w.core.close() }

// Verify the remote writer satisfies the harness Pen contract at compile time —
// the whole point is that a remote cell's pen is indistinguishable from a local
// one. It is a relay-only proxy pen (never injects identity / never fail-fasts);
// the host emitSink's Mint welds the bound identity.
var _ harness.Pen = (*RemoteWriter)(nil)
