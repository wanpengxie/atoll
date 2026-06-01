package behavior

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// RequestID is the wait anchor — it is exactly the request envelope's id
// (the "request_id" wire form). Submit returns one; Await / Watch / Abandon
// take one back.
type RequestID = message.ID

// CallRequest is the input a caller supplies to start one downstream call.
// parent / correlation / sender are derived from the current Handle envelope
// context by the framework — the caller does NOT fill them in.
type CallRequest struct {
	TargetActor actor.ActorID   // target actor (audience[0])
	Type        string          // envelope type (must be a request-allowed type for the target)
	Payload     json.RawMessage // business payload (adapter-boundary validated; protocol does not check schema)
	Timeout     time.Duration   // 0 = use type default (R5); only governs "give up waiting", never the substrate
}

// SubmitResult is what Submit returns once the request write is accepted by
// the harness. At this moment the receiver has not processed the request, so
// the ack only carries the machine kernel + template/type-level hint — the
// receiver's semantic guidance arrives later as a provisional (§2.3.3).
type SubmitResult struct {
	RequestID RequestID
	Ack       AckDescriptor
}

// AckDescriptor is the dual-form acknowledgement (§2.3.3). On the immediate
// Submit return it can only carry the machine kernel + template hint; the
// receiver's own NL semantics arrive on the first provisional.
type AckDescriptor struct {
	// machine kernel (typed)
	RequestID RequestID
	Accepted  bool   // the harness accepted the write (substrate-level, not business)
	Status    string // immediate ack is always "accepted" (substrate-level); business status arrives on later provisional
	EstWaitMs int64  // source: type.max_pending_ms (R5), not receiver-authored

	// agent intent surface (NL, template-synthesized)
	Guidance     string // framework template ("accepted; to wait use await_result(...)"); no receiver semantics
	ToWait       ToWaitHint
	IfNotWaiting string
}

// ToWaitHint is the structured "how to wait" pointer carried inside an
// AckDescriptor's intent surface.
type ToWaitHint struct {
	Tool   string         // "await_result"
	Params map[string]any // {"request_id":..., "timeout_ms":...}
}

// Terminal is the caller-side, read-only projection of a final response.
type Terminal struct {
	Envelope *message.Envelope // final response original
	Status   string            // completed | failed (Layer 1 closed set)
	OK       bool              // == (Status == "completed")
}

// ResolveRequest is the receiver-side input that produces a final response
// (kept separate from the caller-side read-only Terminal). It carries every
// business field needed to write the final — mirroring the xhs callback path
// (status / payload / reason).
type ResolveRequest struct {
	Status  string          // completed | failed (Layer 1)
	Payload json.RawMessage // business payload
	Reason  string          // terminal reason on failure (3-item closed set); empty when completed
}

// WatchEvent mirrors the SDK WatchEvent shape (both provisional and final
// are surfaced).
type WatchEvent struct {
	Envelope *message.Envelope
	IsFinal  bool // == message.IsFinalStatus(payload.status)
	Err      error
}

// Watcher is the stream handle returned by Watch.
type Watcher interface {
	Events() <-chan WatchEvent
	Close() error
}

// Outcome is one per-item result of AwaitAll (all-settled, §0②).
type Outcome struct {
	RequestID RequestID
	Terminal  *Terminal
	Err       error
}

// ErrHandleDeferred — Handle returns it to signal "the terminal arrives later
// via Resolve". The framework keeps the pending entry + F3 timer alive and
// does NOT auto-finalize on Handle return (§0④).
//
// Sentinel hardening (§2.1):
//   - Manager identifies it via errors.Is(err, ErrHandleDeferred).
//   - Adapters MUST NOT wrap it with fmt.Errorf("%w", ErrHandleDeferred) and
//     then return business semantics — that pollutes the discriminator.
//   - Contract: synchronous failure → return a plain error (framework turns
//     it into a failed terminal); asynchronous path → return Deferred() and
//     MUST later Resolve(failed) as a backstop (F3 is the last resort, not the
//     normal path).
var ErrHandleDeferred = errors.New("adapter: handle deferred")

// Deferred is the semantic constructor for ErrHandleDeferred, for use as
// `return ctx.Deferred()` / `return adapter.Deferred()` from Handle.
func Deferred() error { return ErrHandleDeferred }
