package storespec

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// StoredRow wraps a protocol Envelope with the store-derived columns kernel
// deliberately keeps OUT of the pure Envelope (they are store-derived, not
// protocol fields). Read paths return StoredRow; write paths (Append) take
// the pure envelope + the harness-computed is_terminal and the store
// allocates seq.
type StoredRow struct {
	Envelope message.Envelope

	Seq        int64
	IsTerminal bool
}

// AppendResult is what MessageLog.Append returns on a successful row write.
// It carries only what the STORE authoritatively produces and the caller
// could not already know: the store-allocated seq. (is_terminal is NOT echoed
// back — the harness COMPUTES it and passes it INTO Append, so reflecting it
// in the result would be a dead output mirroring the caller's own input.)
type AppendResult struct {
	// Seq is the store-allocated monotonic position (messages.seq).
	Seq int64
	// Replayed means no row was appended: a client idempotency key matched an
	// existing row carrying the same canonical submission fingerprint.
	Replayed bool
}

// AppendMetadata carries persistence-only material alongside an envelope.
// It never enters protocol/message.Envelope. ClientFingerprint is computed at
// the shell ingress from the raw client submission before harness defaults are
// applied, then stored atomically with the message row.
type AppendMetadata struct {
	ClientFingerprint string
}

// AppendError is the typed error returned for protocol-level rejects inside
// the engine append step (e.g. UNIQUE violation on terminal_response_per_
// request → terminal_duplicate).
//
// Reason is a plain-string diagnostic code for a protocol-level violation the
// store detected at write time. Its value set is CLOSED and lives right here
// (the two consts below): this contract leaf is the one package both the
// producer (the store driver's classifyAppendErr) and the vocabulary owner
// (harness's HarnessRejectReason const block) can import, so it is the single
// home of the store-produced reject words. Before this, the driver minted the
// strings as bare literals and harness cast them through — one word
// (harness_id_duplicate_conflict) had a live producer but no const anywhere,
// and every consumer (the timer FireSink dup/poison split, lib/behavior's
// benign-duplicate checks) rode on string coincidence.
type AppendError struct {
	Reason           string
	Detail           string
	PartialMessageID message.ID
}

// Error implements the error interface.
func (e *AppendError) Error() string {
	if e.Detail != "" {
		return e.Reason + ": " + e.Detail
	}
	return e.Reason
}

// The two AppendError.Reason values the store can stamp — the complete set
// (classifyAppendErr produces exactly these; anything else surfaces as a
// plain error, not an AppendError). Untyped so harness can lift them into
// its HarnessRejectReason const block as constant expressions.
const (
	// AppendRejectIDDuplicate — messages.id UNIQUE hit: this envelope id is
	// already in truth. For a deterministic producer (the timer fire id) this
	// is the crash-replay idempotency signal; for a random uuid it is a pure
	// integrity error.
	AppendRejectIDDuplicate = "harness_id_duplicate_conflict"
	// AppendRejectTerminalDuplicate — ux_terminal_response_per_request UNIQUE
	// hit: a second terminal response for the same parent request.
	AppendRejectTerminalDuplicate = "harness_terminal_duplicate"
)

// MessageLog is the channel-local messages-table append contract. Append is
// the only mutation entry point; reads are not declared here because readers
// query messages through the MessageQuery role below.
//
// Concrete sqlite impl lives in runtime/internal/store/messages.go. v2 changes:
//   - no fencing parameter — the store is scoped to one channel at construction
//     time, and that scope is the unique write path, so an extra fencing token
//     would only re-assert what construction already fixes.
//   - is_terminal is passed EXPLICITLY: kernel purified it off the Envelope
//     (it is store-derived, not a protocol field). It is computed by the caller
//     because it depends on message-kind semantics the store does not interpret;
//     the store persists it verbatim (it stays the dumb persister).
type MessageLog interface {
	Append(ctx context.Context, env *message.Envelope, isTerminal bool, metadata AppendMetadata) (AppendResult, error)

	// FindByID returns the stored row for id (seq / is_terminal / envelope).
	// No channelID parameter: the store is bound to one channel at OpenChannel,
	// so a per-call channel arg is a pseudo-parameter — it implied a scoping the
	// query never performed (illegal-state-representable). The binding is the scope.
	FindByID(ctx context.Context, id message.ID) (*StoredRow, bool, error)

	// HasFinalResponse reports whether a terminal (final) response row already
	// exists for parentID — the store-level pre-check for the
	// one-terminal-response-per-request invariant.
	HasFinalResponse(ctx context.Context, parentID message.ID) (bool, error)
}

// MessageQuery is the channel-log READ role — segregated from MessageLog so a
// read-only consumer receives only the read surface, WITHOUT Append. Bundling
// reads with Append (one fat interface) would grant write capability to every
// reader; the ISP/CQRS role-split keeps a reader unable to reach the write
// path. The concrete satisfies both. The channel scope is the store assembly
// itself (one sqlite per channel), so no method re-takes a channel id — it
// would only re-specify what the file already fixes (and within one channel's
// file every row shares the channel_id).
type MessageQuery interface {
	// MaxSeq is the channel's highest committed seq.
	MaxSeq(ctx context.Context) (int64, error)
	// LatestBySenderAndType returns the latest row, ordered by the store's
	// monotonic seq, for one welded sender and message type.
	LatestBySenderAndType(ctx context.Context, sender actor.ActorID, typ string) (StoredRow, bool, error)
	// ReadAfterSeq returns envelopes with seq > afterSeq, in ascending seq order.
	ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]StoredRow, error)
	// OpenRequestsForActor returns ALL open requests addressed to actorID.
	// It is the closure drain: closing a dead actor's in-flight requests must
	// drain every one of them, so this is unbounded by construction — a limit
	// would silently leave the overflow callers hanging (no closure).
	OpenRequestsForActor(ctx context.Context, actorID actor.ActorID) ([]StoredRow, error)

	// DistinctOpenRequestReceivers returns the set of receivers (first-audience
	// member) that still have at least one open request. It is the truth-derived
	// view the closure RECONCILER scans: closure is a level-triggered reconciler
	// (orphan open-request × receiver-absent → receiver_unavailable), and the
	// authoritative "who has an open request" question is answered by the message
	// log, not by membership (a member with no open request needs no closure; an
	// open request is the only thing that demands one). The reconciler intersects
	// this set with substrate liveness to find absent receivers, then drains each
	// via OpenRequestsForActor. Unbounded by construction (same closure law).
	DistinctOpenRequestReceivers(ctx context.Context) ([]actor.ActorID, error)
}

// VisibleMessageQuery is the reader-scoped history face. Raw MessageQuery is
// retained for the delivery pump and audit internals.
type VisibleMessageQuery interface {
	ReadVisibleAfterSeq(context.Context, int64, int) ([]StoredRow, int64, error)
	// ReadVisibleBeforeSeq returns one bounded page selected backwards from an
	// exclusive seq cursor. beforeSeq == 0 means the current tail. Rows cross
	// the store boundary in ascending ledger order; head is the transaction's
	// snapshot head and hasOlder reports another visible row before this page.
	ReadVisibleBeforeSeq(context.Context, int64, int) (rows []StoredRow, head int64, hasOlder bool, err error)
}

// RequestLookup recovers an original request envelope by id.
// Channel-scoped: implementations refuse cross-channel reads.
type RequestLookup interface {
	FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error)
}

// ExpiryCursor is ExpiryQuery's keyset position — (expires_at, seq) of the
// last row a sweep consumed. The zero value means "from the top". Correctness
// never depends on it (the reaper is a level-scan; restarting from the top is
// harmless re-reading) — the cursor only buys batch fairness: a persistently
// failing (poison) row must not occupy the head of every batch and starve the
// tail.
type ExpiryCursor struct {
	ExpiresAt int64
	Seq       int64
}

// ExpiredRow is one expired-open-request result with PER-ROW error isolation:
// a row whose scan fails carries Err (zero Row) instead of aborting the whole
// batch — the sweep must be able to step over a poison row and keep closing
// the rest (the sibling queries' scan-error-aborts-batch shape is exactly
// what this type exists to avoid).
type ExpiredRow struct {
	Row StoredRow
	Err error
}

// ExpiryQuery is the expiry reaper's READ role (期12 S3) — deliberately its
// OWN narrow interface, not a MessageQuery method: MessageQuery is shared by
// tail/behavior/channelkit consumers whose fakes would all be forced to grow
// an irrelevant method. ix_messages_expires's first consumer.
type ExpiryQuery interface {
	// ExpiredOpenRequests returns request rows whose sliding deadline has
	// passed at beforeMs — span = expires_at − ts elapsed since the latest of
	// the request and its provisional responses — with no terminal response,
	// ordered by (expires_at, seq)
	// ascending, strictly after cur, at most limit rows. nextCur points past
	// the last row returned; the ZERO nextCur means the scan reached the end
	// (wrap to the top next sweep) — the implementation, not the caller,
	// derives it.
	ExpiredOpenRequests(ctx context.Context, beforeMs int64, cur ExpiryCursor, limit int) (rows []ExpiredRow, nextCur ExpiryCursor, err error)
}
