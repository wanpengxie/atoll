package actorbase

import (
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Hooks is the engine's out-of-band, per-host configuration (spec §3's
// out-generation matrix) — the two questions a call ledger cannot answer
// from inside the closed capability face alone. Both fields are OPTIONAL and
// nil means an honest, documented degrade, never a silent one.
type Hooks struct {
	// Canceller reaches the protocol-level cancel signal for one in-flight
	// outbound request (pending.Cancel's "经 Canceller 投递" half, spec
	// §1.5) — the assembly root wires it to Home.CancelRequest (cell hosts)
	// or leaves it nil (daemon hosts, spec §3's known gap: the caller-side
	// cancel upstream frame does not exist yet). nil = Cancel still commits
	// the caller's own unanswered_timeout terminal; only the signal to the
	// receiver's in-station account is skipped.
	Canceller func(target actor.ActorID, requestID message.ID)

	// TimeoutResolver supplies the per-(target, request type) closure
	// deadline (author#2's ExpiresAt) for a Call/Submit that leaves its own
	// deadline unset. nil, or a false ok, falls back to DefaultTimeout — the
	// injection-point contract vs implementation-fill split (a describe
	// cache/catalog layer fills this once it exists; the engine itself
	// carries none).
	TimeoutResolver func(target actor.ActorID, reqType string) (time.Duration, bool)
}

// DefaultTimeout is the closure deadline used when neither the caller nor
// Hooks.TimeoutResolver supplies one.
const DefaultTimeout = 30 * time.Second
