// Package actor defines the L0 channel-actor identity model: ActorID and
// actor Kind primitives.
package actor

import (
	"database/sql/driver"
	"fmt"
)

// ActorID is the channel-local sender identifier. It is identical to
// envelope `sender.id` (L0 §2.1 — same namespace, see L1 §3.2).
//
// Naming convention reference (informative — non-normative; L1 §3.2):
//
//	user:<short-id>      - human members
//	system               - the channel-local system actor (fixed id)
//	agent:<role-name>    - channel agents / sub-agents
//	tool:<adapter-name>  - tool/adapter actors (embedded /
//	                       runtime_outbound / runtime_inbound_via_relay per
//	                       L1 §11.7)
//
// Cross-channel uniqueness is NOT guaranteed; mapping a real-world user
// to multiple channels is out of scope for the channel-local registry
// (L1 §12.5).
type ActorID string

// Well-known fixed actor ids per L1 §3.2.
const (
	// SystemActorID is the channel-local system actor — every channel
	// has exactly one, written by the bootstrap saga (L2 §1.4.7 step 3).
	SystemActorID ActorID = "system"
)

// String returns the wire form of the actor id.
func (a ActorID) String() string { return string(a) }

// Value implements driver.Valuer for SQL TEXT boundaries.
func (a ActorID) Value() (driver.Value, error) { return string(a), nil }

// Scan implements sql.Scanner for SQL TEXT boundaries.
func (a *ActorID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*a = ""
		return nil
	case string:
		*a = ActorID(v)
		return nil
	case []byte:
		*a = ActorID(string(v))
		return nil
	default:
		return fmt.Errorf("actor.ActorID: scan unsupported %T", src)
	}
}
