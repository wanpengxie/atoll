package resourcespec

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/resource"
)

// Driver is the giftless byte realizer for ONE ResourceKind. It has NO Create
// method: create's byte half lives inside Registry.Create's atomic transaction
// (see there), so the door never orchestrates "make the row, then write the
// bytes". A Driver does NOT implement set either — set's executor is the
// substrate authz manager (Registry), operating on R, not on bytes. The Driver
// set is substrate-owned and closed-but-additive, NOT an open extension point
// for domain code.
type Driver interface {
	// Read returns the current bytes. found == false is a resolved-but-empty
	// state — a LEGAL outcome, not a failure.
	Read(ctx context.Context, id resource.ResourceID) (value []byte, found bool, err error)

	// Write overwrites existing content (PUT semantics, naturally idempotent).
	Write(ctx context.Context, id resource.ResourceID, value []byte) error

	// Delete removes the bytes. The door calls it before Registry.Delete (see
	// there). It is the orchestration slot that earns its keep for a FUTURE
	// external-byte driver; for the day-1 inline-byte kind the bytes live in the
	// resource row itself, so the concrete implementation is a no-op.
	Delete(ctx context.Context, id resource.ResourceID) error
}
