package message

import (
	"context"

)

// RequestLookup is the framework-private seam Respond uses to recover
// the original request envelope by id (L2 §8 F5).
//
// Lives in kernel/adapter because runtime/store needs to implement it
// without taking a dependency on adapters/** (go-arch-lint enforces
// runtime ↛ adapters).
//
// The lookup MUST be channel-scoped — callers pass an envelope id and
// trust the implementation to refuse cross-channel reads. Manager
// validates the returned envelope.channel_id matches its bound channel.
type RequestLookup interface {
	// FindByID returns the envelope at id. Returns ok=false when the
	// row does not exist or has been deleted.
	FindByID(ctx context.Context, id ID) (*Envelope, bool, error)
}
