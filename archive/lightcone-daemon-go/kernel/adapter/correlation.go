package adapter

import "context"

// Correlation is the v4 adapter framework F2 contract — adapter-side
// request↔response correlation.
//
// Used by binding=ViaServerTransit (device adapter) to pair an
// outbound device_transit.send frame_id with the inbound recv frame.
// Also used by binding=OutboundHTTP when the remote does callback-style
// completion (e.g. GitHub webhook → adapter response).
//
// kernel/adapter only defines the contract; concrete tracking
// (in-memory map / sqlite-backed adapter_state) lives in the adapter
// implementation.
type Correlation interface {
	// Track records (frameID → envelopeID); future Resolve on the
	// same frameID returns envelopeID.
	Track(ctx context.Context, frameID string, envelopeID string) error

	// Resolve returns the envelopeID associated with a previously
	// tracked frameID. The second return value is false if no record
	// exists.
	Resolve(ctx context.Context, frameID string) (string, bool, error)

	// Forget removes the record (after terminal response observed or
	// timeout fired).
	Forget(ctx context.Context, frameID string) error
}
