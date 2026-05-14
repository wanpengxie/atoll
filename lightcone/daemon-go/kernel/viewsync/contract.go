package viewsync

import "context"

// Pusher is the daemon-side outbox-driven push interface. The runtime
// transit client implements this against a persistent outbox + a single
// WS-mux frame channel to server.
//
// kernel/viewsync only defines the contract.
type Pusher interface {
	// Push hands a frame to the transport. Implementations MUST
	// persist the frame in the daemon outbox BEFORE returning, so
	// at-least-once delivery survives a daemon restart.
	Push(ctx context.Context, frame PushFrame) error
}

// Receiver is the server-side apply interface. Server applies frames in
// contiguous Seq order; gaps buffer until Resync closes them.
type Receiver interface {
	// Apply ingests a single frame inside a transaction. Returns the
	// updated LastReceivedSeq; the caller (transit ack-handler) is
	// responsible for sending the AckFrame back to the daemon.
	Apply(ctx context.Context, frame PushFrame) (LastReceivedSeq, error)
}

// Resyncer is the daemon-side Resync RPC server (the server calls into
// it to fetch a missing closed interval).
type Resyncer interface {
	Resync(ctx context.Context, req ResyncRequest) (ResyncResponse, error)
}
