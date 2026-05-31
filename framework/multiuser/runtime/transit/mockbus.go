package transit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
)

// MockBus is an in-process Transport pair: the daemon side gets the
// MockBus returned by NewMockBus(); the (mock) server side accesses the
// inverse via MockBus.ServerSide().
//
// Both directions use unbounded chan-of-Frame queues. Closing the bus
// drains both queues with io.EOF semantics (Recv returns ErrClosed).
type MockBus struct {
	// daemon-side reads from inbox (frames the mock server pushed to
	// daemon) and writes to outbox.
	inbox  chan daemonbus.Frame
	outbox chan daemonbus.Frame

	epochAlloc atomic.Int64

	closeOnce sync.Once
	closed    chan struct{}
}

// NewMockBus constructs a fresh MockBus pair. bufSize controls the
// internal buffer per direction.
func NewMockBus(bufSize int) *MockBus {
	if bufSize <= 0 {
		bufSize = 256
	}
	return &MockBus{
		inbox:  make(chan daemonbus.Frame, bufSize),
		outbox: make(chan daemonbus.Frame, bufSize),
		closed: make(chan struct{}),
	}
}

// ErrBusClosed is returned by Send / Recv after Close().
var ErrBusClosed = errors.New("transit: mock bus closed")

// Connect on the daemon side allocates a new monotonic epoch.
func (b *MockBus) Connect(ctx context.Context) (daemonbus.ConnectionEpoch, error) {
	select {
	case <-b.closed:
		return 0, ErrBusClosed
	default:
	}
	epoch := b.epochAlloc.Add(1)
	return daemonbus.ConnectionEpoch(epoch), nil
}

// Send pushes a frame daemon → server.
func (b *MockBus) Send(ctx context.Context, frame daemonbus.Frame) error {
	select {
	case <-b.closed:
		return ErrBusClosed
	case <-ctx.Done():
		return ctx.Err()
	case b.outbox <- frame:
		return nil
	}
}

// Recv pulls the next frame server → daemon.
func (b *MockBus) Recv(ctx context.Context) (daemonbus.Frame, error) {
	select {
	case <-b.closed:
		return daemonbus.Frame{}, ErrBusClosed
	case <-ctx.Done():
		return daemonbus.Frame{}, ctx.Err()
	case f := <-b.inbox:
		return f, nil
	}
}

// Close releases blocked Send / Recv calls.
func (b *MockBus) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

// ServerSide returns a view of the bus from the mock server's
// perspective: SendToDaemon pushes a frame into daemon's inbox,
// RecvFromDaemon pulls a frame the daemon emitted.
//
// (We expose this as concrete methods rather than another Transport
// because the mock server isn't bound by the Transport contract; tests
// drive it directly.)
func (b *MockBus) ServerSide() *MockServer { return &MockServer{bus: b} }

// MockServer is the test-side façade of a MockBus.
type MockServer struct{ bus *MockBus }

// SendToDaemon delivers a frame to the daemon-side Recv loop.
func (s *MockServer) SendToDaemon(ctx context.Context, frame daemonbus.Frame) error {
	select {
	case <-s.bus.closed:
		return ErrBusClosed
	case <-ctx.Done():
		return ctx.Err()
	case s.bus.inbox <- frame:
		return nil
	}
}

// RecvFromDaemon pulls the next frame the daemon emitted.
func (s *MockServer) RecvFromDaemon(ctx context.Context) (daemonbus.Frame, error) {
	select {
	case <-s.bus.closed:
		return daemonbus.Frame{}, ErrBusClosed
	case <-ctx.Done():
		return daemonbus.Frame{}, ctx.Err()
	case f := <-s.bus.outbox:
		return f, nil
	}
}
