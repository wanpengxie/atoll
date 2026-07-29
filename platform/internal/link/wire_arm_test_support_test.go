package link

// Shared rig for the capability-arm WIRE ROUND TRIP tests (relaywire.go).
//
// The unit tests around it each cover one half: relaycore_test.go pins the
// slot semantics of the FIFO machine, remoteingress/accessdoor pin the home
// door. Nobody drove one real frame from a daemon-side proxy, over a real
// codec, into the real home relay handlers and back — which is exactly where
// the identity weld and the (verdict | error | unknown) mapping live.
//
// The rig pairs the PRODUCTION objects on both ends over one in-memory pipe:
//
//	daemon: remoteResourceHandle / remoteAccessHandle / remoteScheduleHandle
//	        → relayClient (ipc.KindAccess | ipc.KindSchedule)
//	        → Dialer.streamReadLoop (the real ack router AND the real
//	          transport-death teardown that closes every arm)
//	  wire: net.Pipe + ipc.Codec
//	  home: serverActorEndpoint.readLoop → handleRelay
//	        → Acceptor.relayAccess / relaySchedule (the real decode+encode)
//	        → remoteingress.RemoteIngress (recorded here)
//
// The one thing the rig fakes is the ingress, because that is precisely the
// observation point: it records the coordinate the HOME supplied (which must
// be the endpoint's authenticated (id, key), never anything a frame carried)
// alongside the decoded operand.

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// wireArmTimeout is deliberately generous: these tests only ever wait for
// events that MUST happen (a frame arriving, a read loop unwinding), never for
// a duration to elapse, so a slow/loaded machine cannot flake them.
const wireArmTimeout = 15 * time.Second

// wireArmAccessCall / wireArmScheduleCall are one recorded home-side arrival:
// the coordinate the ENDPOINT handed the ingress plus the decoded operand.
type wireArmAccessCall struct {
	id  actor.ActorID
	key actorhost.AttemptKey
	req remoteingress.AccessRequest
}

type wireArmScheduleCall struct {
	id  actor.ActorID
	req remoteingress.ScheduleRequest
}

// wireArmIngress is the recording RemoteIngress at the far end of the wire.
// Emit/Fork/EndSelf are unused by these arm tests and stay inert.
type wireArmIngress struct {
	mu            sync.Mutex
	accessCalls   []wireArmAccessCall
	scheduleCalls []wireArmScheduleCall

	accessFn   func(remoteingress.AccessRequest) (remoteingress.AccessResponse, error)
	scheduleFn func(remoteingress.ScheduleRequest) (remoteingress.ScheduleResponse, error)
}

func (i *wireArmIngress) setAccess(
	fn func(remoteingress.AccessRequest) (remoteingress.AccessResponse, error),
) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.accessFn = fn
}

func (i *wireArmIngress) setSchedule(
	fn func(remoteingress.ScheduleRequest) (remoteingress.ScheduleResponse, error),
) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.scheduleFn = fn
}

func (i *wireArmIngress) recordedAccess() []wireArmAccessCall {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]wireArmAccessCall(nil), i.accessCalls...)
}

func (i *wireArmIngress) recordedSchedule() []wireArmScheduleCall {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]wireArmScheduleCall(nil), i.scheduleCalls...)
}

func (i *wireArmIngress) Emit(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	*message.Envelope,
) (harness.WriteResult, error) {
	return harness.WriteResult{}, nil
}

func (i *wireArmIngress) Access(
	_ context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
	req remoteingress.AccessRequest,
) (remoteingress.AccessResponse, error) {
	i.mu.Lock()
	i.accessCalls = append(i.accessCalls, wireArmAccessCall{id: id, key: key, req: req})
	fn := i.accessFn
	i.mu.Unlock()
	// Called OUTSIDE the lock: a deliberately parked fn (the in-flight
	// scenarios) must not wedge the recorder for the asserting goroutine.
	if fn == nil {
		return remoteingress.AccessResponse{}, nil
	}
	return fn(req)
}

func (i *wireArmIngress) Schedule(
	_ context.Context,
	id actor.ActorID,
	req remoteingress.ScheduleRequest,
) (remoteingress.ScheduleResponse, error) {
	i.mu.Lock()
	i.scheduleCalls = append(i.scheduleCalls, wireArmScheduleCall{id: id, req: req})
	fn := i.scheduleFn
	i.mu.Unlock()
	if fn == nil {
		return remoteingress.ScheduleResponse{}, nil
	}
	return fn(req)
}

func (i *wireArmIngress) Fork(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	remoteingress.ForkRequest,
) (actor.ActorID, error) {
	return "", nil
}

func (i *wireArmIngress) EndSelf(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	actorcaps.EndSelfRequest,
) error {
	return nil
}

var _ remoteingress.RemoteIngress = (*wireArmIngress)(nil)

// wireArmRig is one live daemon↔home capability link.
type wireArmRig struct {
	t *testing.T

	// id/key are the endpoint's AUTHENTICATED coordinate — fixed at
	// construction exactly as accept.go fixes it from the handshake, before
	// any frame is read. No test frame ever carries them.
	id  actor.ActorID
	key actorhost.AttemptKey

	ingress *wireArmIngress

	access accessdoor.ResourceAccessHandle // resource face (remoteResourceHandle)
	state  accessdoor.AccessHandle         // state face (remoteAccessHandle)
	sched  schedule.ScheduleHandle         // time axis (remoteScheduleHandle)

	accessRelay *relayClient
	schedRelay  *relayClient
	stream      *actorStream

	homeConn   net.Conn
	daemonConn net.Conn
	endpoint   *serverActorEndpoint

	rawMu       sync.Mutex
	rawAccess   [][]byte
	rawSchedule [][]byte
}

func newWireArmRig(t *testing.T, ing *wireArmIngress) *wireArmRig {
	t.Helper()
	if ing == nil {
		ing = &wireArmIngress{}
	}
	homeConn, daemonConn := net.Pipe()
	rig := &wireArmRig{
		t: t, id: "agent:wire-arm", key: physicalKey(t), ingress: ing,
		homeConn: homeConn, daemonConn: daemonConn,
	}

	// Home side. relayAccess/relaySchedule are Acceptor methods that read
	// nothing but the ingress, so the arm decode/encode under test is the
	// production one; the wrapper only tees the exact bytes that crossed.
	acceptor := &Acceptor{ingress: ing, logger: slog.New(slog.DiscardHandler)}
	handlers := serverActorHandlers{
		access: func(
			ctx context.Context, id actor.ActorID, key actorhost.AttemptKey, payload []byte,
		) ([]byte, error) {
			rig.recordRaw(&rig.rawAccess, payload)
			return acceptor.relayAccess(ctx, id, key, payload)
		},
		schedule: func(ctx context.Context, id actor.ActorID, payload []byte) ([]byte, error) {
			rig.recordRaw(&rig.rawSchedule, payload)
			return acceptor.relaySchedule(ctx, id, payload)
		},
	}
	rig.endpoint = newServerActorEndpoint(
		context.Background(), rig.id, rig.key, homeConn, nil, handlers,
	)
	go func() { _ = rig.endpoint.Run(context.Background()) }()

	// Daemon side: the same objects OpenExactActorStream builds (dial.go), on
	// the real read loop so transport death runs the real arm teardown.
	codec := ipc.NewCodec(daemonConn, daemonConn)
	rig.accessRelay = newRelayClient(codec, ipc.KindAccess)
	rig.schedRelay = newRelayClient(codec, ipc.KindSchedule)
	dialer := testDialer()
	rig.stream = &actorStream{
		id: rig.id, stream: daemonConn, codec: codec,
		writer: NewRemoteWriter(codec),
		access: rig.accessRelay, sched: rig.schedRelay,
		done: make(chan struct{}),
	}
	go dialer.streamReadLoop(rig.stream, nil)

	rig.access = &remoteResourceHandle{relay: rig.accessRelay, dialer: dialer}
	rig.state = &remoteAccessHandle{relay: rig.accessRelay, scope: accessScopeState}
	rig.sched = &remoteScheduleHandle{relay: rig.schedRelay}

	t.Cleanup(func() {
		_ = daemonConn.Close()
		_ = homeConn.Close()
		select {
		case <-rig.stream.done:
		case <-time.After(wireArmTimeout):
			t.Error("daemon stream read loop never unwound after the transport closed")
		}
	})
	return rig
}

func (r *wireArmRig) recordRaw(into *[][]byte, payload []byte) {
	r.rawMu.Lock()
	defer r.rawMu.Unlock()
	*into = append(*into, append([]byte(nil), payload...))
}

func (r *wireArmRig) rawAccessFrames() [][]byte {
	r.rawMu.Lock()
	defer r.rawMu.Unlock()
	return append([][]byte(nil), r.rawAccess...)
}

func (r *wireArmRig) rawScheduleFrames() [][]byte {
	r.rawMu.Lock()
	defer r.rawMu.Unlock()
	return append([][]byte(nil), r.rawSchedule...)
}

// drainHome closes the daemon end and waits for the home endpoint's read loop
// to unwind, so EVERY frame that ever crossed has been counted before a test
// asserts that none did. (An absence assertion taken without draining would
// pass just as happily on a frame still in flight.)
func (r *wireArmRig) drainHome() {
	r.t.Helper()
	_ = r.daemonConn.Close()
	select {
	case <-r.endpoint.Done():
	case <-time.After(wireArmTimeout):
		r.t.Fatal("home endpoint never unwound after the daemon end closed")
	}
}

// killTransport kills the link from the HOME end (the daemon process observing
// its home die mid-operation) and waits for the daemon read loop to run its
// deferred arm teardown, which is what settles every in-flight round trip.
func (r *wireArmRig) killTransport() {
	r.t.Helper()
	_ = r.homeConn.Close()
	select {
	case <-r.stream.done:
	case <-time.After(wireArmTimeout):
		r.t.Fatal("daemon arms were never torn down after transport death")
	}
}

// wireArmParkedAccess builds an ingress whose access arm PARKS every call
// until release is called, announcing each arrival on entered. It makes the
// "operation is genuinely in flight at the home" precondition observable
// instead of assumed.
func wireArmParkedAccess() (ing *wireArmIngress, entered <-chan struct{}, release func()) {
	arrivals := make(chan struct{}, 16)
	held := make(chan struct{})
	var once sync.Once
	ing = &wireArmIngress{}
	ing.setAccess(func(remoteingress.AccessRequest) (remoteingress.AccessResponse, error) {
		arrivals <- struct{}{}
		<-held
		return remoteingress.AccessResponse{}, nil
	})
	return ing, arrivals, func() { once.Do(func() { close(held) }) }
}

// wireArmParkedSchedule is wireArmParkedAccess's time-axis twin.
func wireArmParkedSchedule() (ing *wireArmIngress, entered <-chan struct{}, release func()) {
	arrivals := make(chan struct{}, 16)
	held := make(chan struct{})
	var once sync.Once
	ing = &wireArmIngress{}
	ing.setSchedule(func(remoteingress.ScheduleRequest) (remoteingress.ScheduleResponse, error) {
		arrivals <- struct{}{}
		<-held
		return remoteingress.ScheduleResponse{}, nil
	})
	return ing, arrivals, func() { once.Do(func() { close(held) }) }
}

// awaitWireArm blocks for one event that must happen, failing the test rather
// than hanging the package if it does not.
func awaitWireArm(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(wireArmTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}
