package link_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
)

const testChannelID = channel.ID("test-channel")

// stubIngress is the whole substrate side of these transport tests: the link
// under test is supposed to decode a frame and call exactly one of these arms,
// so an accept-everything ingress is all the capability plane it needs.
type stubIngress struct{}

func (stubIngress) Emit(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	*message.Envelope,
) (harness.WriteResult, error) {
	return harness.WriteResult{}, nil
}

func (stubIngress) Access(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	remoteingress.AccessRequest,
) (remoteingress.AccessResponse, error) {
	return remoteingress.AccessResponse{}, nil
}

func (stubIngress) Schedule(
	context.Context,
	actor.ActorID,
	remoteingress.ScheduleRequest,
) (remoteingress.ScheduleResponse, error) {
	return remoteingress.ScheduleResponse{ID: "test-timer"}, nil
}

func (stubIngress) Fork(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	remoteingress.ForkRequest,
) (actor.ActorID, error) {
	return "agent:child", nil
}

func (stubIngress) EndSelf(
	context.Context,
	actor.ActorID,
	actorhost.AttemptKey,
	actorcaps.EndSelfRequest,
) error {
	return nil
}

type fakeStorageHostControl struct {
	mu sync.Mutex

	committedFound bool
	committedLost  bool
	committedErr   error
	committedCalls []struct{ sender, reservationID string }
}

func (f *fakeStorageHostControl) Committed(
	_ context.Context,
	senderDaemonID string,
	reservationID string,
) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committedCalls = append(f.committedCalls, struct{ sender, reservationID string }{
		sender: senderDaemonID, reservationID: reservationID,
	})
	return f.committedFound, f.committedLost, f.committedErr
}
func (*fakeStorageHostControl) ReclaimAck(context.Context, string, string) (bool, error) {
	return false, nil
}
func (*fakeStorageHostControl) ReconcilePull(
	context.Context,
	string,
	[]string,
) ([]link.ReconcileResource, []link.ReconcileReservation, []link.ReconcileTombstone, error) {
	return nil, nil, nil, nil
}

type storageRig struct {
	acc *link.Acceptor
	shc *fakeStorageHostControl
	srv *httptest.Server
}

func newStorageRig(t *testing.T) *storageRig {
	t.Helper()
	shc := &fakeStorageHostControl{}
	acc, err := link.NewAcceptor(link.Config{
		Ingress:   stubIngress{},
		ChannelID: testChannelID,
		AuthorizeAttach: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error {
			return nil
		},
		AttachBinding: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error {
			return nil
		},
		BindingDown:        func(actor.ActorID, actorhost.Binding) {},
		StorageHostControl: shc,
		Plan:               func(context.Context, string) ([]platform.PlanActor, error) { return nil, nil },
		CanAttach:          func(context.Context, string) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewAcceptor: %v", err)
	}
	rig := &storageRig{acc: acc, shc: shc}
	rig.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rig.acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() {
		_ = rig.acc.Close()
		rig.srv.Close()
	})
	return rig
}

func (r *storageRig) wsURL() string { return "ws" + r.srv.URL[4:] }

func dialStorageDaemon(t *testing.T, r *storageRig, configs ...link.DialConfig) *link.Dialer {
	t.Helper()
	cfg := link.DialConfig{}
	if len(configs) > 0 {
		cfg = configs[0]
	}
	d, err := link.Dial(context.Background(), r.wsURL(), cfg, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

type fakeCommitWH struct{ committed, aborted bool }

func (f *fakeCommitWH) Write(p []byte) (int, error) { return len(p), nil }
func (f *fakeCommitWH) Commit() error               { f.committed = true; return nil }
func (f *fakeCommitWH) Abort() error                { f.aborted = true; return nil }
