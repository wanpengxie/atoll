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
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const testChannelID = channel.ID("test-channel")

type testPen struct{}

func (testPen) Write(context.Context, *message.Envelope) (harness.WriteResult, error) {
	return harness.WriteResult{}, nil
}

type stubMinter struct{}

func (*stubMinter) Mint(actor.ActorID, actor.Kind, channel.ID) harness.Pen {
	return testPen{}
}
func (*stubMinter) MintAdmitted(storespec.IdentityAdmission, channel.ID) harness.Pen {
	return testPen{}
}

type testStateHandle struct{}

func (testStateHandle) Invoke(
	context.Context,
	access.Operation,
	resource.ResourceID,
	[]byte,
	*access.Grant,
) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}

type testResourceHandle struct{ testStateHandle }

func (testResourceHandle) Create(
	context.Context,
	resource.ResourceID,
	accessdoor.CreateSpec,
	[]byte,
) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (testResourceHandle) Stat(context.Context, resource.ResourceID) (accessdoor.StatResult, error) {
	return accessdoor.StatResult{}, nil
}
func (testResourceHandle) List(context.Context, accessdoor.ListQuery) (accessdoor.ListPage, error) {
	return accessdoor.ListPage{}, nil
}
func (testResourceHandle) Open(
	context.Context,
	resource.ResourceID,
	access.Operation,
) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, accessdoor.ErrFileCapabilityUnavailable
}
func (testResourceHandle) Redeem(context.Context, accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	return accessdoor.FileAccess{}, accessdoor.ErrFileCapabilityUnavailable
}

type testAccessMinter struct{}

func (testAccessMinter) Mint(storespec.AuthorStamp) accessdoor.ResourceAccessHandle {
	return testResourceHandle{}
}
func (testAccessMinter) MintState(storespec.AuthorStamp) accessdoor.AccessHandle {
	return testStateHandle{}
}
func (testAccessMinter) MintAdmitted(storespec.IdentityAdmission) accessdoor.ResourceAccessHandle {
	return testResourceHandle{}
}
func (testAccessMinter) MintStateAdmitted(storespec.IdentityAdmission) accessdoor.AccessHandle {
	return testStateHandle{}
}

type testStateResolver struct{}

func (testStateResolver) AdmitRun(actor.ActorID) error { return nil }
func (testStateResolver) Resolve(context.Context, storespec.AuthorStamp) (accessdoor.AccessHandle, error) {
	return testStateHandle{}, nil
}
func (testStateResolver) ResolveAdmitted(storespec.IdentityAdmission) (accessdoor.AccessHandle, error) {
	return testStateHandle{}, nil
}
func (testStateResolver) EndBatch([]actor.ActorID) {}

type testScheduleHandle struct{}

func (testScheduleHandle) Schedule(context.Context, schedule.ScheduleReq) (schedule.TimerID, error) {
	return "test-timer", nil
}
func (testScheduleHandle) Cancel(context.Context, schedule.TimerID) error { return nil }
func (testScheduleHandle) Ack(context.Context, schedule.TimerID) error    { return nil }

type testScheduleMinter struct{}

func (testScheduleMinter) Mint(storespec.AuthorStamp) schedule.ScheduleHandle {
	return testScheduleHandle{}
}
func (testScheduleMinter) MintAdmitted(storespec.IdentityAdmission) schedule.ScheduleHandle {
	return testScheduleHandle{}
}

type testAuthority struct{}

func (testAuthority) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	return storespec.ActorControlRow{ID: id, Kind: actor.KindAgent, CurrentDeclVersion: 1}, true, nil
}
func (testAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (testAuthority) WorldOf(context.Context, actor.ActorID) (storespec.ActorWorld, bool, error) {
	return storespec.WorldDurable, true, nil
}
func (testAuthority) CheckAuthor(context.Context, storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	return storespec.AuthorOK, nil
}
func (testAuthority) AdmitIdentity(
	_ context.Context,
	id actor.ActorID,
) (storespec.IdentityAdmission, bool, error) {
	return storespec.IdentityAdmission{
		Row: storespec.ActorControlRow{
			ID: id, Kind: actor.KindAgent, CurrentDeclVersion: 1,
			Placement: storespec.NewServerPlacement(),
		},
		World: storespec.WorldDurable,
	}, true, nil
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
		Minter:       &stubMinter{},
		Access:       testAccessMinter{},
		StateHandles: testStateResolver{},
		Schedule:     testScheduleMinter{},
		Authority:    testAuthority{},
		ChannelID:    testChannelID,
		AuthorizeAttach: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error {
			return nil
		},
		AttachBinding: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error {
			return nil
		},
		BindingDown: func(actor.ActorID, actorhost.Binding) {},
		Fork: func(
			context.Context,
			actor.ActorID,
			actorhost.AttemptKey,
			message.ID,
			actorcaps.ForkSpec,
		) (actor.ActorID, error) {
			return "agent:child", nil
		},
		EndSelf:            func(context.Context, actor.ActorID, actorhost.AttemptKey, actorcaps.EndSelfRequest) error { return nil },
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
