package accessdoor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type blockingReadDriver struct {
	entered chan struct{}
	release chan struct{}
}

func (d blockingReadDriver) Read(context.Context, resource.ResourceID) ([]byte, bool, error) {
	close(d.entered)
	<-d.release
	return []byte("old"), true, nil
}
func (blockingReadDriver) Write(context.Context, resource.ResourceID, []byte) error { return nil }
func (blockingReadDriver) Delete(context.Context, resource.ResourceID) error        { return nil }

func TestResourceGateSpansAuthorizeThroughExecute(t *testing.T) {
	reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
	drv := blockingReadDriver{entered: make(chan struct{}), release: make(chan struct{})}
	d := newDoor(reg, nil, &fakeMembership{isMember: true})
	d.deps.Drivers[resourcespec.KindKV] = drv
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = d.invoke(context.Background(), "old-grantee", access.OpRead, "r1", nil, nil)
	}()
	<-drv.entered
	createDone := make(chan struct{})
	go func() {
		defer close(createDone)
		_, _ = d.create(context.Background(), "creator", "r1", resourcespec.CreateSpec{Kind: resourcespec.KindKV}, nil)
	}()
	select {
	case <-createDone:
		t.Fatal("create crossed resource gate while an authorized read was executing")
	case <-time.After(30 * time.Millisecond):
	}
	close(drv.release)
	<-readDone
	<-createDone
}

// --- create branch (期11 §3.1: create is its own method, door.create — no
//     longer reachable through invoke at all) ---

func TestDoorCreate(t *testing.T) {
	kvSpec := resourcespec.CreateSpec{Kind: resourcespec.KindKV}

	t.Run("existing id rejects already_exists", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.create(t.Context(), "a", "r1", kvSpec, []byte("v"))
		mustVerdict(t, out, err, access.AlreadyExists)
		if len(reg.createCalls) != 0 {
			t.Fatalf("Create must not run when id exists")
		}
	})

	t.Run("non-member rejects access_denied", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: false})
		out, err := d.create(t.Context(), "x", "r1", kvSpec, nil)
		mustVerdict(t, out, err, access.AccessDenied)
		if len(reg.createCalls) != 0 {
			t.Fatalf("Create must not run for a non-member")
		}
	})

	t.Run("member creates with KindKV and initial bytes", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.create(t.Context(), "a", "r1", kvSpec, []byte("hi"))
		mustAccept(t, out, err)
		if len(reg.createCalls) != 1 {
			t.Fatalf("expected one Create call, got %d", len(reg.createCalls))
		}
		got := reg.createCalls[0]
		if got.kind != resourcespec.KindKV || got.creator != "a" || string(got.initial) != "hi" {
			t.Fatalf("Create args = %+v", got)
		}
	})

	t.Run("Create ErrAlreadyExists maps to already_exists verdict", func(t *testing.T) {
		reg := &fakeRegistry{createErr: resourcespec.ErrAlreadyExists}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.create(t.Context(), "a", "r1", kvSpec, nil)
		mustVerdict(t, out, err, access.AlreadyExists)
	})

	t.Run("Create other error maps to driver_error verdict", func(t *testing.T) {
		reg := &fakeRegistry{createErr: errors.New("boom")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.create(t.Context(), "a", "r1", kvSpec, nil)
		mustVerdict(t, out, err, access.DriverError)
	})

	t.Run("membership error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{err: errors.New("mem down")})
		_, err := d.create(t.Context(), "a", "r1", kvSpec, nil)
		if err == nil {
			t.Fatalf("expected Go error on membership failure")
		}
	})

	t.Run("file kind fails honestly when placement Deps are unwired", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		_, err := d.create(t.Context(), "a", "r1", resourcespec.CreateSpec{Kind: resourcespec.KindFile}, nil)
		if err == nil {
			t.Fatalf("expected a Go error when Deps.StorageMounts is nil")
		}
		if len(reg.createCalls) != 0 {
			t.Fatalf("Registry.Create must not run when placement routing is unavailable")
		}
	})
}

func TestChannelOwnerRootAuthorizesEveryObjectOperation(t *testing.T) {
	owner := &fakeMembership{isMember: true, isOwner: true}
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete} {
		t.Run(string(op), func(t *testing.T) {
			reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
			drv := &fakeDriver{readFound: true}
			var grant *access.Grant
			if op == access.OpSet {
				grant = &access.Grant{GranteeKind: access.GranteeActor, Grantee: "peer", Ops: []access.Operation{access.OpRead, access.OpWrite}}
			}
			out, err := newDoor(reg, drv, owner).invoke(t.Context(), "owner", op, "r1", []byte("v"), grant)
			mustAccept(t, out, err)
		})
	}

	t.Run("set missing resource sentinel maps to not_found verdict", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), setGrantErr: resourcespec.ErrResourceNotFound}
		grant := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "peer"}
		out, err := newDoor(reg, &fakeDriver{}, owner).invoke(t.Context(), "owner", access.OpSet, "gone", nil, grant)
		mustVerdict(t, out, err, access.ResourceNotFound)
	})
}

// --- file-kind create: placement chain wiring (期11 §4) ---

func TestDoorCreateFileKindPlacement(t *testing.T) {
	fileSpec := resourcespec.CreateSpec{Kind: resourcespec.KindFile}
	fileSpecWithContent := resourcespec.CreateSpec{Kind: resourcespec.KindFile, WithContent: true}

	t.Run("creator affinity (chain ①) picks the creator's own online host", func(t *testing.T) {
		reg := &fakeRegistry{commitReservationFound: true}
		mem := &fakeMembership{isMember: true, lookupHost: "daemon-1", lookupFound: true}
		mounts := &fakeStorageMounts{mounts: []StorageMount{
			{DaemonID: "daemon-1", Online: true},
			{DaemonID: "daemon-2", Online: true},
		}}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		out, err := d.create(t.Context(), "a", "r1", fileSpec, nil)
		mustAccept(t, out, err)

		if len(ctl.calls) != 1 || ctl.calls[0].daemonID != "daemon-1" {
			t.Fatalf("AllocRequest calls = %+v, want exactly one to daemon-1", ctl.calls)
		}
		if ctl.calls[0].spec.ChannelID != "ch1" {
			t.Errorf("AllocRequest ChannelID = %q, want ch1", ctl.calls[0].spec.ChannelID)
		}
		if len(reg.reserveCreateCalls) != 1 || reg.reserveCreateCalls[0].placementDaemonID != "daemon-1" {
			t.Fatalf("ReserveCreate calls = %+v, want one to daemon-1", reg.reserveCreateCalls)
		}
		if len(reg.commitReservationCalls) != 1 {
			t.Fatalf("CommitReservation calls = %d, want 1 (content-less create commits synchronously on AllocRequest ack)", len(reg.commitReservationCalls))
		}
	})

	t.Run("unique online candidate (chain ③) when creator has no host", func(t *testing.T) {
		reg := &fakeRegistry{commitReservationFound: true}
		mem := &fakeMembership{isMember: true} // no host: home-placed / human creator
		mounts := &fakeStorageMounts{mounts: []StorageMount{{DaemonID: "solo-daemon", Online: true}}}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		out, err := d.create(t.Context(), "a", "r1", fileSpec, nil)
		mustAccept(t, out, err)
		if len(ctl.calls) != 1 || ctl.calls[0].daemonID != "solo-daemon" {
			t.Fatalf("AllocRequest calls = %+v, want exactly one to solo-daemon", ctl.calls)
		}
	})

	t.Run("chain ④ zero online daemons is an honest ErrNoStoragePlacement", func(t *testing.T) {
		reg := &fakeRegistry{}
		mem := &fakeMembership{isMember: true}
		mounts := &fakeStorageMounts{}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		_, err := d.create(t.Context(), "a", "r1", fileSpec, nil)
		if !errors.Is(err, ErrNoStoragePlacement) {
			t.Fatalf("err = %v, want ErrNoStoragePlacement", err)
		}
		if len(reg.reserveCreateCalls) != 0 || len(ctl.calls) != 0 {
			t.Fatalf("no reservation/alloc must be attempted with no placement chosen")
		}
	})

	t.Run("chain ④ multiple online daemons with no creator affinity is ambiguous", func(t *testing.T) {
		reg := &fakeRegistry{}
		mem := &fakeMembership{isMember: true}
		mounts := &fakeStorageMounts{mounts: []StorageMount{
			{DaemonID: "daemon-1", Online: true},
			{DaemonID: "daemon-2", Online: true},
		}}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		_, err := d.create(t.Context(), "a", "r1", fileSpec, nil)
		if !errors.Is(err, ErrAmbiguousStoragePlacement) {
			t.Fatalf("err = %v, want ErrAmbiguousStoragePlacement", err)
		}
	})

	t.Run("AllocRequest failure surfaces as a Go error, reservation left standing", func(t *testing.T) {
		reg := &fakeRegistry{}
		mem := &fakeMembership{isMember: true}
		mounts := &fakeStorageMounts{mounts: []StorageMount{{DaemonID: "d1", Online: true}}}
		ctl := &fakeStorageControl{err: errors.New("daemon unreachable")}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		_, err := d.create(t.Context(), "a", "r1", fileSpec, nil)
		if err == nil {
			t.Fatal("expected a Go error when AllocRequest fails")
		}
		if len(reg.commitReservationCalls) != 0 {
			t.Fatal("CommitReservation must not run when AllocRequest failed")
		}
	})

	t.Run("content-less create losing the reservation race maps to already_exists, never a fabricated success (期11 S2)", func(t *testing.T) {
		reg := &fakeRegistry{commitReservationErr: resourcespec.ErrReservationLost}
		mem := &fakeMembership{isMember: true, lookupHost: "daemon-1", lookupFound: true}
		mounts := &fakeStorageMounts{mounts: []StorageMount{{DaemonID: "daemon-1", Online: true}}}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		out, err := d.create(t.Context(), "a", "r1", fileSpec, nil)
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		mustVerdict(t, out, err, access.AlreadyExists)
		if len(reg.commitReservationCalls) != 1 {
			t.Fatalf("CommitReservation calls = %d, want 1", len(reg.commitReservationCalls))
		}
		// 期11 review §2.5 #B: the loser's orphaned coord (its AllocRequest
		// already created an empty live/<coord>) is reclaimed synchronously via
		// StorageControl.ReclaimRequest, on the SAME daemon the AllocRequest
		// went to and for the SAME coord.
		if len(ctl.reclaimCalls) != 1 {
			t.Fatalf("ReclaimRequest calls = %d, want 1 (content-less loser's coord must be reclaimed, #B)", len(ctl.reclaimCalls))
		}
		if ctl.reclaimCalls[0].daemonID != "daemon-1" {
			t.Fatalf("ReclaimRequest daemonID = %q, want daemon-1", ctl.reclaimCalls[0].daemonID)
		}
		if len(ctl.calls) != 1 || ctl.reclaimCalls[0].coord != ctl.calls[0].spec.Coord {
			t.Fatalf("ReclaimRequest coord = %q, want the same coord the AllocRequest used (%q)", ctl.reclaimCalls[0].coord, ctl.calls[0].spec.Coord)
		}
	})

	t.Run("content-less create found=false (no error) must not fabricate success (期11 review残余#4)", func(t *testing.T) {
		// fakeRegistry's zero value already models this: commitReservationFound
		// defaults false and commitReservationErr defaults nil — CommitReservation
		// found nothing to land (superseded by a Delete, or swept by §1.7's
		// timeout sweep) but returned a CLEAN no-op, not ErrReservationLost.
		reg := &fakeRegistry{}
		mem := &fakeMembership{isMember: true, lookupHost: "daemon-1", lookupFound: true}
		mounts := &fakeStorageMounts{mounts: []StorageMount{{DaemonID: "daemon-1", Online: true}}}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		out, err := d.create(t.Context(), "a", "r1", fileSpec, nil)
		if err != nil {
			t.Fatalf("unexpected Go error: %v", err)
		}
		if out.Accepted() {
			t.Fatal("found=false must never report success — the pre-fix 假成功 bug")
		}
		if len(reg.commitReservationCalls) != 1 {
			t.Fatalf("CommitReservation calls = %d, want 1", len(reg.commitReservationCalls))
		}
	})

	t.Run("with_content=true resolves placement + reservation and returns a write Route", func(t *testing.T) {
		reg := &fakeRegistry{}
		mem := &fakeMembership{isMember: true, lookupHost: "daemon-1", lookupFound: true}
		mounts := &fakeStorageMounts{mounts: []StorageMount{{DaemonID: "daemon-1", Online: true}}}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		out, err := d.create(t.Context(), "a", "r1", fileSpecWithContent, nil)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if !out.Accepted() {
			t.Fatalf("want accepted, got reject %q", out.RejectReason)
		}
		if out.Route == nil || out.Route.Mode != access.OpWrite {
			t.Fatalf("want a write Route (creator is daemon-1, same as placement), got %+v", out.Route)
		}
		if len(reg.reserveCreateCalls) != 1 {
			t.Fatalf("ReserveCreate calls = %d, want 1 (placement+reservation ARE resolved for with_content)", len(reg.reserveCreateCalls))
		}
		if out.Route.ReservationID != "reservation-1" {
			t.Fatalf("Route.ReservationID = %q, want the reservation ReserveCreate minted (\"reservation-1\", the fake's default)", out.Route.ReservationID)
		}
		if len(ctl.calls) != 0 {
			t.Fatalf("AllocRequest must not run for with_content=true (that is the write path's job, §5)")
		}
		if len(reg.commitReservationCalls) != 0 {
			t.Fatalf("CommitReservation must not run synchronously for with_content=true — that lands via the daemon's Committed RPC (§1.7)")
		}
	})
}

// --- resolve / not-found for object ops ---

func TestInvokeObjectOpNotFound(t *testing.T) {
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpDelete} {
		reg := &fakeRegistry{resolveExists: false}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		out, err := d.invoke(t.Context(), "a", op, "r1", nil, nil)
		mustVerdict(t, out, err, access.ResourceNotFound)
	}
	// set too (needs a grant operand, but resolve happens first).
	reg := &fakeRegistry{resolveExists: false}
	d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
	g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: []access.Operation{access.OpRead}}
	out, err := d.invoke(t.Context(), "a", access.OpSet, "r1", nil, g)
	mustVerdict(t, out, err, access.ResourceNotFound)
}

// --- A8 union authorization ---

func TestInvokeAuthorizationUnion(t *testing.T) {
	t.Run("actor entry allows", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		d := newDoor(reg, &fakeDriver{readFound: true, readValue: []byte("v")}, &fakeMembership{})
		out, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		mustAccept(t, out, err)
		if string(out.Value) != "v" {
			t.Fatalf("value = %q", out.Value)
		}
	})

	t.Run("members entry allows a current member", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: false, membersAllow: true}
		d := newDoor(reg, &fakeDriver{readFound: true}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "c", access.OpRead, "r1", nil, nil)
		mustAccept(t, out, err)
	})

	t.Run("members entry denies a non-member", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: false, membersAllow: true}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: false})
		out, err := d.invoke(t.Context(), "c", access.OpRead, "r1", nil, nil)
		mustVerdict(t, out, err, access.AccessDenied)
	})

	t.Run("no entry denies", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: false, membersAllow: false}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "b", access.OpRead, "r1", nil, nil)
		mustVerdict(t, out, err, access.AccessDenied)
	})

	t.Run("ActorAllows error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllowsErr: errors.New("x")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		_, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})

	t.Run("MembersAllow error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), membersAllowErr: errors.New("x")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		_, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})

	t.Run("IsMember error during union is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), membersAllow: true}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{err: errors.New("x")})
		_, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})
}

// --- execute side-effects for each op ---

func TestInvokeExecuteEffects(t *testing.T) {
	t.Run("read returns driver value and found", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{readValue: []byte("payload"), readFound: true}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		mustAccept(t, out, err)
		if !out.Found || string(out.Value) != "payload" {
			t.Fatalf("out = %+v", out)
		}
	})

	t.Run("read resolved-but-empty is accepted with found=false", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{readFound: false}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		mustAccept(t, out, err)
		if out.Found {
			t.Fatalf("found should be false")
		}
	})

	t.Run("write hits the driver", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpWrite, "r1", []byte("new"), nil)
		mustAccept(t, out, err)
		if len(drv.writeCalls) != 1 || string(drv.writeCalls[0]) != "new" {
			t.Fatalf("write calls = %v", drv.writeCalls)
		}
	})

	t.Run("set hits the registry not the driver", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{}
		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: []access.Operation{access.OpRead}}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpSet, "r1", nil, g)
		mustAccept(t, out, err)
		if len(reg.setGrants) != 1 || reg.setGrants[0].Grantee != "b" {
			t.Fatalf("set grants = %v", reg.setGrants)
		}
	})

	t.Run("delete orders driver before registry", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpDelete, "r1", nil, nil)
		mustAccept(t, out, err)
		if drv.deleteCalls != 1 || len(reg.deleteCalls) != 1 {
			t.Fatalf("driver deletes=%d registry deletes=%d", drv.deleteCalls, len(reg.deleteCalls))
		}
	})
}

// --- driver_error materialization: EXECUTE failures are verdicts, not Go errors ---

func TestInvokeDriverErrorVerdict(t *testing.T) {
	base := func() *fakeRegistry {
		return &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
	}

	t.Run("read error", func(t *testing.T) {
		out, err := newDoor(base(), &fakeDriver{readErr: errors.New("io")}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		mustVerdict(t, out, err, access.DriverError)
	})
	t.Run("write error", func(t *testing.T) {
		out, err := newDoor(base(), &fakeDriver{writeErr: errors.New("io")}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpWrite, "r1", []byte("v"), nil)
		mustVerdict(t, out, err, access.DriverError)
	})
	t.Run("delete driver error", func(t *testing.T) {
		reg := base()
		out, err := newDoor(reg, &fakeDriver{deleteErr: errors.New("io")}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpDelete, "r1", nil, nil)
		mustVerdict(t, out, err, access.DriverError)
		if len(reg.deleteCalls) != 0 {
			t.Fatalf("registry Delete must not run after a driver Delete failure")
		}
	})
	t.Run("delete registry error", func(t *testing.T) {
		reg := base()
		reg.deleteErr = errors.New("db")
		out, err := newDoor(reg, &fakeDriver{}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpDelete, "r1", nil, nil)
		mustVerdict(t, out, err, access.DriverError)
	})
	t.Run("set registry error", func(t *testing.T) {
		reg := base()
		reg.setGrantErr = errors.New("db")
		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: []access.Operation{access.OpRead}}
		out, err := newDoor(reg, &fakeDriver{}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpSet, "r1", nil, g)
		mustVerdict(t, out, err, access.DriverError)
	})
}

// --- assembly defects surface as Go errors (not verdicts) ---

func TestInvokeResolveErrorIsGoError(t *testing.T) {
	reg := &fakeRegistry{resolveErr: errors.New("db down")}
	_, err := newDoor(reg, &fakeDriver{}, &fakeMembership{}).invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
	if err == nil {
		t.Fatalf("resolve failure must be a Go error")
	}
}

func TestInvokeMissingDriverIsGoError(t *testing.T) {
	// A kv-shaped kind resolved with no registered driver → assembly defect
	// (this is the GENUINE "someone added a kind without wiring its driver"
	// gap — distinct from file, which structurally never gets a DriverTable
	// entry at all, see TestInvokeFileReadWriteRedirectsToByteRoute below).
	const bogusKind = resourcespec.ResourceKind("bogus-inline-kind")
	reg := &fakeRegistry{resolveExists: true, resolveMeta: resourcespec.ResourceMeta{Kind: bogusKind}, actorAllows: true}
	d := &door{deps: Deps{
		Registry:  reg,
		Drivers:   DriverTable{resourcespec.KindKV: &fakeDriver{}}, // KindKV present, bogusKind absent
		Authority: &fakeMembership{},
	}}
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpDelete} {
		_, err := d.invoke(context.Background(), "a", op, "r1", nil, nil)
		if err == nil {
			t.Fatalf("op %q with no driver must be a Go error", op)
		}
	}
}

// TestInvokeFileReadWriteProducesRoute pins 期11 spec §3.4/§5's execute-arm
// kind branch: file read/write through Invoke never touches a Driver (file
// structurally has none — its bytes are realized by the daemon-side
// Allocator/Streamer, §4) and never carries bytes on Outcome.Value (§8.1) —
// an accepted outcome carries a FileRoute instead. A route exists ONLY for a
// caller on the file's own daemon; every other caller is refused, since this
// deployment has no daemon-to-daemon byte transport.
func TestInvokeFileReadWriteProducesRoute(t *testing.T) {
	meta := resourcespec.ResourceMeta{Kind: resourcespec.KindFile, PlacementDaemonID: "daemon-1", PlacementCoord: "coord-1"}

	t.Run("same-daemon caller gets a route, no bytes on Value", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: meta, actorAllows: true}
		mem := &fakeMembership{lookupHost: "daemon-1", lookupFound: true}
		transfers := &fakeTransferControl{ticket: "ticket-local"}
		d := newDoor(reg, &fakeDriver{}, mem)
		d.deps.TransferControl = transfers
		for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
			out, err := d.invoke(context.Background(), "a", op, "r1", nil, nil)
			if err != nil {
				t.Fatalf("op %q: unexpected error %v", op, err)
			}
			if !out.Accepted() {
				t.Fatalf("op %q: want accepted, got reject %q", op, out.RejectReason)
			}
			if out.Value != nil {
				t.Fatalf("op %q: file bytes must never ride Outcome.Value, got %v", op, out.Value)
			}
			if out.Route == nil {
				t.Fatalf("op %q: want a route, got none", op)
			}
			if out.Route.Mode != op {
				t.Fatalf("op %q: route Mode = %q, want %q", op, out.Route.Mode, op)
			}
			if out.Route.Token != "ticket-local" {
				t.Fatalf("op %q: route ticket = %q, want the minted ticket", op, out.Route.Token)
			}
		}
		if len(transfers.calls) != 2 {
			t.Fatalf("OpenTransfer calls = %d, want 2 (one per op)", len(transfers.calls))
		}
		if transfers.calls[0].targetDaemonID != "daemon-1" || transfers.calls[0].coord != "coord-1" {
			t.Fatalf("OpenTransfer call = %+v, want target=daemon-1 coord=coord-1", transfers.calls[0])
		}
	})

	// Byte access is same-daemon only. A caller elsewhere is refused with
	// capability_unavailable and NO route — never a route whose only possible
	// redemption is a transport this deployment does not have.
	t.Run("caller on another daemon is refused, no route minted", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: meta, actorAllows: true}
		mem := &fakeMembership{lookupHost: "daemon-2", lookupFound: true}
		transfers := &fakeTransferControl{ticket: "never-minted"}
		d := newDoor(reg, &fakeDriver{}, mem)
		d.deps.TransferControl = transfers

		out, err := d.invoke(context.Background(), "a", access.OpRead, "r1", nil, nil)
		if !errors.Is(err, ErrFileCapabilityUnavailable) {
			t.Fatalf("err = %v, want ErrFileCapabilityUnavailable", err)
		}
		if out.Route != nil {
			t.Fatalf("a refused caller must get no route, got %+v", out.Route)
		}
		if len(transfers.calls) != 0 {
			t.Fatalf("a refused caller must mint nothing, got %+v", transfers.calls)
		}
	})

	t.Run("home-hosted caller (no storage host) is refused the same way", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: meta, actorAllows: true}
		mem := &fakeMembership{} // lookupFound: false
		transfers := &fakeTransferControl{ticket: "never-minted"}
		d := newDoor(reg, &fakeDriver{}, mem)
		d.deps.TransferControl = transfers

		_, err := d.invoke(context.Background(), "a", access.OpWrite, "r1", nil, nil)
		if !errors.Is(err, ErrFileCapabilityUnavailable) {
			t.Fatalf("err = %v, want ErrFileCapabilityUnavailable", err)
		}
		if len(transfers.calls) != 0 {
			t.Fatalf("a refused caller must mint nothing, got %+v", transfers.calls)
		}
	})

	t.Run("an empty ticket fails closed", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: meta, actorAllows: true}
		mem := &fakeMembership{lookupHost: "daemon-1", lookupFound: true}
		d := newDoor(reg, &fakeDriver{}, mem)
		d.deps.TransferControl = &fakeTransferControl{}
		if _, err := d.invoke(context.Background(), "a", access.OpRead, "r1", nil, nil); err == nil {
			t.Fatal("an empty ticket was accepted")
		}
	})

	t.Run("nil Deps.TransferControl is an honest Go error, never a fabricated route", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: meta, actorAllows: true}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{lookupHost: "daemon-1", lookupFound: true})
		_, err := d.invoke(context.Background(), "a", access.OpRead, "r1", nil, nil)
		if err == nil {
			t.Fatal("expected a Go error when Deps.TransferControl is nil")
		}
	})
}

// TestInvokeFileDeleteRowFirstBytesLast pins 期11 spec §1.8/§3's flip: file
// delete calls ONLY Registry.Delete (row-first-bytes-last, already a single
// transaction inside Registry.Delete since S1) — no Driver.Delete call at
// all, since file has no Driver.
func TestInvokeFileDeleteRowFirstBytesLast(t *testing.T) {
	reg := &fakeRegistry{resolveExists: true, resolveMeta: resourcespec.ResourceMeta{Kind: resourcespec.KindFile}, actorAllows: true}
	drv := &fakeDriver{}
	d := newDoor(reg, drv, &fakeMembership{})
	out, err := d.invoke(context.Background(), "a", access.OpDelete, "r1", nil, nil)
	mustAccept(t, out, err)
	if drv.deleteCalls != 0 {
		t.Fatalf("file delete must not call the Driver (file has none), got %d driver deletes", drv.deleteCalls)
	}
	if len(reg.deleteCalls) != 1 {
		t.Fatalf("expected one Registry.Delete call, got %d", len(reg.deleteCalls))
	}
}

// mustVerdict asserts an accepted-error-free reject with the given reason.
func mustVerdict(t *testing.T, out Outcome, err error, want access.FailureReason) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if out.RejectReason != want {
		t.Fatalf("reason = %q, want %q", out.RejectReason, want)
	}
	if out.Accepted() {
		t.Fatalf("Accepted() = true, want reject %q", want)
	}
}

// mustAccept asserts an accepted, error-free outcome.
func mustAccept(t *testing.T, out Outcome, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !out.Accepted() {
		t.Fatalf("Accepted() = false, reason %q", out.RejectReason)
	}
}
