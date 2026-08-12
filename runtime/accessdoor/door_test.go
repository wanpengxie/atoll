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
	reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
	drv := blockingReadDriver{entered: make(chan struct{}), release: make(chan struct{})}
	d := newDoor(reg, nil, &fakeMembership{isMember: true})
	d.deps.Drivers[resourcespec.KindKV] = drv
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = d.invoke(context.Background(), "old-member", access.OpRead, "r1", nil)
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
		_, err := d.create(t.Context(), "a", "daemon://daemon-1/r1", resourcespec.CreateSpec{Kind: resourcespec.KindFile}, nil)
		if err == nil {
			t.Fatalf("expected a Go error when Deps.StorageMounts is nil")
		}
		if len(reg.createCalls) != 0 {
			t.Fatalf("Registry.Create must not run when placement routing is unavailable")
		}
	})
}

func TestChannelOwnerRootAuthorizesEveryObjectOperation(t *testing.T) {
	// The channel owner root covers all three object ops — including delete on
	// a resource SOMEONE ELSE created (the PM-D3 兜底 half: creator ∨ owner).
	owner := &fakeMembership{isMember: true, isOwner: true}
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpDelete} {
		t.Run(string(op), func(t *testing.T) {
			reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKVBy("someone-else")}
			drv := &fakeDriver{readFound: true}
			var args []byte
			if op != access.OpDelete { // delete is by-id, carries no Args
				args = []byte("v")
			}
			out, err := newDoor(reg, drv, owner).invoke(t.Context(), "owner", op, "r1", args)
			mustAccept(t, out, err)
		})
	}
}

// --- file-kind create: address-directed placement wiring ---

func TestDoorCreateFileKindPlacement(t *testing.T) {
	fileSpec := resourcespec.CreateSpec{Kind: resourcespec.KindFile}
	fileSpecWithContent := resourcespec.CreateSpec{Kind: resourcespec.KindFile, WithContent: true}

	t.Run("address host is authoritative when the creator runs there", func(t *testing.T) {
		reg := &fakeRegistry{commitReservationFound: true}
		mem := &fakeMembership{isMember: true, lookupHost: "daemon-1", lookupFound: true}
		mounts := &fakeStorageMounts{mounts: []StorageMount{
			{DaemonID: "daemon-1", Online: true},
			{DaemonID: "daemon-2", Online: true},
		}}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		out, err := d.create(t.Context(), "a", "daemon://daemon-1/r1", fileSpec, nil)
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

	t.Run("address host is authoritative for a home-placed creator", func(t *testing.T) {
		reg := &fakeRegistry{commitReservationFound: true}
		mem := &fakeMembership{isMember: true} // no host: home-placed / human creator
		mounts := &fakeStorageMounts{mounts: []StorageMount{{DaemonID: "solo-daemon", Online: true}}}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		out, err := d.create(t.Context(), "a", "daemon://solo-daemon/r1", fileSpec, nil)
		mustAccept(t, out, err)
		if len(ctl.calls) != 1 || ctl.calls[0].daemonID != "solo-daemon" {
			t.Fatalf("AllocRequest calls = %+v, want exactly one to solo-daemon", ctl.calls)
		}
	})

	t.Run("unresolved explicit host is an honest ErrNoStoragePlacement", func(t *testing.T) {
		reg := &fakeRegistry{}
		mem := &fakeMembership{isMember: true}
		mounts := &fakeStorageMounts{}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		_, err := d.create(t.Context(), "a", "daemon://daemon-1/r1", fileSpec, nil)
		if !errors.Is(err, ErrNoStoragePlacement) {
			t.Fatalf("err = %v, want ErrNoStoragePlacement", err)
		}
		if len(reg.reserveCreateCalls) != 0 || len(ctl.calls) != 0 {
			t.Fatalf("no reservation/alloc must be attempted with no placement chosen")
		}
	})

	t.Run("the address chooses one daemon even when several are online", func(t *testing.T) {
		reg := &fakeRegistry{commitReservationFound: true}
		mem := &fakeMembership{isMember: true}
		mounts := &fakeStorageMounts{mounts: []StorageMount{
			{DaemonID: "daemon-1", Online: true},
			{DaemonID: "daemon-2", Online: true},
		}}
		ctl := &fakeStorageControl{}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		out, err := d.create(t.Context(), "a", "daemon://daemon-1/r1", fileSpec, nil)
		mustAccept(t, out, err)
		if len(ctl.calls) != 1 || ctl.calls[0].daemonID != "daemon-1" {
			t.Fatalf("AllocRequest calls = %+v, want address-selected daemon-1", ctl.calls)
		}
	})

	t.Run("AllocRequest failure surfaces as a Go error, reservation left standing", func(t *testing.T) {
		reg := &fakeRegistry{}
		mem := &fakeMembership{isMember: true}
		mounts := &fakeStorageMounts{mounts: []StorageMount{{DaemonID: "d1", Online: true}}}
		ctl := &fakeStorageControl{err: errors.New("daemon unreachable")}
		d := newFileDoor(reg, &fakeDriver{}, mem, mounts, ctl, "ch1")

		_, err := d.create(t.Context(), "a", "daemon://d1/r1", fileSpec, nil)
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

		out, err := d.create(t.Context(), "a", "daemon://daemon-1/r1", fileSpec, nil)
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

		out, err := d.create(t.Context(), "a", "daemon://daemon-1/r1", fileSpec, nil)
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

		out, err := d.create(t.Context(), "a", "daemon://daemon-1/r1", fileSpecWithContent, nil)
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
		out, err := d.invoke(t.Context(), "a", op, "r1", nil)
		mustVerdict(t, out, err, access.ResourceNotFound)
	}
}

// --- membrane-uniform authorization (PM-D1) + creator-delete (PM-D3) ---

func TestInvokeMembraneUniformAuthorization(t *testing.T) {
	t.Run("any active member reads any resource", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKVBy("someone-else")}
		d := newDoor(reg, &fakeDriver{readFound: true, readValue: []byte("v")}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil)
		mustAccept(t, out, err)
		if string(out.Value) != "v" {
			t.Fatalf("value = %q", out.Value)
		}
	})

	t.Run("any active member writes any resource", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKVBy("someone-else")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "a", access.OpWrite, "r1", []byte("v"))
		mustAccept(t, out, err)
	})

	t.Run("a non-member is denied every op", func(t *testing.T) {
		for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpDelete} {
			reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKVBy("a")}
			d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: false})
			var args []byte
			if op == access.OpWrite {
				args = []byte("v")
			}
			out, err := d.invoke(t.Context(), "a", op, "r1", args)
			mustVerdict(t, out, err, access.AccessDenied)
		}
	})

	t.Run("delete by a member who is neither creator nor owner is denied (PM-D3)", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKVBy("someone-else")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "b", access.OpDelete, "r1", nil)
		mustVerdict(t, out, err, access.AccessDenied)
	})

	t.Run("delete by the creator is allowed (PM-D3)", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKVBy("a")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "a", access.OpDelete, "r1", nil)
		mustAccept(t, out, err)
	})

	t.Run("facts error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{err: errors.New("x")})
		_, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil)
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})
}

// --- execute side-effects for each op ---

func TestInvokeExecuteEffects(t *testing.T) {
	member := func() *fakeMembership { return &fakeMembership{isMember: true} }

	t.Run("read returns driver value and found", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		drv := &fakeDriver{readValue: []byte("payload"), readFound: true}
		out, err := newDoor(reg, drv, member()).invoke(t.Context(), "a", access.OpRead, "r1", nil)
		mustAccept(t, out, err)
		if !out.Found || string(out.Value) != "payload" {
			t.Fatalf("out = %+v", out)
		}
	})

	t.Run("read resolved-but-empty is accepted with found=false", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		drv := &fakeDriver{readFound: false}
		out, err := newDoor(reg, drv, member()).invoke(t.Context(), "a", access.OpRead, "r1", nil)
		mustAccept(t, out, err)
		if out.Found {
			t.Fatalf("found should be false")
		}
	})

	t.Run("write hits the driver", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		drv := &fakeDriver{}
		out, err := newDoor(reg, drv, member()).invoke(t.Context(), "a", access.OpWrite, "r1", []byte("new"))
		mustAccept(t, out, err)
		if len(drv.writeCalls) != 1 || string(drv.writeCalls[0]) != "new" {
			t.Fatalf("write calls = %v", drv.writeCalls)
		}
	})

	t.Run("delete orders driver before registry", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKVBy("a")}
		drv := &fakeDriver{}
		out, err := newDoor(reg, drv, member()).invoke(t.Context(), "a", access.OpDelete, "r1", nil)
		mustAccept(t, out, err)
		if drv.deleteCalls != 1 || len(reg.deleteCalls) != 1 {
			t.Fatalf("driver deletes=%d registry deletes=%d", drv.deleteCalls, len(reg.deleteCalls))
		}
	})
}

// --- driver_error materialization: EXECUTE failures are verdicts, not Go errors ---

func TestInvokeDriverErrorVerdict(t *testing.T) {
	base := func() *fakeRegistry {
		return &fakeRegistry{resolveExists: true, resolveMeta: metaKVBy("a")}
	}
	member := func() *fakeMembership { return &fakeMembership{isMember: true} }

	t.Run("read error", func(t *testing.T) {
		out, err := newDoor(base(), &fakeDriver{readErr: errors.New("io")}, member()).
			invoke(t.Context(), "a", access.OpRead, "r1", nil)
		mustVerdict(t, out, err, access.DriverError)
	})
	t.Run("write error", func(t *testing.T) {
		out, err := newDoor(base(), &fakeDriver{writeErr: errors.New("io")}, member()).
			invoke(t.Context(), "a", access.OpWrite, "r1", []byte("v"))
		mustVerdict(t, out, err, access.DriverError)
	})
	t.Run("delete driver error", func(t *testing.T) {
		reg := base()
		out, err := newDoor(reg, &fakeDriver{deleteErr: errors.New("io")}, member()).
			invoke(t.Context(), "a", access.OpDelete, "r1", nil)
		mustVerdict(t, out, err, access.DriverError)
		if len(reg.deleteCalls) != 0 {
			t.Fatalf("registry Delete must not run after a driver Delete failure")
		}
	})
	t.Run("delete registry error", func(t *testing.T) {
		reg := base()
		reg.deleteErr = errors.New("db")
		out, err := newDoor(reg, &fakeDriver{}, member()).
			invoke(t.Context(), "a", access.OpDelete, "r1", nil)
		mustVerdict(t, out, err, access.DriverError)
	})
}

// --- assembly defects surface as Go errors (not verdicts) ---

func TestInvokeResolveErrorIsGoError(t *testing.T) {
	reg := &fakeRegistry{resolveErr: errors.New("db down")}
	_, err := newDoor(reg, &fakeDriver{}, &fakeMembership{}).invoke(t.Context(), "a", access.OpRead, "r1", nil)
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
	reg := &fakeRegistry{resolveExists: true, resolveMeta: resourcespec.ResourceMeta{Kind: bogusKind, CreatedBy: "a"}}
	d := &door{deps: Deps{
		Registry:  reg,
		Drivers:   DriverTable{resourcespec.KindKV: &fakeDriver{}}, // KindKV present, bogusKind absent
		Authority: &fakeMembership{isMember: true},
	}}
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpDelete} {
		_, err := d.invoke(context.Background(), "a", op, "r1", nil)
		if err == nil {
			t.Fatalf("op %q with no driver must be a Go error", op)
		}
	}
}

// TestInvokeFileReadWriteProducesRoute pins 期11 spec §3.4/§5's execute-arm
// kind branch: file read/write through Invoke never touches a Driver (file
// structurally has none — its bytes are realized by the daemon-side
// Allocator/Streamer, §4) and never carries bytes on Outcome.Value (§8.1) —
// an accepted outcome carries a FileRoute instead. The route explicitly
// selects local redemption or remote exchange from the caller/host identity.
func TestInvokeFileReadWriteProducesRoute(t *testing.T) {
	meta := resourcespec.ResourceMeta{Kind: resourcespec.KindFile, PlacementDaemonID: "daemon-1", PlacementCoord: "coord-1"}
	routed := func(mem *fakeMembership, transfers *fakeTransferControl) *door {
		d := newDoor(&fakeRegistry{resolveExists: true, resolveMeta: meta}, &fakeDriver{}, mem)
		d.deps.StorageMounts = &fakeStorageMounts{mounts: []StorageMount{{DaemonID: "daemon-1", Name: "daemon-1", Online: true}}}
		if transfers != nil {
			d.deps.TransferControl = transfers
		}
		return d
	}

	t.Run("same-daemon caller gets a route, no bytes on Value", func(t *testing.T) {
		mem := &fakeMembership{lookupHost: "daemon-1", lookupFound: true}
		transfers := &fakeTransferControl{ticket: "ticket-local"}
		d := routed(mem, transfers)
		for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
			out, err := d.invoke(context.Background(), "a", op, "daemon://daemon-1/r1", nil)
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
			if out.Route.Redeem != FileRedeemLocal {
				t.Fatalf("op %q: redeem=%q, want local", op, out.Route.Redeem)
			}
		}
		if len(transfers.calls) != 2 {
			t.Fatalf("IssueTransfer calls = %d, want 2 (one per op)", len(transfers.calls))
		}
		if transfers.calls[0].targetDaemonID != "daemon-1" || transfers.calls[0].coord != "coord-1" {
			t.Fatalf("IssueTransfer call = %+v, want target=daemon-1 coord=coord-1", transfers.calls[0])
		}
	})

	t.Run("caller on another daemon gets a remote route", func(t *testing.T) {
		mem := &fakeMembership{lookupHost: "daemon-2", lookupFound: true}
		transfers := &fakeTransferControl{ticket: "ticket-remote"}
		d := routed(mem, transfers)

		out, err := d.invoke(context.Background(), "a", access.OpRead, "daemon://daemon-1/r1", nil)
		if err != nil || out.Route == nil || out.Route.Redeem != FileRedeemRemote {
			t.Fatalf("out=%+v err=%v, want remote route", out, err)
		}
	})

	t.Run("home-hosted caller gets the remote redemption shape", func(t *testing.T) {
		mem := &fakeMembership{isMember: true} // member, but lookupFound: false — no storage host
		transfers := &fakeTransferControl{ticket: "ticket-http"}
		d := routed(mem, transfers)

		out, err := d.invoke(context.Background(), "a", access.OpWrite, "daemon://daemon-1/r1", nil)
		if err != nil || out.Route == nil || out.Route.Redeem != FileRedeemRemote {
			t.Fatalf("out=%+v err=%v, want remote route", out, err)
		}
	})

	t.Run("an empty ticket fails closed", func(t *testing.T) {
		mem := &fakeMembership{lookupHost: "daemon-1", lookupFound: true}
		d := routed(mem, &fakeTransferControl{})
		if _, err := d.invoke(context.Background(), "a", access.OpRead, "daemon://daemon-1/r1", nil); err == nil {
			t.Fatal("an empty ticket was accepted")
		}
	})

	t.Run("nil Deps.TransferControl is an honest Go error, never a fabricated route", func(t *testing.T) {
		d := routed(&fakeMembership{lookupHost: "daemon-1", lookupFound: true}, nil)
		_, err := d.invoke(context.Background(), "a", access.OpRead, "daemon://daemon-1/r1", nil)
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
	reg := &fakeRegistry{resolveExists: true, resolveMeta: resourcespec.ResourceMeta{Kind: resourcespec.KindFile, CreatedBy: "a"}}
	drv := &fakeDriver{}
	d := newDoor(reg, drv, &fakeMembership{isMember: true})
	out, err := d.invoke(context.Background(), "a", access.OpDelete, "r1", nil)
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
