package engineboot

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/google/uuid"
	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func bootRegistrarTest(t *testing.T) (*Engine, *sql.DB, actor.ActorID) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: dir, Addr: "127.0.0.1:0", RootPassword: "root-test-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close(context.Background()) })
	path := filepath.Join(filepath.Dir(dir), "registry.db")
	u := &url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", u.String()+"?mode=rw&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	root, err := eng.principalActor(context.Background(), protocol.C0ChannelID, protocol.RootPrincipalID)
	if err != nil {
		t.Fatal(err)
	}
	return eng, db, root
}

func registrarCall(t *testing.T, eng *Engine, sender actor.ActorID, source channel.ID, word lagoon.Word, payload any) (lagoon.Reply, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bundle, ok := eng.host.Acquire(source)
	if !ok {
		return lagoon.Reply{}, errors.New("source channel unavailable")
	}
	declID := lagoon.SpaceToolDeclID
	if source == protocol.C0ChannelID {
		declID = lagoon.RegistrarSeatDeclID
	}
	targets, err := bundle.View().DeclaredInstances(ctx, declID)
	if err != nil || len(targets) != 1 {
		return lagoon.Reply{}, errors.Join(err, errors.New("target seat unavailable"))
	}
	slot, ok := bundle.Gateway().SubjectSlotFor(sender)
	if !ok {
		return lagoon.Reply{}, errors.New("sender slot unavailable")
	}
	requestID := message.ID(uuid.NewString())
	frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, uuid.NewString(), subjectgate.SubmitPayload{
		ChannelID: string(source), ID: string(requestID), MsgType: string(word), Kind: "request",
		Audience: []string{string(targets[0])}, Visibility: "public", Payload: mustJSON(payload),
	})
	if err != nil {
		return lagoon.Reply{}, err
	}
	result, err := slot.Deliver(ctx, frame)
	if err != nil {
		return lagoon.Reply{}, err
	}
	if result.Frame.Type != subjectgate.FrameReceipt {
		return lagoon.Reply{}, errors.New("request frame rejected")
	}
	reader := channel.Reader{ActorID: sender, Mode: channel.ReaderMember}
	var cursor int64
	for {
		rows, next, err := bundle.View().ReadVisibleAfterSeq(ctx, reader, cursor, 256)
		if err != nil {
			return lagoon.Reply{}, err
		}
		cursor = next
		for _, row := range rows {
			if !row.IsTerminal || row.Envelope.Kind != message.KindResponse || row.Envelope.ParentID != requestID {
				continue
			}
			raw, err := decodeRegistrarTerminal(row.Envelope.Payload)
			if err != nil {
				return lagoon.Reply{}, err
			}
			if source == protocol.C0ChannelID {
				var reply lagoon.Reply
				if err := json.Unmarshal(raw, &reply); err != nil {
					return lagoon.Reply{}, err
				}
				if err := reply.ValidValue(); err != nil {
					return lagoon.Reply{}, err
				}
				return reply, nil
			}
			return lagoon.Reply{Value: raw}, nil
		}
		select {
		case <-ctx.Done():
			return lagoon.Reply{}, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func requireLagoonCode(t *testing.T, err error, want lagoon.ErrorCode) {
	t.Helper()
	var lagoonErr *lagoon.Error
	if !errors.As(err, &lagoonErr) || lagoonErr.Code != want {
		t.Fatalf("registrar error=%v, want code %s", err, want)
	}
}

func createChannelForTest(t *testing.T, eng *Engine, root actor.ActorID, name string, parent channel.ID) regspec.ChannelRow {
	t.Helper()
	reply, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordChannelCreate, lagoon.ChannelCreate{Name: name, Parent: parent})
	if err != nil {
		t.Fatal(err)
	}
	var row regspec.ChannelRow
	if reply.DecodeValue(&row) != nil {
		t.Fatalf("invalid channel reply: %#v", reply.Value)
	}
	return row
}

func TestPrincipalRegisterLeavesCompletedRowsWhenDefaultBindingFails(t *testing.T) {
	eng, db, _ := bootRegistrarTest(t)
	local, ok, err := eng.registry.GetDevice(context.Background(), protocol.LocalDeviceID)
	if err != nil || !ok {
		t.Fatalf("local device before cut=%+v ok=%v err=%v", local, ok, err)
	}
	if _, err := db.Exec(`DELETE FROM devices WHERE id=?`, protocol.LocalDeviceID); err != nil {
		t.Fatal(err)
	}
	register := lagoon.PrincipalRegister{
		ID: "alice", Email: "alice@example.test", SecretHash: "already-hashed", DisplayName: "Alice",
	}
	_, err = eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, register)
	if err == nil {
		t.Fatal("register succeeded without the default-binding device")
	}
	for label, check := range map[string]struct {
		query string
		want  int
	}{
		"principal":  {`SELECT count(*) FROM principals WHERE id='alice'`, 1},
		"credential": {`SELECT count(*) FROM credentials WHERE principal_id='alice'`, 1},
		"home":       {`SELECT count(*) FROM channels WHERE owner_principal='alice'`, 1},
		"binding":    {`SELECT count(*) FROM bindings WHERE channel_id IN (SELECT id FROM channels WHERE owner_principal='alice')`, 0},
	} {
		var count int
		if scanErr := db.QueryRow(check.query).Scan(&count); scanErr != nil || count != check.want {
			t.Fatalf("%s residual count=%d, want %d (err=%v)", label, count, check.want, scanErr)
		}
	}
	if _, err := db.Exec(`INSERT INTO devices(id,owner_principal,name,key,status,created_at) VALUES(?,?,?,?,?,?)`,
		local.ID, local.OwnerPrincipal, local.Name, local.Key, local.Status, local.CreatedAt); err != nil {
		t.Fatal(err)
	}
	reply, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, register)
	if err != nil {
		t.Fatalf("idempotent replay did not repair residual registration: %v", err)
	}
	var principal regspec.PrincipalRow
	if err := reply.DecodeValue(&principal); err != nil || principal.ID != register.ID {
		t.Fatalf("replay principal=%+v err=%v", principal, err)
	}
	var bindings int
	if err := db.QueryRow(`SELECT count(*) FROM bindings WHERE channel_id IN (SELECT id FROM channels WHERE owner_principal='alice') AND device_id=?`, protocol.LocalDeviceID).Scan(&bindings); err != nil || bindings != 1 {
		t.Fatalf("repaired binding count=%d err=%v", bindings, err)
	}
}

func TestChannelCreateReplayRepairsItsResidualDefaultBinding(t *testing.T) {
	eng, db, root := bootRegistrarTest(t)
	local, ok, err := eng.registry.GetDevice(context.Background(), protocol.LocalDeviceID)
	if err != nil || !ok {
		t.Fatalf("local device before cut=%+v ok=%v err=%v", local, ok, err)
	}
	if _, err := db.Exec(`DELETE FROM devices WHERE id=?`, protocol.LocalDeviceID); err != nil {
		t.Fatal(err)
	}
	input := lagoon.ChannelCreate{Name: "residual-channel", Parent: protocol.C0ChannelID}
	if _, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordChannelCreate, input); err == nil {
		t.Fatal("channel.create succeeded without its default-binding device")
	}
	rows, err := eng.registry.ListChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var residual regspec.ChannelRow
	for _, row := range rows {
		if row.Name == input.Name {
			residual = row
		}
	}
	if residual.ID == "" {
		t.Fatal("channel row did not remain after binding failure")
	}
	if _, err := db.Exec(`INSERT INTO devices(id,owner_principal,name,key,status,created_at) VALUES(?,?,?,?,?,?)`,
		local.ID, local.OwnerPrincipal, local.Name, local.Key, local.Status, local.CreatedAt); err != nil {
		t.Fatal(err)
	}
	reply, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordChannelCreate, input)
	if err != nil {
		t.Fatalf("idempotent replay did not repair residual channel: %v", err)
	}
	var repaired regspec.ChannelRow
	if err := reply.DecodeValue(&repaired); err != nil || repaired.ID != residual.ID {
		t.Fatalf("repaired channel=%+v residual=%+v err=%v", repaired, residual, err)
	}
	if bound, err := eng.registry.IsBound(context.Background(), repaired.ID, protocol.LocalDeviceID); err != nil || !bound {
		t.Fatalf("repaired default binding=%v err=%v", bound, err)
	}
}

func TestRetireReparentsChildrenAndKeepsTombstone(t *testing.T) {
	eng, db, root := bootRegistrarTest(t)
	_ = createChannelForTest(t, eng, root, "child", protocol.C0ChannelID)
	parent := createChannelForTest(t, eng, root, "parent", protocol.C0ChannelID)
	child := createChannelForTest(t, eng, root, "child", parent.ID)
	if _, err := db.Exec(`INSERT INTO decl_overlays(decl_id,channel_id,config_json,updated_at) VALUES(?,?,?,?)`, lagoon.SpaceToolDeclID, parent.ID, `{}`, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordChannelRetire, lagoon.ChannelRetire{ChannelID: parent.ID}); err != nil {
		t.Fatal(err)
	}
	retired, ok, err := eng.registry.GetChannelDesired(context.Background(), parent.ID)
	if err != nil || !ok || retired.Status != regspec.ChannelRetired {
		t.Fatalf("retired row=%+v ok=%v err=%v", retired, ok, err)
	}
	reparented, ok, err := eng.registry.GetChannelDesired(context.Background(), child.ID)
	if err != nil || !ok || reparented.ParentID != protocol.C0ChannelID {
		t.Fatalf("child row=%+v ok=%v err=%v", reparented, ok, err)
	}
	for label, query := range map[string]string{
		"binding": `SELECT count(*) FROM bindings WHERE channel_id=?`,
		"overlay": `SELECT count(*) FROM decl_overlays WHERE channel_id=?`,
	} {
		var count int
		if err := db.QueryRow(query, parent.ID).Scan(&count); err != nil || count != 1 {
			t.Fatalf("retire removed %s: count=%d err=%v", label, count, err)
		}
	}
}

func TestCreateReplayAfterDetachDoesNotRestoreBinding(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	created := createChannelForTest(t, eng, root, "detached", protocol.C0ChannelID)
	mintedReply, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeviceMint, lagoon.DeviceMint{Name: "detachable"})
	if err != nil {
		t.Fatal(err)
	}
	var device regspec.DeviceRow
	if mintedReply.DecodeValue(&device) != nil {
		t.Fatalf("invalid mint reply: %#v", mintedReply.Value)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.waitChannel(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	homeRoot, err := eng.principalActor(ctx, created.ID, protocol.RootPrincipalID)
	if err != nil {
		t.Fatal(err)
	}
	binding := lagoon.DeviceBinding{ChannelID: created.ID, DeviceID: device.ID}
	if _, err := registrarCall(t, eng, homeRoot, created.ID, lagoon.WordDeviceAttach, binding); err != nil {
		t.Fatal(err)
	}
	if _, err := registrarCall(t, eng, homeRoot, created.ID, lagoon.WordDeviceDetach, binding); err != nil {
		t.Fatal(err)
	}
	if bound, err := eng.registry.IsBound(context.Background(), created.ID, device.ID); err != nil || bound {
		t.Fatalf("binding after detach=%v err=%v", bound, err)
	}
	replayed := createChannelForTest(t, eng, root, "detached", protocol.C0ChannelID)
	if replayed.ID != created.ID {
		t.Fatalf("replay returned %s, want %s", replayed.ID, created.ID)
	}
	if bound, err := eng.registry.IsBound(context.Background(), created.ID, device.ID); err != nil || bound {
		t.Fatalf("create replay resurrected detached binding: bound=%v err=%v", bound, err)
	}
}

func TestCreateReplayRejectsAmbiguousMatchesAndDoesNotReuseTombstone(t *testing.T) {
	t.Run("ambiguous natural key", func(t *testing.T) {
		eng, db, root := bootRegistrarTest(t)
		first := createChannelForTest(t, eng, root, "ambiguous", protocol.C0ChannelID)
		if _, err := db.Exec(`INSERT INTO channels(id,parent_id,name,type,status,owner_principal,spec_json,created_at) VALUES(?,?,'ambiguous','group','present',?,?,?)`, "duplicate", protocol.C0ChannelID, protocol.RootPrincipalID, string(first.Spec), time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		_, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordChannelCreate, lagoon.ChannelCreate{Name: "ambiguous", Parent: protocol.C0ChannelID})
		var lagoonErr *lagoon.Error
		if !errors.As(err, &lagoonErr) || lagoonErr.Code != lagoon.CodeConflictExists {
			t.Fatalf("ambiguous create error=%v", err)
		}
	})
	t.Run("retired name is a new birth", func(t *testing.T) {
		eng, _, root := bootRegistrarTest(t)
		first := createChannelForTest(t, eng, root, "reborn", protocol.C0ChannelID)
		if _, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordChannelRetire, lagoon.ChannelRetire{ChannelID: first.ID}); err != nil {
			t.Fatal(err)
		}
		second := createChannelForTest(t, eng, root, "reborn", protocol.C0ChannelID)
		if second.ID == first.ID {
			t.Fatal("channel.create reused a retired row")
		}
	})
}

func TestRetiredDeviceIsExcludedFromEffectiveBindings(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	created := createChannelForTest(t, eng, root, "retired-device", protocol.C0ChannelID)
	mintedReply, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeviceMint, lagoon.DeviceMint{Name: "second"})
	if err != nil {
		t.Fatal(err)
	}
	var second regspec.DeviceRow
	if mintedReply.DecodeValue(&second) != nil {
		t.Fatalf("invalid mint reply: %#v", mintedReply.Value)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.waitChannel(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	homeRoot, err := eng.principalActor(ctx, created.ID, protocol.RootPrincipalID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registrarCall(t, eng, homeRoot, created.ID, lagoon.WordDeviceAttach, lagoon.DeviceBinding{ChannelID: created.ID, DeviceID: second.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeviceRetire, lagoon.DeviceRetire{DeviceID: second.ID}); err != nil {
		t.Fatal(err)
	}
	bound, err := eng.registry.ListBoundDevices(context.Background(), created.ID)
	if err != nil || len(bound) != 1 || bound[0].ID != protocol.LocalDeviceID {
		t.Fatalf("effective bindings=%+v err=%v", bound, err)
	}
	if yes, err := eng.registry.IsBound(context.Background(), created.ID, second.ID); err != nil || yes {
		t.Fatalf("retired device remained effectively bound: %v err=%v", yes, err)
	}
}

func TestSpaceToolDeclarationMutationsAreReserved(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	name := "renamed"
	visibility := "private"
	for label, edit := range map[string]lagoon.DeclEdit{
		"name":       {ID: lagoon.SpaceToolDeclID, Name: &name},
		"config":     {ID: lagoon.SpaceToolDeclID, Config: json.RawMessage(`{"changed":true}`)},
		"visibility": {ID: lagoon.SpaceToolDeclID, Visibility: &visibility},
	} {
		t.Run(label, func(t *testing.T) {
			_, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeclEdit, edit)
			requireLagoonCode(t, err, lagoon.CodeReserved)
		})
	}
	_, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeclRevoke, lagoon.DeclRevoke{ID: lagoon.SpaceToolDeclID})
	requireLagoonCode(t, err, lagoon.CodeReserved)
}

func TestOrdinaryDeclarationEditAndRevokeRemainAllowed(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	input := lagoon.DeclRegister{ID: "mutable-decl", Name: "mutable", Class: "codex", Config: json.RawMessage(`{}`), Visibility: "private"}
	if _, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeclRegister, input); err != nil {
		t.Fatal(err)
	}
	name := "edited"
	if _, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeclEdit, lagoon.DeclEdit{ID: input.ID, Name: &name}); err != nil {
		t.Fatal(err)
	}
	reply, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeclRevoke, lagoon.DeclRevoke{ID: input.ID})
	if err != nil {
		t.Fatal(err)
	}
	var revoked regspec.DeclRow
	if reply.DecodeValue(&revoked) != nil || revoked.Status != regspec.DeclRevoked {
		t.Fatalf("invalid revoke reply: %#v", reply.Value)
	}
}

func TestLocalDeviceRetireAndDetachAreReserved(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	_, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeviceRetire, lagoon.DeviceRetire{DeviceID: protocol.LocalDeviceID})
	requireLagoonCode(t, err, lagoon.CodeReserved)
	_, err = registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeviceDetach, lagoon.DeviceBinding{ChannelID: protocol.C0ChannelID, DeviceID: protocol.LocalDeviceID})
	requireLagoonCode(t, err, lagoon.CodeReserved)
	device, ok, err := eng.registry.GetDevice(context.Background(), protocol.LocalDeviceID)
	if err != nil || !ok || device.Status != regspec.DevicePresent {
		t.Fatalf("local device row=%+v ok=%v err=%v", device, ok, err)
	}
}

func TestRegisterConflictDoesNotMintSessionShapedSuccess(t *testing.T) {
	eng, _, _ := bootRegistrarTest(t)
	register := lagoon.PrincipalRegister{ID: "alice", Email: "same@example.test", SecretHash: "hash"}
	if _, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, register); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		in   lagoon.PrincipalRegister
	}{
		{name: "duplicate email unique", in: lagoon.PrincipalRegister{ID: "bob", Email: register.Email, SecretHash: "hash"}},
		{name: "duplicate explicit id primary key", in: lagoon.PrincipalRegister{ID: register.ID, Email: "other@example.test", SecretHash: "hash"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, tc.in)
			var lagoonErr *lagoon.Error
			if !errors.As(err, &lagoonErr) || lagoonErr.Code != lagoon.CodeConflictExists {
				t.Fatalf("duplicate register error=%v", err)
			}
		})
	}
}

func TestRegisterHomeEventuallyServes(t *testing.T) {
	eng, _, _ := bootRegistrarTest(t)
	reply, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, lagoon.PrincipalRegister{ID: "alice", Email: "alice@example.test", SecretHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	var principal regspec.PrincipalRow
	if reply.DecodeValue(&principal) != nil || principal.ID != "alice" {
		t.Fatalf("invalid register reply: %#v", reply.Value)
	}
	rows, err := eng.registry.ListPresentChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.OwnerPrincipal == principal.ID && row.ParentID == protocol.C0ChannelID && row.Name == principal.ID {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := eng.waitChannel(ctx, row.ID); err != nil {
				t.Fatalf("registered home did not become serving: %v", err)
			}
			return
		}
	}
	t.Fatal("registered principal has no home channel")
}

func TestMembraneOpenPokesOwnerSession(t *testing.T) {
	eng, _, _ := bootRegistrarTest(t)
	session, err := eng.gateway.Attach("membrane-owner", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	session.StartFeed()

	reply, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, lagoon.PrincipalRegister{
		ID: "membrane-owner", Email: "membrane-owner@example.test", SecretHash: "hash",
	})
	if err != nil {
		t.Fatal(err)
	}
	var principal regspec.PrincipalRow
	if reply.DecodeValue(&principal) != nil {
		t.Fatalf("invalid register reply: %#v", reply.Value)
	}
	home := homeChannelForTest(t, eng, principal.ID)
	probe, err := subjectgate.NewFrame(subjectgate.FrameCancel, "membrane-probe", subjectgate.CancelPayload{ChannelID: string(home), ReqID: "missing"})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if code := frameErrorCode(t, session.Upstream(probe)); code != subjectgate.CodeForbidden && code != subjectgate.CodeUnavailable {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("owner session did not gain its home route after membrane open poke")
		case <-tick.C:
		}
	}
}

func registerHumanForTest(t *testing.T, eng *Engine, id string) regspec.PrincipalRow {
	t.Helper()
	reply, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, lagoon.PrincipalRegister{
		ID: id, Email: id + "@example.test", SecretHash: "hash",
	})
	if err != nil {
		t.Fatal(err)
	}
	var principal regspec.PrincipalRow
	if reply.DecodeValue(&principal) != nil {
		t.Fatalf("invalid register reply: %#v", reply.Value)
	}
	home := homeChannelForTest(t, eng, principal.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := eng.waitChannel(ctx, home); err != nil {
		t.Fatalf("registered home did not become serving: %v", err)
	}
	return principal
}

func homeChannelForTest(t *testing.T, eng *Engine, principal string) channel.ID {
	t.Helper()
	rows, err := eng.registry.ListPresentChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.OwnerPrincipal == principal && row.ParentID == protocol.C0ChannelID && row.Name == principal {
			return row.ID
		}
	}
	t.Fatalf("principal %q has no home channel", principal)
	return ""
}

func frameErrorCode(t *testing.T, frame subjectgate.Frame) string {
	t.Helper()
	if frame.Type != subjectgate.FrameError {
		return ""
	}
	var payload subjectgate.ErrorPayload
	if err := frame.DecodePayload(&payload); err != nil {
		t.Fatal(err)
	}
	return payload.Code
}

func TestRetiredPrincipalResolvesToNoEntitlements(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	principal := registerHumanForTest(t, eng, "retire-level")

	routes, failed, err := eng.resolveEntitlements(context.Background(), principal.ID)
	if err != nil || len(routes) == 0 || len(failed) != 0 {
		t.Fatalf("present principal routes=%d failed=%v err=%v", len(routes), failed, err)
	}
	if _, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordPrincipalRetire, lagoon.PrincipalRetire{PrincipalID: principal.ID}); err != nil {
		t.Fatal(err)
	}
	routes, failed, err = eng.resolveEntitlements(context.Background(), principal.ID)
	if err != nil || len(routes) != 0 || len(failed) != 0 {
		t.Fatalf("retired principal routes=%d failed=%v err=%v", len(routes), failed, err)
	}
}

func TestRetirePokeConvergesExistingSessionEntitlements(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	principal := registerHumanForTest(t, eng, "retire-edge")
	home := homeChannelForTest(t, eng, principal.ID)
	session, err := eng.gateway.Attach(principal.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	session.StartFeed()

	probe, err := subjectgate.NewFrame(subjectgate.FrameCancel, "retire-probe", subjectgate.CancelPayload{ChannelID: string(home), ReqID: "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if code := frameErrorCode(t, session.Upstream(probe)); code == subjectgate.CodeForbidden {
		t.Fatal("present principal session started without its home route")
	}
	if _, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordPrincipalRetire, lagoon.PrincipalRetire{PrincipalID: principal.ID}); err != nil {
		t.Fatal(err)
	}

	// The two-second bound is deliberately far below the 30-second sweep: this can
	// only pass through the registrar onCommit -> Gateway.Poke edge.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if code := frameErrorCode(t, session.Upstream(probe)); code == subjectgate.CodeForbidden {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("retired principal session did not converge after commit poke")
		case <-tick.C:
		}
	}
}

func TestCredentialSetRejectsAgentAndAllowsHuman(t *testing.T) {
	eng, db, root := bootRegistrarTest(t)
	setAgent := lagoon.CredentialSet{PrincipalID: protocol.StewardPrincipalID, SecretHash: "agent-hash"}
	_, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordCredentialSet, setAgent)
	var lagoonErr *lagoon.Error
	if !errors.As(err, &lagoonErr) || lagoonErr.Code != lagoon.CodePermissionDenied {
		t.Fatalf("agent credential.set error=%v", err)
	}
	var agentCredentials int
	if err := db.QueryRow(`SELECT COUNT(*) FROM credentials WHERE principal_id=?`, protocol.StewardPrincipalID).Scan(&agentCredentials); err != nil {
		t.Fatal(err)
	}
	if agentCredentials != 0 {
		t.Fatalf("agent credentials rows=%d", agentCredentials)
	}

	setHuman := lagoon.CredentialSet{PrincipalID: protocol.RootPrincipalID, SecretHash: "human-hash"}
	reply, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordCredentialSet, setHuman)
	if err != nil {
		t.Fatal(err)
	}
	var credential lagoon.CredentialReply
	if reply.DecodeValue(&credential) != nil || credential.PrincipalID != protocol.RootPrincipalID || credential.Status != regspec.CredentialActive {
		t.Fatalf("human credential reply=%#v", reply.Value)
	}
	var stored string
	if err := db.QueryRow(`SELECT secret_hash FROM credentials WHERE principal_id=? AND kind='password'`, protocol.RootPrincipalID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != setHuman.SecretHash {
		t.Fatalf("human credential hash=%q", stored)
	}
}

func TestDeviceMintAndClaimMissAreIntentionallyNonIdempotent(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	mint := func(word lagoon.Word, payload any) regspec.DeviceRow {
		t.Helper()
		reply, err := registrarCall(t, eng, root, protocol.C0ChannelID, word, payload)
		if err != nil {
			t.Fatal(err)
		}
		var row regspec.DeviceRow
		if reply.DecodeValue(&row) != nil {
			t.Fatalf("invalid device reply: %#v", reply.Value)
		}
		return row
	}
	first := mint(lagoon.WordDeviceMint, lagoon.DeviceMint{Name: "box"})
	second := mint(lagoon.WordDeviceMint, lagoon.DeviceMint{Name: "box"})
	if first.ID == second.ID {
		t.Fatal("device.mint replay reused an identity")
	}
	missOne := mint(lagoon.WordDeviceClaim, lagoon.DeviceClaim{DeviceID: "missing"})
	missTwo := mint(lagoon.WordDeviceClaim, lagoon.DeviceClaim{DeviceID: "missing"})
	if missOne.ID == missTwo.ID {
		t.Fatal("claim miss replay reused its residual mint")
	}
	hit := mint(lagoon.WordDeviceClaim, lagoon.DeviceClaim{DeviceID: first.ID})
	if hit.ID != first.ID {
		t.Fatalf("owned claim hit returned %s, want %s", hit.ID, first.ID)
	}
}

func TestRetryableWritesReturnExistingValues(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	call := func(word lagoon.Word, payload any, out any) {
		t.Helper()
		reply, err := registrarCall(t, eng, root, protocol.C0ChannelID, word, payload)
		if err != nil {
			t.Fatal(err)
		}
		if reply.DecodeValue(out) != nil {
			t.Fatalf("invalid %s reply: %#v", word, reply.Value)
		}
	}

	var credentialOne, credentialTwo lagoon.CredentialReply
	set := lagoon.CredentialSet{PrincipalID: protocol.RootPrincipalID, SecretHash: "replacement-hash"}
	call(lagoon.WordCredentialSet, set, &credentialOne)
	time.Sleep(2 * time.Millisecond)
	call(lagoon.WordCredentialSet, set, &credentialTwo)
	if credentialOne != credentialTwo {
		t.Fatalf("credential retry changed row: %+v then %+v", credentialOne, credentialTwo)
	}

	declInput := lagoon.DeclRegister{ID: "retry-decl", Name: "retry", Class: "codex", Config: json.RawMessage(`{}`), Visibility: "private"}
	var registered regspec.DeclRow
	call(lagoon.WordDeclRegister, declInput, &registered)
	time.Sleep(2 * time.Millisecond)
	name := declInput.Name
	visibility := declInput.Visibility
	var edited regspec.DeclRow
	call(lagoon.WordDeclEdit, lagoon.DeclEdit{ID: declInput.ID, Name: &name, Config: json.RawMessage(`{ }`), Visibility: &visibility}, &edited)
	if edited.UpdatedAt != registered.UpdatedAt {
		t.Fatalf("equal declaration edit rewrote timestamp: %d then %d", registered.UpdatedAt, edited.UpdatedAt)
	}

	created := createChannelForTest(t, eng, root, "retry-overlay", protocol.C0ChannelID)
	member, err := eng.principalActor(context.Background(), created.ID, protocol.RootPrincipalID)
	if err != nil {
		t.Fatal(err)
	}
	input := lagoon.OverlaySet{DeclID: declInput.ID, ChannelID: created.ID, Config: json.RawMessage(`{}`)}
	var overlayOne, overlayTwo regspec.OverlayRow
	reply, err := registrarCall(t, eng, member, created.ID, lagoon.WordOverlaySet, input)
	if err != nil || reply.DecodeValue(&overlayOne) != nil {
		t.Fatalf("first overlay reply=%#v err=%v", reply.Value, err)
	}
	time.Sleep(2 * time.Millisecond)
	input.Config = json.RawMessage(`{ }`)
	reply, err = registrarCall(t, eng, member, created.ID, lagoon.WordOverlaySet, input)
	if err != nil || reply.DecodeValue(&overlayTwo) != nil {
		t.Fatalf("second overlay reply=%#v err=%v", reply.Value, err)
	}
	if overlayOne.UpdatedAt != overlayTwo.UpdatedAt {
		t.Fatalf("equal overlay retry rewrote timestamp: %d then %d", overlayOne.UpdatedAt, overlayTwo.UpdatedAt)
	}
}

func TestAllRegistrarWordsReachTheReceiverThroughBothActorEntrances(t *testing.T) {
	eng, _, root := bootRegistrarTest(t)
	ordinary := createChannelForTest(t, eng, root, "two-entrances", protocol.C0ChannelID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.waitChannel(ctx, ordinary.ID); err != nil {
		t.Fatal(err)
	}
	ordinaryRoot, err := eng.principalActor(ctx, ordinary.ID, protocol.RootPrincipalID)
	if err != nil {
		t.Fatal(err)
	}
	entrances := []struct {
		name   string
		sender actor.ActorID
		source channel.ID
	}{
		{name: "c0 direct member", sender: root, source: protocol.C0ChannelID},
		{name: "ordinary channel space-tool", sender: ordinaryRoot, source: ordinary.ID},
	}
	words := append(append([]lagoon.Word(nil), lagoon.WriteWords[:]...), lagoon.ReadWords[:]...)
	for _, entrance := range entrances {
		for _, word := range words {
			t.Run(entrance.name+"/"+string(word), func(t *testing.T) {
				if entrance.source != protocol.C0ChannelID && (word == lagoon.WordChannelList || word == lagoon.WordDeclList || word == lagoon.WordDeviceList) {
					// List values are arrays, while actor response payloads are
					// objects carrying status. Exercise these words through the
					// system call port's forwarded attribution verdict instead of
					// waiting for an unrepresentable source-leg success payload.
					bundle, ok := eng.host.Acquire(entrance.source)
					if !ok {
						t.Fatal("ordinary channel unavailable")
					}
					targets, err := bundle.View().DeclaredInstances(ctx, lagoon.SpaceToolDeclID)
					if err != nil || len(targets) != 1 {
						t.Fatalf("space-tool targets=%v err=%v", targets, err)
					}
					raw, err := bundle.Call().Call(ctx, targets[0], string(word), map[string]any{})
					if err == nil {
						_, err = decodeRegistrarTerminal(raw)
					}
					var lagoonErr *lagoon.Error
					if !errors.As(err, &lagoonErr) || lagoonErr.Code == lagoon.CodeResultUnknown || lagoonErr.Detail == "unknown registrar word" {
						t.Fatalf("word did not return a registrar verdict: %v", err)
					}
					return
				}
				_, err := registrarCall(t, eng, entrance.sender, entrance.source, word, map[string]any{})
				if err == nil {
					return
				}
				var lagoonErr *lagoon.Error
				if !errors.As(err, &lagoonErr) {
					t.Fatalf("word did not return a registrar verdict: %v", err)
				}
				if lagoonErr.Code == lagoon.CodeResultUnknown || lagoonErr.Detail == "unknown registrar word" {
					t.Fatalf("word did not reach its registrar handler: %v", err)
				}
			})
		}
		if _, err := registrarCall(t, eng, entrance.sender, entrance.source, lagoon.Word("future.word"), map[string]any{"future": true}); err == nil {
			t.Fatal("unknown word was accepted")
		} else {
			var lagoonErr *lagoon.Error
			if !errors.As(err, &lagoonErr) || lagoonErr.Code != lagoon.CodeInvalidArgs || lagoonErr.Detail != "unknown registrar word" {
				t.Fatalf("unknown word verdict=%v", err)
			}
		}
		if _, err := registrarCall(t, eng, entrance.sender, entrance.source, lagoon.WordPrincipalMe, map[string]any{"future_field": map[string]any{"kept": true}}); err != nil {
			t.Fatalf("unknown payload field was not passed to registrar: %v", err)
		}
	}
}

func TestProvisionLocalNodeReplaysThroughRegistrar(t *testing.T) {
	eng, _, _ := bootRegistrarTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := eng.ProvisionLocalNode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := eng.ProvisionLocalNode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.HomeChannelID == "" || first.HomeChannelID != second.HomeChannelID {
		t.Fatalf("home replay=%+v then %+v", first, second)
	}
	if first.DeviceID != protocol.LocalDeviceID || first.DeviceKey == "" || first.DeviceKey != second.DeviceKey {
		t.Fatalf("device replay=%+v then %+v", first, second)
	}
	c0, ok := eng.host.Acquire(protocol.C0ChannelID)
	if !ok {
		t.Fatal("c0 unavailable after provision")
	}
	stewardDecl := lagoon.StableBootstrapDeclID(protocol.RootPrincipalID, "steward")
	stewards, err := c0.View().DeclaredInstances(ctx, stewardDecl)
	if err != nil || len(stewards) != 1 {
		t.Fatalf("steward instances=%+v err=%v", stewards, err)
	}
	facts, found, err := c0.View().ActorFacts(ctx, stewards[0])
	if err != nil || !found || facts.Principal != protocol.StewardPrincipalID || facts.Kind != actor.KindAgent {
		t.Fatalf("steward facts=%+v found=%v err=%v", facts, found, err)
	}
	home, ok := eng.host.Acquire(first.HomeChannelID)
	if !ok {
		t.Fatal("root home unavailable after provision")
	}
	for label, declID := range map[string]string{
		"space-tool": lagoon.SpaceToolDeclID,
		"home-codex": lagoon.HomeCodexDeclID(protocol.RootPrincipalID),
	} {
		instances, err := home.View().DeclaredInstances(ctx, declID)
		if err != nil || len(instances) != 1 {
			t.Fatalf("%s instances=%+v err=%v", label, instances, err)
		}
	}
	if bound, err := eng.registry.IsBound(ctx, first.HomeChannelID, protocol.LocalDeviceID); err != nil || !bound {
		t.Fatalf("home local binding=%v err=%v", bound, err)
	}
}

func TestProvisionWaitsFailFastWithStepAndTerminalReasons(t *testing.T) {
	eng, _, _ := bootRegistrarTest(t)

	t.Run("channel timeout", func(t *testing.T) {
		eng.provisionTimeout = 50 * time.Millisecond
		started := time.Now()
		err := eng.waitChannel(context.Background(), "never-created")
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "wait for channel") {
			t.Fatalf("waitChannel error=%v", err)
		}
		if time.Since(started) > time.Second {
			t.Fatal("waitChannel did not fail fast")
		}
	})

	t.Run("principal timeout", func(t *testing.T) {
		eng.provisionTimeout = 50 * time.Millisecond
		err := func() error {
			_, err := eng.principalActor(context.Background(), protocol.C0ChannelID, "never-member")
			return err
		}()
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "wait for principal") {
			t.Fatalf("principalActor error=%v", err)
		}
	})

	t.Run("introduce rejection", func(t *testing.T) {
		eng.provisionTimeout = 2 * time.Second
		started := time.Now()
		err := eng.introduce(context.Background(), protocol.C0ChannelID, protocol.RootPrincipalID, "missing-declaration", "")
		if err == nil || !strings.Contains(err.Error(), "rejected") {
			t.Fatalf("introduce error=%v", err)
		}
		if time.Since(started) >= eng.provisionTimeout {
			t.Fatalf("explicit rejection waited for timeout: %v", err)
		}
	})
}

func TestStartupRepairsFixedRegistryRowsWithoutTouchingCredential(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: dir, Addr: "127.0.0.1:0", RootPassword: "first-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	c0Path, err := channelhost.DBPath(dir, protocol.C0ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	c0URL := &url.URL{Scheme: "file", Path: c0Path}
	c0db, err := sql.Open("sqlite", c0URL.String()+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	var physicalID, physicalType, physicalOwner string
	var physicalParent, physicalInitiator sql.NullString
	var physicalCreatedAt int64
	if err := c0db.QueryRow(`SELECT channel_id,type,owner_principal,parent_channel_id,initiator_principal,created_at FROM channel_genesis`).Scan(
		&physicalID, &physicalType, &physicalOwner, &physicalParent, &physicalInitiator, &physicalCreatedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := c0db.Close(); err != nil {
		t.Fatal(err)
	}
	physical := lagoon.GenesisSpec{
		ChannelID: channel.ID(physicalID), Type: physicalType, OwnerPrincipal: physicalOwner,
		ParentID: channel.ID(physicalParent.String), InitiatorPrincipal: physicalInitiator.String,
		CreatedAt: physicalCreatedAt,
	}
	path := filepath.Join(filepath.Dir(dir), "registry.db")
	u := &url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", u.String()+"?mode=rw&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	var before string
	if err := db.QueryRow(`SELECT secret_hash FROM credentials WHERE principal_id=?`, protocol.RootPrincipalID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DELETE FROM decls WHERE id='space-tool'`,
		`DELETE FROM channels WHERE id='c0'`,
		`DELETE FROM principals WHERE id='steward'`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()

	reopened, err := Boot(Config{ChannelDBDir: dir, Addr: "127.0.0.1:0", RootPassword: "second-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	row, ok, err := reopened.registry.GetChannelDesired(context.Background(), protocol.C0ChannelID)
	if err != nil || !ok || row.Status != regspec.ChannelPresent {
		t.Fatalf("c0 row=%+v ok=%v err=%v", row, ok, err)
	}
	var rebuilt lagoon.GenesisSpec
	if err := json.Unmarshal(row.Spec, &rebuilt); err != nil {
		t.Fatalf("rebuilt c0 spec is not JSON: %v (%s)", err, row.Spec)
	}
	if row.CreatedAt != physical.CreatedAt || !reflect.DeepEqual(rebuilt, physical) {
		t.Fatalf("rebuilt c0 genesis differs from physical ledger: row.created_at=%d spec=%+v physical=%+v", row.CreatedAt, rebuilt, physical)
	}
	if row, ok, err := reopened.registry.GetDecl(context.Background(), lagoon.SpaceToolDeclID); err != nil || !ok || row.Status != regspec.DeclPresent {
		t.Fatalf("space-tool row=%+v ok=%v err=%v", row, ok, err)
	}
	db, err = sql.Open("sqlite", u.String()+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var after string
	if err := db.QueryRow(`SELECT secret_hash FROM credentials WHERE principal_id=?`, protocol.RootPrincipalID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("startup reconciliation rewrote root credential")
	}
}
