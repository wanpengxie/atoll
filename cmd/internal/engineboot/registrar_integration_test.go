package engineboot

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/google/uuid"
	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func bootRegistrarTest(t *testing.T) (*Engine, *sql.DB, actor.ActorID) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: dir, Addr: "127.0.0.1:0", RootPassword: "root-test-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close(context.Background()) })
	path, err := channelhost.DBPath(dir, protocol.C0ChannelID)
	if err != nil {
		t.Fatal(err)
	}
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
	return eng.submitter.Submit(ctx, lagoon.SubmitIn{Source: source, Sender: sender, RequestID: uuid.NewString(), Word: word, Payload: payload})
}

func createChannelForTest(t *testing.T, eng *Engine, root actor.ActorID, name string, parent channel.ID) lagoon.ChannelRow {
	t.Helper()
	reply, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordChannelCreate, lagoon.ChannelCreate{Name: name, Parent: parent})
	if err != nil {
		t.Fatal(err)
	}
	var row lagoon.ChannelRow
	if !decodeValue(reply.Value, &row) {
		t.Fatalf("invalid channel reply: %#v", reply.Value)
	}
	return row
}

func TestPrincipalRegisterRollsBackAllFourRows(t *testing.T) {
	eng, db, _ := bootRegistrarTest(t)
	if _, err := db.Exec(`DELETE FROM devices WHERE id=?`, protocol.LocalDeviceID); err != nil {
		t.Fatal(err)
	}
	_, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, lagoon.PrincipalRegister{
		ID: "alice", Email: "alice@example.test", SecretHash: "already-hashed", DisplayName: "Alice",
	})
	if err == nil {
		t.Fatal("register succeeded without the default-binding device")
	}
	for label, query := range map[string]string{
		"principal":  `SELECT count(*) FROM principals WHERE id='alice'`,
		"credential": `SELECT count(*) FROM credentials WHERE principal_id='alice'`,
		"home":       `SELECT count(*) FROM channels WHERE owner_principal='alice'`,
		"binding":    `SELECT count(*) FROM bindings WHERE channel_id IN (SELECT id FROM channels WHERE owner_principal='alice')`,
	} {
		var count int
		if scanErr := db.QueryRow(query).Scan(&count); scanErr != nil || count != 0 {
			t.Fatalf("%s survived rollback: count=%d err=%v", label, count, scanErr)
		}
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
	if err != nil || !ok || retired.Status != lagoon.ChannelRetired {
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.waitChannel(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	homeRoot, err := eng.principalActor(ctx, created.ID, protocol.RootPrincipalID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registrarCall(t, eng, homeRoot, created.ID, lagoon.WordDeviceDetach, lagoon.DeviceBinding{ChannelID: created.ID, DeviceID: protocol.LocalDeviceID}); err != nil {
		t.Fatal(err)
	}
	if bound, err := eng.registry.IsBound(context.Background(), created.ID, protocol.LocalDeviceID); err != nil || bound {
		t.Fatalf("binding after detach=%v err=%v", bound, err)
	}
	replayed := createChannelForTest(t, eng, root, "detached", protocol.C0ChannelID)
	if replayed.ID != created.ID {
		t.Fatalf("replay returned %s, want %s", replayed.ID, created.ID)
	}
	if bound, err := eng.registry.IsBound(context.Background(), created.ID, protocol.LocalDeviceID); err != nil || bound {
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
	var second lagoon.DeviceRow
	if !decodeValue(mintedReply.Value, &second) {
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
	if _, err := registrarCall(t, eng, root, protocol.C0ChannelID, lagoon.WordDeviceRetire, lagoon.DeviceRetire{DeviceID: protocol.LocalDeviceID}); err != nil {
		t.Fatal(err)
	}
	bound, err := eng.registry.ListBoundDevices(context.Background(), created.ID)
	if err != nil || len(bound) != 1 || bound[0].ID != second.ID {
		t.Fatalf("effective bindings=%+v err=%v", bound, err)
	}
	if yes, err := eng.registry.IsBound(context.Background(), created.ID, protocol.LocalDeviceID); err != nil || yes {
		t.Fatalf("retired device remained effectively bound: %v err=%v", yes, err)
	}
}

func TestRegisterConflictDoesNotMintSessionShapedSuccess(t *testing.T) {
	eng, _, _ := bootRegistrarTest(t)
	register := lagoon.PrincipalRegister{ID: "alice", Email: "same@example.test", SecretHash: "hash"}
	if _, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, register); err != nil {
		t.Fatal(err)
	}
	_, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, register)
	var lagoonErr *lagoon.Error
	if !errors.As(err, &lagoonErr) || lagoonErr.Code != lagoon.CodeConflictExists {
		t.Fatalf("duplicate register error=%v", err)
	}
}

func TestRegisterReturnsOnlyAfterHomeIsServing(t *testing.T) {
	eng, _, _ := bootRegistrarTest(t)
	reply, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, lagoon.PrincipalRegister{ID: "alice", Email: "alice@example.test", SecretHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	var principal lagoon.PrincipalRow
	if !decodeValue(reply.Value, &principal) || principal.ID != "alice" {
		t.Fatalf("invalid register reply: %#v", reply.Value)
	}
	rows, err := eng.registry.ListPresentChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.OwnerPrincipal == principal.ID && row.ParentID == protocol.C0ChannelID && row.Name == principal.ID {
			if _, ok := eng.host.Acquire(row.ID); !ok {
				t.Fatal("register returned before its home channel was serving")
			}
			return
		}
	}
	t.Fatal("registered principal has no home channel")
}

func registerHumanForTest(t *testing.T, eng *Engine, id string) lagoon.PrincipalRow {
	t.Helper()
	reply, err := eng.submitter.SubmitApplication(context.Background(), lagoon.WordPrincipalRegister, lagoon.PrincipalRegister{
		ID: id, Email: id + "@example.test", SecretHash: "hash",
	})
	if err != nil {
		t.Fatal(err)
	}
	var principal lagoon.PrincipalRow
	if !decodeValue(reply.Value, &principal) {
		t.Fatalf("invalid register reply: %#v", reply.Value)
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
	if !decodeValue(reply.Value, &credential) || credential.PrincipalID != protocol.RootPrincipalID || credential.Status != lagoon.CredentialActive {
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
	mint := func(word lagoon.Word, payload any) lagoon.DeviceRow {
		t.Helper()
		reply, err := registrarCall(t, eng, root, protocol.C0ChannelID, word, payload)
		if err != nil {
			t.Fatal(err)
		}
		var row lagoon.DeviceRow
		if !decodeValue(reply.Value, &row) {
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
		if !decodeValue(reply.Value, out) {
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
	var registered lagoon.DeclRow
	call(lagoon.WordDeclRegister, declInput, &registered)
	time.Sleep(2 * time.Millisecond)
	name := declInput.Name
	visibility := declInput.Visibility
	var edited lagoon.DeclRow
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
	var overlayOne, overlayTwo lagoon.OverlayRow
	reply, err := registrarCall(t, eng, member, created.ID, lagoon.WordOverlaySet, input)
	if err != nil || !decodeValue(reply.Value, &overlayOne) {
		t.Fatalf("first overlay reply=%#v err=%v", reply.Value, err)
	}
	time.Sleep(2 * time.Millisecond)
	input.Config = json.RawMessage(`{ }`)
	reply, err = registrarCall(t, eng, member, created.ID, lagoon.WordOverlaySet, input)
	if err != nil || !decodeValue(reply.Value, &overlayTwo) {
		t.Fatalf("second overlay reply=%#v err=%v", reply.Value, err)
	}
	if overlayOne.UpdatedAt != overlayTwo.UpdatedAt {
		t.Fatalf("equal overlay retry rewrote timestamp: %d then %d", overlayOne.UpdatedAt, overlayTwo.UpdatedAt)
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

func TestStartupRepairsFixedRegistryRowsWithoutTouchingCredential(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: dir, Addr: "127.0.0.1:0", RootPassword: "first-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	path, _ := channelhost.DBPath(dir, protocol.C0ChannelID)
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
	if row, ok, err := reopened.registry.GetChannelDesired(context.Background(), protocol.C0ChannelID); err != nil || !ok || row.Status != lagoon.ChannelPresent {
		t.Fatalf("c0 row=%+v ok=%v err=%v", row, ok, err)
	}
	if row, ok, err := reopened.registry.GetDecl(context.Background(), lagoon.SpaceToolDeclID); err != nil || !ok || row.Status != lagoon.DeclPresent {
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
