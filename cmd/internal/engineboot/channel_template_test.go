package engineboot

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/wanpengxie/atoll/drivers/tools/echo"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func terminalValue(t *testing.T, raw json.RawMessage, out any) {
	t.Helper()
	var terminal struct {
		Status    string          `json:"status"`
		ErrorCode string          `json:"error_code"`
		Detail    string          `json:"detail"`
		Value     json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw, &terminal); err != nil {
		t.Fatal(err)
	}
	if terminal.Status != message.StatusCompleted {
		t.Fatalf("terminal failed code=%s detail=%s raw=%s", terminal.ErrorCode, terminal.Detail, raw)
	}
	if out != nil {
		if err := json.Unmarshal(terminal.Value, out); err != nil {
			t.Fatalf("decode value: %v raw=%s", err, raw)
		}
	}
}

func TestBootPublishesCoreAndLobbyOnly(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, core := eng.host.Acquire(protocol.C0ChannelID)
		_, lobby := eng.host.Acquire(protocol.LobbyChannelID)
		if core && lobby {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("core/lobby not serving")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range []channel.ID{protocol.C0ChannelID, protocol.LobbyChannelID} {
		row, ok, err := eng.registry.GetChannelDesired(context.Background(), id)
		if err != nil || !ok {
			t.Fatalf("channel %s row ok=%v err=%v", id, ok, err)
		}
		var spec lagoon.GenesisSpec
		if err := json.Unmarshal(row.Spec, &spec); err != nil || spec.Profile.Description == nil || *spec.Profile.Description != row.Description || spec.Profile.Serving == nil || *spec.Profile.Serving != row.Serving {
			t.Fatalf("channel %s frozen profile=%+v row=(%q,%d) err=%v", id, spec.Profile, row.Description, row.Serving, err)
		}
		if id == protocol.C0ChannelID && len(spec.Profile.Endpoints) != len(lagoon.WriteWords)+len(lagoon.ReadWords) {
			t.Fatalf("c0 frozen endpoints=%d", len(spec.Profile.Endpoints))
		}
	}
}

func callMember(t *testing.T, ch channel.ID, bundle channelhost.Bundle, principal string, target actor.ActorID, word string, payload any) json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sender, ok, err := bundle.View().ResolvePrincipal(ctx, principal)
	if err != nil || !ok {
		t.Fatalf("resolve %s: %v %v", principal, ok, err)
	}
	slot, ok := bundle.Gateway().SubjectSlotFor(sender)
	if !ok {
		t.Fatal("subject slot missing")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	id := message.ID(uuid.NewString())
	frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, uuid.NewString(), subjectgate.SubmitPayload{ChannelID: string(ch), ID: string(id), MsgType: word, Kind: string(message.KindRequest), Audience: []string{string(target)}, Visibility: string(message.VisibilityPublic), Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := slot.Deliver(ctx, frame); err != nil {
		t.Fatal(err)
	}
	reader := channel.Reader{ActorID: sender, Mode: channel.ReaderMember}
	var cursor int64
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		rows, next, err := bundle.View().ReadVisibleAfterSeq(ctx, reader, cursor, 256)
		if err != nil {
			t.Fatal(err)
		}
		cursor = next
		for _, row := range rows {
			if row.IsTerminal && row.Envelope.ParentID == id {
				return row.Envelope.Payload
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

func onlyDecl(t *testing.T, bundle channelhost.Bundle, id string) actor.ActorID {
	t.Helper()
	ids, err := bundle.View().DeclaredInstances(context.Background(), id)
	if err != nil || len(ids) != 1 {
		t.Fatalf("decl %s ids=%v err=%v", id, ids, err)
	}
	return ids[0]
}

func TestRootCreatesHomeFromCoreAndCallsBackThroughCoreactor(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	terminal := callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "root"})
	var outer struct {
		Status string          `json:"status"`
		Value  json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(terminal, &outer); err != nil || outer.Status != message.StatusCompleted {
		t.Fatalf("terminal raw=%s err=%v", terminal, err)
	}
	var created struct {
		ID         channel.ID     `json:"id"`
		Introduced map[string]any `json:"introduced"`
	}
	if err := json.Unmarshal(outer.Value, &created); err != nil || created.ID == "" {
		t.Fatalf("created=%+v raw=%s err=%v", created, terminal, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var home channelhost.Bundle
	for {
		if b, ok := eng.host.Acquire(created.ID); ok {
			home = b
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("home not serving")
		}
		time.Sleep(20 * time.Millisecond)
	}
	coreactor := onlyDecl(t, home, lagoon.CoreActorDeclID)
	listed := callMember(t, created.ID, home, protocol.RootPrincipalID, coreactor, string(lagoon.WordChannelList), map[string]any{})
	var reply struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(listed, &reply); err != nil || reply.Status != message.StatusCompleted {
		t.Fatalf("list raw=%s err=%v", listed, err)
	}
	if ids, err := core.View().DeclaredInstances(context.Background(), "peer:"+string(created.ID)); err != nil || len(ids) != 1 {
		t.Fatalf("core peer=%v err=%v", ids, err)
	}
}

func TestPortalRegistersThroughLobbyGuestCell(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := eng.host.Acquire(protocol.LobbyChannelID); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lobby unavailable")
		}
		time.Sleep(20 * time.Millisecond)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/identity/register", bytes.NewBufferString(`{"id":"alice","email":"alice@example.test","password":"secret"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	eng.handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var principal struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &principal); err != nil || principal.ID != "alice" {
		t.Fatalf("principal=%+v err=%v", principal, err)
	}
	rows, err := eng.registry.ListPresentChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var homeID string
	for _, row := range rows {
		if row.OwnerPrincipal == "alice" && row.ParentID == protocol.C0ChannelID {
			homeID = string(row.ID)
		}
	}
	if homeID == "" {
		t.Fatal("registered home missing")
	}
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	ids, err := core.View().DeclaredInstances(context.Background(), "peer:"+homeID)
	if err != nil || len(ids) != 1 {
		t.Fatalf("core peer ids=%v err=%v", ids, err)
	}
	lobby, _ := eng.host.Acquire(protocol.LobbyChannelID)
	guest, found, err := lobby.View().ResolvePrincipal(context.Background(), protocol.GuestPrincipalID)
	if err != nil || !found {
		t.Fatalf("guest=%q found=%v err=%v", guest, found, err)
	}
	guestRows, _, err := lobby.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{ActorID: guest, Mode: channel.ReaderMember}, 0, 512)
	if err != nil {
		t.Fatal(err)
	}
	var guestRequest, guestReply bool
	for _, row := range guestRows {
		if row.Envelope.Type != string(lagoon.WordPrincipalRegister) {
			continue
		}
		guestRequest = guestRequest || (row.Envelope.Kind == message.KindRequest && row.Envelope.Sender.ID == guest)
		guestReply = guestReply || row.IsTerminal
	}
	if !guestRequest || !guestReply {
		t.Fatalf("lobby registration ledger request=%v reply=%v", guestRequest, guestReply)
	}
	rootActor, found, err := core.View().ResolvePrincipal(context.Background(), protocol.RootPrincipalID)
	if err != nil || !found {
		t.Fatalf("root actor=%q found=%v err=%v", rootActor, found, err)
	}
	svc := onlyDecl(t, core, lagoon.SvcActorDeclID)
	coreRows, _, err := core.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{ActorID: rootActor, Mode: channel.ReaderMember}, 0, 1024)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := false
	for _, row := range coreRows {
		if row.Envelope.Kind != message.KindRequest || row.Envelope.Type != string(lagoon.WordPrincipalRegister) || row.Envelope.Sender.ID != svc {
			continue
		}
		var payload struct {
			Origin struct {
				Channel channel.ID    `json:"channel"`
				Actor   actor.ActorID `json:"actor"`
			} `json:"origin"`
			Args json.RawMessage `json:"args"`
		}
		if json.Unmarshal(row.Envelope.Payload, &payload) == nil && payload.Origin.Channel == protocol.LobbyChannelID && payload.Origin.Actor == guest && len(payload.Args) > 0 {
			wrapped = true
		}
	}
	if !wrapped {
		t.Fatal("c0 registrar ledger omitted svcactor {origin,args} registration request")
	}
	guestCore := onlyDecl(t, lobby, lagoon.CoreActorDeclID)
	guestList := decodeTerminal(t, callMember(t, protocol.LobbyChannelID, lobby, protocol.GuestPrincipalID, guestCore, string(lagoon.WordChannelList), map[string]any{}))
	if guestList.Status != message.StatusFailed || guestList.ErrorCode != string(lagoon.CodePermissionDenied) {
		t.Fatalf("guest channel.list=%+v", guestList)
	}
	aliceBundle := waitBundle(t, eng, channel.ID(homeID))
	aliceCore := onlyDecl(t, aliceBundle, lagoon.CoreActorDeclID)
	authRegister := decodeTerminal(t, callMember(t, channel.ID(homeID), aliceBundle, "alice", aliceCore, string(lagoon.WordPrincipalRegister), map[string]any{"id": "bob", "email": "bob@example.test", "secret_hash": "hash"}))
	if authRegister.Status != message.StatusFailed || authRegister.ErrorCode != string(lagoon.CodePermissionDenied) {
		t.Fatalf("authenticated register=%+v", authRegister)
	}
}

func TestChannelTemplateMergeForkAndProfileRoundTrip(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	call := func(word lagoon.Word, payload any, out any) {
		terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(word), payload), out)
	}
	for _, oldWord := range []string{"decl.register", "overlay.set"} {
		terminal := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, oldWord, map[string]any{}))
		if terminal.Status != message.StatusFailed || terminal.ErrorCode != string(lagoon.CodeInvalidArgs) {
			t.Fatalf("old word %s terminal=%+v", oldWord, terminal)
		}
	}
	withParent := decodeTerminal(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{"name": "bad-parent", "parent": protocol.C0ChannelID}))
	if withParent.Status != message.StatusFailed || withParent.ErrorCode != string(lagoon.CodeInvalidArgs) {
		t.Fatalf("create parent terminal=%+v", withParent)
	}
	for _, id := range []string{"echo-a", "echo-b", "echo-c"} {
		call(lagoon.WordDeclRegister, map[string]any{"id": id, "name": id, "class": "echo", "config": map[string]any{"max_seconds": 30}, "visibility": "public"}, nil)
	}
	body := map[string]any{
		"declarations": []any{
			map[string]any{"decl_id": "echo-a", "config": map[string]any{"max_seconds": 10}},
			map[string]any{"decl_id": "echo-b", "config": map[string]any{"max_seconds": 20}},
		},
		"profile": map[string]any{
			"description": "sealed template profile", "serving": 0,
			"endpoints": map[string]any{"echo.say": map[string]any{"description": "echo", "receiver": "echo-a"}},
		},
	}
	call(lagoon.WordChannelTemplateRegister, map[string]any{"id": "pair", "name": "Pair", "visibility": "public", "body": body}, nil)
	var listed []struct {
		ID string `json:"id"`
	}
	call(lagoon.WordChannelTemplateList, map[string]any{}, &listed)
	found := false
	for _, row := range listed {
		found = found || row.ID == "pair"
	}
	if !found {
		t.Fatalf("template list=%+v", listed)
	}
	var gotTemplate struct {
		Body json.RawMessage `json:"body"`
	}
	call(lagoon.WordChannelTemplateGet, map[string]any{"id": "pair"}, &gotTemplate)
	wantBody, _ := json.Marshal(body)
	if !jsonEqualTest(gotTemplate.Body, wantBody) {
		t.Fatalf("template body=%s want=%s", gotTemplate.Body, wantBody)
	}

	create := func(name string, payload map[string]any) lagoon.ChannelCreateReply {
		payload["name"] = name
		var created lagoon.ChannelCreateReply
		call(lagoon.WordChannelCreate, payload, &created)
		return created
	}
	one := create("templated-one", map[string]any{"template": "pair"})
	two := create("templated-two", map[string]any{"template": "pair"})
	if one.ID == two.ID || one.Description != "sealed template profile" || one.Serving != 0 || two.Serving != 0 {
		t.Fatalf("created one=%+v two=%+v", one, two)
	}
	var view regspec.ChannelRow
	call(lagoon.WordChannelGet, map[string]any{"channel_id": one.ID}, &view)
	if view.Profile == nil || view.Profile.Description == nil || *view.Profile.Description != "sealed template profile" || len(view.Profile.Endpoints) != 1 || len(view.Recipe.Declarations) != 2 {
		t.Fatalf("channel view=%+v", view)
	}
	var frozen lagoon.GenesisSpec
	if err := json.Unmarshal(view.Spec, &frozen); err != nil || len(frozen.Declarations) != 4 || frozen.Declarations[0].DeclID != lagoon.SvcActorDeclID || frozen.Declarations[1].DeclID != lagoon.CoreActorDeclID {
		t.Fatalf("frozen recipe=%+v err=%v", frozen, err)
	}
	fork := create("templated-fork", map[string]any{"overrides": view.Recipe})
	var forkView regspec.ChannelRow
	call(lagoon.WordChannelGet, map[string]any{"channel_id": fork.ID}, &forkView)
	if !jsonEqualTest(mustMarshal(t, view.Recipe), mustMarshal(t, forkView.Recipe)) {
		t.Fatalf("fork recipe=%s source=%s", mustMarshal(t, forkView.Recipe), mustMarshal(t, view.Recipe))
	}
	overridden := create("templated-override", map[string]any{"template": "pair", "overrides": map[string]any{"declarations": []any{
		map[string]any{"decl_id": "echo-a", "config": map[string]any{"max_seconds": 99}},
		map[string]any{"decl_id": "echo-c", "config": map[string]any{"max_seconds": 33}},
	}}})
	var overriddenView regspec.ChannelRow
	call(lagoon.WordChannelGet, map[string]any{"channel_id": overridden.ID}, &overriddenView)
	if len(overriddenView.Recipe.Declarations) != 3 || string(overriddenView.Recipe.Declarations[0].Config) != `{"max_seconds":99}` {
		t.Fatalf("overridden recipe=%+v", overriddenView.Recipe)
	}

	bad := callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordChannelTemplateRegister), map[string]any{
		"id": "bad-receiver", "name": "Bad", "visibility": "public",
		"body": map[string]any{"declarations": []any{}, "profile": map[string]any{"endpoints": map[string]any{"x": map[string]any{"receiver": "missing"}}}},
	})
	var failed struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(bad, &failed)
	if failed.Status != message.StatusFailed {
		t.Fatalf("bad receiver raw=%s", bad)
	}

	call(lagoon.WordDeclRegister, map[string]any{"id": "private-root", "name": "Private Root", "class": "echo", "config": map[string]any{}, "visibility": "private"}, nil)
	call(lagoon.WordChannelTemplateRegister, map[string]any{
		"id": "private-template", "name": "Private Decl Template", "visibility": "public",
		"body": map[string]any{"declarations": []any{map[string]any{"decl_id": "private-root"}}},
	}, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/identity/register", bytes.NewBufferString(`{"id":"template-alice","email":"template-alice@example.test","password":"secret"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	eng.handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("register alice status=%d body=%s", w.Code, w.Body.String())
	}
	channels, err := eng.registry.ListPresentChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var aliceHome channel.ID
	for _, row := range channels {
		if row.OwnerPrincipal == "template-alice" {
			aliceHome = row.ID
		}
	}
	aliceBundle := waitBundle(t, eng, aliceHome)
	aliceCore := onlyDecl(t, aliceBundle, lagoon.CoreActorDeclID)
	nonOwnerEdit := decodeTerminal(t, callMember(t, aliceHome, aliceBundle, "template-alice", aliceCore, string(lagoon.WordChannelTemplateEdit), map[string]any{"id": "pair", "name": "Stolen"}))
	if nonOwnerEdit.Status != message.StatusFailed || nonOwnerEdit.ErrorCode != string(lagoon.CodePermissionDenied) {
		t.Fatalf("non-owner edit=%+v", nonOwnerEdit)
	}
	privateCreate := decodeTerminal(t, callMember(t, aliceHome, aliceBundle, "template-alice", aliceCore, string(lagoon.WordChannelCreate), map[string]any{"name": "private-denied", "template": "private-template"}))
	if privateCreate.Status != message.StatusFailed || privateCreate.ErrorCode != string(lagoon.CodePermissionDenied) {
		t.Fatalf("private declaration create=%+v", privateCreate)
	}
}

func mustMarshal(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func jsonEqualTest(a, b json.RawMessage) bool {
	var av, bv any
	return json.Unmarshal(a, &av) == nil && json.Unmarshal(b, &bv) == nil && string(mustJSONTest(av)) == string(mustJSONTest(bv))
}

func mustJSONTest(v any) []byte { raw, _ := json.Marshal(v); return raw }
