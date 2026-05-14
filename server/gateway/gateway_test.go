package gateway_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/daemonbus"
	"github.com/wanpengxie/ActOS/server/gateway"
	"github.com/wanpengxie/ActOS/server/identity"
	"github.com/wanpengxie/ActOS/server/pushhub"
	"github.com/wanpengxie/ActOS/server/store"
)

// newTestApp boots a real App with an in-tempdir sqlite + fast
// reconcile so tests don't have to wait minutes.
func newTestApp(t *testing.T) *gateway.App {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "g.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	app, err := gateway.New(ctx, gateway.Config{
		DB:                        db,
		SessionSecret:             "test-session",
		DaemonSharedSecret:        "test-daemon",
		DeviceTokenSecret:         "test-device",
		HumanCallerSecret:         "test-human",
		ReconcileGracePeriod:      50 * time.Millisecond,
		ReconcileCreateTimeout:    100 * time.Millisecond,
		ReconcileHeartbeatTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("gateway.New: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	return app
}

// TestEndToEndRegisterLoginCreateChannel exercises the full
// identity → catalog flow over real HTTP.
func TestEndToEndRegisterLoginCreateChannel(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	client := &http.Client{Jar: nil}
	ctx := context.Background()

	// IssueCode → capture the code from notify hook by reading the
	// row through the service directly (HTTP only returns "issued").
	post(t, client, srv.URL+"/api/identity/verification/issue", `{"email":"alice@example.com"}`, http.StatusAccepted)

	// Read the code straight from the database (test-only short-cut).
	var code string
	if err := app.DB().QueryRowContext(ctx,
		`SELECT code FROM verification_codes WHERE email = ? ORDER BY created_at DESC LIMIT 1`,
		"alice@example.com",
	).Scan(&code); err != nil {
		t.Fatalf("read code: %v", err)
	}

	// Register.
	post(t, client, srv.URL+"/api/identity/register",
		`{"email":"alice@example.com","password":"topsecret123","code":"`+code+`"}`,
		http.StatusCreated)

	// Login — collect cookie for downstream calls.
	loginResp := postRaw(t, client, srv.URL+"/api/identity/login",
		`{"email":"alice@example.com","password":"topsecret123"}`,
		http.StatusOK)
	defer loginResp.Body.Close()
	var loginBody struct {
		User struct{ ID string } `json:"user"`
	}
	if err := json.NewDecoder(loginResp.Body).Decode(&loginBody); err != nil {
		t.Fatalf("decode login: %v", err)
	}
	cookie := extractSessionCookie(t, loginResp)
	if cookie == "" {
		t.Fatal("session cookie missing")
	}

	// /me — authenticated.
	meReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/identity/me", nil)
	meReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: cookie})
	meResp, err := client.Do(meReq)
	if err != nil || meResp.StatusCode != 200 {
		t.Fatalf("/me err=%v status=%d", err, meResp.StatusCode)
	}
	meResp.Body.Close()

	// Create workspace.
	wsReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/workspaces",
		strings.NewReader(`{"name":"Demo"}`))
	wsReq.Header.Set("Content-Type", "application/json")
	wsReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: cookie})
	wsResp, err := client.Do(wsReq)
	if err != nil || wsResp.StatusCode != http.StatusCreated {
		t.Fatalf("workspace err=%v status=%d", err, wsResp.StatusCode)
	}
	var ws struct{ ID string }
	_ = json.NewDecoder(wsResp.Body).Decode(&ws)
	wsResp.Body.Close()

	// Create channel.
	chReq, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/api/workspaces/"+ws.ID+"/channels",
		strings.NewReader(`{"name":"general"}`))
	chReq.Header.Set("Content-Type", "application/json")
	chReq.AddCookie(&http.Cookie{Name: identity.CookieName, Value: cookie})
	chResp, err := client.Do(chReq)
	if err != nil || chResp.StatusCode != http.StatusCreated {
		t.Fatalf("channel err=%v status=%d", err, chResp.StatusCode)
	}
	var chBody struct {
		Channel struct{ ID string } `json:"channel"`
	}
	_ = json.NewDecoder(chResp.Body).Decode(&chBody)
	chResp.Body.Close()

	if chBody.Channel.ID == "" {
		t.Fatal("channel id empty")
	}
}

// TestMockDaemonViewSyncRoundTrip drives a fake daemonbus connection
// through the App's dispatch loop:
//
//  1. fake daemon pushes viewsync.push seq=1 → server.viewcache.Apply
//     → fan-out (no subscriber yet, but Hub call must succeed)
//  2. ack frame must arrive with last_received_seq=1
//  3. push seq=3 — gap — ack still 1
//  4. push seq=2 — drains; ack 3
//
// This is the §T6 mock validation: viewcache + daemonbus end-to-end.
func TestMockDaemonViewSyncRoundTrip(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Register a daemon row so IssueConnectionEpoch works.
	if err := app.Daemonbus().RegisterDaemon(ctx, placement.DaemonID("mock-d1"), "localhost", "v0", 32, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, err := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("mock-d1"))
	if err != nil {
		t.Fatalf("IssueConnectionEpoch: %v", err)
	}

	// Build in-memory pipe and wire a Connection.
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("mock-d1"), epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("mock-d1"))

	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	send := func(ft kerneldaemonbus.FrameType, payload any) {
		raw, _ := json.Marshal(payload)
		if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID: "f-" + ft.String(), FrameType: ft,
			DaemonID: "mock-d1", DaemonConnectionEpoch: epoch, Payload: raw,
		}); err != nil {
			t.Fatalf("write %s: %v", ft, err)
		}
	}

	mkPush := func(seq viewsync.Seq) viewsync.PushFrame {
		id := "m-" + itoa(int64(seq))
		return viewsync.PushFrame{
			ChannelID: channel.ID("ch-A"), Seq: seq,
			MessageID: id,
			Envelope: message.Envelope{
				ID: id, TS: int64(seq) * 1000, ChannelID: "ch-A",
				Sender: message.Sender{Kind: message.SenderAgent, ID: "a"},
				Kind:   message.KindEvent, Type: "agent.text",
				Payload:    json.RawMessage(`{}`),
				Visibility: message.VisibilityPublic, Audience: []string{"*"},
			},
		}
	}

	expect := func(want viewsync.Seq) {
		t.Helper()
		f, err := dmn.ReadFrame(ctx)
		if err != nil {
			t.Fatalf("read ack: %v", err)
		}
		if f.FrameType != kerneldaemonbus.FrameTypeViewsyncAck {
			t.Fatalf("expected viewsync.ack, got %s", f.FrameType)
		}
		var ack viewsync.AckFrame
		if err := json.Unmarshal(f.Payload, &ack); err != nil {
			t.Fatalf("decode ack: %v", err)
		}
		if int64(ack.LastReceivedSeq) != int64(want) {
			t.Fatalf("ack=%d want %d", ack.LastReceivedSeq, want)
		}
	}

	send(kerneldaemonbus.FrameTypeViewsyncPush, mkPush(1))
	expect(1)

	send(kerneldaemonbus.FrameTypeViewsyncPush, mkPush(3))
	expect(1) // gap; cursor stays at 1

	send(kerneldaemonbus.FrameTypeViewsyncPush, mkPush(2))
	expect(3) // drain 3 from buffer; cursor jumps to 3

	cur, _ := app.Viewcache().Cursor(ctx, channel.ID("ch-A"))
	if int64(cur) != 3 {
		t.Errorf("final cursor=%d want 3", cur)
	}

	conn.Close()
}

// TestMockDaemonCreateChannelACK exercises the placement path:
//
//  1. App.placements.Reserve → row state=creating
//  2. fake daemon sends control.create_channel_ack with matching
//     fields → OnCreateChannelAck → CASActivate
//  3. row state advances to active
func TestMockDaemonCreateChannelACK(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := app.Daemonbus().RegisterDaemon(ctx, placement.DaemonID("d1"), "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("d1"))

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d1"), epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("d1"))

	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	_, req, err := app.Placements().Reserve(ctx, channel.ID("ch-Y"), placement.DaemonID("d1"), placement.ConnectionEpoch(epoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	ack := placement.CreateChannelAck{
		FrameID: "ack-1", ChannelID: req.ChannelID, CreateRequestID: req.CreateRequestID,
		OwnerEpoch: req.OwnerEpoch, FencingToken: req.FencingToken,
		DaemonID: placement.DaemonID("d1"), DaemonEpoch: 1, Status: placement.AckBound,
	}
	raw, _ := json.Marshal(ack)
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID: "f-ack", FrameType: kerneldaemonbus.FrameTypeControlCreateChannelAck,
		DaemonID: "d1", DaemonConnectionEpoch: epoch, Payload: raw,
	}); err != nil {
		t.Fatalf("write ack: %v", err)
	}

	// Poll for state=active.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		p, ok, _ := app.Placements().Get(ctx, req.ChannelID)
		if ok && p.State == placement.StateActive {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("placement never advanced to active")
}

// TestViewSyncGapDrainFanOut covers FIX-T5: when the daemon pushes a
// gap-then-fill sequence (1, 3, 2), the gateway must:
//
//  1. fan-out seq 1 to subscribers (contiguous)
//  2. NOT fan-out seq 3 alone (gap — client must not see it before 2)
//  3. fan-out seq 2 AND seq 3, in that order, when 2 closes the gap
//
// Ack frames still carry only the contiguous cursor (1, 1, 3).
func TestViewSyncGapDrainFanOut(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Suppress the auto-trigger goroutine — no daemon-side resync
	// server is wired in this test; we feed the recovery frame
	// manually via dmn.WriteFrame.
	app.Viewcache().SetFireResyncForTest(func(channel.ID, viewsync.Seq, viewsync.Seq) {})

	// Capture fan-out from pushhub.
	var (
		obsMu sync.Mutex
		obs   []pushhub.PushedFrame
	)
	cancelObs := app.Pushhub().RegisterPushObserverForTest(channel.ID("ch-fanout"), func(f pushhub.PushedFrame) {
		obsMu.Lock()
		obs = append(obs, f)
		obsMu.Unlock()
	})
	defer cancelObs()

	if err := app.Daemonbus().RegisterDaemon(ctx, placement.DaemonID("d-fanout"), "h", "v", 0, "test-daemon"); err != nil {
		t.Fatalf("RegisterDaemon: %v", err)
	}
	epoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("d-fanout"))

	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("d-fanout"), epoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("d-fanout"))
	go func() { _ = conn.Run(ctx, app.DaemonbusHandlers()) }()

	mkPush := func(seq viewsync.Seq) viewsync.PushFrame {
		id := "m-" + itoa(int64(seq))
		return viewsync.PushFrame{
			ChannelID: channel.ID("ch-fanout"), Seq: seq, MessageID: id,
			Envelope: message.Envelope{
				ID: id, TS: int64(seq) * 1000, ChannelID: "ch-fanout",
				Sender: message.Sender{Kind: message.SenderAgent, ID: "a"},
				Kind:   message.KindEvent, Type: "agent.text",
				Payload:    json.RawMessage(`{}`),
				Visibility: message.VisibilityPublic, Audience: []string{"*"},
			},
		}
	}
	send := func(payload viewsync.PushFrame) {
		raw, _ := json.Marshal(payload)
		if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
			FrameID: "f-" + itoa(int64(payload.Seq)),
			FrameType: kerneldaemonbus.FrameTypeViewsyncPush,
			DaemonID: "d-fanout", DaemonConnectionEpoch: epoch, Payload: raw,
		}); err != nil {
			t.Fatalf("write push %d: %v", payload.Seq, err)
		}
		// Drain matching ack so the gateway dispatch loop is unblocked
		// before the next push goes in.
		if _, err := dmn.ReadFrame(ctx); err != nil {
			t.Fatalf("read ack %d: %v", payload.Seq, err)
		}
	}

	// 1. push 1 — contiguous → fan-out [1]
	send(mkPush(1))
	// 2. push 3 — gap → fan-out unchanged
	send(mkPush(3))
	// 3. push 2 — drains buffer → fan-out [2, 3]
	send(mkPush(2))

	// Allow pushhub goroutines (synchronous in-process) to settle.
	deadline := time.Now().Add(1 * time.Second)
	for {
		obsMu.Lock()
		n := len(obs)
		obsMu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	obsMu.Lock()
	defer obsMu.Unlock()
	if len(obs) != 3 {
		t.Fatalf("fan-out len=%d want 3 (seqs=%v)", len(obs), seqList(obs))
	}
	want := []viewsync.Seq{1, 2, 3}
	for i, w := range want {
		if obs[i].Seq != w {
			t.Errorf("fan-out[%d].seq=%d want %d (full=%v)", i, obs[i].Seq, w, seqList(obs))
		}
	}
	conn.Close()
}

// TestReclaimDaemonIDMismatch is the FIX-T4 regression: a daemonbus
// connection authenticated as "d1" must not be able to reclaim
// placements by setting `ReclaimRequest.DaemonID` to some other
// daemon id. The dispatch hook must reject the frame (the Run loop
// surfaces the error and terminates the connection).
func TestReclaimDaemonIDMismatch(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Register two daemons; "owner" actually holds the placement,
	// "attacker" is the misbehaving connection that tries to reclaim.
	for _, did := range []placement.DaemonID{"owner", "attacker"} {
		if err := app.Daemonbus().RegisterDaemon(ctx, did, "h", "v", 0, "test-daemon"); err != nil {
			t.Fatalf("RegisterDaemon %s: %v", did, err)
		}
	}
	ownerEpoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("owner"))
	attackerEpoch, _ := app.Daemonbus().IssueConnectionEpoch(ctx, placement.DaemonID("attacker"))

	// Seed an active placement owned by "owner".
	p, _, err := app.Placements().Reserve(ctx, channel.ID("ch-victim"),
		placement.DaemonID("owner"), placement.ConnectionEpoch(ownerEpoch), nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: p.OwnerEpoch, FencingToken: p.FencingToken,
		DaemonID: placement.DaemonID("owner"), Status: placement.AckBound,
	}
	if ok, err := app.Placements().Activate(ctx, ack, placement.ConnectionEpoch(ownerEpoch)); err != nil || !ok {
		t.Fatalf("Activate: ok=%v err=%v", ok, err)
	}

	// Wire a daemonbus connection authenticated as "attacker".
	svr, dmn := newPipePair()
	conn := daemonbus.NewConnection(placement.DaemonID("attacker"), attackerEpoch, svr)
	app.Daemonbus().Register(conn)
	defer app.Daemonbus().Unregister(placement.DaemonID("attacker"))

	runDone := make(chan error, 1)
	go func() { runDone <- conn.Run(ctx, app.DaemonbusHandlers()) }()

	// Attacker pushes a reclaim claiming to be "owner". OnReclaim
	// MUST reject; conn.Run returns the validation error.
	raw, _ := json.Marshal(placement.ReclaimRequest{
		DaemonID:    placement.DaemonID("owner"),
		DaemonEpoch: 1,
		Channels: []placement.ReclaimChannel{{
			ChannelID:    p.ChannelID,
			FencingToken: p.FencingToken,
			OwnerEpoch:   p.OwnerEpoch,
		}},
	})
	if err := dmn.WriteFrame(ctx, kerneldaemonbus.Frame{
		FrameID:               "f-attack",
		FrameType:             kerneldaemonbus.FrameTypeControlDaemonReclaim,
		DaemonID:              "attacker",
		DaemonConnectionEpoch: attackerEpoch,
		Payload:               raw,
	}); err != nil {
		t.Fatalf("write reclaim: %v", err)
	}

	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("conn.Run returned nil; expected daemon_id mismatch error")
		}
		if !strings.Contains(err.Error(), "does not match authenticated conn") {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("conn.Run did not return within 1s — DaemonID mismatch was not enforced")
	}

	// Placement row must still belong to "owner" — the attacker
	// cannot hijack ownership even though the (epoch, token) tuple
	// they presented was valid.
	got, _, err := app.Placements().Get(ctx, p.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DaemonID != "owner" {
		t.Errorf("post-attack daemon_id=%q want owner", got.DaemonID)
	}

	conn.Close()
}

// ----------------------------------------------------------------------
// Test helpers
// ----------------------------------------------------------------------

func post(t *testing.T, c *http.Client, url, body string, wantStatus int) {
	t.Helper()
	resp, err := c.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s status=%d want %d", url, resp.StatusCode, wantStatus)
	}
}

func postRaw(t *testing.T, c *http.Client, url, body string, wantStatus int) *http.Response {
	t.Helper()
	resp, err := c.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	if resp.StatusCode != wantStatus {
		resp.Body.Close()
		t.Fatalf("POST %s status=%d want %d", url, resp.StatusCode, wantStatus)
	}
	return resp
}

func extractSessionCookie(t *testing.T, resp *http.Response) string {
	t.Helper()
	for _, c := range resp.Cookies() {
		if c.Name == identity.CookieName {
			return c.Value
		}
	}
	return ""
}

// pipeTransport satisfies daemonbus.Transport in-memory.
type pipeTransport struct {
	in, out chan kerneldaemonbus.Frame
	done    chan struct{}
}

func newPipePair() (server, daemon *pipeTransport) {
	a := make(chan kerneldaemonbus.Frame, 16)
	b := make(chan kerneldaemonbus.Frame, 16)
	done := make(chan struct{})
	server = &pipeTransport{in: a, out: b, done: done}
	daemon = &pipeTransport{in: b, out: a, done: done}
	return
}

func (p *pipeTransport) ReadFrame(ctx context.Context) (kerneldaemonbus.Frame, error) {
	select {
	case f := <-p.in:
		return f, nil
	case <-p.done:
		return kerneldaemonbus.Frame{}, context.Canceled
	case <-ctx.Done():
		return kerneldaemonbus.Frame{}, ctx.Err()
	}
}
func (p *pipeTransport) WriteFrame(ctx context.Context, f kerneldaemonbus.Frame) error {
	select {
	case p.out <- f:
		return nil
	case <-p.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *pipeTransport) Close() error { return nil }

// seqList extracts the seq field of every captured PushedFrame; used
// for readable failure messages in fan-out ordering tests.
func seqList(fs []pushhub.PushedFrame) []viewsync.Seq {
	out := make([]viewsync.Seq, len(fs))
	for i, f := range fs {
		out[i] = f.Seq
	}
	return out
}

// itoa is a tiny strconv-free int64 → string used in test message ids
// (kept inline so the test file stays self-contained).
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
