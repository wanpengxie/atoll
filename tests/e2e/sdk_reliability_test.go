//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/adapters/cmd"
	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/adapters/kimibridge"
	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/pkg/coagentsdk"
	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

func TestE2E_SDK_Reliability_KimibridgeAvailableExtensionDisconnected(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, false)
	s := harness.Start(t, harness.Options{
		UseScaffoldXHS: true,
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannel(t, s, "kb-disconnected")

	kimiActor := waitSDKActor(t, client, chID, string(kimibridge.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return !a.Ready && a.ReadyReason == "extension_disconnected"
	})
	if len(kimiActor.Types) == 0 {
		t.Fatalf("kimibridge actor has no types: %+v", kimiActor)
	}

	status, err := client.ActorStatus(context.Background(), chID, string(kimibridge.DefaultAdapterActorID))
	if err != nil {
		t.Fatalf("ActorStatus: %v", err)
	}
	if status.Available || status.Reason != "extension_disconnected" {
		t.Fatalf("ActorStatus=%+v", status)
	}

	start := time.Now()
	res, err := client.CallActor(context.Background(), coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   string(kimibridge.DefaultAdapterActorID),
		Type:      kimibridge.TypeNavigate,
		Payload:   json.RawMessage(`{"url":"https://example.invalid"}`),
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if res.OK || res.Error == nil || res.Error.Code != "extension_disconnected" {
		t.Fatalf("CallActor result=%+v", res)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("pre-check took %s; expected fail-fast, not SDK timeout", elapsed)
	}
	if !strings.Contains(res.Error.RecoveryHint, "extension") {
		t.Fatalf("recovery_hint=%q", res.Error.RecoveryHint)
	}
}

func TestE2E_SDK_Reliability_XHSDeviceOffline(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannel(t, s, "xhs-offline")

	waitSDKActor(t, client, chID, string(xhs.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return !a.Ready && a.ReadyReason == "device_offline"
	})

	start := time.Now()
	res, err := client.CallActor(context.Background(), coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   string(xhs.DefaultAdapterActorID),
		Type:      xhs.TypePublish,
		Payload:   json.RawMessage(`{"title":"offline","content":"device absent"}`),
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if res.OK || res.Error == nil || res.Error.Code != "device_offline" {
		t.Fatalf("CallActor result=%+v", res)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("pre-check took %s; expected fail-fast, not SDK timeout", elapsed)
	}
}

func TestE2E_SDK_Reliability_EmbeddedScaffoldReady(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		UseScaffoldXHS: true,
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannel(t, s, "scaffold-ready")

	waitSDKActor(t, client, chID, string(xhs.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return a.Ready && a.ReadyReason == "ok"
	})

	res, err := client.CallActor(context.Background(), coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   string(xhs.DefaultAdapterActorID),
		Type:      xhs.TypePublish,
		Payload:   json.RawMessage(`{"title":"sdk hello","content":"sdk world"}`),
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if !res.OK {
		t.Fatalf("CallActor OK=false: %+v", res.Error)
	}
}

func TestE2E_SDK_Reliability_ReadinessEventEmitted(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		DeviceAllowedOrigins: []string{harness.MockExtensionOriginID},
		ExtraDaemonEnv:       []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannel(t, s, "readiness-event")

	placement := harness.EventuallyValue(t, "placement reaches active", 5*time.Second, func() (harness.PlacementRow, bool) {
		p, ok := s.GetPlacement(chID)
		return p, ok && p.State == "active"
	})
	deviceID := "device-" + uniqSuffix()
	issued := s.RegisterDeviceActor(chID, deviceID, placement.DaemonID)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = harness.NewMockExtension(t, ctx, harness.MockExtensionConfig{
		WSURL:     s.DevicebusWSURL(issued.ActorID, issued.Token),
		ActorID:   issued.ActorID,
		Token:     issued.Token,
		ChannelID: chID,
		DeviceID:  deviceID,
	})

	waitSDKActor(t, client, chID, string(xhs.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return a.Ready && a.ReadyReason == "ok"
	})
	harness.Eventually(t, "readiness and xhs lifecycle events", 10*time.Second, func() bool {
		var readinessReady, xhsOnline bool
		for _, m := range s.ListChannelMessages(chID) {
			switch m.Type {
			case "actor.readiness.changed":
				var payload struct {
					ActorID string `json:"actor_id"`
					Current struct {
						Ready  bool   `json:"ready"`
						Reason string `json:"reason"`
					} `json:"current"`
				}
				if json.Unmarshal(m.Payload, &payload) == nil &&
					payload.ActorID == string(xhs.DefaultAdapterActorID) &&
					payload.Current.Ready &&
					payload.Current.Reason == "ok" {
					readinessReady = true
				}
			case "xhs.device.online":
				xhsOnline = true
			}
		}
		return readinessReady && xhsOnline
	})
}

// TestE2E_SDK_Reliability_ToolSurface_Scaffold covers the happy-path SDK
// invocation across the **scaffold** xhs adapter (5 R/R types) +
// kimibridge (13 R/R types). The scaffold mock-acks every request so this
// test verifies SDK ↔ server ↔ daemon ↔ adapter plumbing for every type
// without needing a real device. Production xhs surface (14 R/R types in
// adapters/device/xhs) is exercised by
// TestE2E_SDK_Reliability_ToolSurface_DeviceXHS_OfflineFastFail below
// — that one cannot mock-ack (no fake chrome-extension WS), so it
// verifies the dispatch pre-check fast-fail path instead.
func TestE2E_SDK_Reliability_ToolSurface_Scaffold(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		UseScaffoldXHS: true,
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannel(t, s, "full-surface")

	waitSDKActor(t, client, chID, string(xhs.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return a.Ready
	})
	waitSDKActor(t, client, chID, string(kimibridge.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return a.Ready
	})

	for _, typ := range xhs.RequestResponseTypes {
		res, err := client.CallActor(context.Background(), coagentsdk.CallActorRequest{
			ChannelID: chID,
			ActorID:   string(xhs.DefaultAdapterActorID),
			Type:      typ,
			Payload:   json.RawMessage(`{}`),
			Timeout:   5 * time.Second,
		})
		if err != nil {
			t.Fatalf("CallActor xhs %s: %v", typ, err)
		}
		if !res.OK {
			t.Fatalf("CallActor xhs %s failed: %+v", typ, res.Error)
		}
	}
	for _, typ := range kimibridge.RequestResponseTypes {
		res, err := client.CallActor(context.Background(), coagentsdk.CallActorRequest{
			ChannelID: chID,
			ActorID:   string(kimibridge.DefaultAdapterActorID),
			Type:      typ,
			Payload:   json.RawMessage(`{}`),
			Timeout:   5 * time.Second,
		})
		if err != nil {
			t.Fatalf("CallActor kimibridge %s: %v", typ, err)
		}
		if !res.OK {
			t.Fatalf("CallActor kimibridge %s failed: %+v", typ, res.Error)
		}
	}
}

// TestE2E_SDK_Reliability_ToolSurface_DeviceXHS_OfflineFastFail exercises
// every type in the **production** device/xhs adapter surface (14 R/R types)
// when no device is bound. Each call MUST pre-check fail fast with
// reason=receiver_unavailable, error_code=device_offline within seconds
// (R4 dispatch pre-check + R5 30s budget), proving the SDK + framework
// don't hang on the 5min legacy timer for any of the production types.
func TestE2E_SDK_Reliability_ToolSurface_DeviceXHS_OfflineFastFail(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		// UseScaffoldXHS:false → daemon loads adapters/device/xhs (14 R/R).
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannel(t, s, "devxhs-offline")

	// Wait for production xhs to register as not_ready (no device bound).
	waitSDKActor(t, client, chID, string(devicexhs.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return !a.Ready
	})

	for _, typ := range devicexhs.RequestResponseTypes {
		start := time.Now()
		res, err := client.CallActor(context.Background(), coagentsdk.CallActorRequest{
			ChannelID: chID,
			ActorID:   string(devicexhs.DefaultAdapterActorID),
			Type:      typ,
			Payload:   json.RawMessage(`{}`),
			// 8s budget catches anything that falls back to F3 default
			// timer (which was 5min before R5) — fast-fail MUST resolve
			// well under this.
			Timeout: 8 * time.Second,
		})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("CallActor device/xhs %s: %v", typ, err)
		}
		if res.OK {
			t.Fatalf("CallActor device/xhs %s unexpectedly ok=true (no device bound)", typ)
		}
		if res.Error == nil {
			t.Fatalf("CallActor device/xhs %s: ok=false but no Error field", typ)
		}
		// Pre-check fast-fail must reply in seconds, not minutes.
		if elapsed > 5*time.Second {
			t.Errorf("CallActor device/xhs %s took %v — fast-fail invariant says <5s", typ, elapsed)
		}
		// Reason MUST be in the protocol closed-set; error_code SHOULD
		// indicate device offline / token issue.
		if res.Error.Code == "" {
			t.Errorf("CallActor device/xhs %s missing error_code", typ)
		}
	}
}

func setupReliabilityChannel(t *testing.T, s *harness.Stack, name string) (string, string, *coagentsdk.Client) {
	return setupReliabilityChannelWithType(t, s, name, "xhs-creator")
}

func setupReliabilityChannelWithType(t *testing.T, s *harness.Stack, name, channelType string) (string, string, *coagentsdk.Client) {
	t.Helper()
	email := "reliability-" + name + "+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-" + name + "-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-"+name+"-"+uniqSuffix(), channelType)
	s.BindChannel(wsID, chID)
	return wsID, chID, &coagentsdk.Client{
		BaseURL:      s.ServerURLBase(),
		SessionToken: s.SessionToken(),
	}
}

func waitSDKActor(
	t *testing.T,
	client *coagentsdk.Client,
	channelID string,
	actorID string,
	timeout time.Duration,
	predicate func(coagentsdk.ActorInfo) bool,
) coagentsdk.ActorInfo {
	t.Helper()
	return harness.EventuallyValue(t, "SDK actor "+actorID, timeout, func() (coagentsdk.ActorInfo, bool) {
		actors, err := client.ListActors(context.Background(), channelID)
		if err != nil {
			return coagentsdk.ActorInfo{}, false
		}
		for _, a := range actors {
			if a.ActorID == actorID && predicate(a) {
				return a, true
			}
		}
		return coagentsdk.ActorInfo{}, false
	})
}

func newFakeKimiBridgeDaemon(t *testing.T, extensionConnected bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"running":             true,
			"version":             "e2e-fake",
			"port":                0,
			"uptime_seconds":      7,
			"extension_connected": extensionConnected,
			"extension_id":        "e2e-extension",
			"extension_version":   "test",
		})
	})
	mux.HandleFunc("/command", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Action string `json:"action"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"data": map[string]any{
				"action": req.Action,
				"ok":     true,
			},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// ============================================================
// cmd cli-adapter — actor-cli-pattern §19.3 living e2e
// ============================================================
//
// Drives a real CLI binary (echo / false / sleep / a non-allowed one)
// through the SDK 4-verb chain on a `cmd-sandbox` channel. Verifies the
// new adapter (added 2026-05-26) honours the full A-H exposed surface
// + R1-R6 reliability invariants from actor-adapter.md.

// TestE2E_SDK_CmdAdapter_FullChain walks the canonical agent cognitive
// chain (actor-adapter.md §5) as far as the current SDK surface allows:
//
//   Step 1 Discover    → list_actors finds tool:cmd + its types
//   Step 2 Pre-flight  → readiness=ready (embedded baseline)
//   Step 3 Plan        → cmd.which to pre-flight the binary
//   Step 4 Execute     → call_actor echo "hello"
//   Step 5 Interpret   → success, decode {stdout, exit_code, ...}
//
// describe_actor / describe_type endpoints are not yet exposed via SDK
// (worker bridge only — see actor-adapter.md §16 SDK gap); LLM-side
// agents get them through the kimi meta-tool path.
func TestE2E_SDK_CmdAdapter_FullChain(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		UseScaffoldXHS: true,
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannelWithType(t, s, "cmd-fullchain", "cmd-sandbox")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Step 1: Discover — universe contains tool:cmd with its 2 types.
	// cmd-sandbox channel binds ONLY tool:cmd; xhs / kimibridge MUST NOT
	// appear here (per CmdSandboxChannelType gate in CmdFactory).
	universe := waitSDKActor(t, client, chID, string(cmd.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return a.Ready && len(a.Types) >= len(cmd.AllTypes)
	})
	// AllTypes (cmd.exec, cmd.which) MUST be present. The framework
	// auto-registers `adapter.<name>.orphan_callback` for F7 observability,
	// which appears here too — tolerate it without flagging unexpected.
	want := map[string]bool{cmd.TypeExec: false, cmd.TypeWhich: false}
	for _, ty := range universe.Types {
		if _, ok := want[ty.Type]; ok {
			want[ty.Type] = true
		}
	}
	for ty, seen := range want {
		if !seen {
			t.Errorf("cmd actor missing type %s in list_actors (got %+v)", ty, universe.Types)
		}
	}
	allActors, err := client.ListActors(ctx, chID)
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	for _, a := range allActors {
		if a.ActorID == string(devicexhs.DefaultAdapterActorID) || a.ActorID == string(kimibridge.DefaultAdapterActorID) {
			t.Errorf("cmd-sandbox channel leaked %s actor", a.ActorID)
		}
	}

	// Step 2: Pre-flight via cmd.which — `echo` exists + allowed.
	whichRes, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   string(cmd.DefaultAdapterActorID),
		Type:      cmd.TypeWhich,
		Payload:   json.RawMessage(`{"binary":"echo"}`),
		Timeout:   5 * time.Second,
	})
	if err != nil || !whichRes.OK {
		t.Fatalf("which echo failed: err=%v error=%+v", err, whichRes.Error)
	}
	var which cmd.WhichResponse
	_ = json.Unmarshal(whichRes.Data, &which)
	if !which.Allowed || which.Path == "" {
		t.Fatalf("which echo allowed=%v path=%q", which.Allowed, which.Path)
	}

	// Step 3+4: Execute — real echo.
	res, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   string(cmd.DefaultAdapterActorID),
		Type:      cmd.TypeExec,
		Payload:   json.RawMessage(`{"binary":"echo","args":["hello","world"]}`),
		Timeout:   10 * time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor cmd.exec echo: %v", err)
	}
	if !res.OK {
		t.Fatalf("CallActor cmd.exec echo failed: %+v", res.Error)
	}
	var execResp cmd.ExecResponse
	if err := json.Unmarshal(res.Data, &execResp); err != nil {
		t.Fatalf("decode ExecResponse: %v raw=%s", err, string(res.Data))
	}
	if execResp.Stdout != "hello world\n" {
		t.Fatalf("stdout=%q want \"hello world\\n\"", execResp.Stdout)
	}
	if execResp.ExitCode != 0 {
		t.Fatalf("exit_code=%d want 0", execResp.ExitCode)
	}
}

// TestE2E_SDK_CmdAdapter_BinaryNotAllowed verifies the allowlist
// fail-closed semantics surface to the SDK as a failed terminal with
// the documented error_code + recovery hint.
func TestE2E_SDK_CmdAdapter_BinaryNotAllowed(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		UseScaffoldXHS: true,
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannelWithType(t, s, "cmd-deny", "cmd-sandbox")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   string(cmd.DefaultAdapterActorID),
		Type:      cmd.TypeExec,
		Payload:   json.RawMessage(`{"binary":"rm","args":["-rf","/"]}`),
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if res.OK {
		t.Fatalf("rm allowed through allowlist; this is a security regression")
	}
	if res.Error == nil || res.Error.Code != "binary_not_allowed" {
		t.Fatalf("error code=%q want binary_not_allowed", func() string {
			if res.Error == nil {
				return "<nil>"
			}
			return res.Error.Code
		}())
	}
}

// TestE2E_SDK_CmdAdapter_NonZeroExit verifies that a non-zero exit
// code is delivered as a SUCCESS terminal (status=completed) — the
// binary ran; the caller decides what to do with exit_code.
func TestE2E_SDK_CmdAdapter_NonZeroExit(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		UseScaffoldXHS: true,
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannelWithType(t, s, "cmd-false", "cmd-sandbox")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   string(cmd.DefaultAdapterActorID),
		Type:      cmd.TypeExec,
		Payload:   json.RawMessage(`{"binary":"false"}`),
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if !res.OK {
		t.Fatalf("false exited non-zero but terminal=failed (binary did run): %+v", res.Error)
	}
	var execResp cmd.ExecResponse
	_ = json.Unmarshal(res.Data, &execResp)
	if execResp.ExitCode == 0 {
		t.Fatalf("exit_code=0; expected non-zero from `false`")
	}
}

// TestE2E_SDK_CmdAdapter_Describe verifies the actor.describe reserved
// type path: SDK calls DescribeActor + DescribeType, framework intercepts
// at dispatch, daemon answers from Module.Declares() — server is dumb
// pipe per INVARIANT-0 (no server-side metadata cache).
func TestE2E_SDK_CmdAdapter_Describe(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		UseScaffoldXHS: true,
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannelWithType(t, s, "cmd-describe", "cmd-sandbox")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Wait for cmd actor to be installed + ready.
	waitSDKActor(t, client, chID, string(cmd.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return a.Ready
	})

	// DescribeActor — must return Description + SkillDoc + Types map.
	actor, err := client.DescribeActor(ctx, chID, string(cmd.DefaultAdapterActorID))
	if err != nil {
		t.Fatalf("DescribeActor: %v", err)
	}
	if actor.Description == "" {
		t.Fatalf("DescribeActor returned empty Description")
	}
	if len(actor.SkillDoc) < 100 {
		t.Errorf("DescribeActor SkillDoc too short: %d chars", len(actor.SkillDoc))
	}
	if _, ok := actor.Types[cmd.TypeExec]; !ok {
		t.Fatalf("DescribeActor types missing %s; got %v", cmd.TypeExec, mapKeys(actor.Types))
	}

	// DescribeType cmd.exec — full payload schema hints + error_codes.
	exec, err := client.DescribeType(ctx, chID, string(cmd.DefaultAdapterActorID), cmd.TypeExec)
	if err != nil {
		t.Fatalf("DescribeType cmd.exec: %v", err)
	}
	if exec.Description == "" {
		t.Fatalf("DescribeType cmd.exec Description empty")
	}
	if len(exec.PayloadExample) == 0 {
		t.Fatalf("DescribeType cmd.exec PayloadExample empty")
	}
	if len(exec.PayloadFields) == 0 {
		t.Fatalf("DescribeType cmd.exec PayloadFields empty")
	}
	if len(exec.ErrorCodes) == 0 {
		t.Fatalf("DescribeType cmd.exec ErrorCodes empty")
	}
	// Every error_code MUST carry a recovery hint (actor-adapter.md F.6).
	for _, ec := range exec.ErrorCodes {
		if ec.Recovery == "" {
			t.Errorf("error_code %q missing recovery hint", ec.Code)
		}
	}
	if exec.HandlerBinding != "embedded" {
		t.Errorf("HandlerBinding=%q want embedded", exec.HandlerBinding)
	}
	hasRequest := false
	for _, k := range exec.AllowedKinds {
		if k == "request" {
			hasRequest = true
		}
	}
	if !hasRequest {
		t.Errorf("AllowedKinds=%v missing request", exec.AllowedKinds)
	}

	// DescribeType on unknown type — fail-shaped envelope, error_code=unknown_type.
	_, err = client.DescribeType(ctx, chID, string(cmd.DefaultAdapterActorID), "cmd.totally_not_a_real_type")
	if err == nil {
		t.Fatalf("DescribeType unknown type: want error")
	}
	if !strings.Contains(err.Error(), "unknown_type") {
		t.Errorf("err=%v should mention unknown_type", err)
	}
}

func mapKeys(m map[string]coagentsdk.TypeConventionDoc) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestE2E_SDK_CmdAdapter_Which exercises the cheap pre-flight verb
// that the skill_doc tells agents to run before cmd.exec.
func TestE2E_SDK_CmdAdapter_Which(t *testing.T) {
	kimi := newFakeKimiBridgeDaemon(t, true)
	s := harness.Start(t, harness.Options{
		UseScaffoldXHS: true,
		ExtraDaemonEnv: []string{"COAGENT_KIMIBRIDGE_BASE_URL=" + kimi.URL},
	})
	_, chID, client := setupReliabilityChannelWithType(t, s, "cmd-which", "cmd-sandbox")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Allowed + present.
	res, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   string(cmd.DefaultAdapterActorID),
		Type:      cmd.TypeWhich,
		Payload:   json.RawMessage(`{"binary":"echo"}`),
		Timeout:   3 * time.Second,
	})
	if err != nil || !res.OK {
		t.Fatalf("which echo: err=%v err_obj=%+v", err, res.Error)
	}
	var w cmd.WhichResponse
	_ = json.Unmarshal(res.Data, &w)
	if !w.Allowed || w.Path == "" {
		t.Fatalf("which echo allowed=%v path=%q want true, non-empty", w.Allowed, w.Path)
	}

	// Disallowed binary — which still reports it cleanly with allowed=false.
	res2, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   string(cmd.DefaultAdapterActorID),
		Type:      cmd.TypeWhich,
		Payload:   json.RawMessage(`{"binary":"rm"}`),
		Timeout:   3 * time.Second,
	})
	if err != nil || !res2.OK {
		t.Fatalf("which rm: err=%v err_obj=%+v", err, res2.Error)
	}
	var w2 cmd.WhichResponse
	_ = json.Unmarshal(res2.Data, &w2)
	if w2.Allowed {
		t.Fatalf("rm allowed=true (allowlist regression)")
	}
}
