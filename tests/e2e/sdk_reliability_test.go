//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/pkg/coagentsdk"
	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

func TestE2E_SDK_Reliability_DaemonDoesNotDirectHostKimibridge(t *testing.T) {
	s := harness.Start(t, harness.Options{UseScaffoldXHS: true})
	_, chID, client := setupReliabilityChannel(t, s, "kb-disconnected")

	harness.Eventually(t, "xhs actor registered without direct kimibridge", 10*time.Second, func() bool {
		actors, err := client.ListActors(context.Background(), chID)
		if err != nil {
			return false
		}
		var sawXHS bool
		for _, a := range actors {
			if a.ActorID == "tool:kimi-webbridge" {
				t.Fatalf("daemon directly hosted deprecated kimibridge actor: %+v", a)
			}
			if a.ActorID == string(xhs.DefaultAdapterActorID) {
				sawXHS = true
			}
		}
		return sawXHS
	})
}

func TestE2E_SDK_Reliability_XHSDeviceOffline(t *testing.T) {
	s := harness.Start(t, harness.Options{})
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
	s := harness.Start(t, harness.Options{UseScaffoldXHS: true})
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

// TestE2E_SDK_Reliability_ToolSurface_Scaffold covers the happy-path SDK
// invocation across the **scaffold** xhs adapter (5 R/R types). The
// scaffold mock-acks every request so this test verifies SDK ↔ server ↔
// daemon ↔ adapter plumbing without needing a real device. Production xhs
// surface (14 R/R types in adapters/device/xhs) is exercised by
// TestE2E_SDK_Reliability_ToolSurface_DeviceXHS_OfflineFastFail below
// — that one cannot mock-ack (no fake chrome-extension WS), so it
// verifies the dispatch pre-check fast-fail path instead.
func TestE2E_SDK_Reliability_ToolSurface_Scaffold(t *testing.T) {
	s := harness.Start(t, harness.Options{UseScaffoldXHS: true})
	_, chID, client := setupReliabilityChannel(t, s, "full-surface")

	waitSDKActor(t, client, chID, string(xhs.DefaultAdapterActorID), 10*time.Second, func(a coagentsdk.ActorInfo) bool {
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
}

// TestE2E_SDK_Reliability_ToolSurface_DeviceXHS_OfflineFastFail exercises
// every type in the **production** device/xhs adapter surface (14 R/R types)
// when no device is bound. Each call MUST pre-check fail fast with
// reason=receiver_unavailable, error_code=device_offline within seconds
// (R4 dispatch pre-check + R5 30s budget), proving the SDK + framework
// don't hang on the 5min legacy timer for any of the production types.
func TestE2E_SDK_Reliability_ToolSurface_DeviceXHS_OfflineFastFail(t *testing.T) {
	// UseScaffoldXHS:false → daemon loads adapters/device/xhs (14 R/R).
	s := harness.Start(t, harness.Options{})
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
	t.Helper()
	email := "reliability-" + name + "+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-" + name + "-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-"+name+"-"+uniqSuffix(), "xhs-creator")
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
