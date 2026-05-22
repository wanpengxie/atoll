package main

import (
	"context"
	"testing"

	deviceframework "github.com/wanpengxie/ActOS/adapters/device/framework"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// TestDeviceSessionBinder_BindUpsertsRow covers T147 phase-4b — OnBind
// must Upsert a framework.DeviceSession row into the shared store with
// the wire payload mapped 1:1, including TokenFingerprint and the
// daemon-side State=ready jump (server-side row transitions
// pending→ready via MarkBound on the ack arrival).
func TestDeviceSessionBinder_BindUpsertsRow(t *testing.T) {
	t.Parallel()
	store := deviceframework.NewInMemorySessionStore()
	binder := NewDeviceSessionBinder(store)

	body := transit.BindDeviceSessionBody{
		FrameID:          "f-1",
		BindRequestID:    "bind-req-1",
		DeviceSessionID:  devicetransit.DeviceSessionID("sess-A"),
		ChannelID:        channel.ID("ch-X"),
		DeviceID:         "dev-1",
		DeviceType:       "xhs.chrome_extension",
		DaemonID:         "daemon-A",
		TokenFingerprint: "abc123def4567890",
		ExpiresAt:        50_000,
		BoundAt:          10_000,
	}
	ack := binder.OnBind(context.Background(), body)

	if ack.Result != daemonbus.DeviceSessionBindAccepted {
		t.Fatalf("ack.Result=%q: %+v", ack.Result, ack)
	}
	if ack.FrameID != body.FrameID {
		t.Errorf("ack.FrameID=%q want %q", ack.FrameID, body.FrameID)
	}
	if ack.DeviceSessionID != body.DeviceSessionID {
		t.Errorf("ack.DeviceSessionID=%q want %q", ack.DeviceSessionID, body.DeviceSessionID)
	}

	row, ok, err := store.Get(context.Background(), body.DeviceSessionID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if !ok {
		t.Fatal("row missing after OnBind")
	}
	if row.State != deviceframework.StateReady {
		t.Errorf("row.State=%q want ready", row.State)
	}
	if row.TokenFingerprint != body.TokenFingerprint {
		t.Errorf("row.TokenFingerprint=%q want %q", row.TokenFingerprint, body.TokenFingerprint)
	}
	if row.ChannelID != body.ChannelID {
		t.Errorf("row.ChannelID=%q want %q", row.ChannelID, body.ChannelID)
	}
	if row.ExpiresAt != body.ExpiresAt {
		t.Errorf("row.ExpiresAt=%d want %d", row.ExpiresAt, body.ExpiresAt)
	}
}

// TestDeviceSessionBinder_BindIdempotent verifies that re-running OnBind
// with the same SessionID overwrites the row without error (T1.10 replay
// safety — server may resend bind on transient daemon reconnect).
func TestDeviceSessionBinder_BindIdempotent(t *testing.T) {
	t.Parallel()
	store := deviceframework.NewInMemorySessionStore()
	binder := NewDeviceSessionBinder(store)

	body := transit.BindDeviceSessionBody{
		FrameID:         "f-1",
		BindRequestID:   "bind-req-1",
		DeviceSessionID: devicetransit.DeviceSessionID("sess-A"),
		ChannelID:       channel.ID("ch-X"),
		DeviceID:        "dev-1",
		DeviceType:      "xhs.chrome_extension",
	}
	if ack := binder.OnBind(context.Background(), body); ack.Result != daemonbus.DeviceSessionBindAccepted {
		t.Fatalf("first OnBind: %+v", ack)
	}
	// Re-bind with a different fingerprint — must succeed and overwrite.
	body.TokenFingerprint = "rotated00fingerpr"
	if ack := binder.OnBind(context.Background(), body); ack.Result != daemonbus.DeviceSessionBindAccepted {
		t.Fatalf("second OnBind: %+v", ack)
	}
	row, _, _ := store.Get(context.Background(), body.DeviceSessionID)
	if row.TokenFingerprint != "rotated00fingerpr" {
		t.Errorf("row.TokenFingerprint not refreshed: %q", row.TokenFingerprint)
	}
	if got := store.Len(); got != 1 {
		t.Errorf("store.Len=%d want 1 (idempotent overwrite)", got)
	}
}

func TestDeviceSessionBinder_BindRejectReasons(t *testing.T) {
	t.Parallel()
	binder := NewDeviceSessionBinder(deviceframework.NewInMemorySessionStore())
	base := transit.BindDeviceSessionBody{
		FrameID:         "f-1",
		BindRequestID:   "bind-req-1",
		DeviceSessionID: "sess-A",
		ChannelID:       "ch-X",
		DeviceID:        "dev-1",
		DeviceType:      "xhs.chrome_extension",
	}
	cases := []struct {
		name   string
		mutate func(*transit.BindDeviceSessionBody)
		reason daemonbus.DeviceSessionRejectReason
	}{
		{
			name:   "missing channel",
			mutate: func(b *transit.BindDeviceSessionBody) { b.ChannelID = "" },
			reason: daemonbus.DeviceSessionRejectBindChannelNotActive,
		},
		{
			name:   "unsupported device type",
			mutate: func(b *transit.BindDeviceSessionBody) { b.DeviceType = "feishu_mobile" },
			reason: daemonbus.DeviceSessionRejectBindDeviceTypeUnsupported,
		},
		{
			name:   "adapter not present",
			mutate: func(b *transit.BindDeviceSessionBody) { b.AdapterActorID = "tool:other" },
			reason: daemonbus.DeviceSessionRejectBindAdapterNotPresent,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			body := base
			tc.mutate(&body)
			ack := binder.OnBind(context.Background(), body)
			if ack.Result == daemonbus.DeviceSessionBindAccepted {
				t.Fatalf("ack accepted; want reject %+v", ack)
			}
			if ack.Reason != tc.reason {
				t.Fatalf("reason=%q want %q", ack.Reason, tc.reason)
			}
		})
	}
}

// TestDeviceSessionBinder_UnbindDeletesRow verifies the happy unbind
// path drops the mirror row.
func TestDeviceSessionBinder_UnbindDeletesRow(t *testing.T) {
	t.Parallel()
	store := deviceframework.NewInMemorySessionStore()
	binder := NewDeviceSessionBinder(store)

	bind := transit.BindDeviceSessionBody{
		FrameID:         "f-1",
		BindRequestID:   "bind-req-1",
		DeviceSessionID: devicetransit.DeviceSessionID("sess-A"),
		ChannelID:       channel.ID("ch-X"),
		DeviceID:        "dev-1",
		DeviceType:      "xhs.chrome_extension",
	}
	if ack := binder.OnBind(context.Background(), bind); ack.Result != daemonbus.DeviceSessionBindAccepted {
		t.Fatalf("OnBind: %+v", ack)
	}

	unbind := transit.UnbindDeviceSessionBody{
		FrameID:         "f-2",
		DeviceSessionID: bind.DeviceSessionID,
		ChannelID:       bind.ChannelID,
		Reason:          "revoked",
	}
	ack := binder.OnUnbind(context.Background(), unbind)
	if ack.Result != daemonbus.DeviceSessionBindAccepted {
		t.Fatalf("OnUnbind: %+v", ack)
	}
	if ack.DeviceSessionID != bind.DeviceSessionID {
		t.Errorf("ack.DeviceSessionID=%q want %q", ack.DeviceSessionID, bind.DeviceSessionID)
	}

	if _, ok, _ := store.Get(context.Background(), bind.DeviceSessionID); ok {
		t.Error("row still present after OnUnbind")
	}
}

// TestDeviceSessionBinder_UnbindMissingIsAccepted verifies idempotent
// delete: when the row never existed (or was already purged), unbind
// MUST still succeed so retries are safe.
func TestDeviceSessionBinder_UnbindMissingIsAccepted(t *testing.T) {
	t.Parallel()
	store := deviceframework.NewInMemorySessionStore()
	binder := NewDeviceSessionBinder(store)

	ack := binder.OnUnbind(context.Background(), transit.UnbindDeviceSessionBody{
		FrameID:         "f-1",
		DeviceSessionID: devicetransit.DeviceSessionID("never-existed"),
		ChannelID:       "ch-X",
	})
	if ack.Result != daemonbus.DeviceSessionBindAccepted {
		t.Errorf("OnUnbind missing row: ack=%+v want accepted", ack)
	}
}
