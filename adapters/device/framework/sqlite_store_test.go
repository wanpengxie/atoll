package framework_test

import (
	"context"
	"path/filepath"
	"testing"

	deviceframework "github.com/wanpengxie/ActOS/adapters/device/framework"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
)

func TestSQLiteSessionStorePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "device_sessions.sqlite")
	store1, close1, err := deviceframework.OpenSQLiteSessionStore(ctx, path)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	row := deviceframework.DeviceSession{
		SessionID:        "sess-1",
		ChannelID:        "ch-1",
		AdapterActorID:   "tool:xhs-adapter",
		DeviceID:         "dev-1",
		DeviceType:       "xhs",
		State:            deviceframework.StateReady,
		BoundAt:          100,
		TokenFingerprint: "abc",
		ExpiresAt:        200,
	}
	if err := store1.Upsert(ctx, row); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := close1(); err != nil {
		t.Fatalf("close1: %v", err)
	}

	store2, close2, err := deviceframework.OpenSQLiteSessionStore(ctx, path)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer func() { _ = close2() }()
	got, ok, err := store2.Get(ctx, devicetransit.DeviceSessionID("sess-1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("session missing after reopen")
	}
	if got.ChannelID != row.ChannelID || got.State != row.State || got.TokenFingerprint != row.TokenFingerprint {
		t.Fatalf("got=%+v want persisted row %+v", got, row)
	}
}
