package devicebus

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/store"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(context.Background(), ":memory:", store.OpenOptions{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestRegisterActorTokenRoundTrip(t *testing.T) {
	ctx := context.Background()
	svc := NewService(testDB(t), Config{TokenSecret: "secret"})
	svc.WithClock(func() time.Time { return time.UnixMilli(1_000) })

	res, err := svc.RegisterActor(ctx, RegisterInput{
		ActorID:    "tool:xhs-adapter",
		ChannelID:  "ch-1",
		UserID:     "user-1",
		DaemonID:   "daemon-1",
		DeviceID:   "xhs-chrome-default",
		DeviceType: "xhs.chrome_extension",
	})
	if err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	if res.Token == "" || res.TokenFingerprint == "" {
		t.Fatalf("missing token material: %+v", res)
	}
	got, err := svc.ValidateToken(ctx, "tool:xhs-adapter", res.Token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if got.ChannelID != "ch-1" || got.ActorID != "tool:xhs-adapter" {
		t.Fatalf("registration mismatch: %+v", got)
	}
}

func TestRegisterActorReplacesRouteToken(t *testing.T) {
	ctx := context.Background()
	svc := NewService(testDB(t), Config{TokenSecret: "secret"})
	in := RegisterInput{
		ActorID:    "tool:xhs-adapter",
		ChannelID:  "ch-1",
		UserID:     "user-1",
		DaemonID:   "daemon-1",
		DeviceID:   "xhs-chrome-default",
		DeviceType: "xhs.chrome_extension",
	}
	first, err := svc.RegisterActor(ctx, in)
	if err != nil {
		t.Fatalf("first register: %v", err)
	}
	second, err := svc.RegisterActor(ctx, in)
	if err != nil {
		t.Fatalf("second register: %v", err)
	}
	if _, err := svc.ValidateToken(ctx, in.ActorID, first.Token); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("old token err=%v want ErrTokenInvalid", err)
	}
	if _, err := svc.ValidateToken(ctx, in.ActorID, second.Token); err != nil {
		t.Fatalf("new token invalid: %v", err)
	}
}

type noopDeviceTransport struct{}

func (noopDeviceTransport) ReadFrame(context.Context) (DeviceFrame, error) {
	return DeviceFrame{}, errors.New("unused")
}

func (noopDeviceTransport) WriteFrame(context.Context, DeviceFrame) error { return nil }
func (noopDeviceTransport) Close() error                                  { return nil }

func TestUnregisterConnectionCompareAndDelete(t *testing.T) {
	svc := NewService(nil, Config{})
	reg := ActorRegistration{
		ActorID:   actor.ActorID("tool:xhs-adapter"),
		ChannelID: channel.ID("ch-1"),
		DaemonID:  placement.DaemonID("daemon-1"),
	}
	oldConn := NewConnection(reg, noopDeviceTransport{})
	newConn := NewConnection(reg, noopDeviceTransport{})

	svc.registerConnection(reg.ChannelID, reg.ActorID, oldConn)
	svc.registerConnection(reg.ChannelID, reg.ActorID, newConn)

	key := routeKey(reg.ChannelID, reg.ActorID)
	if svc.unregisterConnection(reg.ChannelID, reg.ActorID, oldConn) {
		t.Fatal("old connection unregister removed current connection")
	}
	if got := svc.routes[key]; got != newConn {
		t.Fatalf("current connection = %p want %p", got, newConn)
	}
	if !svc.unregisterConnection(reg.ChannelID, reg.ActorID, newConn) {
		t.Fatal("current connection unregister did not remove entry")
	}
	if got := svc.routes[key]; got != nil {
		t.Fatalf("route still registered: %p", got)
	}
}
