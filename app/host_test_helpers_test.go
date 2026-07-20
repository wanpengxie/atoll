package app

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func newBareAppForTest(t *testing.T) *App {
	t.Helper()
	root := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(root, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	a := &App{db: db, logger: slog.New(slog.DiscardHandler), daemonLocks: newKeyedLockSet(), channelLocks: newKeyedLockSet()}
	host, err := channelhost.New(filepath.Join(root, "channels"), channelhost.HomeDeps{CompositionResolver: compositionResolver{app: a}, IntroductionResolver: compositionResolver{app: a}, Logger: a.logger})
	if err != nil {
		t.Fatal(err)
	}
	a.host = host
	t.Cleanup(func() { _ = host.Close() })
	return a
}

func openTestChannelForTest(t *testing.T, a *App, id channel.ID, declarations []channelhost.GenesisDeclaration) *home.Home {
	t.Helper()
	spec := channelhost.ProvisionSpec{ChannelID: id, Type: "group", OwnerPrincipal: "owner", GenesisDeclarations: declarations, CreatedAt: time.Now().UnixMilli()}
	if _, err := a.host.Provision(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if err := a.host.Open(context.Background(), channelhost.OpenSpec{ChannelID: id, ExpectedType: "group"}); err != nil {
		t.Fatal(err)
	}
	h, ok := a.host.Borrow(id)
	if !ok {
		t.Fatal("borrow failed")
	}
	return h
}
