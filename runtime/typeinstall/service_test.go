package typeinstall_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/typeinstall"
)

func TestServiceInstallTypeEmitsMirrorEvent(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-typeinstall")
	svc, reg, msgs := newServiceFixture(t, chID, 12345)

	row := adapter.TypeRow{
		Type:               "xhs.publish",
		HandlerActorID:     "tool:xhs-adapter",
		HandlerBinding:     actor.BindingEmbedded,
		MaxPendingMs:       60_000,
		AllowedKinds:       []message.Kind{message.KindRequest, message.KindResponse},
		TerminalConvention: adapter.TerminalPayloadStatus,
	}
	if _, err := svc.InstallType(ctx, row); err != nil {
		t.Fatalf("InstallType: %v", err)
	}
	if _, ok, err := reg.Lookup(ctx, "xhs.publish"); err != nil || !ok {
		t.Fatalf("registry lookup ok=%v err=%v", ok, err)
	}

	env, ok, err := msgs.FindByID(ctx, chID, "system.type.installed:xhs.publish:12345")
	if err != nil || !ok {
		t.Fatalf("FindByID mirror ok=%v err=%v", ok, err)
	}
	if env.Type != "system.type.installed" || env.Kind != message.KindEvent {
		t.Fatalf("mirror envelope type=%s kind=%s", env.Type, env.Kind)
	}
	if env.Sender.ID != actor.SystemActorID || env.Sender.Kind != actor.KindSystem {
		t.Fatalf("mirror sender=%+v", env.Sender)
	}
	if env.Visibility != message.VisibilitySystem || !env.Audience.IsWildcard() {
		t.Fatalf("mirror visibility=%s audience=%v", env.Visibility, env.Audience)
	}

	var payload map[string]any
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["type"] != "xhs.publish" ||
		payload["handler_actor_id"] != "tool:xhs-adapter" ||
		payload["handler_binding"] != "embedded" ||
		payload["mutation_kind"] != "create" ||
		payload["installed_at"].(float64) != 12345 ||
		payload["max_pending_ms"].(float64) != 60000 {
		t.Fatalf("payload=%v", payload)
	}
	gotKinds := payload["allowed_kinds"].([]any)
	if len(gotKinds) != 2 || gotKinds[0] != "request" || gotKinds[1] != "response" {
		t.Fatalf("allowed_kinds=%v", gotKinds)
	}
}

func TestServiceInstallTypeRejectsReservedNamespace(t *testing.T) {
	ctx := context.Background()
	svc, reg, _ := newServiceFixture(t, "ch-typeinstall", 12345)
	for _, typeName := range []string{
		"system.foo",
		"system.type.installed",
	} {
		_, err := svc.InstallType(ctx, adapter.TypeRow{
			Type:           typeName,
			HandlerActorID: "tool:xhs-adapter",
			HandlerBinding: actor.BindingEmbedded,
			MaxPendingMs:   60_000,
			AllowedKinds:   []message.Kind{message.KindEvent},
		})
		var ie *typeinstall.Error
		if !errors.As(err, &ie) || ie.Reason != message.InstallTypeRegistryReservedNamespace {
			t.Fatalf("%s: expected type_registry_reserved_namespace, got %v", typeName, err)
		}
		if _, ok, err := reg.Lookup(ctx, typeName); err != nil || ok {
			t.Fatalf("%s: reserved row written ok=%v err=%v", typeName, ok, err)
		}
	}
}

func TestInstallType_RegistryAndMirror_AtomicCommit(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-typeinstall-atomic")
	svc, reg, msgs := newServiceFixture(t, chID, 22222)
	row := typeInstallRow("xhs.atomic")

	if _, err := svc.InstallType(ctx, row); err != nil {
		t.Fatalf("InstallType: %v", err)
	}
	if _, ok, err := reg.Lookup(ctx, row.Type); err != nil || !ok {
		t.Fatalf("registry lookup ok=%v err=%v", ok, err)
	}
	status, reason, ok, err := reg.InstallStatus(ctx, row.Type)
	if err != nil || !ok {
		t.Fatalf("InstallStatus ok=%v err=%v", ok, err)
	}
	if status != store.TypeInstallStatusInstalled || reason != "" {
		t.Fatalf("status=%q reason=%q want installed empty", status, reason)
	}
	if _, ok, err := msgs.FindByID(ctx, chID, "system.type.installed:xhs.atomic:22222"); err != nil || !ok {
		t.Fatalf("mirror ok=%v err=%v", ok, err)
	}
}

func TestInstallType_MirrorEmitFail_MarkedFailed(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-typeinstall-fail")
	actors, reg, _ := newTypeInstallStores(t, chID, 33333)
	svc, err := typeinstall.New(typeinstall.Config{
		ChannelID:     chID,
		ActorRegistry: actors,
		TypeRegistry:  reg,
		HarnessChain:  failingChain{err: errors.New("mirror unavailable")},
		NowFn:         func() int64 { return 33333 },
	})
	if err != nil {
		t.Fatalf("typeinstall.New: %v", err)
	}
	row := typeInstallRow("xhs.fail")
	if _, err := svc.InstallType(ctx, row); err == nil {
		t.Fatal("InstallType succeeded despite mirror failure")
	}
	if _, ok, err := reg.Lookup(ctx, row.Type); err != nil || ok {
		t.Fatalf("failed install visible in registry ok=%v err=%v", ok, err)
	}
	status, reason, ok, err := reg.InstallStatus(ctx, row.Type)
	if err != nil || !ok {
		t.Fatalf("InstallStatus ok=%v err=%v", ok, err)
	}
	if status != store.TypeInstallStatusFailed || reason == "" {
		t.Fatalf("status=%q reason=%q want failed with reason", status, reason)
	}
}

func TestInstallType_RecoveryAfterCrashMidEmit(t *testing.T) {
	ctx := context.Background()
	svc, reg, _ := newServiceFixture(t, "ch-typeinstall-recovery", 44444)
	row := typeInstallRow("xhs.recovery")
	if _, _, err := reg.BeginInstall(ctx, row); err != nil {
		t.Fatalf("BeginInstall: %v", err)
	}
	if _, ok, err := reg.Lookup(ctx, row.Type); err != nil || ok {
		t.Fatalf("installing row visible ok=%v err=%v", ok, err)
	}
	n, err := svc.RecoverInstalling(ctx, "crash recovered")
	if err != nil {
		t.Fatalf("RecoverInstalling: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovered=%d want 1", n)
	}
	status, reason, ok, err := reg.InstallStatus(ctx, row.Type)
	if err != nil || !ok {
		t.Fatalf("InstallStatus ok=%v err=%v", ok, err)
	}
	if status != store.TypeInstallStatusFailed || reason != "crash recovered" {
		t.Fatalf("status=%q reason=%q", status, reason)
	}
}

func TestInstallType_CrashAfterMirrorBeforeMark_RecoveryCompletesInstall(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-typeinstall-recovery-complete")
	svc, reg, msgs := newServiceFixture(t, chID, 55555)
	row := typeInstallRow("xhs.recovery.complete")
	if _, _, err := reg.BeginInstall(ctx, row); err != nil {
		t.Fatalf("BeginInstall: %v", err)
	}
	payload := json.RawMessage(`{"type":"xhs.recovery.complete"}`)
	if _, err := msgs.Append(ctx, &message.Envelope{
		ID:         "system.type.installed:xhs.recovery.complete:55555",
		TS:         55555,
		TSReceived: 55555,
		ChannelID:  chID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.type.installed",
		Payload:    payload,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{message.AudienceWildcard},
	}, klog.FencingTuple{}); err != nil {
		t.Fatalf("append mirror: %v", err)
	}
	n, err := svc.RecoverInstalling(ctx, "crash recovered")
	if err != nil {
		t.Fatalf("RecoverInstalling: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovered=%d want 1", n)
	}
	status, reason, ok, err := reg.InstallStatus(ctx, row.Type)
	if err != nil || !ok {
		t.Fatalf("InstallStatus ok=%v err=%v", ok, err)
	}
	if status != store.TypeInstallStatusInstalled || reason != "" {
		t.Fatalf("status=%q reason=%q want installed empty", status, reason)
	}
	if _, ok, err := reg.Lookup(ctx, row.Type); err != nil || !ok {
		t.Fatalf("Lookup after recovery ok=%v err=%v", ok, err)
	}
}

func TestInstallType_ConcurrentSameType_NoDoubleEmit(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("ch-typeinstall-concurrent")
	actors, reg, msgs, db := newTypeInstallStoresWithDB(t, chID, 66666)
	var now atomic.Int64
	now.Store(66666)
	nowFn := func() int64 { return now.Add(1) }
	chain, err := rtharness.New(rtharness.Deps{
		ChannelID:     chID,
		ActorRegistry: actors,
		TypeRegistry:  reg.HarnessView(),
		Log:           msgs,
		NowMs:         nowFn,
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	svc, err := typeinstall.New(typeinstall.Config{
		ChannelID:     chID,
		ActorRegistry: actors,
		TypeRegistry:  reg,
		HarnessChain:  chain,
		NowFn:         nowFn,
	})
	if err != nil {
		t.Fatalf("typeinstall.New: %v", err)
	}
	row := typeInstallRow("xhs.concurrent")
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.InstallType(ctx, row)
		}(i)
	}
	wg.Wait()
	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes == 0 {
		t.Fatalf("both concurrent installs failed: %v", errs)
	}
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE type='system.type.installed' AND payload LIKE '%"type":"xhs.concurrent"%'`,
	).Scan(&count); err != nil {
		t.Fatalf("count mirror messages: %v", err)
	}
	if count != 1 {
		t.Fatalf("mirror count=%d want 1", count)
	}
	if _, ok, err := reg.Lookup(ctx, row.Type); err != nil || !ok {
		t.Fatalf("Lookup after concurrent install ok=%v err=%v", ok, err)
	}
}

func newServiceFixture(t *testing.T, chID channel.ID, now int64) (*typeinstall.Service, *store.TypeRegistry, *store.Messages) {
	t.Helper()
	actors, reg, msgs := newTypeInstallStores(t, chID, now)
	chain, err := rtharness.New(rtharness.Deps{
		ChannelID:     chID,
		ActorRegistry: actors,
		TypeRegistry:  reg.HarnessView(),
		Log:           msgs,
		NowMs:         func() int64 { return now },
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	svc, err := typeinstall.New(typeinstall.Config{
		ChannelID:     chID,
		ActorRegistry: actors,
		TypeRegistry:  reg,
		HarnessChain:  chain,
		NowFn:         func() int64 { return now },
	})
	if err != nil {
		t.Fatalf("typeinstall.New: %v", err)
	}
	return svc, reg, msgs
}

func newTypeInstallStores(t *testing.T, _ channel.ID, now int64) (*store.ActorRegistry, *store.TypeRegistry, *store.Messages) {
	actors, reg, msgs, _ := newTypeInstallStoresWithDB(t, "", now)
	return actors, reg, msgs
}

func newTypeInstallStoresWithDB(t *testing.T, _ channel.ID, now int64) (*store.ActorRegistry, *store.TypeRegistry, *store.Messages, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	actors := store.NewActorRegistry(db)
	for _, rec := range []actorreg.Record{
		{ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: now},
		{ID: "tool:xhs-adapter", Kind: actor.KindTool, Binding: actor.BindingEmbedded, CreatedAt: now},
	} {
		if err := actors.Insert(ctx, rec); err != nil {
			t.Fatalf("insert actor %s: %v", rec.ID, err)
		}
	}
	reg := store.NewTypeRegistry(db, func() int64 { return now })
	msgs := store.NewMessages(db)
	return actors, reg, msgs, db
}

func typeInstallRow(typeName string) adapter.TypeRow {
	return adapter.TypeRow{
		Type:               typeName,
		HandlerActorID:     "tool:xhs-adapter",
		HandlerBinding:     actor.BindingEmbedded,
		MaxPendingMs:       60_000,
		AllowedKinds:       []message.Kind{message.KindRequest, message.KindResponse},
		TerminalConvention: adapter.TerminalPayloadStatus,
	}
}

type failingChain struct {
	err error
	res khar.WriteResult
}

func (f failingChain) Write(context.Context, *message.Envelope) (khar.WriteResult, error) {
	return f.res, f.err
}
