package main

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// TestIntegration_XHSCreatorTemplate_BootSeedsChannel verifies M1.6-T5
// phase-2 acceptance B1-B4 end-to-end:
//
//	B1. POST channel.create type=xhs-creator → daemon receives
//	    control.create_channel with ChannelType="xhs-creator" → bootstrap
//	    saga + adapter framework install populate channel sqlite:
//	      - type_registry has all 6 xhs.* business types,
//	      - actor_registry has tool:xhs-adapter,
//	      - channel_lock.channel_type persisted as "xhs-creator".
//	B2. Workdir physically contains published-notes/, drafts/, assets/
//	    (saga step 5c — workdir subdirs from xhs-creator template).
//	B3. DomainPrompt projection is wired (resolver returns the L4 §2.4
//	    prompt segment for type=xhs-creator); smoke-checked by the
//	    well-known L4 §2.4 first-line marker.
//	B4. A "group" channel created with type="group" does NOT install the
//	    xhs adapter (XHSScaffoldFactory gating works), so the same daemon
//	    hosting both channels keeps the boundary clean — no xhs.* types
//	    in the group channel's type_registry, no tool:xhs-adapter in its
//	    actor_registry, and no xhs workdir subdirs on disk.
func TestIntegration_XHSCreatorTemplate_BootSeedsChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, channelsDir := startIntegrationDaemon(t, ctx, integDaemonOpts{})
	defer func() { _ = d.Close() }()

	// --- xhs-creator channel (createChannel helper sets ChannelType) ---
	createChannel(t, ctx, d, srv, "ch-xhs", []placement.InitialMember{
		{ActorIDInChannel: "user:alice", Kind: "human", DisplayName: "Alice"},
	})

	xhsSqlitePath := filepath.Join(channelsDir, "ch-xhs", "channel.sqlite")
	xhsDB, err := store.OpenChannel(ctx, xhsSqlitePath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatalf("open ch-xhs sqlite: %v", err)
	}
	defer func() { _ = xhsDB.Close() }()

	// B1.1 — type_registry contains all 6 xhs.* business types.
	gotTypes := listTypeRegistryNames(t, ctx, xhsDB)
	for _, want := range xhs.AllTypes {
		if !containsString(gotTypes, want) {
			t.Errorf("type_registry missing %q (got=%v)", want, gotTypes)
		}
	}
	if len(gotTypes) < len(xhs.AllTypes) {
		t.Errorf("type_registry count=%d want >= %d (got=%v)",
			len(gotTypes), len(xhs.AllTypes), gotTypes)
	}

	// B1.2 — actor_registry has tool:xhs-adapter from the template seed.
	reg := store.NewActorRegistry(xhsDB)
	rec, ok, err := reg.Lookup(ctx, xhs.DefaultAdapterActorID)
	if err != nil {
		t.Fatalf("actor_registry lookup %s: %v", xhs.DefaultAdapterActorID, err)
	}
	if !ok {
		t.Fatalf("actor_registry missing %s — saga step 5b did not seed", xhs.DefaultAdapterActorID)
	}
	if rec.Binding != actor.BindingInProcess {
		t.Errorf("adapter actor binding=%q want %q", rec.Binding, actor.BindingInProcess)
	}

	// B1.3 — channel_lock.channel_type persisted so cold-start resolves
	// the same template without re-asking the server.
	lock := store.NewChannelLock(xhsDB)
	lockRow, ok, err := lock.Get(ctx)
	if err != nil {
		t.Fatalf("channel_lock get: %v", err)
	}
	if !ok {
		t.Fatal("channel_lock row missing after create")
	}
	if lockRow.ChannelType != XHSCreatorChannelType {
		t.Errorf("channel_lock.channel_type=%q want %q", lockRow.ChannelType, XHSCreatorChannelType)
	}

	// B2 — workdir subdirs from saga step 5c.
	xhsChannelDir := filepath.Join(channelsDir, "ch-xhs")
	for _, sub := range xhs.WorkdirSubdirs() {
		got := filepath.Join(xhsChannelDir, sub)
		fi, err := os.Stat(got)
		if err != nil {
			t.Errorf("workdir subdir %s missing: %v", sub, err)
			continue
		}
		if !fi.IsDir() {
			t.Errorf("workdir subdir %s is not a dir", got)
		}
	}

	// B3 — domain prompt smoke check via the adapter accessor (the
	// daemon's resolver hands the same string back through ChannelHooks
	// for phase-3 worker spawn env). Asserts the L4 §2.4 first-line
	// marker so phase-3 can grep telemetry for the same prefix.
	prompt := xhs.DomainPrompt()
	if len(prompt) < 128 {
		t.Errorf("domain prompt suspiciously short (%d bytes)", len(prompt))
	}
	const marker = "你是 xhs（小红书）内容创作 agent"
	if !strings.HasPrefix(prompt, marker) {
		t.Errorf("domain prompt missing L4 §2.4 first-line marker %q; got prefix=%q",
			marker, firstNRunes(prompt, len([]rune(marker))))
	}

	// --- B4 — a generic group channel must NOT seed xhs.* ---
	createChannelOfType(t, ctx, d, srv, "ch-group", "group",
		[]placement.InitialMember{
			{ActorIDInChannel: "user:bob", Kind: "human", DisplayName: "Bob"},
		})

	groupSqlitePath := filepath.Join(channelsDir, "ch-group", "channel.sqlite")
	groupDB, err := store.OpenChannel(ctx, groupSqlitePath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatalf("open ch-group sqlite: %v", err)
	}
	defer func() { _ = groupDB.Close() }()

	gotGroupTypes := listTypeRegistryNames(t, ctx, groupDB)
	for _, banned := range xhs.AllTypes {
		if containsString(gotGroupTypes, banned) {
			t.Errorf("ch-group leaked xhs business type %q into type_registry", banned)
		}
	}
	groupReg := store.NewActorRegistry(groupDB)
	if _, ok, _ := groupReg.Lookup(ctx, xhs.DefaultAdapterActorID); ok {
		t.Errorf("ch-group leaked %s actor — XHSScaffoldFactory gating broken", xhs.DefaultAdapterActorID)
	}

	// Group channel ALSO must not get the xhs workdir subdirs.
	groupChannelDir := filepath.Join(channelsDir, "ch-group")
	for _, sub := range xhs.WorkdirSubdirs() {
		got := filepath.Join(groupChannelDir, sub)
		if _, err := os.Stat(got); err == nil {
			t.Errorf("ch-group should not have xhs subdir %s", got)
		}
	}
}

// createChannelOfType is the createChannel companion that lets the
// caller pin a non-default ChannelType. Mirrors createChannel's wait /
// ack-drain loop; kept local to the integration tests.
func createChannelOfType(
	t *testing.T,
	ctx context.Context,
	d *runtime.Daemon,
	srv *transit.MockServer,
	channelID string,
	channelType string,
	members []placement.InitialMember,
) {
	t.Helper()
	req := placement.CreateChannelRequest{
		ChannelID:       channel.ID(channelID),
		CreateRequestID: placement.CreateRequestID("req-" + channelID),
		OwnerEpoch:      placement.OwnerEpoch(1),
		FencingToken:    placement.FencingToken(1),
		InitialMembers:  members,
		ChannelType:     channelType,
	}
	frame, err := transit.Encode("frame-create-"+channelID,
		daemonbus.FrameTypeControlCreateChannel,
		"server", d.Transit().Epoch(), nowMs(), req)
	if err != nil {
		t.Fatalf("encode create_channel %s: %v", channelID, err)
	}
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatalf("SendToDaemon create_channel %s: %v", channelID, err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("create_channel %s ack never arrived", channelID)
		default:
		}
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		f, err := srv.RecvFromDaemon(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("RecvFromDaemon %s: %v", channelID, err)
		}
		if f.FrameType != daemonbus.FrameTypeControlCreateChannelAck {
			continue
		}
		var ack placement.CreateChannelAck
		if err := transit.DecodePayload(f, &ack); err != nil {
			t.Fatalf("decode ack %s: %v", channelID, err)
		}
		if ack.Status != placement.AckBound {
			t.Fatalf("create %s rejected: %s reason=%s", channelID, ack.Status, ack.Reason)
		}
		return
	}
}

// listTypeRegistryNames returns the sorted set of `type` column values
// present in the channel-local type_registry table.
func listTypeRegistryNames(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT type FROM type_registry`)
	if err != nil {
		t.Fatalf("query type_registry: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan type_registry: %v", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	sort.Strings(out)
	return out
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func firstNRunes(s string, n int) string {
	r := []rune(s)
	if len(r) < n {
		return s
	}
	return string(r[:n])
}
