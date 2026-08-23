package engineboot

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/peeractor"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

const peerFailureReceiverClass = "peer-failure-receiver-test"
const peerFailureProxyClass = "peer-failure-proxy-test"

var registerPeerFailureReceiver sync.Once
var peerFailureProxyDefs sync.Map

func ensurePeerFailureReceiver() {
	registerPeerFailureReceiver.Do(func() {
		manifest := introspect.Manifest{Class: peerFailureReceiverClass, Interfaces: []string{"actor"}, Words: map[string]introspect.WordSpec{
			"remote.unsupported": {Description: "return a receiver-owned failure for peer propagation tests"},
			"remote.inactive":    {Description: "route to a receiver removed during peer propagation tests"},
			"remote.shutdown":    {Description: "end the receiver while a peer request is in flight"},
			"remote.bad-origin":  {Description: "exercise the peer relationship gate"},
			"remote.timeout":     {Description: "outlive a B-side receiver deadline"},
			"remote.hang":        {Description: "remain in flight for A-side closure tests"},
			"remote.port-closed": {Description: "exercise a closed service port"},
		}}
		registry.Register(peerFailureReceiverClass, registry.ClassDecl{
			Kind: actor.KindTool, Placement: channelspec.PlacementServer, Manifest: manifest,
			New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
				def := actorbase.Def{Manifest: manifest, New: func() (actorbase.Proc, error) {
					return func(sys actorbase.Sys) error {
						for {
							msg, err := sys.Recv()
							if err != nil {
								return err
							}
							if msg.Type == "remote.shutdown" {
								return sys.End()
							}
							if msg.Type == "remote.timeout" || msg.Type == "remote.hang" {
								<-msg.Ctx().Done()
								continue
							}
							_, _ = sys.Fail(msg, "receiver_test_failure", "receiver supplied detail")
						}
					}, nil
				}}
				return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool, Factory: platform.ActorFactory{Proc: def}}, nil
			},
		})
		proxyManifest := introspect.Manifest{Class: peerFailureProxyClass, Interfaces: []string{"actor", "peer"}, Words: map[string]introspect.WordSpec{}}
		registry.Register(peerFailureProxyClass, registry.ClassDecl{
			Kind: actor.KindTool, Placement: channelspec.PlacementServer, Manifest: proxyManifest,
			New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
				var cfg struct {
					Token string `json:"token"`
				}
				if err := json.Unmarshal(spec.Config, &cfg); err != nil {
					return platform.ActorDecl{}, err
				}
				value, ok := peerFailureProxyDefs.Load(cfg.Token)
				if !ok {
					return platform.ActorDecl{}, context.Canceled
				}
				return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool, Factory: platform.ActorFactory{Proc: value.(actorbase.Def)}}, nil
			},
		})
	})
}

// The peer member is found by the declaration it was seated from: the birth
// name in the middle id segment is the peer declaration's name (the child's
// qualified name), not the child channel id.
func waitPeerMember(t *testing.T, coreRoster func() ([]channelspec.ObsRosterRow, error), child channel.ID) actor.ActorID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := coreRoster()
		if err == nil {
			for _, row := range rows {
				if row.Kind == actor.KindPeer && row.DeclID == string(child) {
					return row.ID
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("c0 roster never acquired peer for %s", child)
	return ""
}

func assertCallerLedgerFailure(t *testing.T, raw json.RawMessage, code string) {
	t.Helper()
	terminal := decodeTerminal(t, raw)
	if terminal.Status != message.StatusFailed || terminal.ErrorCode != code || terminal.Detail == "" {
		t.Fatalf("terminal=%s decoded=%+v", raw, terminal)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, leaked := fields["stage"]; leaked {
		t.Fatalf("peer stage leaked into caller ledger: %s", raw)
	}
}

type callerClosure struct {
	Status    string `json:"status"`
	Reason    string `json:"reason"`
	ErrorCode string `json:"error_code"`
	Cancelled bool   `json:"cancelled"`
}

func decodeCallerClosure(t *testing.T, raw json.RawMessage) callerClosure {
	t.Helper()
	var got callerClosure
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func installPeerFailureProxy(t *testing.T, core channelhost.Bundle, registrar actor.ActorID, token string, def actorbase.Def) actor.ActorID {
	t.Helper()
	peerFailureProxyDefs.Store(token, def)
	t.Cleanup(func() { peerFailureProxyDefs.Delete(token) })
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{
		"id": token, "name": token, "class": peerFailureProxyClass, "visibility": "public", "config": map[string]any{"token": token},
	}), nil)
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, actor.SystemActorID, message.TypeSystemMemberCreate, map[string]any{"decl_id": token}), nil)
	return onlyDecl(t, core, token)
}

func controlledCallerLedger(
	t *testing.T,
	bundle channelhost.Bundle,
	target actor.ActorID,
	word string,
	expiresAt *int64,
	cancel bool,
	afterSubmit func(),
) json.RawMessage {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 40*time.Second)
	defer stop()
	sender, ok, err := bundle.View().ResolvePrincipal(ctx, channelspec.RootPrincipalID)
	if err != nil || !ok {
		t.Fatalf("resolve root: ok=%v err=%v", ok, err)
	}
	slot, ok := bundle.Gateway().SubjectSlotFor(sender)
	if !ok {
		t.Fatal("subject slot missing")
	}
	id := message.ID(uuid.NewString())
	frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, uuid.NewString(), subjectgate.SubmitPayload{
		ChannelID: string(channelspec.C0ChannelID), ID: string(id), MsgType: word, Kind: string(message.KindRequest),
		Audience: []string{string(target)}, Visibility: string(message.VisibilityPublic), Payload: json.RawMessage(`{}`), ExpiresAt: expiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := slot.Deliver(ctx, frame); err != nil {
		t.Fatal(err)
	}
	if cancel {
		time.Sleep(50 * time.Millisecond)
		cancelFrame, err := subjectgate.NewFrame(subjectgate.FrameCancel, uuid.NewString(), subjectgate.CancelPayload{ChannelID: string(channelspec.C0ChannelID), ReqID: string(id)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := slot.Deliver(ctx, cancelFrame); err != nil {
			t.Fatal(err)
		}
	}
	if afterSubmit != nil {
		afterSubmit()
	}
	var cursor int64
	for {
		rows, next, err := bundle.View().ReadVisibleAfterSeq(ctx, cursor, 256)
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
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestA9AllElevenFailureRowsTraverseFramesAndRealCallerLedger(t *testing.T) {
	ensurePeerFailureReceiver()
	eng, _, core, registrar := newProtocolDeliveryRig(t)
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{
		"id": "failure-receiver", "name": "failure-receiver", "class": peerFailureReceiverClass, "visibility": "public", "config": map[string]any{},
	}), nil)
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{
		"id": "inactive-receiver", "name": "inactive-receiver", "class": peerFailureReceiverClass, "visibility": "public", "config": map[string]any{},
	}), nil)
	child := createdChannelID(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "peer-failure-ledger", "initial_actor_ids": []any{currentMemberID(t, core, channelspec.RootPrincipalID)}, "recipe": map[string]any{
			"declarations": []any{map[string]any{"decl_id": "failure-receiver"}, map[string]any{"decl_id": "inactive-receiver"}},
			"profile": map[string]any{"svc_agent": nil, "endpoints": map[string]any{
				"remote.unsupported": map[string]any{"receiver": "failure-receiver"},
				"remote.inactive":    map[string]any{"receiver": "inactive-receiver"},
				"remote.shutdown":    map[string]any{"receiver": "inactive-receiver"},
				"remote.bad-origin":  map[string]any{"receiver": "failure-receiver"},
				"remote.timeout":     map[string]any{"receiver": "failure-receiver"},
				"remote.hang":        map[string]any{"receiver": "failure-receiver"},
				"remote.port-closed": map[string]any{"receiver": "failure-receiver"},
			}},
		},
	}))
	waitBundle(t, eng, child)
	peer := waitPeerMember(t, func() ([]channelspec.ObsRosterRow, error) {
		return core.View().Roster(context.Background())
	}, child)

	assertCallerLedgerFailure(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, peer, "remote.shutdown", map[string]any{}), string(message.TerminalReceiverUnavailable))

	for _, tc := range []struct {
		name   string
		word   string
		code   string
		detail string
	}{
		{name: "endpoint absent", word: "remote.missing", code: string(channel.GateEndpointNotFound)},
		{name: "service agent absent", word: "agent.ask", code: string(channel.GateNoServiceAgent)},
		{name: "receiver inactive", word: "remote.inactive", code: string(channel.GateReceiverInactive)},
		{name: "receiver application failure", word: "remote.unsupported", code: "receiver_test_failure", detail: "receiver supplied detail"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, peer, tc.word, map[string]any{})
			assertCallerLedgerFailure(t, raw, tc.code)
			if tc.detail != "" && decodeTerminal(t, raw).Detail != tc.detail {
				t.Fatalf("terminal=%s want detail %q", raw, tc.detail)
			}
		})
	}

	callTarget := func(ctx context.Context, caller, target channel.ID, req channel.Request, progress func(channel.Progress)) (channel.Result, error) {
		port, _, ok := eng.host.AcquirePort(target)
		if !ok {
			return channel.Result{}, context.Canceled
		}
		return port.Call(ctx, caller, req, progress)
	}
	describeTarget := func(ctx context.Context, caller, target channel.ID, frame channel.Describe) (channel.Card, error) {
		port, _, ok := eng.host.AcquirePort(target)
		if !ok {
			return channel.Card{}, context.Canceled
		}
		return port.Describe(ctx, caller, frame)
	}
	seam := func(ctx context.Context, caller, target channel.ID, req channel.Request, progress func(channel.Progress)) (channel.Result, error) {
		switch req.Type {
		case "remote.bad-origin":
			req.From.Channel = "not-the-bound-channel"
		}
		return callTarget(ctx, caller, target, req, progress)
	}
	proxyDef := peeractor.Def(peeractor.Deps{Caller: channelspec.C0ChannelID, Target: child, Seam: seam, Describe: describeTarget})
	proxy := installPeerFailureProxy(t, core, registrar, "failure-proxy", proxyDef)

	assertCallerLedgerFailure(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, proxy, "remote.bad-origin", map[string]any{}), string(channel.GateBadOrigin))
	assertCallerLedgerFailure(t, controlledCallerLedger(t, core, proxy, "remote.timeout", nil, false, nil), string(message.TerminalUnansweredTimeout))

	expires := time.Now().Add(150 * time.Millisecond).UnixMilli()
	deadlineRaw := controlledCallerLedger(t, core, proxy, "remote.hang", &expires, false, func() {
		time.Sleep(200 * time.Millisecond)
		if !eng.host.Poke(channelspec.C0ChannelID) {
			t.Fatal("failed to wake c0 deadline reaper")
		}
	})
	deadlineTerminal := decodeCallerClosure(t, deadlineRaw)
	if deadlineTerminal.Status != message.StatusFailed || deadlineTerminal.Reason != string(message.TerminalUnansweredTimeout) || deadlineTerminal.ErrorCode != "" {
		t.Fatalf("A deadline terminal=%s decoded=%+v", deadlineRaw, deadlineTerminal)
	}
	cancelRaw := controlledCallerLedger(t, core, proxy, "remote.hang", nil, true, nil)
	cancelTerminal := decodeCallerClosure(t, cancelRaw)
	if cancelTerminal.Status != message.StatusFailed || cancelTerminal.Reason != string(message.TerminalUnansweredTimeout) || !cancelTerminal.Cancelled || cancelTerminal.ErrorCode != string(message.TerminalUnansweredTimeout) {
		t.Fatalf("A cancel terminal=%s decoded=%+v", cancelRaw, cancelTerminal)
	}

	deathProxy := installPeerFailureProxy(t, core, registrar, "death-proxy", proxyDef)
	deathRaw := controlledCallerLedger(t, core, deathProxy, "remote.hang", nil, false, func() {
		time.Sleep(50 * time.Millisecond)
		removed := decodeTerminal(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, actor.SystemActorID, message.TypeSystemMemberDelete, map[string]any{"member": deathProxy}))
		if removed.Status != message.StatusCompleted {
			t.Fatalf("remove peer terminal=%+v", removed)
		}
	})
	deathTerminal := decodeCallerClosure(t, deathRaw)
	if deathTerminal.Status != message.StatusFailed || deathTerminal.Reason != string(message.TerminalReceiverUnavailable) || deathTerminal.ErrorCode != "" {
		t.Fatalf("peer death terminal=%s decoded=%+v", deathRaw, deathTerminal)
	}

	port, _, ok := eng.host.AcquirePort(child)
	if !ok {
		t.Fatal("child port missing")
	}
	port.Close()
	assertCallerLedgerFailure(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, proxy, "remote.port-closed", map[string]any{}), string(channel.GateChannelUnavailable))
}
