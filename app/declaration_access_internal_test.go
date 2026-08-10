package app

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

func init() {
	registry.Register("decl-access-test-agent-a", registry.ClassDecl{Kind: actor.KindAgent, New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent}, nil
	}})
	registry.Register("decl-access-test-agent-b", registry.ClassDecl{Kind: actor.KindAgent, New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent}, nil
	}})
	registry.Register("decl-access-test-tool", registry.ClassDecl{Kind: actor.KindTool, New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool}, nil
	}})
}

func TestDeclarationVisibleToGrid(t *testing.T) {
	tests := []struct {
		visibility, owner, principal string
		want                         bool
	}{
		{"public", "owner", "owner", true},
		{"public", "owner", "other", true},
		{"private", "owner", "owner", true},
		{"private", "owner", "other", false},
	}
	for _, tt := range tests {
		if got := declarationVisibleTo(tt.visibility, tt.owner, tt.principal); got != tt.want {
			t.Fatalf("visible(%q,%q,%q)=%v want %v", tt.visibility, tt.owner, tt.principal, got, tt.want)
		}
	}
}

func TestDeclarationClassTransitionAndReservedClass(t *testing.T) {
	a := &App{}
	ctx := context.Background()
	if ok, err := a.declarationClassTransition(ctx, "decl-access-test-agent-a", "decl-access-test-agent-b"); err != nil || !ok {
		t.Fatalf("same-kind transition: ok=%v err=%v", ok, err)
	}
	if ok, err := a.declarationClassTransition(ctx, "decl-access-test-agent-a", "decl-access-test-tool"); err != nil || ok {
		t.Fatalf("cross-kind transition: ok=%v err=%v", ok, err)
	}
	if _, ok, err := a.declarationClassKind(ctx, spaceToolClass); err != nil || ok {
		t.Fatalf("reserved space-tool: found=%v err=%v", ok, err)
	}
}

func TestSysopErrorClassificationAdapters(t *testing.T) {
	if got := classifySysopError(string(sysopCodeDaemonNotFound)); got != sysopNotFound {
		t.Fatalf("daemon_not_found class=%v", got)
	}
	if got := sysopErrorHTTP(string(sysopCodeDaemonNotFound)); got != 404 {
		t.Fatalf("daemon_not_found HTTP=%d", got)
	}
	// SpaceOps has no daemon attach operation, and daemon identities are not the
	// resource family addressed by SpaceResourceNotFound. If malformed persisted
	// input ever crosses this adapter, fail honestly as a space-level outage.
	if got := sysopSpaceErrorCode(string(sysopCodeDaemonNotFound)); got != channelspec.SpaceUnavailable {
		t.Fatalf("daemon_not_found Space=%q", got)
	}
	if got := classifySysopError(string(channelspec.ErrCodeChannelUnavailable)); got != sysopConflict {
		t.Fatalf("channel_unavailable class=%v", got)
	}
	if got := sysopErrorHTTP(string(channelspec.ErrCodeChannelUnavailable)); got != 409 {
		t.Fatalf("channel_unavailable HTTP=%d", got)
	}
	if got := sysopSpaceErrorCode(string(channelspec.ErrCodeChannelUnavailable)); got != channelspec.SpaceChannelUnavailable {
		t.Fatalf("channel_unavailable Space=%q", got)
	}
}
