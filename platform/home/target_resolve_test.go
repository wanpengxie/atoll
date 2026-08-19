package home

import (
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestResolveActorTargetUsesD24SegmentMatching(t *testing.T) {
	active := []storespec.ActiveIdentity{
		{ID: "tool:echo:101"},
		{ID: "agent:echo:202"},
		{ID: "agent:other:101"},
	}
	tests := []struct {
		name   string
		target string
		want   actor.ActorID
		code   string
	}{
		{name: "system gate", target: "system", want: actor.SystemActorID},
		{name: "full", target: "tool:echo:101", want: "tool:echo:101"},
		{name: "kind seed", target: "tool:echo", want: "tool:echo:101"},
		{name: "seed timestamp", target: "echo:202", want: "agent:echo:202"},
		{name: "seed", target: "other", want: "agent:other:101"},
		{name: "ambiguous", target: "echo", code: "actor_ambiguous"},
		{name: "not found", target: "missing", code: "not_found"},
		{name: "empty", target: "", code: "invalid_args"},
		{name: "empty segment", target: "tool::101", code: "invalid_args"},
		{name: "too many segments", target: "a:b:c:d", code: "invalid_args"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveActorTarget(tc.target, active)
			if tc.code == "" {
				if err != nil || got != tc.want {
					t.Fatalf("ResolveActorTarget(%q)=(%q,%v), want (%q,nil)", tc.target, got, err, tc.want)
				}
				return
			}
			var targetErr *actorbase.TargetResolveError
			if !errors.As(err, &targetErr) || targetErr.Code != tc.code {
				t.Fatalf("ResolveActorTarget(%q) error=%v, want code %q", tc.target, err, tc.code)
			}
		})
	}
}
