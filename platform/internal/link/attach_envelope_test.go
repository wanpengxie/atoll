package link

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestValidateAttachEnvelopeProtocolAndDuplicateGates(t *testing.T) {
	cases := []struct {
		name string
		req  *AttachRequest
		want string
	}{
		{name: "nil", want: "protocol_too_old"},
		{name: "old", req: &AttachRequest{Proto: 1}, want: "protocol_too_old"},
		{name: "duplicate", req: &AttachRequest{Proto: 2, Declarations: []Declaration{
			{ActorID: "tool:a", Kind: actor.KindTool, Version: 1},
			{ActorID: "tool:a", Kind: actor.KindAgent, Version: 2},
		}}, want: "duplicate_declaration"},
		{name: "distinct", req: &AttachRequest{Proto: 2, Declarations: []Declaration{
			{ActorID: "tool:a", Kind: actor.KindTool},
			{ActorID: "tool:b", Kind: actor.KindTool},
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := validateAttachEnvelope(tc.req); got != tc.want {
				t.Fatalf("reason=%q want %q", got, tc.want)
			}
		})
	}
}
