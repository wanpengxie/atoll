package link

// Lane transfer ticket tests: mint/redeem lifecycle of the single-use token.

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
)

// A lane transfer ticket is single-use: consumed at its first valid
// redemption, so a replay within the TTL finds nothing.
func TestLaneTransferTokenIsSingleUse(t *testing.T) {
	acc := &Acceptor{lane: newLaneState(), sessions: newSessionRegistry(nil)}
	token, err := acc.OpenLaneTransfer(
		context.Background(), "target-daemon", "req-daemon", "coord-1", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	redeem := func() laneAck {
		home, daemon := net.Pipe()
		defer daemon.Close()
		go acc.handleLaneRedeem("req-daemon", home)
		if err := writeLaneJSON(daemon, laneRedeemHeader{Token: token}); err != nil {
			t.Fatal(err)
		}
		var ack laneAck
		if err := readLaneJSON(daemon, &ack); err != nil {
			t.Fatal(err)
		}
		return ack
	}
	first := redeem()
	if first.OK || !strings.Contains(first.Reason, "no live link") {
		t.Fatalf("first redemption ack=%+v want target-unreachable", first)
	}
	second := redeem()
	if second.OK || !strings.Contains(second.Reason, "unknown or mismatched") {
		t.Fatalf("replayed ticket was honored: ack=%+v", second)
	}
}
