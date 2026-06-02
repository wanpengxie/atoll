package channel_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

// TestIDStringWireForm — ID.String returns the raw string.
func TestIDStringWireForm(t *testing.T) {
	cases := []string{"", "ch-1", "channel:abc:0", "ch:文字"}
	for _, c := range cases {
		if got := channel.ID(c).String(); got != c {
			t.Errorf("channel.ID(%q).String()=%q want %q", c, got, c)
		}
	}
}

// (Ref / federation tests removed with channel.Ref — federation is a痛感-driven
// additive future, not a pre-allocated kernel placeholder.)
