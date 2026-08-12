package lagoon

import (
	"context"
	"testing"
)

type valueSubmitter struct{ reply Reply }

func (s valueSubmitter) Submit(context.Context, SubmitIn) (Reply, error) { return s.reply, nil }
func (s valueSubmitter) SubmitApplication(context.Context, Word, any) (Reply, error) {
	return s.reply, nil
}

func TestSpaceOpsMissingValueReturnsError(t *testing.T) {
	ops, _ := NewSpaceOps(valueSubmitter{}).Bind(SubmitIn{})
	if _, err := ops.CreateChannel(context.Background(), ChannelCreate{Name: "x"}); err == nil {
		t.Fatal("space operation accepted a reply with no value")
	}
}
