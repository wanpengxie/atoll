package framework

import (
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestValidateRespondReasonClosedSet(t *testing.T) {
	cases := []struct {
		name    string
		status  string
		reason  string
		wantErr string
	}{
		{
			name:   "empty reason allowed",
			status: "completed",
		},
		{
			name:   "terminal failure reason allowed",
			status: "failed",
			reason: string(message.TerminalAdapterExecutionFailed),
		},
		{
			name:    "reason requires failed status",
			status:  "completed",
			reason:  string(message.TerminalReceiverUnavailable),
			wantErr: "requires status=failed",
		},
		{
			name:    "open set rejected",
			status:  "failed",
			reason:  "device_session_missing",
			wantErr: "closed set",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRespondReason(tc.status, tc.reason)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateRespondReason: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateRespondReason err=%v want substring %q", err, tc.wantErr)
			}
		})
	}
}
