package engineboot

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestDecodeRegistrarTerminalRejectsMalformedAndUnknownStatus(t *testing.T) {
	for name, raw := range map[string]json.RawMessage{
		"malformed": []byte(`{"status":`),
		"unknown":   []byte(`{"status":"processing"}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeRegistrarTerminal(raw); err == nil {
				t.Fatal("invalid terminal payload was accepted")
			}
		})
	}
}

func TestDecodeRegistrarTerminalUsesClosedStatusAndLagoonError(t *testing.T) {
	completed := json.RawMessage(`{"status":"` + message.StatusCompleted + `","value":{"ok":true}}`)
	got, err := decodeRegistrarTerminal(completed)
	if err != nil || string(got) != string(completed) {
		t.Fatalf("completed terminal=(%s,%v)", got, err)
	}

	_, err = decodeRegistrarTerminal(json.RawMessage(`{"status":"` + message.StatusFailed + `","error_code":"conflict_exists","detail":"duplicate"}`))
	var lagoonErr *lagoon.Error
	if !errors.As(err, &lagoonErr) || lagoonErr.Code != lagoon.CodeConflictExists || lagoonErr.Detail != "duplicate" {
		t.Fatalf("failed terminal error=%v", err)
	}
}
