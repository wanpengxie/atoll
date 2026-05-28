package transit

import (
	"os"
	"regexp"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestUIReasonTableCoversWireRejectReasons(t *testing.T) {
	entries := readUIReasonEntries(t)

	for _, reason := range message.AllHarnessRejectReasons {
		assertUIReasonEntry(t, entries, string(reason), expectedHarnessUIClass(reason))
	}
	for _, reason := range []daemonbus.MuxRejectReason{
		daemonbus.MuxRejectUnknownFrameKind,
		daemonbus.MuxRejectUnknownFrameField,
		daemonbus.MuxRejectUnknownPayloadField,
		daemonbus.MuxRejectPayloadSchemaInvalid,
		daemonbus.MuxRejectProtocolVersionUnsupported,
		daemonbus.MuxRejectAuthFailed,
		daemonbus.MuxRejectDuplicateDaemon,
		daemonbus.MuxRejectChannelIDUnknown,
		daemonbus.MuxRejectOwnerEpochStale,
		daemonbus.MuxRejectFrameTooLarge,
		daemonbus.MuxRejectIdleTimeout,
		daemonbus.MuxRejectInternalError,
	} {
		assertUIReasonEntry(t, entries, string(reason), "protocol_system")
	}
	for _, tc := range []struct {
		reason string
		class  string
	}{
		{RejectReasonAuthFailed, "identity"},
		{RejectReasonChannelUnbound, "protocol_system"},
		{RejectReasonInternal, "protocol_system"},
		{RejectReasonReplayWindow, "identity"},
		{RejectReasonReplayNonce, "identity"},
	} {
		assertUIReasonEntry(t, entries, tc.reason, tc.class)
	}
	for _, reason := range message.AllInstallReasons {
		assertUIReasonEntry(t, entries, string(reason), "install_system")
	}
	for _, reason := range message.AllTerminalFailureReasons {
		assertUIReasonEntry(t, entries, string(reason), "failed_terminal")
	}
}

func TestUIReasonTableDropsLegacyUnprefixedHarnessReasons(t *testing.T) {
	entries := readUIReasonEntries(t)
	for _, legacy := range []string{
		"kind_invalid",
		"request_audience_invalid",
		"unknown_type",
		"kind_not_allowed",
		"message_id_conflict",
		"sender_mismatch",
		"sender_kind_mismatch",
		"sender_deregistered",
		"engine_acl_denied",
		"terminal_duplicate",
	} {
		if _, ok := entries[legacy]; ok {
			t.Fatalf("ui/src/errors.js still contains legacy unprefixed harness reason %q", legacy)
		}
	}
}

func readUIReasonEntries(t *testing.T) map[string]string {
	t.Helper()

	b, err := os.ReadFile("../../ui/src/errors.js")
	if err != nil {
		t.Fatalf("read ui/src/errors.js: %v", err)
	}
	re := regexp.MustCompile(`(?m)^\s*([A-Za-z][A-Za-z0-9_]*):\s*\[\s*'([^']+)'`)
	out := map[string]string{}
	for _, match := range re.FindAllSubmatch(b, -1) {
		out[string(match[1])] = string(match[2])
	}
	if len(out) == 0 {
		t.Fatalf("ui/src/errors.js reason table parse found no keys")
	}
	return out
}

func assertUIReasonEntry(t *testing.T, entries map[string]string, key, wantClass string) {
	t.Helper()
	got, ok := entries[key]
	if !ok {
		t.Fatalf("ui/src/errors.js missing reason key %q", key)
	}
	if got != wantClass {
		t.Fatalf("ui/src/errors.js reason %q class=%q want %q", key, got, wantClass)
	}
}

func expectedHarnessUIClass(reason message.HarnessRejectReason) string {
	switch reason {
	case message.HarnessSenderMismatch,
		message.HarnessSenderKindMismatch,
		message.HarnessSenderDeregistered,
		message.HarnessAudienceMemberNotActive,
		message.HarnessResponseUnauthorizedSender,
		message.HarnessResponseStatusNamespaceMismatch:
		return "identity"
	case message.HarnessWorkerFencingStale,
		message.HarnessReservedTypeUnauthorizedSender,
		message.HarnessAudienceHandlerMismatch,
		message.HarnessTerminalDuplicate,
		message.HarnessProvisionalAfterFinal,
		message.HarnessEngineACLDenied:
		return "protocol_system"
	default:
		return "user_input"
	}
}
