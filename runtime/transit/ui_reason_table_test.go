package transit

import (
	"os"
	"regexp"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestUIReasonTableCoversWireRejectReasons(t *testing.T) {
	keys := readUIReasonKeys(t)

	for _, reason := range message.AllHarnessRejectReasons {
		assertUIReasonKey(t, keys, string(reason))
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
		assertUIReasonKey(t, keys, string(reason))
	}
	for _, reason := range []string{
		RejectReasonAuthFailed,
		RejectReasonChannelUnbound,
		RejectReasonInternal,
		RejectReasonReplayWindow,
		RejectReasonReplayNonce,
	} {
		assertUIReasonKey(t, keys, reason)
	}
}

func TestUIReasonTableDropsLegacyUnprefixedHarnessReasons(t *testing.T) {
	keys := readUIReasonKeys(t)
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
		if _, ok := keys[legacy]; ok {
			t.Fatalf("ui/src/errors.js still contains legacy unprefixed harness reason %q", legacy)
		}
	}
}

func readUIReasonKeys(t *testing.T) map[string]struct{} {
	t.Helper()

	b, err := os.ReadFile("../../ui/src/errors.js")
	if err != nil {
		t.Fatalf("read ui/src/errors.js: %v", err)
	}
	re := regexp.MustCompile(`(?m)^\s*([A-Za-z][A-Za-z0-9_]*):\s*\[`)
	out := map[string]struct{}{}
	for _, match := range re.FindAllSubmatch(b, -1) {
		out[string(match[1])] = struct{}{}
	}
	if len(out) == 0 {
		t.Fatalf("ui/src/errors.js reason table parse found no keys")
	}
	return out
}

func assertUIReasonKey(t *testing.T, keys map[string]struct{}, key string) {
	t.Helper()
	if _, ok := keys[key]; !ok {
		t.Fatalf("ui/src/errors.js missing reason key %q", key)
	}
}
