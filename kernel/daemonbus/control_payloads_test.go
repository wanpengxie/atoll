package daemonbus

import "testing"

func TestMuxRejectReasonClosedSet(t *testing.T) {
	t.Parallel()
	got := []MuxRejectReason{
		MuxRejectUnknownFrameKind,
		MuxRejectUnknownFrameField,
		MuxRejectUnknownPayloadField,
		MuxRejectPayloadSchemaInvalid,
		MuxRejectProtocolVersionUnsupported,
		MuxRejectAuthFailed,
		MuxRejectDuplicateDaemon,
		MuxRejectChannelIDUnknown,
		MuxRejectOwnerEpochStale,
		MuxRejectFrameTooLarge,
		MuxRejectIdleTimeout,
		MuxRejectInternalError,
	}
	want := []string{
		"mux_unknown_frame_kind",
		"mux_unknown_frame_field",
		"mux_unknown_payload_field",
		"mux_payload_schema_invalid",
		"mux_protocol_version_unsupported",
		"mux_auth_failed",
		"mux_duplicate_daemon",
		"mux_channel_id_unknown",
		"mux_owner_epoch_stale",
		"mux_frame_too_large",
		"mux_idle_timeout",
		"mux_internal_error",
	}
	if len(got) != len(want) {
		t.Fatalf("reason count=%d want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("reason[%d]=%q want %q", i, got[i], want[i])
		}
	}
}
