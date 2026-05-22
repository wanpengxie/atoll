package daemonbus

import "testing"

func TestDeviceSessionRejectReasonClosedSet(t *testing.T) {
	t.Parallel()
	got := []DeviceSessionRejectReason{
		DeviceSessionRejectBindChannelNotActive,
		DeviceSessionRejectBindAdapterNotPresent,
		DeviceSessionRejectBindAdapterBindingInvalid,
		DeviceSessionRejectBindDeviceTypeUnsupported,
		DeviceSessionRejectBindCapacityExceeded,
		DeviceSessionRejectBindInternalError,
		DeviceSessionRejectTokenInvalid,
		DeviceSessionRejectTokenExpired,
		DeviceSessionRejectSessionRevoked,
		DeviceSessionRejectSessionUnknown,
		DeviceSessionRejectTransitPayloadTooLarge,
		DeviceSessionRejectTransitRouteUnavailable,
		DeviceSessionRejectTransitInternalError,
		DeviceSessionRejectUnbindSessionUnknown,
		DeviceSessionRejectUnbindInternalError,
	}
	want := []string{
		"bind_channel_not_active",
		"bind_adapter_not_present",
		"bind_adapter_binding_invalid",
		"bind_device_type_unsupported",
		"bind_capacity_exceeded",
		"bind_internal_error",
		"device_token_invalid",
		"device_token_expired",
		"device_session_revoked",
		"device_session_unknown",
		"transit_payload_too_large",
		"transit_route_unavailable",
		"transit_internal_error",
		"unbind_session_unknown",
		"unbind_internal_error",
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
