// errors.js — L1 §10.3 + L2 §1.8 reason closed-set → UI placement + 中文 i18n.
//
// Authoritative spec: impl-layer3.md §1.4.1 (Error reason → UI 映射)
// + proto-layer1.md §2.11.1 reason 闭集.
//
// 5 classes per §8.3:
//   user_input        — caller-correctable; show inline under composer
//   identity          — auth/identity; show under composer + admin link
//   protocol_system   — caller shouldn't see detail; generic message,
//                       detail goes to system-events drawer
//   failed_terminal   — render under request row in chat
//   install_system    — operator-only; system-events drawer
//
// 中文 string table lives here so renderers don't sprinkle Chinese
// throughout — easier to find / audit / extract to gettext later.

export const REASON_CLASS = Object.freeze({
  USER_INPUT: 'user_input',
  IDENTITY: 'identity',
  PROTOCOL_SYSTEM: 'protocol_system',
  FAILED_TERMINAL: 'failed_terminal',
  INSTALL_SYSTEM: 'install_system',
});

// Reason → class + 中文.
//
// Keep this table in lockstep with kernel/message.AllHarnessRejectReasons,
// kernel/message.AllInstallReasons, kernel/message.AllTerminalFailureReasons,
// kernel/daemonbus.MuxRejectReason, and daemon-edge write_message rejects.
// Adding a new wire reason must update this file.
const REASON_TABLE = Object.freeze({
  // --- harness reject ---------------------------------------------------
  harness_worker_fencing_stale: ['protocol_system', 'worker fencing 已过期'],
  harness_envelope_field_missing: ['user_input', '缺少必填字段'],
  harness_channel_mismatch: ['user_input', 'channel_id 不匹配'],
  harness_kind_invalid: ['user_input', 'kind 字段非法'],
  harness_visibility_invalid: ['user_input', 'visibility 字段非法'],
  harness_visibility_audience_invalid: ['user_input', 'private visibility 必须指定 audience'],
  harness_envelope_unknown_field: ['user_input', '消息包含未知字段'],
  harness_id_duplicate_conflict: ['user_input', '消息 id 冲突'],
  harness_time_invalid: ['user_input', '消息时间字段非法'],
  harness_type_unknown: ['user_input', '未知 type'],
  harness_kind_not_allowed_for_type: ['user_input', '此 type 不允许该 kind'],
  harness_reserved_type_unauthorized_sender: ['protocol_system', 'system type 只能由系统发送'],
  harness_sender_mismatch: ['identity', 'sender 与 caller 不符'],
  harness_sender_kind_mismatch: ['identity', 'sender kind 与 actor_registry 不符'],
  harness_sender_deregistered: ['identity', 'sender actor 已注销'],
  harness_audience_empty: ['user_input', 'audience 不能为空'],
  harness_audience_wildcard_forbidden: ['user_input', 'audience 不再支持 *，请使用明确 actor id'],
  harness_audience_member_not_active: ['identity', 'audience 成员不可用'],
  harness_request_audience_invalid: ['user_input', 'request 必须恰好一个 audience'],
  harness_response_audience_invalid: ['user_input', 'response audience 非法'],
  harness_audience_handler_mismatch: ['protocol_system', 'audience handler 不匹配'],
  harness_response_missing_parent: ['user_input', 'response 缺少 parent_id'],
  harness_response_parent_not_found: ['user_input', 'response parent 不存在'],
  harness_response_parent_not_request: ['user_input', 'response parent 不是 request'],
  harness_response_status_invalid: ['user_input', 'response status 非法'],
  harness_response_status_namespace_mismatch: ['identity', 'response status namespace 与 sender 不匹配'],
  harness_response_reason_invalid: ['user_input', 'response reason 非法'],
  harness_response_unauthorized_sender: ['identity', 'response sender 无权回复该 request'],
  harness_response_audience_mismatch: ['user_input', 'response audience 与 request 不匹配'],
  harness_terminal_duplicate: ['protocol_system', '重复 terminal'],
  harness_provisional_after_final: ['protocol_system', 'final response 之后不可再发 provisional'],
  harness_engine_acl_denied: ['protocol_system', '服务器内部权限异常'],

  // --- daemonbus mux reject --------------------------------------------
  mux_unknown_frame_kind: ['protocol_system', '未知 daemonbus frame_kind'],
  mux_unknown_frame_field: ['protocol_system', 'daemonbus frame 包含未知字段'],
  mux_unknown_payload_field: ['protocol_system', 'daemonbus payload 包含未知字段'],
  mux_payload_schema_invalid: ['protocol_system', 'daemonbus payload schema 非法'],
  mux_protocol_version_unsupported: ['protocol_system', 'daemonbus 协议版本不支持'],
  mux_auth_failed: ['protocol_system', '连接认证失败，请稍后重试或联系 admin'],
  mux_duplicate_daemon: ['protocol_system', 'daemon 重复连接'],
  mux_channel_id_unknown: ['protocol_system', 'channel_id 未知'],
  mux_owner_epoch_stale: ['protocol_system', 'owner epoch 已过期'],
  mux_frame_too_large: ['protocol_system', 'daemonbus frame 过大'],
  mux_idle_timeout: ['protocol_system', 'daemonbus 连接空闲超时'],
  mux_internal_error: ['protocol_system', 'daemonbus 内部错误'],

  // --- daemon-edge write_message reject --------------------------------
  auth_failed: ['identity', '认证失败 — 请重新登录'],
  channel_unbound: ['protocol_system', '当前 daemon 未绑定该 channel'],
  internal: ['protocol_system', 'daemon 内部错误'],
  replay_window_expired: ['identity', '认证时间窗口已过期，请重试'],
  replay_nonce_seen: ['identity', '重复请求已拒绝，请重试'],

  // --- failed_terminal --------------------------------------------------
  unanswered_timeout: ['failed_terminal', '无响应超时'],
  receiver_internal_error: ['failed_terminal', 'receiver 内部错误'],
  receiver_unavailable: ['failed_terminal', 'receiver 不在线'],

  // --- install_system ---------------------------------------------------
  adapter_timeout_missing: ['install_system', 'adapter 未声明 max_pending_ms'],
  handler_actor_not_registered: ['install_system', 'handler actor 未注册'],
  handler_actor_binding_mismatch: ['install_system', 'handler binding 不匹配'],
  type_registry_invalid: ['install_system', 'type_registry 行非法'],
  type_registry_reserved_namespace: ['install_system', 'type_registry 保留命名空间不可安装'],
  worker_lock_held: ['install_system', 'worker lock 被占'],
  bootstrap_in_progress: ['install_system', 'bootstrap 进行中'],
});

/**
 * Resolve a reason string to its UI classification + 中文 label.
 * Returns { class, label } — class is one of REASON_CLASS values.
 */
export function classifyReason(reason) {
  if (!reason || typeof reason !== 'string') {
    return { class: REASON_CLASS.USER_INPUT, label: '消息发送失败' };
  }
  const entry = REASON_TABLE[reason];
  if (!entry) {
    if (reason.startsWith('harness_') || reason.startsWith('mux_')) {
      return { class: REASON_CLASS.PROTOCOL_SYSTEM, label: reason };
    }
    return { class: REASON_CLASS.USER_INPUT, label: reason };
  }
  return { class: entry[0], label: entry[1] };
}

/**
 * Build the composer-area error string for a class. Always returns a
 * non-empty string so the inline error bar shows something even when the
 * reason is unknown.
 */
export function composerMessage(reason) {
  const { class: cls, label } = classifyReason(reason);
  switch (cls) {
    case REASON_CLASS.USER_INPUT:
      return `消息发送失败: ${label}`;
    case REASON_CLASS.IDENTITY:
      return `身份/权限错误: ${label}（请联系 admin）`;
    case REASON_CLASS.PROTOCOL_SYSTEM:
      return `消息无法发送（系统问题: ${reason}）`;
    case REASON_CLASS.FAILED_TERMINAL:
      // Failed terminals are surfaced under the request row, not the
      // composer — but if a sync path returns one we still degrade.
      return `失败：${label}`;
    case REASON_CLASS.INSTALL_SYSTEM:
      // Install reasons shouldn't reach the composer; show a stub so
      // operators notice if they ever do.
      return `系统配置错误: ${reason}（联系运维）`;
    default:
      return `消息发送失败: ${label}`;
  }
}

/**
 * Build the under-request-row failure label for a failed terminal
 * envelope. Reads payload.reason; falls back to payload.error / generic.
 */
export function failedTerminalLabel(responseEnvelope) {
  const payload = responseEnvelope?.payload;
  const reason = (payload && typeof payload === 'object' && (payload.reason || payload.error)) || '';
  const { label } = classifyReason(reason);
  return `✗ 失败：${label}`;
}
