// errors.js — L1 §10.3 reason closed-set → UI placement + 中文 i18n.
//
// Authoritative spec: .dalek/pm/v4-layer3-spec.md §8.3 (Error reason → UI
// 映射) + .dalek/pm/v4-layer1-spec.md §10.3 reason 闭集 (~20 reasons).
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
// Reasons NOT in this table fall back to {class: USER_INPUT, label: reason}
// so an unknown server reason still surfaces visibly during development.
const REASON_TABLE = Object.freeze({
  // --- user_input -------------------------------------------------------
  missing_required_field: ['user_input', '缺少必填字段'],
  kind_invalid: ['user_input', 'kind 字段非法'],
  request_audience_invalid: ['user_input', 'request 必须恰好一个 audience'],
  doc_refs_invalid: ['user_input', 'doc_refs 字段非法'],
  unknown_type: ['user_input', '未知 type'],
  kind_not_allowed: ['user_input', '此 type 不允许该 kind'],
  response_missing_parent_id: ['user_input', 'response 缺少 parent_id'],
  message_id_conflict: ['user_input', '消息 id 冲突'],

  // --- identity ---------------------------------------------------------
  auth_failed: ['identity', '认证失败 — 请重新登录'],
  sender_mismatch: ['identity', 'sender 与 caller 不符'],
  sender_kind_mismatch: ['identity', 'sender kind 与 actor_registry 不符'],
  sender_deregistered: ['identity', 'sender actor 已注销'],
  engine_acl_denied: ['identity', '权限不足'],

  // --- protocol_system --------------------------------------------------
  terminal_duplicate: ['protocol_system', '重复 terminal'],
  audience_actor_not_registered: ['protocol_system', 'audience actor 未注册'],
  audience_handler_mismatch: ['protocol_system', 'audience handler 不匹配'],
  response_parent_invalid: ['protocol_system', 'response parent 非法'],
  worker_fencing_stale: ['protocol_system', 'worker fencing 已过期'],

  // --- failed_terminal --------------------------------------------------
  unanswered_timeout: ['failed_terminal', '无响应超时'],
  adapter_default_timeout: ['failed_terminal', 'adapter 默认超时'],
  receiver_unavailable: ['failed_terminal', 'receiver 不在线'],
  human_unanswered_timeout: ['failed_terminal', 'human 未回应超时'],

  // --- install_system ---------------------------------------------------
  adapter_timeout_missing: ['install_system', 'adapter 未声明 max_pending_ms'],
  handler_actor_not_registered: ['install_system', 'handler actor 未注册'],
  handler_actor_binding_mismatch: ['install_system', 'handler binding 不匹配'],
  type_registry_invalid: ['install_system', 'type_registry 行非法'],
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
