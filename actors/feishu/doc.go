// Package feishu is the 飞书 (Lark) outbound actor. It implements
// actorrt.Actor (tool:feishu-adapter) and handles every feishu.*
// request type by calling the public Feishu OpenAPI on
// https://open.feishu.cn.
//
// Coverage:
//
//   - feishu.chat.send   — POST /im/v1/messages (receive_id_type=chat_id)
//   - feishu.chat.create — POST /im/v1/chats
//
// Inbound (Feishu events -> channel) is out of scope; the actor is
// outbound-only.
//
// Credential surface (read from environment variables):
//
//   - FEISHU_APP_ID      — app identifier (printed at INFO, NOT redacted)
//   - FEISHU_APP_SECRET  — secret key (NEVER printed; redacted on every log)
//
// tenant_access_token is fetched lazily via
// /auth/v3/tenant_access_token/internal and cached locally (2 hour TTL,
// refresh 60 s before expiry).
package feishu
