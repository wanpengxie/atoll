// Package feishu is the launch 飞书 (Lark) outbound adapter. It runs as
// an in-process daemon actor (tool:feishu-adapter) with
// Binding=runtime_outbound and handles every feishu.* request type by
// calling the public Feishu OpenAPI on https://open.feishu.cn.
//
// Coverage (launch baseline):
//
//   - feishu.chat.send   — POST /im/v1/messages (receive_id_type=chat_id)
//   - feishu.chat.create — POST /im/v1/chats
//
// Inbound (Feishu events → channel) is out of scope for launch per §T4
// "不做"; the adapter is outbound-only.
//
// Credential surface (read from framework.CredentialStore at Init):
//
//   - feishu.app_id      — app identifier (printed at INFO, NOT redacted)
//   - feishu.app_secret  — secret key (NEVER printed; redacted on every log)
//
// tenant_access_token is fetched lazily via
// /auth/v3/tenant_access_token/internal and cached locally per L4
// §2.5.x token lifecycle (2 hour TTL, refresh 60 s before expiry).
//
// The adapter registers itself into the framework registry from init()
// so a daemon that blank-imports `adapters/feishu` automatically picks
// it up via framework.BuildAllRegistered.
package feishu
