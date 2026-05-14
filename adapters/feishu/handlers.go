package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/coagent-ai/coagent/adapters/framework"
	"github.com/coagent-ai/coagent/kernel/adapter"
	"github.com/coagent-ai/coagent/kernel/message"
)

// Closed set of envelope.type values this adapter handles. Order is
// preserved in Declaration.Types so type_registry rows land
// deterministically.
const (
	TypeChatSend   = "feishu.chat.send"
	TypeChatCreate = "feishu.chat.create"
)

// AllTypes is the canonical Declaration.Types slice the adapter exposes.
var AllTypes = []string{TypeChatSend, TypeChatCreate}

// ChatSendPayload is the request payload schema for feishu.chat.send.
type ChatSendPayload struct {
	// ChatID is the receive_id (chat_id form). Either ChatID or OpenID
	// is required; ChatID takes precedence when both are set.
	ChatID string `json:"chat_id,omitempty"`

	// OpenID is the alternative receive_id (open_id form) — used for
	// private DMs.
	OpenID string `json:"open_id,omitempty"`

	// Text is the plain-text body (msg_type=text).
	Text string `json:"text"`
}

// Validate returns a friendly error if a required field is missing.
func (p ChatSendPayload) Validate() error {
	if p.ChatID == "" && p.OpenID == "" {
		return errors.New("feishu.chat.send: chat_id or open_id required")
	}
	if p.Text == "" {
		return errors.New("feishu.chat.send: text required")
	}
	return nil
}

// ChatSendResult is the response data block adapters emit to the channel.
type ChatSendResult struct {
	// MessageID is the feishu-side im_message_<hash> id.
	MessageID string `json:"message_id"`

	// ChatID echoes the chat the message landed in (useful for follow-up).
	ChatID string `json:"chat_id,omitempty"`
}

// handleChatSend implements feishu.chat.send.
func (m *Module) handleChatSend(ctx context.Context, env *message.Envelope) error {
	var payload ChatSendPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return m.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
	}
	if err := payload.Validate(); err != nil {
		return m.fail(ctx, env, "payload_invalid", err.Error())
	}
	receiveIDType := "chat_id"
	receiveID := payload.ChatID
	if receiveID == "" {
		receiveIDType = "open_id"
		receiveID = payload.OpenID
	}

	contentBytes, err := json.Marshal(map[string]string{"text": payload.Text})
	if err != nil {
		return m.fail(ctx, env, "payload_invalid", fmt.Sprintf("marshal content: %v", err))
	}
	data, err := m.client.SendMessage(ctx, receiveIDType, SendMessageRequest{
		ReceiveID: receiveID,
		MsgType:   "text",
		Content:   string(contentBytes),
	})
	if err != nil {
		return m.handleAPIError(ctx, env, "send_message", err)
	}

	result := ChatSendResult{
		MessageID: data.MessageID,
		ChatID:    data.ChatID,
	}
	raw, _ := json.Marshal(result)
	_, err = m.mctx.Respond(ctx, env.ID, raw, adapter.RespondOptions{Status: "completed"})
	if err != nil {
		m.logger.Warn("feishu.chat.send.respond.error",
			"request_id", env.ID,
			"err", err.Error())
		return err
	}
	m.metrics.IncCounter("adapter.feishu.send.ok")
	return nil
}

// ChatCreatePayload is the request payload schema for feishu.chat.create.
type ChatCreatePayload struct {
	// Name is the group name (required).
	Name string `json:"name"`

	// Description is the optional group description.
	Description string `json:"description,omitempty"`

	// UserIDs is the list of users (open_id form) to add.
	UserIDs []string `json:"user_ids,omitempty"`
}

// Validate returns a friendly error if a required field is missing.
func (p ChatCreatePayload) Validate() error {
	if p.Name == "" {
		return errors.New("feishu.chat.create: name required")
	}
	return nil
}

// ChatCreateResult is the response data block.
type ChatCreateResult struct {
	ChatID string `json:"chat_id"`
}

// handleChatCreate implements feishu.chat.create.
func (m *Module) handleChatCreate(ctx context.Context, env *message.Envelope) error {
	var payload ChatCreatePayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return m.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
	}
	if err := payload.Validate(); err != nil {
		return m.fail(ctx, env, "payload_invalid", err.Error())
	}
	data, err := m.client.CreateChat(ctx, CreateChatRequest{
		Name:        payload.Name,
		Description: payload.Description,
		UserIDList:  payload.UserIDs,
	})
	if err != nil {
		return m.handleAPIError(ctx, env, "create_chat", err)
	}
	result := ChatCreateResult{ChatID: data.ChatID}
	raw, _ := json.Marshal(result)
	_, err = m.mctx.Respond(ctx, env.ID, raw, adapter.RespondOptions{Status: "completed"})
	if err != nil {
		m.logger.Warn("feishu.chat.create.respond.error",
			"request_id", env.ID,
			"err", err.Error())
		return err
	}
	m.metrics.IncCounter("adapter.feishu.create.ok")
	return nil
}

// handleAPIError converts a Feishu transport / envelope error into a
// status=failed terminal response. The error message is run through
// the framework redact helper indirectly (the client already redacts
// before returning); we still pass it through fail() which calls the
// adapter's local redact for double safety.
func (m *Module) handleAPIError(ctx context.Context, env *message.Envelope, op string, err error) error {
	var apiErr *APIError
	reason := op + "_failed"
	detail := err.Error()
	if errors.As(err, &apiErr) {
		reason = fmt.Sprintf("feishu_code_%d", apiErr.Code)
		detail = fmt.Sprintf("%s: %s", op, apiErr.Msg)
	}
	m.metrics.IncCounter("adapter.feishu."+op+".error", "reason", reason)
	return m.fail(ctx, env, reason, detail)
}

// fail emits a status=failed Respond terminal with the supplied
// reason+detail. detail is passed through the adapter's redact wrapper
// to ensure any leaked secret substring is scrubbed.
func (m *Module) fail(ctx context.Context, env *message.Envelope, reason, detail string) error {
	redacted := m.redactString(detail)
	payload, err := json.Marshal(map[string]any{
		"detail": redacted,
	})
	if err != nil {
		return fmt.Errorf("feishu: marshal fail payload: %w", err)
	}
	_, err = m.mctx.Respond(ctx, env.ID, payload, adapter.RespondOptions{
		Status: "failed",
		Reason: reason,
	})
	if err != nil {
		m.logger.Error("feishu.fail.respond.error",
			"request_id", env.ID,
			"reason", reason,
			"err", err.Error())
		return err
	}
	m.logger.Warn("feishu.handler.failed",
		"type", env.Type,
		"request_id", env.ID,
		"reason", reason,
		"detail", redacted,
	)
	return nil
}

// redactString scrubs any credential substring from msg.
func (m *Module) redactString(msg string) string {
	secrets := []string{m.creds.AppSecret}
	if tok, _ := m.tokens.snapshot(); tok != "" {
		secrets = append(secrets, tok)
	}
	return framework.RedactSubstrings(msg, secrets...)
}
