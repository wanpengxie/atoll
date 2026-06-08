package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

// Closed set of envelope.type values this adapter handles.
const (
	TypeChatSend   = "feishu.chat.send"
	TypeChatCreate = "feishu.chat.create"
)

// AllTypes is the canonical types slice the adapter exposes.
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
func (a *Actor) handleChatSend(ctx context.Context, env *message.Envelope) error {
	var payload ChatSendPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
	}
	if err := payload.Validate(); err != nil {
		return a.fail(ctx, env, "payload_invalid", err.Error())
	}
	receiveIDType := "chat_id"
	receiveID := payload.ChatID
	if receiveID == "" {
		receiveIDType = "open_id"
		receiveID = payload.OpenID
	}

	contentBytes, err := json.Marshal(map[string]string{"text": payload.Text})
	if err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("marshal content: %v", err))
	}
	data, err := a.client.SendMessage(ctx, receiveIDType, SendMessageRequest{
		ReceiveID: receiveID,
		MsgType:   "text",
		Content:   string(contentBytes),
	})
	if err != nil {
		return a.handleAPIError(ctx, env, "send_message", err)
	}

	result := ChatSendResult{
		MessageID: data.MessageID,
		ChatID:    data.ChatID,
	}
	raw, _ := json.Marshal(result)
	_, err = behavior.Respond(ctx, a.writer, a.clock, env,
		message.Sender{Kind: actor.KindTool, ID: a.actorID},
		behavior.ResponseSpec{Status: "completed", Payload: raw})
	if err != nil {
		a.logger.Warn("feishu.chat.send.respond.error",
			"request_id", string(env.ID),
			"err", err.Error())
		return err
	}
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
func (a *Actor) handleChatCreate(ctx context.Context, env *message.Envelope) error {
	var payload ChatCreatePayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
	}
	if err := payload.Validate(); err != nil {
		return a.fail(ctx, env, "payload_invalid", err.Error())
	}
	data, err := a.client.CreateChat(ctx, CreateChatRequest{
		Name:        payload.Name,
		Description: payload.Description,
		UserIDList:  payload.UserIDs,
	})
	if err != nil {
		return a.handleAPIError(ctx, env, "create_chat", err)
	}
	result := ChatCreateResult{ChatID: data.ChatID}
	raw, _ := json.Marshal(result)
	_, err = behavior.Respond(ctx, a.writer, a.clock, env,
		message.Sender{Kind: actor.KindTool, ID: a.actorID},
		behavior.ResponseSpec{Status: "completed", Payload: raw})
	if err != nil {
		a.logger.Warn("feishu.chat.create.respond.error",
			"request_id", string(env.ID),
			"err", err.Error())
		return err
	}
	return nil
}

// handleAPIError converts a Feishu transport / envelope error into a
// status=failed terminal response.
func (a *Actor) handleAPIError(ctx context.Context, env *message.Envelope, op string, err error) error {
	var apiErr *APIError
	errorCode := op + "_failed"
	detail := err.Error()
	if errors.As(err, &apiErr) {
		errorCode = fmt.Sprintf("feishu_code_%d", apiErr.Code)
		detail = fmt.Sprintf("%s: %s", op, apiErr.Msg)
	}
	return a.fail(ctx, env, errorCode, detail)
}

// fail emits a status=failed Respond terminal with a closed-set terminal
// reason. The adapter-specific code is preserved in payload.error_code;
// detail is passed through the adapter's redact wrapper.
func (a *Actor) fail(ctx context.Context, env *message.Envelope, errorCode, detail string) error {
	redacted := a.redactString(detail)
	payload, _ := json.Marshal(map[string]any{"detail": redacted, "error_code": errorCode})
	_, err := behavior.Respond(ctx, a.writer, a.clock, env,
		message.Sender{Kind: actor.KindTool, ID: a.actorID},
		behavior.ResponseSpec{
			Status:  "failed",
			Reason:  string(message.TerminalReceiverInternalError),
			Payload: payload,
		})
	if err != nil {
		a.logger.Error("feishu.fail.respond.error",
			"request_id", string(env.ID),
			"error_code", errorCode,
			"err", err)
	}
	return err
}

// redactString scrubs any credential substring from msg.
func (a *Actor) redactString(msg string) string {
	secrets := []string{a.creds.AppSecret}
	if tok, _ := a.tokens.snapshot(); tok != "" {
		secrets = append(secrets, tok)
	}
	for _, s := range secrets {
		if s != "" {
			msg = strings.ReplaceAll(msg, s, "***")
		}
	}
	return msg
}
