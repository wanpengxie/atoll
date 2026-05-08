package xhs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// EnvDaemonHTTP 是 daemon HTTP 入口的 env 名（必填）。
// EnvDaemonToken 是 Bearer token 的 env 名（必填）。
// EnvChannelID 是 dispatch 所属 channel 的 env 名（必填）。
const (
	EnvDaemonHTTP  = "COAGENT_DAEMON_HTTP"
	EnvDaemonToken = "COAGENT_DAEMON_TOKEN"
	EnvChannelID   = "COAGENT_CHANNEL_ID"
)

// daemon 端的 RPC method 名。
const rpcMethodDeviceCommandSend = "device.command.send"

// 业务子 type，与 spec §4.1 align。
const (
	cmdTypePublish       = "xhs.publish"
	cmdTypeSearch        = "xhs.search"
	cmdTypeGetMyRecent   = "xhs.get-my-recent"
	cmdTypeGetNote       = "xhs.get-note"
	cmdTypePublishStatus = "xhs.publish-status"
)

// RealConfig 描述 RealProvider 调 daemon 所需的 endpoint + auth + identity。
type RealConfig struct {
	DaemonHTTP string        // e.g. http://127.0.0.1:7070
	Token      string        // Bearer token
	ChannelID  string        // 当前 channel id（dispatch 落地 channel）
	Timeout    time.Duration // HTTP 超时；零值 30s
	HTTPClient *http.Client  // 可选注入；零值用 timeout 构造的 client
}

// LoadRealConfigFromEnv 从环境变量加载 RealConfig，缺失时返回带 code 的 CodeError。
func LoadRealConfigFromEnv() (RealConfig, error) {
	cfg := RealConfig{
		DaemonHTTP: strings.TrimSpace(os.Getenv(EnvDaemonHTTP)),
		Token:      strings.TrimSpace(os.Getenv(EnvDaemonToken)),
		ChannelID:  strings.TrimSpace(os.Getenv(EnvChannelID)),
	}
	if cfg.DaemonHTTP == "" {
		return cfg, &CodeError{Code: "config_missing", Msg: fmt.Sprintf("%s is required for real backend", EnvDaemonHTTP)}
	}
	if cfg.Token == "" {
		return cfg, &CodeError{Code: "config_missing", Msg: fmt.Sprintf("%s is required for real backend", EnvDaemonToken)}
	}
	if cfg.ChannelID == "" {
		return cfg, &CodeError{Code: "config_missing", Msg: fmt.Sprintf("%s is required for real backend", EnvChannelID)}
	}
	u, err := url.Parse(cfg.DaemonHTTP)
	if err != nil {
		return cfg, &CodeError{Code: "config_invalid", Msg: fmt.Sprintf("invalid %s: %s", EnvDaemonHTTP, err)}
	}
	// url.Parse 几乎不会返回 error；显式校验 absolute URL（scheme + host 非空）。
	if u.Scheme == "" || u.Host == "" {
		return cfg, &CodeError{Code: "config_invalid", Msg: fmt.Sprintf("invalid %s: must be absolute URL with scheme and host (got %q)", EnvDaemonHTTP, cfg.DaemonHTTP)}
	}
	return cfg, nil
}

// RealProvider 把 5 命令统一翻译成 daemon HTTP RPC `device.command.send`。
//
// 行为契约（与 spec §4.1 align）：
//  1. 所有命令仅 dispatch，不阻塞等结果；
//  2. RPC 成功 → 返回 DispatchAck{correlation_id, status:"dispatched"}；
//  3. RPC 网络层失败 → CodeError{Code:"daemon_unreachable"}；
//  4. RPC envelope ok=false → CodeError 透传 daemon 给的 code/message；
//  5. RPC 200 但 result 缺 correlation_id → CodeError{Code:"invalid_daemon_response"}.
type RealProvider struct {
	cfg    RealConfig
	client *http.Client
}

// NewRealProvider 构造 RealProvider；HTTPClient 为空时构造带 timeout 的 client。
func NewRealProvider(cfg RealConfig) *RealProvider {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	c := cfg.HTTPClient
	if c == nil {
		c = &http.Client{Timeout: cfg.Timeout}
	}
	return &RealProvider{cfg: cfg, client: c}
}

// Name 实现 Provider.Name。
func (p *RealProvider) Name() string { return "real" }

// Publish dispatch xhs.publish。返回 DispatchAck（含 correlation_id）。
//
// real 模式契约（spec §5.1.5）：
//   - 不发 inline content；只发 absolute content_path（daemon 端按需读盘）。
//   - images 已由 CLI 层归一化为 []{type:data, value:data:..., fileName} 塞 ImageData，
//     RPC 字段名 "images" 对齐 extension publish-content.ts 期望。
func (p *RealProvider) Publish(ctx context.Context, args PublishArgs) (any, error) {
	params := map[string]any{
		"title":        args.Title,
		"content_path": args.ContentPath,
		"tags":         args.Tags,
	}
	if len(args.ImageData) > 0 {
		params["images"] = args.ImageData
	}
	return p.dispatch(ctx, cmdTypePublish, params)
}

// Search dispatch xhs.search。
func (p *RealProvider) Search(ctx context.Context, args SearchArgs) (any, error) {
	return p.dispatch(ctx, cmdTypeSearch, map[string]any{
		"keyword": args.Keyword,
		"limit":   args.Limit,
	})
}

// GetMyRecent dispatch xhs.get-my-recent。
func (p *RealProvider) GetMyRecent(ctx context.Context, args GetMyRecentArgs) (any, error) {
	return p.dispatch(ctx, cmdTypeGetMyRecent, map[string]any{
		"limit": args.Limit,
	})
}

// GetNote dispatch xhs.get-note。
//
// real 模式 RPC 必须 url，或 (note_id && xsec_token)（CLI 层校验，与 daemon validator
// + extension 端期望对齐 — fix-spec §R4-T1 / round-3 codex#t61.1）。
// xsec_token 单独无 note_id 在 XHS API 上是 dead-end，已在 CLI 层提前拒绝。
func (p *RealProvider) GetNote(ctx context.Context, args GetNoteArgs) (any, error) {
	params := map[string]any{}
	if args.NoteID != "" {
		params["note_id"] = args.NoteID
	}
	if args.URL != "" {
		params["url"] = args.URL
	}
	if args.XsecToken != "" {
		params["xsec_token"] = args.XsecToken
	}
	return p.dispatch(ctx, cmdTypeGetNote, params)
}

// PublishStatus dispatch xhs.publish-status。
func (p *RealProvider) PublishStatus(ctx context.Context, args PublishStatusArgs) (any, error) {
	return p.dispatch(ctx, cmdTypePublishStatus, map[string]any{
		"note_id": args.NoteID,
	})
}

// ---- low level RPC ----

// rpcRequest 是 daemon /rpc 的请求体（与 lightcone/daemon/src/rpc-server.js align）。
type rpcRequest struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

// rpcResponse 是 daemon /rpc 的响应体。
//
// 与 lightcone/daemon/src/rpc-server.js writeJson(res, 200, {ok:true, result}) align：
// 字段名是 `result`，不是 spec §4.1 文字写的 `data`。我们解析以实际为准。
type rpcResponse struct {
	OK     bool             `json:"ok"`
	Result *json.RawMessage `json:"result,omitempty"`
	Error  *EnvelopeError   `json:"error,omitempty"`
}

// EnvelopeError 是 daemon 返回的 envelope 错误负载。
type EnvelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// dispatch 是 RealProvider 5 命令共用的 RPC 调用。返回 DispatchAck。
func (p *RealProvider) dispatch(ctx context.Context, cmdType string, params map[string]any) (DispatchAck, error) {
	endpoint, err := joinURL(p.cfg.DaemonHTTP, "/rpc")
	if err != nil {
		return DispatchAck{}, &CodeError{Code: "config_invalid", Msg: fmt.Sprintf("invalid daemon http: %s", err)}
	}

	body, err := json.Marshal(rpcRequest{
		Method: rpcMethodDeviceCommandSend,
		Params: map[string]any{
			"channel_id": p.cfg.ChannelID,
			"type":       cmdType,
			"params":     params,
			"task_id":    nil,
		},
	})
	if err != nil {
		return DispatchAck{}, fmt.Errorf("marshal rpc request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return DispatchAck{}, fmt.Errorf("build rpc request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.cfg.Token)

	resp, err := p.client.Do(req)
	if err != nil {
		return DispatchAck{}, &CodeError{
			Code: "daemon_unreachable",
			Msg:  fmt.Sprintf("failed to reach daemon at %s: %s", p.cfg.DaemonHTTP, err),
		}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return DispatchAck{}, &CodeError{
			Code: "daemon_read_failed",
			Msg:  fmt.Sprintf("read daemon response: %s", err),
		}
	}

	if len(bytes.TrimSpace(raw)) == 0 {
		return DispatchAck{}, &CodeError{
			Code: "invalid_daemon_response",
			Msg:  fmt.Sprintf("daemon returned empty body (status=%d)", resp.StatusCode),
		}
	}

	var env rpcResponse
	if err := json.Unmarshal(raw, &env); err != nil {
		return DispatchAck{}, &CodeError{
			Code: "invalid_daemon_response",
			Msg:  fmt.Sprintf("decode daemon envelope: %s (status=%d body=%s)", err, resp.StatusCode, truncate(string(raw), 200)),
		}
	}

	if !env.OK {
		code := "rpc_error"
		msg := fmt.Sprintf("daemon RPC %s failed", rpcMethodDeviceCommandSend)
		if env.Error != nil {
			if env.Error.Code != "" {
				code = env.Error.Code
			}
			if env.Error.Message != "" {
				msg = env.Error.Message
			}
		}
		return DispatchAck{}, &CodeError{Code: code, Msg: msg}
	}

	if env.Result == nil {
		return DispatchAck{}, &CodeError{
			Code: "invalid_daemon_response",
			Msg:  "daemon ok=true but result missing",
		}
	}

	var result struct {
		CorrelationID string         `json:"correlation_id"`
		SelfCheck     map[string]any `json:"self_check,omitempty"`
	}
	if err := json.Unmarshal(*env.Result, &result); err != nil {
		return DispatchAck{}, &CodeError{
			Code: "invalid_daemon_response",
			Msg:  fmt.Sprintf("decode result: %s", err),
		}
	}
	if strings.TrimSpace(result.CorrelationID) == "" {
		return DispatchAck{}, &CodeError{
			Code: "invalid_daemon_response",
			Msg:  "daemon result missing correlation_id",
		}
	}

	return DispatchAck{
		CorrelationID: result.CorrelationID,
		Status:        "dispatched",
		SelfCheck:     result.SelfCheck,
	}, nil
}

// ---- helpers ----

// joinURL 把 base + path 合并成绝对 url，处理尾部斜杠。
func joinURL(base, path string) (string, error) {
	u, err := url.Parse(strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/"))
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
