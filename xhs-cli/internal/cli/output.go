package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/coagent-ai/xhs-cli/internal/xhs"
)

// Exit code 约定：
//   0 = ok
//   1 = runtime/IO/daemon 错误（也包含 provider 业务错误）
//   3 = usage 错误（参数缺失 / env 无效）
const (
	ExitOK          = 0
	ExitRuntime     = 1
	ExitUsageError  = 3
)

// Envelope 是 stdout 输出的统一外壳。
//
// 成功:  {"ok":true,  "data": {...}}
// 失败:  {"ok":false, "error": {"code":"...", "message":"..."}}
type Envelope struct {
	OK    bool         `json:"ok"`
	Data  any          `json:"data,omitempty"`
	Error *EnvelopeErr `json:"error,omitempty"`
}

// EnvelopeErr 是 envelope 错误负载。
type EnvelopeErr struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// CLIError 表示 provider/CLI 内部带 code 的错误，可被 WriteErr 解开成 envelope。
type CLIError struct {
	Code    string
	Message string
}

func (e *CLIError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewCLIError 构造带 code 的 CLIError。
func NewCLIError(code, format string, args ...any) *CLIError {
	return &CLIError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// WriteOK 把成功 envelope 写到 w。
func WriteOK(w io.Writer, data any) error {
	return writeJSON(w, Envelope{OK: true, Data: data})
}

// WriteErr 把错误 envelope 写到 w。
func WriteErr(w io.Writer, code, message string) error {
	return writeJSON(w, Envelope{OK: false, Error: &EnvelopeErr{Code: code, Message: message}})
}

// WriteErrFrom 根据 error 类型自动选 code：
//   - *CLIError       → 用其 Code/Message
//   - *xhs.CodeError  → 透传 provider 给的 Code/Msg
//   - 其他 error      → code="internal_error"
func WriteErrFrom(w io.Writer, err error) error {
	if err == nil {
		return WriteErr(w, "internal_error", "nil error")
	}
	var ce *CLIError
	if errors.As(err, &ce) && ce != nil {
		return WriteErr(w, ce.Code, ce.Message)
	}
	var xe *xhs.CodeError
	if errors.As(err, &xe) && xe != nil {
		return WriteErr(w, xe.Code, xe.Msg)
	}
	return WriteErr(w, "internal_error", err.Error())
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// EmitErrAndExit 写错误 envelope 到 stdout 后以指定 code 退出。
// 仅在命令最外层调用；不要在内部库里调用。
func EmitErrAndExit(code int, errCode, message string) {
	_ = WriteErr(os.Stdout, errCode, message)
	os.Exit(code)
}
