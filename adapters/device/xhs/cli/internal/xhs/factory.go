package xhs

import (
	"fmt"
	"os"
	"strings"
)

// EnvBackend 是 provider 模式的环境变量名。
//
// 取值（大小写不敏感）：
//   - "" / "mock" → MockProvider（默认）
//   - "real"      → RealProvider（走 daemon HTTP RPC）
const (
	EnvBackend = "COAGENT_XHS_BACKEND"
)

// Backend 是合法的 provider 模式枚举。
type Backend string

const (
	BackendMock Backend = "mock"
	BackendReal Backend = "real"
)

// NewProviderFromEnv 按 EnvBackend 环境变量构造 Provider。
func NewProviderFromEnv() (Provider, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(EnvBackend)))
	switch raw {
	case "", string(BackendMock):
		return NewMockProvider(), nil
	case string(BackendReal):
		cfg, err := LoadRealConfigFromEnv()
		if err != nil {
			return nil, err
		}
		return NewRealProvider(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported %s value %q (want mock|real)", EnvBackend, raw)
	}
}

// IsRealBackendFromEnv 仅检查 EnvBackend 是否被显式设置为 real，
// 不构造 Provider、不读其它依赖 env。用于 CLI 校验早期分支判断。
func IsRealBackendFromEnv() bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(EnvBackend)))
	return raw == string(BackendReal)
}

// NewProvider 按显式 backend 构造 Provider（测试方便）。
func NewProvider(b Backend) (Provider, error) {
	switch b {
	case "", BackendMock:
		return NewMockProvider(), nil
	case BackendReal:
		cfg, err := LoadRealConfigFromEnv()
		if err != nil {
			return nil, err
		}
		return NewRealProvider(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported backend %q (want mock|real)", b)
	}
}
