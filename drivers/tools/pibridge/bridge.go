// Package pibridge owns the private Go↔Node process boundary shared by the Pi
// LLM and Pi workspace actors. The JavaScript and all Pi dependencies are a
// build-time bundle embedded in the Atoll executable; no npm install occurs at
// actor start.
package pibridge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

const Version = "pi-0.85.1-atoll-v8"
const BundleSHA256 = "041367f893bc6a54083c78c090d23f5a78d19fb768fa81870b4ccc5a4dafb3bf"
const maxBridgeFrameBytes = 24 << 20

//go:embed bridge.mjs
var source []byte

type frame struct {
	Status       int             `json:"status,omitempty"`
	ProviderCode string          `json:"provider_code,omitempty"`
	RetryAfterMS int64           `json:"retry_after_ms,omitempty"`
	ID           string          `json:"id,omitempty"`
	Kind         string          `json:"kind"`
	Code         string          `json:"code,omitempty"`
	Detail       string          `json:"detail,omitempty"`
	Value        json.RawMessage `json:"value,omitempty"`
	Event        json.RawMessage `json:"event,omitempty"`
	Protocol     int             `json:"protocol,omitempty"`
	Node         string          `json:"node,omitempty"`
	Pi           string          `json:"pi,omitempty"`
}

type call struct {
	frames chan frame
	done   chan struct{}
}

type Bridge struct {
	cmd     *exec.Cmd
	in      io.WriteCloser
	writeMu sync.Mutex
	mu      sync.Mutex
	pending map[string]*call
	done    chan struct{}
	err     error
	logger  *slog.Logger
}

type EnvironmentPolicy uint8

const (
	// RuntimeEnvironment is sufficient for Pi's local workspace tools and faux
	// provider. Provider credentials are not copied into this child.
	RuntimeEnvironment EnvironmentPolicy = iota
	// ProviderEnvironment additionally admits Pi's documented provider auth and
	// network configuration variables.
	ProviderEnvironment
)

func Start(ctx context.Context, node, workspace string, logger *slog.Logger, policies ...EnvironmentPolicy) (*Bridge, error) {
	policy := RuntimeEnvironment
	if len(policies) > 1 {
		return nil, errors.New("Pi bridge accepts at most one environment policy")
	}
	if len(policies) == 1 {
		policy = policies[0]
	}
	if policy != RuntimeEnvironment && policy != ProviderEnvironment {
		return nil, errors.New("invalid Pi bridge environment policy")
	}
	if fmt.Sprintf("%x", sha256.Sum256(source)) != BundleSHA256 {
		return nil, errors.New("embedded Pi bridge digest mismatch")
	}
	if node == "" {
		node = "node"
	}
	if workspace == "" {
		workspace = os.TempDir()
	}
	dir := filepath.Join(workspace, ".atoll", "runtime", Version)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "bridge.mjs")
	if err := materialize(path, source); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, node, path)
	cmd.Dir = workspace
	cmd.Env = selectedEnvironment(policy)
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	b := &Bridge{cmd: cmd, in: in, pending: map[string]*call{}, done: make(chan struct{}), logger: logger}
	ready := make(chan frame, 1)
	go b.read(out, ready)
	go b.readStderr(stderr)
	go func() {
		err := cmd.Wait()
		b.mu.Lock()
		if b.err == nil {
			b.err = err
		}
		b.mu.Unlock()
		// Calls also select on done. Per-call frame channels deliberately remain
		// open: the stdout reader may already hold a pointer to one, and closing it
		// here would race that final delivery into a send-on-closed-channel panic.
		close(b.done)
	}()
	select {
	case first := <-ready:
		if first.Kind != "ready" || first.Protocol != 1 {
			b.Close()
			return nil, fmt.Errorf("Pi bridge did not become ready: %s", first.Detail)
		}
		if !supportedNode(first.Node) {
			b.Close()
			return nil, fmt.Errorf("Pi bridge requires Node >=22.19.0, got %q", first.Node)
		}
		return b, nil
	case <-time.After(10 * time.Second):
		b.Close()
		return nil, errors.New("Pi bridge readiness timeout")
	case <-ctx.Done():
		b.Close()
		return nil, ctx.Err()
	case <-b.done:
		return nil, fmt.Errorf("Pi bridge exited during start: %v", b.terminalError())
	}
}

// materialize publishes a complete immutable asset with one atomic rename.
// The LLM and Workspace actors commonly start together in the same Channel;
// neither may execute a bridge file while the other is still writing it.
func materialize(path string, content []byte) error {
	if current, err := os.ReadFile(path); err == nil && bytes.Equal(current, content) {
		return nil
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".bridge-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err = tmp.Write(content); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		// Another simultaneous starter may have won the race on platforms that
		// refuse replacement. Its byte-identical result is equally valid.
		if current, readErr := os.ReadFile(path); readErr == nil && bytes.Equal(current, content) {
			return nil
		}
		return err
	}
	return os.Chmod(path, 0o600)
}

func supportedNode(version string) bool {
	var major, minor, patch int
	if _, err := fmt.Sscanf(version, "v%d.%d.%d", &major, &minor, &patch); err != nil {
		return false
	}
	return major > 22 || (major == 22 && (minor > 19 || minor == 19))
}

func selectedEnvironment(policy EnvironmentPolicy) []string {
	keys := []string{
		"PATH", "HOME", "LANG", "LC_ALL", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR", "NODE_EXTRA_CA_CERTS",
		"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy",
	}
	if policy == ProviderEnvironment {
		keys = append(keys,
			"ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "AZURE_OPENAI_API_KEY",
			"NVIDIA_API_KEY", "DEEPSEEK_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_CLOUD_API_KEY", "GROQ_API_KEY",
			"CEREBRAS_API_KEY", "XAI_API_KEY", "RADIUS_API_KEY", "OPENROUTER_API_KEY", "AI_GATEWAY_API_KEY", "ZAI_API_KEY",
			"MISTRAL_API_KEY", "MINIMAX_API_KEY", "MINIMAX_CN_API_KEY", "MOONSHOT_API_KEY", "HF_TOKEN", "FIREWORKS_API_KEY",
			"TOGETHER_API_KEY", "BASETEN_API_KEY", "OPENCODE_API_KEY", "KIMI_API_KEY", "CLOUDFLARE_API_KEY", "XIAOMI_API_KEY",
			"XIAOMI_TOKEN_PLAN_CN_API_KEY", "XIAOMI_TOKEN_PLAN_AMS_API_KEY", "XIAOMI_TOKEN_PLAN_SGP_API_KEY", "ANT_LING_API_KEY",
			"QWEN_TOKEN_PLAN_API_KEY", "COPILOT_GITHUB_TOKEN",
			"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION",
			"AWS_BEARER_TOKEN_BEDROCK", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
			"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_BEDROCK_SKIP_AUTH", "AWS_BEDROCK_FORCE_HTTP1", "AWS_BEDROCK_FORCE_CACHE",
			"AZURE_OPENAI_API_VERSION", "AZURE_OPENAI_BASE_URL", "AZURE_OPENAI_DEPLOYMENT_NAME_MAP", "AZURE_OPENAI_RESOURCE_NAME",
			"GCLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CLOUD_LOCATION", "GOOGLE_CLOUD_PROJECT",
			"KIMI_CODE_OAUTH_HOST", "KIMI_OAUTH_HOST", "PI_CACHE_RETENTION", "PI_OAUTH_CALLBACK_HOST",
		)
	}
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			out = append(out, key+"="+value)
		}
	}
	return out
}

func (b *Bridge) read(r io.Reader, ready chan<- frame) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64<<10), maxBridgeFrameBytes)
	first := true
	for s.Scan() {
		var f frame
		if err := json.Unmarshal(s.Bytes(), &f); err != nil {
			b.logger.Error("Pi bridge invalid frame", "error", err)
			continue
		}
		if first {
			first = false
			ready <- f
			close(ready)
			continue
		}
		b.mu.Lock()
		c := b.pending[f.ID]
		b.mu.Unlock()
		if c != nil {
			if f.Kind == "progress" {
				// Streaming progress is presentational and the final result contains
				// Pi's complete event list. Never let one slow progress consumer
				// head-of-line block unrelated LLM calls on this shared stdout.
				select {
				case c.frames <- f:
				case <-c.done:
				case <-b.done:
				default:
				}
			} else {
				// There is at most one terminal frame per call. Its own delivery may
				// wait behind that call's progress, but the shared reader stays free.
				go func(c *call, terminal frame) {
					select {
					case c.frames <- terminal:
					case <-c.done:
					case <-b.done:
					}
				}(c, f)
			}
		}
	}
	if first {
		close(ready)
	}
	err := s.Err()
	if err == nil {
		err = errors.New("Pi bridge stdout closed")
	} else {
		err = fmt.Errorf("Pi bridge stdout: %w", err)
	}
	b.mu.Lock()
	if b.err == nil {
		b.err = err
	}
	b.mu.Unlock()
	// Without a stdout reader no pending request can ever settle. Terminating
	// the child converts the broken protocol stream into one shared done fact.
	if b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
}

func (b *Bridge) readStderr(r io.Reader) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		b.logger.Warn("Pi bridge stderr", "line", s.Text())
	}
}

func (b *Bridge) write(v any) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(raw) > maxBridgeFrameBytes {
		return fmt.Errorf("Pi bridge frame exceeds %d bytes", maxBridgeFrameBytes)
	}
	raw = append(raw, '\n')
	_, err = b.in.Write(raw)
	return err
}

func (b *Bridge) Call(ctx context.Context, op string, args any, cwd string, progress func(json.RawMessage)) (json.RawMessage, error) {
	id := uuid.NewString()
	c := &call{frames: make(chan frame, 32), done: make(chan struct{})}
	b.mu.Lock()
	select {
	case <-b.done:
		err := b.err
		b.mu.Unlock()
		return nil, fmt.Errorf("Pi bridge unavailable: %v", err)
	default:
	}
	b.pending[id] = c
	b.mu.Unlock()
	defer func() {
		close(c.done)
		b.mu.Lock()
		delete(b.pending, id)
		b.mu.Unlock()
	}()
	if err := b.write(map[string]any{"id": id, "kind": "request", "op": op, "args": args, "cwd": cwd}); err != nil {
		return nil, err
	}
	for {
		select {
		case f, ok := <-c.frames:
			if !ok {
				return nil, fmt.Errorf("Pi bridge exited: %v", b.terminalError())
			}
			switch f.Kind {
			case "progress":
				if progress != nil {
					progress(append(json.RawMessage(nil), f.Event...))
				}
			case "result":
				return append(json.RawMessage(nil), f.Value...), nil
			case "error":
				if f.Code == "cancelled" {
					return nil, context.Canceled
				}
				return nil, &Error{Code: f.Code, Detail: f.Detail, Status: f.Status, ProviderCode: f.ProviderCode, RetryAfterMS: f.RetryAfterMS}
			}
		case <-ctx.Done():
			_ = b.write(map[string]any{"id": id, "kind": "cancel"})
			return nil, ctx.Err()
		case <-b.done:
			return nil, fmt.Errorf("Pi bridge exited: %v", b.terminalError())
		}
	}
}

func (b *Bridge) terminalError() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

type Error struct {
	Code, Detail string
	Status       int
	ProviderCode string
	RetryAfterMS int64
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return e.Code
	}
	return e.Code + ": " + e.Detail
}

func (b *Bridge) Close() {
	if b == nil {
		return
	}
	_ = b.in.Close()
	select {
	case <-b.done:
		return
	case <-time.After(2 * time.Second):
	}
	if b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	<-b.done
}
