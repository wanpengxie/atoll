// Package autoupdate checks the two official release origins and replaces the
// running atoll binary with a verified release. It owns node process mechanics,
// not channel state: ledgers, memberships and workspaces are never touched.
package autoupdate

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	OSSBase    = "https://atoll-package.oss-cn-beijing.aliyuncs.com"
	GitHubBase = "https://github.com/wanpengxie/atoll"
)

type Snapshot struct {
	CurrentVersion string `json:"current_version"`
	LatestVersion  string `json:"latest_version,omitempty"`
	Available      bool   `json:"available"`
	Status         string `json:"status"`
	Detail         string `json:"detail,omitempty"`
	CheckedAt      string `json:"checked_at,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
	WorkerPID      int    `json:"worker_pid,omitempty"`
}

type Config struct {
	Home           string
	CurrentVersion string
	Executable     string
	ParentPID      int
	StartArgs      []string
	HTTPClient     *http.Client
	OSSBase        string
	GitHubBase     string
}

type Manager struct {
	cfg Config
	mu  sync.Mutex
}

func New(cfg Config) (*Manager, error) {
	if cfg.Home == "" || cfg.Executable == "" || cfg.ParentPID <= 0 {
		return nil, errors.New("autoupdate: home, executable and parent pid are required")
	}
	if resolved, err := filepath.EvalSymlinks(cfg.Executable); err == nil {
		cfg.Executable = resolved
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	if cfg.OSSBase == "" {
		cfg.OSSBase = OSSBase
	}
	if cfg.GitHubBase == "" {
		cfg.GitHubBase = GitHubBase
	}
	return &Manager{cfg: cfg}, nil
}

func (m *Manager) Status(ctx context.Context, check bool) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.readState()
	if active(s.Status) && s.WorkerPID > 0 && !pidAlive(s.WorkerPID) {
		s.Status, s.Detail, s.Available = "failed", "升级进程意外退出，请重试", s.LatestVersion != "" && s.LatestVersion != m.cfg.CurrentVersion
		s.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		_ = writeState(m.cfg.Home, s)
	}
	if active(s.Status) {
		return s, nil
	}
	if !check {
		return s, nil
	}
	latest, err := latestVersion(ctx, m.cfg.HTTPClient, m.cfg.OSSBase, m.cfg.GitHubBase)
	now := time.Now().UTC().Format(time.RFC3339)
	s.CurrentVersion = m.cfg.CurrentVersion
	s.CheckedAt, s.UpdatedAt = now, now
	if err != nil {
		s.Status, s.Detail = "failed", "检查更新失败："+err.Error()
		_ = writeState(m.cfg.Home, s)
		return s, err
	}
	s.LatestVersion = latest
	s.Available = releasable(m.cfg.CurrentVersion) && latest != "" && latest != m.cfg.CurrentVersion
	s.Status, s.Detail = "idle", ""
	if !releasable(m.cfg.CurrentVersion) {
		s.Detail = "开发版不执行自动升级"
	}
	if err := writeState(m.cfg.Home, s); err != nil {
		return s, err
	}
	return s, nil
}

func (m *Manager) Start(ctx context.Context) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.readState()
	if active(s.Status) {
		return s, errors.New("升级已经在进行中")
	}
	if !releasable(m.cfg.CurrentVersion) {
		return s, errors.New("开发版不能自动升级")
	}
	if !s.Available || s.LatestVersion == "" || s.LatestVersion == m.cfg.CurrentVersion {
		latest, err := latestVersion(ctx, m.cfg.HTTPClient, m.cfg.OSSBase, m.cfg.GitHubBase)
		if err != nil {
			return s, fmt.Errorf("检查更新：%w", err)
		}
		s.LatestVersion = latest
		s.Available = latest != m.cfg.CurrentVersion
		if !s.Available {
			return s, errors.New("当前已经是最新版")
		}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	s.CurrentVersion = m.cfg.CurrentVersion
	s.Status, s.Detail, s.UpdatedAt = "starting", "准备下载", now
	if err := os.MkdirAll(m.cfg.Home, 0o755); err != nil {
		return s, err
	}
	argsFile := filepath.Join(m.cfg.Home, "atoll-update-request.json")
	request := WorkerRequest{Config: m.cfg, TargetVersion: s.LatestVersion}
	request.Config.HTTPClient = nil
	b, err := json.Marshal(request)
	if err == nil {
		err = writeAtomic(argsFile, b, 0o600)
	}
	if err != nil {
		return s, err
	}
	logFile, err := os.OpenFile(filepath.Join(m.cfg.Home, "atoll-update.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		_ = os.Remove(argsFile)
		return s, err
	}
	if err := writeState(m.cfg.Home, s); err != nil {
		_ = logFile.Close()
		_ = os.Remove(argsFile)
		return s, err
	}
	cmd := exec.Command(m.cfg.Executable, "update-worker", "--request", argsFile)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		_ = os.Remove(argsFile)
		s.Status, s.Detail = "failed", "无法启动升级进程："+err.Error()
		_ = writeState(m.cfg.Home, s)
		return s, err
	}
	_ = cmd.Process.Release()
	_ = logFile.Close()
	s.WorkerPID = cmd.Process.Pid
	if current, err := readState(m.cfg.Home); err == nil && current.Status == "starting" {
		current.WorkerPID = s.WorkerPID
		_ = writeState(m.cfg.Home, current)
	}
	return s, nil
}

func (m *Manager) readState() Snapshot {
	s, err := readState(m.cfg.Home)
	if err != nil {
		return Snapshot{CurrentVersion: m.cfg.CurrentVersion, Status: "idle"}
	}
	s.CurrentVersion = m.cfg.CurrentVersion
	return s
}

type WorkerRequest struct {
	Config        Config `json:"config"`
	TargetVersion string `json:"target_version"`
}

func RunWorker(requestPath string) error {
	b, err := os.ReadFile(requestPath)
	if err != nil {
		return err
	}
	_ = os.Remove(requestPath)
	var req WorkerRequest
	if err := json.Unmarshal(b, &req); err != nil {
		return err
	}
	cfg := req.Config
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Minute}
	}
	if cfg.OSSBase == "" {
		cfg.OSSBase = OSSBase
	}
	if cfg.GitHubBase == "" {
		cfg.GitHubBase = GitHubBase
	}
	w := worker{cfg: cfg, target: req.TargetVersion}
	return w.run()
}

type worker struct {
	cfg       Config
	target    string
	backup    string
	replaced  bool
	restarted bool
}

func (w *worker) run() (err error) {
	defer func() {
		if err != nil {
			detail := err.Error()
			if w.replaced && !w.restarted {
				if pid := nodePID(w.cfg.Home); pid > 0 && pid != w.cfg.ParentPID {
					_ = stopPID(pid)
				}
				if restoreErr := copyFile(w.backup, w.cfg.Executable, 0o755); restoreErr != nil {
					detail += "；恢复上一版失败：" + restoreErr.Error()
				} else if startErr := w.startNode(); startErr != nil {
					detail += "；上一版重新启动失败：" + startErr.Error()
				}
			}
			w.set("failed", detail, true)
		}
	}()
	w.set("downloading", "正在下载 "+w.target, true)
	archive, err := w.downloadRelease()
	if err != nil {
		return err
	}
	defer os.Remove(archive)
	w.set("verifying", "正在校验发行包", true)
	stage, err := extractBinary(archive, w.cfg.Executable, w.target)
	if err != nil {
		return err
	}
	defer os.Remove(stage)
	if err := verifyVersion(stage, w.target); err != nil {
		return err
	}
	w.set("installing", "正在安装", true)
	w.backup = w.cfg.Executable + ".previous"
	if err := copyFile(w.cfg.Executable, w.backup, 0o755); err != nil {
		return fmt.Errorf("保留上一版：%w", err)
	}
	if err := os.Rename(stage, w.cfg.Executable); err != nil {
		return fmt.Errorf("替换二进制：%w", err)
	}
	w.replaced = true
	w.set("restarting", "正在重启并等待连接恢复", true)
	if err := stopPID(w.cfg.ParentPID); err != nil {
		return err
	}
	if err := w.startNode(); err != nil {
		return fmt.Errorf("启动新版本：%w", err)
	}
	w.restarted = true
	w.set("succeeded", "升级完成", false)
	return nil
}

func (w *worker) set(status, detail string, available bool) {
	now := time.Now().UTC().Format(time.RFC3339)
	_ = writeState(w.cfg.Home, Snapshot{CurrentVersion: w.cfg.CurrentVersion, LatestVersion: w.target, Available: available, Status: status, Detail: detail, UpdatedAt: now, WorkerPID: os.Getpid()})
}

func (w *worker) downloadRelease() (string, error) {
	name := fmt.Sprintf("atoll_%s_%s_%s.tar.gz", w.target, runtime.GOOS, runtime.GOARCH)
	origins := []string{
		strings.TrimRight(w.cfg.OSSBase, "/") + "/releases/" + w.target,
		strings.TrimRight(w.cfg.GitHubBase, "/") + "/releases/download/" + w.target,
	}
	var errs []string
	for _, origin := range origins {
		checksum, err := fetchChecksum(w.cfg.HTTPClient, origin+"/checksums.txt", name)
		if err != nil {
			errs = append(errs, origin+": "+err.Error())
			continue
		}
		dest := filepath.Join(filepath.Dir(w.cfg.Executable), ".atoll-update-"+strconv.Itoa(os.Getpid())+".tar.gz")
		if err := download(w.cfg.HTTPClient, origin+"/"+name, dest); err != nil {
			errs = append(errs, origin+": "+err.Error())
			_ = os.Remove(dest)
			continue
		}
		actual, err := fileSHA256(dest)
		if err == nil && actual != checksum {
			err = fmt.Errorf("SHA256 不一致")
		}
		if err != nil {
			errs = append(errs, origin+": "+err.Error())
			_ = os.Remove(dest)
			continue
		}
		return dest, nil
	}
	return "", errors.New(strings.Join(errs, "; "))
}

func (w *worker) startNode() error {
	cmd := exec.Command(w.cfg.Executable, append([]string{"start"}, w.cfg.StartArgs...)...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

func latestVersion(ctx context.Context, client *http.Client, ossBase, githubBase string) (string, error) {
	if version, err := fetchText(ctx, client, strings.TrimRight(ossBase, "/")+"/releases/latest"); err == nil && validTag(version) {
		return version, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(githubBase, "/")+"/releases/latest", nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("GitHub latest: HTTP %d", resp.StatusCode)
	}
	parts := strings.Split(strings.TrimRight(resp.Request.URL.Path, "/"), "/")
	if len(parts) == 0 || !validTag(parts[len(parts)-1]) {
		return "", errors.New("GitHub latest 未返回发行 tag")
	}
	return parts[len(parts)-1], nil
}

func fetchText(ctx context.Context, client *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 256))
	return strings.TrimSpace(string(b)), err
}

func fetchChecksum(client *http.Client, url, name string) (string, error) {
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && strings.TrimPrefix(fields[len(fields)-1], "*") == name && len(fields[0]) == 64 {
			return strings.ToLower(fields[0]), nil
		}
	}
	return "", errors.New("checksums.txt 中没有当前平台发行包")
}

func download(client *http.Client, url, dest string) error {
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		offset := int64(0)
		if st, err := os.Stat(dest); err == nil {
			offset = st.Size()
		}
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		if offset > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
		resp, err := client.Do(req)
		if err != nil {
			last = err
			continue
		}
		flags := os.O_CREATE | os.O_WRONLY
		if offset > 0 && resp.StatusCode == http.StatusPartialContent {
			flags |= os.O_APPEND
		} else {
			offset = 0
			flags |= os.O_TRUNC
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			last = fmt.Errorf("HTTP %d", resp.StatusCode)
			resp.Body.Close()
			continue
		}
		f, err := os.OpenFile(dest, flags, 0o600)
		if err == nil {
			_, err = io.Copy(f, resp.Body)
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
		}
		resp.Body.Close()
		if err == nil {
			return nil
		}
		last = err
	}
	return last
}

func extractBinary(archive, executable, version string) (string, error) {
	f, err := os.Open(archive)
	if err != nil {
		return "", err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()
	want := fmt.Sprintf("atoll_%s_%s_%s/atoll", version, runtime.GOOS, runtime.GOARCH)
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		if h.Name != want {
			continue
		}
		if h.Typeflag != tar.TypeReg || h.Size <= 0 || h.Size > 512<<20 {
			return "", errors.New("发行包中的 atoll 不是有效普通文件")
		}
		stage := filepath.Join(filepath.Dir(executable), ".atoll-update-"+strconv.Itoa(os.Getpid()))
		out, err := os.OpenFile(stage, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
		if err != nil {
			return "", err
		}
		_, copyErr := io.CopyN(out, tr, h.Size)
		closeErr := out.Close()
		if copyErr != nil {
			_ = os.Remove(stage)
			return "", copyErr
		}
		if closeErr != nil {
			_ = os.Remove(stage)
			return "", closeErr
		}
		return stage, nil
	}
	return "", errors.New("发行包中没有 atoll 二进制")
}

func verifyVersion(binary, target string) error {
	cmd := exec.Command(binary, "version")
	out, err := cmd.Output()
	if err != nil {
		return err
	}
	if !strings.HasPrefix(strings.TrimSpace(string(out)), "atoll "+target+" ") {
		return fmt.Errorf("发行包版本不符：%s", strings.TrimSpace(string(out)))
	}
	return nil
}

func stopPID(pid int) error {
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("停止旧节点：%w", err)
	}
	for i := 0; i < 60; i++ {
		if !pidAlive(pid) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	for i := 0; i < 20; i++ {
		if !pidAlive(pid) {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("旧节点 pid %d 无法停止", pid)
}

func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func nodePID(home string) int {
	b, err := os.ReadFile(filepath.Join(home, "atoll.pid"))
	if err != nil {
		return 0
	}
	line := strings.SplitN(strings.TrimSpace(string(b)), "\n", 2)[0]
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

func readState(home string) (Snapshot, error) {
	b, err := os.ReadFile(filepath.Join(home, "update.json"))
	if err != nil {
		return Snapshot{}, err
	}
	var s Snapshot
	err = json.Unmarshal(b, &s)
	return s, err
}

func writeState(home string, s Snapshot) error {
	if err := os.MkdirAll(home, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(home, "update.json"), append(b, '\n'), 0o600)
}

func writeAtomic(path string, b []byte, mode os.FileMode) error {
	tmp := path + ".tmp-" + strconv.Itoa(os.Getpid())
	if err := os.WriteFile(tmp, b, mode); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp-" + strconv.Itoa(os.Getpid())
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if syncErr := out.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func active(status string) bool {
	switch status {
	case "starting", "downloading", "verifying", "installing", "restarting":
		return true
	default:
		return false
	}
}

func releasable(version string) bool { return validTag(version) }

func validTag(version string) bool {
	if len(version) < 2 || version[0] != 'v' {
		return false
	}
	for _, r := range version[1:] {
		if r != '.' && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}
