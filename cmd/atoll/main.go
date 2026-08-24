// Command atoll runs a complete personal node. `atoll up` installs or opens
// c0, starts the runtime, binds the public
// listener, and only then connects the well-known local device.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/cmd/internal/buildinfo"
	"github.com/wanpengxie/atoll/cmd/internal/dotenv"
	"github.com/wanpengxie/atoll/cmd/internal/engineboot"
	"github.com/wanpengxie/atoll/cmd/internal/homelock"
	"github.com/wanpengxie/atoll/drivers/devicehost"

	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	_ "github.com/wanpengxie/atoll/drivers/tools/all"
)

const teardownGrace = 30 * time.Second

func usage() {
	fmt.Fprintln(os.Stderr, "usage: atoll up [--dir DIR] [--addr ADDR] [--root-password PASSWORD] [--steward CLASS] [--open-registration]")
	fmt.Fprintln(os.Stderr, "       atoll start [同 up 的参数]   # 后台启动，日志写 <dir>/atoll-up.log")
	fmt.Fprintln(os.Stderr, "       atoll stop [--dir DIR]      # 停掉后台节点")
	fmt.Fprintln(os.Stderr, "       atoll restart [同 up 的参数] # 停掉再起，等旧进程真的退干净")
	fmt.Fprintln(os.Stderr, "       atoll status [--dir DIR]    # 看后台节点在不在跑")
	fmt.Fprintln(os.Stderr, "       atoll version")
	os.Exit(2)
}

func main() {
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "version", "--version", "-v":
			fmt.Println(buildinfo.Line("atoll"))
			return
		case "start":
			cmdStart(os.Args[2:])
			return
		case "stop":
			cmdStop(os.Args[2:])
			return
		case "restart":
			cmdRestart(os.Args[2:])
			return
		case "status":
			cmdStatus(os.Args[2:])
			return
		}
	}
	if len(os.Args) < 2 || os.Args[1] != "up" {
		usage()
	}
	fs := flag.NewFlagSet("up", flag.ExitOnError)
	dir := fs.String("dir", defaultDir(), "node home root")
	addr := fs.String("addr", "127.0.0.1:8832", "listen address")
	rootPassword := fs.String("root-password", "", "root password used only during installation")
	steward := fs.String("steward", "", "agent class carved as the c0 steward on first install (default codex; env ATOLL_STEWARD)")
	openReg := fs.Bool("open-registration", false, "expose system.principal.create to the lobby (default closed; env ATOLL_OPEN_REGISTRATION=1)")
	_ = fs.Parse(os.Args[2:])

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if n, err := dotenv.Load(".env"); err != nil {
		logger.Warn("up: .env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("up: loaded .env", "vars_set", n)
	}
	// <dir>/atoll.env is the installer's hand-off: ATOLL_ADDR / ATOLL_STEWARD /
	// ATOLL_ROOT_PASSWORD written once by scripts/install.sh so a bare `atoll up`
	// opens the same node. Explicit flags still win.
	if n, err := dotenv.Load(filepath.Join(*dir, "atoll.env")); err != nil {
		logger.Warn("up: atoll.env load failed", "err", err.Error())
	} else if n > 0 {
		logger.Info("up: loaded atoll.env", "dir", *dir, "vars_set", n)
	}
	if !flagGiven(fs, "addr") {
		if v := os.Getenv("ATOLL_ADDR"); v != "" {
			*addr = v
		}
	}
	if !flagGiven(fs, "open-registration") && os.Getenv("ATOLL_OPEN_REGISTRATION") == "1" {
		*openReg = true
	}
	if *steward == "" {
		*steward = os.Getenv("ATOLL_STEWARD")
	}
	if *rootPassword == "" {
		*rootPassword = os.Getenv("ATOLL_ROOT_PASSWORD")
	}

	serverHome := filepath.Join(*dir, "server")
	channelDir := filepath.Join(serverHome, "channels")
	deviceHome := filepath.Join(*dir, "device")
	for _, lock := range []struct{ dir, role string }{
		{serverHome, "server"}, {channelDir, "server"}, {deviceHome, "device"},
	} {
		release, err := homelock.Acquire(lock.dir, lock.role)
		if err != nil {
			log.Fatalf("up: %v", err)
		}
		defer release()
	}
	// 观测用 pidfile：start/stop/status 靠它认领这个节点。它不是锁——
	// 互斥由上面的 homelock 和端口 bind 保证；stale 文件由 kill -0 识破。
	// 第二行是实际监听地址：status 的地址真相从这里来，不靠猜配置。
	if err := os.WriteFile(pidPath(*dir), []byte(fmt.Sprintf("%d\n%s\n", os.Getpid(), *addr)), 0o644); err != nil {
		logger.Warn("up: pidfile write failed", "err", err.Error())
	}
	defer os.Remove(pidPath(*dir))

	eng, err := engineboot.Boot(engineboot.Config{
		ChannelDBDir:     channelDir,
		Addr:             *addr,
		TokenPath:        filepath.Join(serverHome, "atoll-token"),
		RootPassword:     *rootPassword,
		StewardClass:     *steward,
		OpenRegistration: *openReg,
	}, logger)
	if err != nil {
		log.Fatalf("up: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	deviceKey, err := eng.LocalDeviceKey(ctx)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), teardownGrace)
		_ = eng.Close(closeCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		log.Fatalf("up: local device key: %v", err)
	}

	engineCtx, cancelEngine := context.WithCancel(context.Background())
	deviceCtx, cancelDevice := context.WithCancel(context.Background())
	defer cancelEngine()
	defer cancelDevice()
	engineDone := make(chan error, 1)
	deviceDone := make(chan error, 1)
	go func() { engineDone <- eng.Serve(engineCtx) }()
	go func() {
		select {
		case <-eng.Ready():
		case <-deviceCtx.Done():
			deviceDone <- nil
			return
		}
		deviceName, _ := os.Hostname()
		if deviceName == "" {
			deviceName = "local"
		}
		deviceDone <- devicehost.Run(deviceCtx, devicehost.Config{
			ServerWS:   "ws://" + eng.BoundAddr() + "/compute",
			Credential: deviceKey,
			DeviceName: deviceName + "-local",
			AtollHome:  deviceHome,
			Logger:     logger.With("part", "local-device"),
		})
	}()

	exitCode := 0
	deviceExited, engineExited := false, false
	select {
	case <-ctx.Done():
		logger.Info("up: shutting down")
	case err := <-deviceDone:
		deviceExited, exitCode = true, 1
		logger.Error("up: local device exited", "err", errorText(err))
	case err := <-engineDone:
		engineExited, exitCode = true, 1
		logger.Error("up: engine exited", "err", errorText(err))
	}
	cancelDevice()
	if !deviceExited {
		select {
		case <-deviceDone:
		case <-time.After(teardownGrace):
			logger.Error("up: local device did not stop")
		}
	}
	cancelEngine()
	if !engineExited {
		if err := <-engineDone; err != nil {
			logger.Error("up: engine teardown", "err", err.Error())
			exitCode = 1
		}
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func errorText(err error) string {
	if err == nil {
		return "clean exit"
	}
	return err.Error()
}

// ---------------------------------------------------------------------------
// start / stop / status：`atoll up` 保持前台（systemd/launchd/容器/调试的
// 正确形态），这三个命令是给"装完就想让它一直跑"的人的后台三件套。
// pidfile 由 up 自己写（<dir>/atoll.pid），这里只认领与探测。

func pidPath(dir string) string { return filepath.Join(dir, "atoll.pid") }

func loadNodeEnv(dir string) {
	_, _ = dotenv.Load(filepath.Join(dir, "atoll.env"))
}

func readPidFile(dir string) (pid int, addr string, ok bool) {
	b, err := os.ReadFile(pidPath(dir))
	if err != nil {
		return 0, "", false
	}
	lines := strings.SplitN(strings.TrimSpace(string(b)), "\n", 2)
	pid, err = strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil || pid <= 0 {
		return 0, "", false
	}
	if len(lines) > 1 {
		addr = strings.TrimSpace(lines[1])
	}
	return pid, addr, true
}

// pid 存活探测。EPERM 也算活着（进程在，只是不归我们管）。
// stale pidfile 的 pid 被系统重用会造成误认——本机单用户工具接受这个
// 与 rustup/nvm 同级的既有权衡。
func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func probeHost(addr string) string {
	if strings.HasPrefix(addr, "0.0.0.0:") {
		return "127.0.0.1:" + strings.TrimPrefix(addr, "0.0.0.0:")
	}
	return addr
}

func healthzOK(addr string) bool {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + probeHost(addr) + "/healthz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode < 300
}

func tailFile(path string, n int) {
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	for _, line := range lines {
		fmt.Fprintln(os.Stderr, "  "+line)
	}
}

// upFlags 声明与 up 完全相同的 flag 集：start 原样透传参数给子进程 up，
// 自己解析同一份只是为了拿 dir/addr 做探测与提示。
func upFlags(name string) (*flag.FlagSet, *string, *string) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	dir := fs.String("dir", defaultDir(), "node home root")
	addr := fs.String("addr", "127.0.0.1:8832", "listen address")
	fs.String("root-password", "", "root password used only during installation")
	fs.String("steward", "", "agent class carved as the c0 steward on first install")
	fs.Bool("open-registration", false, "expose system.principal.create to the lobby")
	return fs, dir, addr
}

func cmdStart(args []string) {
	fs, dir, addr := upFlags("start")
	_ = fs.Parse(args)
	loadNodeEnv(*dir)
	a := *addr
	if !flagGiven(fs, "addr") {
		if v := os.Getenv("ATOLL_ADDR"); v != "" {
			a = v
		}
	}
	if pid, liveAddr, ok := readPidFile(*dir); ok && pidAlive(pid) {
		if liveAddr != "" {
			a = liveAddr
		}
		fmt.Printf("atoll 已在跑（pid %d）：http://%s\n", pid, a)
		return
	}
	self, err := os.Executable()
	if err != nil {
		log.Fatalf("start: %v", err)
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		log.Fatalf("start: %v", err)
	}
	logPath := filepath.Join(*dir, "atoll-up.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("start: %v", err)
	}
	cmd := exec.Command(self, append([]string{"up"}, args...)...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		log.Fatalf("start: %v", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	_ = logFile.Close()
	fmt.Printf("  … 等待节点起来（http://%s/healthz）\n", probeHost(a))
	for i := 0; i < 30; i++ {
		if healthzOK(a) {
			fmt.Printf("atoll 已在后台跑（pid %d）\n", pid)
			fmt.Printf("  打开   : http://%s\n", a)
			fmt.Printf("  日志   : %s\n", logPath)
			fmt.Printf("  停止   : %s stop --dir %s\n", os.Args[0], *dir)
			return
		}
		if !pidAlive(pid) {
			fmt.Fprintf(os.Stderr, "atoll up 退出了，看日志：%s\n", logPath)
			tailFile(logPath, 20)
			os.Exit(1)
		}
		time.Sleep(time.Second)
	}
	fmt.Fprintf(os.Stderr, "30s 内 healthz 没通；进程还在（pid %d），看日志：%s\n", pid, logPath)
	os.Exit(1)
}

func cmdStop(args []string) {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	dir := fs.String("dir", defaultDir(), "node home root")
	_ = fs.Parse(args)
	pid, _, ok := readPidFile(*dir)
	if !ok || !pidAlive(pid) {
		fmt.Println("atoll 没有在跑")
		_ = os.Remove(pidPath(*dir))
		return
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	for i := 0; i < 30; i++ {
		if !pidAlive(pid) {
			fmt.Printf("已停止（pid %d）\n", pid)
			_ = os.Remove(pidPath(*dir))
			return
		}
		time.Sleep(time.Second)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	fmt.Fprintf(os.Stderr, "30s 没退，已强杀（pid %d）\n", pid)
	_ = os.Remove(pidPath(*dir))
}

// restartWorkerFlag marks the detached half of a restart. It is not in usage:
// nobody types it, restart re-execs itself with it.
const restartWorkerFlag = "--restart-worker"

// cmdRestart stops a running node and starts it again.
//
// The hard part is not stop-then-start; it is that **the process doing the
// restart must not be a descendant of what it is restarting**. The reason to
// have this command at all is to run it from the node's own web terminal —
// and that terminal is a pty owned by the node's daemon. Kill the node from
// inside it and the pty dies with the node, taking the restarter with it
// before it can start anything: the node is now stopped with nobody left to
// start it. (Observed 2026-08-25: `atoll stop` typed in that terminal left
// the node down and unrecoverable from the web.)
//
// So restart re-execs itself detached — new session, no controlling terminal,
// stdio to the log — and that copy does the actual work. Setsid also escapes
// the pty's process group, so the daemon's group-wide SIGHUP/SIGKILL on
// session teardown恒不会跟着打到它。
//
// The parent then reports progress for as long as it happens to live. If the
// caller's terminal dies mid-way (the normal case when restarting from the
// web terminal) the detached worker finishes anyway, and the browser simply
// reconnects into a fresh shell.
func cmdRestart(args []string) {
	if len(args) > 0 && args[0] == restartWorkerFlag {
		restartInPlace(args[1:])
		return
	}
	fs := flag.NewFlagSet("restart", flag.ExitOnError)
	dir := fs.String("dir", defaultDir(), "node home root")
	_ = fs.Parse(restartDirArgs(args))

	self, err := os.Executable()
	if err != nil {
		log.Fatalf("restart: %v", err)
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		log.Fatalf("restart: %v", err)
	}
	logPath := filepath.Join(*dir, "atoll-up.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("restart: %v", err)
	}
	worker := exec.Command(self, append([]string{"restart", restartWorkerFlag}, args...)...)
	worker.Stdout, worker.Stderr = logFile, logFile
	worker.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := worker.Start(); err != nil {
		log.Fatalf("restart: %v", err)
	}
	workerPid := worker.Process.Pid
	_ = worker.Process.Release()
	_ = logFile.Close()

	fmt.Printf("重启已交给后台进程（pid %d），它恒不随本终端一起死。\n", workerPid)
	fmt.Printf("  日志: %s\n", logPath)
	// 如果本命令是从节点自己的网页终端里跑的，下面这段会随终端一起消失——
	// 那是预期的，重启照样完成，浏览器重连后是一个新 shell。
	old, _, had := readPidFile(*dir)
	for i := 0; i < 60; i++ {
		time.Sleep(time.Second)
		cur, _, ok := readPidFile(*dir)
		if ok && pidAlive(cur) && (!had || cur != old) {
			fmt.Printf("新节点已起来（pid %d）\n", cur)
			return
		}
		if !pidAlive(workerPid) {
			// 工作进程没了而 pid 文件没翻新：让人去看日志，恒不假装成功。
			cur, _, ok = readPidFile(*dir)
			if ok && pidAlive(cur) && (!had || cur != old) {
				fmt.Printf("新节点已起来（pid %d）\n", cur)
				return
			}
			fmt.Fprintf(os.Stderr, "重启进程已退出但节点没起来，看日志：%s\n", logPath)
			os.Exit(1)
		}
	}
	fmt.Fprintf(os.Stderr, "60s 内没等到新节点；后台进程仍在（pid %d），看日志：%s\n", workerPid, logPath)
	os.Exit(1)
}

// restartInPlace is the detached half: it is already out of the doomed
// process tree, so it can safely stop the node and start it again.
//
// It also tolerates "not running": restart恒不得因为节点碰巧已经停了就拒绝
// 启动——那是人最不希望被卡住的时刻。
func restartInPlace(args []string) {
	fs := flag.NewFlagSet("restart-worker", flag.ExitOnError)
	dir := fs.String("dir", defaultDir(), "node home root")
	_ = fs.Parse(restartDirArgs(args))

	pid, _, running := readPidFile(*dir)
	if running && pidAlive(pid) {
		_ = syscall.Kill(pid, syscall.SIGTERM)
		gone := false
		for i := 0; i < 30; i++ {
			if !pidAlive(pid) {
				gone = true
				break
			}
			time.Sleep(time.Second)
		}
		if !gone {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			// Even SIGKILL is not instant: the kernel still has to tear the
			// process down and release its listening socket. Starting into
			// that window is exactly the failure this command exists to avoid.
			for i := 0; i < 10; i++ {
				if !pidAlive(pid) {
					gone = true
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			if !gone {
				log.Fatalf("restart: pid %d 杀不掉，恒不接着起——那样只会撞端口且两边都没了", pid)
			}
			fmt.Fprintf(os.Stderr, "30s 没退，已强杀（pid %d）\n", pid)
		} else {
			fmt.Printf("已停止（pid %d）\n", pid)
		}
	} else {
		fmt.Println("atoll 没有在跑，直接启动")
	}
	_ = os.Remove(pidPath(*dir))
	cmdStart(args)
}

// restartDirArgs keeps only --dir so restart can find the pid file without
// having to know every flag start accepts; everything else rides through to
// cmdStart unchanged.
func restartDirArgs(args []string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--dir" || a == "-dir" {
			if i+1 < len(args) {
				out = append(out, a, args[i+1])
				i++
			}
			continue
		}
		if strings.HasPrefix(a, "--dir=") || strings.HasPrefix(a, "-dir=") {
			out = append(out, a)
		}
	}
	return out
}

func cmdStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dir := fs.String("dir", defaultDir(), "node home root")
	_ = fs.Parse(args)
	loadNodeEnv(*dir)
	pid, addr, ok := readPidFile(*dir)
	if !ok || !pidAlive(pid) {
		fmt.Println("atoll 没有在跑")
		os.Exit(1)
	}
	if addr == "" {
		addr = os.Getenv("ATOLL_ADDR")
	}
	if addr == "" {
		addr = "127.0.0.1:8832"
	}
	health := "healthz 不通"
	if healthzOK(addr) {
		health = "healthz 正常"
	}
	fmt.Printf("atoll 在跑（pid %d）：http://%s（%s）\n", pid, addr, health)
}

func defaultDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".atoll-node"
	}
	return filepath.Join(home, ".atoll")
}

func flagGiven(fs *flag.FlagSet, name string) bool {
	given := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			given = true
		}
	})
	return given
}
