package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/adapters/proxy/actors/kimi"
	proxydaemon "github.com/wanpengxie/ActOS/adapters/proxy/daemon"
	"github.com/wanpengxie/ActOS/kernel/actor"
)

var version = "dev"

func main() {
	if err := rootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func rootCommand() *cobra.Command {
	var configPath string
	root := &cobra.Command{
		Use:     "coagent-proxy",
		Short:   "Run the coagent local proxy daemon",
		Version: version,
	}
	root.PersistentFlags().StringVar(&configPath, "config", "", "config path (default ~/.coagent/proxy/config.json)")
	root.AddCommand(startCommand(&configPath))
	root.AddCommand(installCommand(&configPath))
	root.AddCommand(statusCommand(&configPath))
	root.AddCommand(stopCommand())
	root.AddCommand(logsCommand())
	return root
}

func startCommand(configPath *string) *cobra.Command {
	var apiKey, serverWS, enabledActors string
	var port int
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the proxy daemon in the foreground",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadStartConfig(*configPath)
			if err != nil {
				return err
			}
			if apiKey != "" {
				cfg.APIKey = apiKey
			}
			if serverWS != "" {
				cfg.ServerWS = serverWS
			}
			if port != 0 {
				cfg.Port = port
			}
			if enabledActors != "" {
				cfg.EnabledActors = parseActorList(enabledActors)
			}
			cfg = cfg.Normalize()
			if err := cfg.Validate(); err != nil {
				return err
			}
			reg := proxydaemon.NewRegistry()
			if err := reg.Register(kimi.DefaultAdapterActorID, func() (actorapi.ActorModule, error) {
				return kimi.New(), nil
			}); err != nil {
				return err
			}
			logger, closeLog, err := newProxyLogger()
			if err != nil {
				return err
			}
			defer closeLog()
			pidFile, err := writePIDFile()
			if err != nil {
				return err
			}
			defer func() { _ = os.Remove(pidFile) }()
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			d, err := proxydaemon.New(cfg, proxydaemon.Options{
				Registry: reg,
				Logger:   logger,
				Version:  "coagent-proxy/" + version,
			})
			if err != nil {
				return err
			}
			logger.Printf("coagent-proxy starting server_ws=%s enabled_actors=%s", cfg.ServerWS, actorListString(cfg.EnabledActors))
			return d.Run(ctx)
		},
	}
	cmd.Flags().StringVar(&apiKey, "api-key", "", "daemon API key")
	cmd.Flags().StringVar(&serverWS, "server-ws", "", "server websocket base or /devicebus/v2/connect URL")
	cmd.Flags().IntVar(&port, "port", 0, "reserved local proxy port (default 10387)")
	cmd.Flags().StringVar(&enabledActors, "enabled-actors", "", "comma-separated actor ids (default tool:kimi)")
	return cmd
}

func installCommand(configPath *string) *cobra.Command {
	var apiKey, serverWS, enabledActors string
	var port int
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Write proxy daemon config",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(apiKey) == "" {
				return errors.New("--api-key is required")
			}
			if strings.TrimSpace(serverWS) == "" {
				return errors.New("--server-ws is required")
			}
			cfg := proxydaemon.Config{
				APIKey:        apiKey,
				ServerWS:      serverWS,
				Port:          port,
				EnabledActors: parseActorList(enabledActors),
			}.Normalize()
			if err := proxydaemon.WriteConfig(*configPath, cfg); err != nil {
				return err
			}
			path := *configPath
			if path == "" {
				var err error
				path, err = proxydaemon.DefaultConfigPath()
				if err != nil {
					return err
				}
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "wrote %s\n", path)
			return nil
		},
	}
	cmd.Flags().StringVar(&apiKey, "api-key", "", "daemon API key")
	cmd.Flags().StringVar(&serverWS, "server-ws", "", "server websocket base or /devicebus/v2/connect URL")
	cmd.Flags().IntVar(&port, "port", proxydaemon.DefaultPort, "reserved local proxy port")
	cmd.Flags().StringVar(&enabledActors, "enabled-actors", "tool:kimi", "comma-separated actor ids")
	return cmd
}

func statusCommand(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show local proxy daemon status",
		RunE: func(cmd *cobra.Command, args []string) error {
			path := *configPath
			if path == "" {
				var err error
				path, err = proxydaemon.DefaultConfigPath()
				if err != nil {
					return err
				}
			}
			cfg, cfgErr := proxydaemon.LoadConfig(path)
			pid, running := readRunningPID()
			out := cmd.OutOrStdout()
			if cfgErr != nil {
				_, _ = fmt.Fprintf(out, "config: %s (%v)\n", path, cfgErr)
			} else {
				_, _ = fmt.Fprintf(out, "config: %s\nserver_ws: %s\nenabled_actors: %s\n", path, cfg.ServerWS, actorListString(cfg.EnabledActors))
			}
			if running {
				_, _ = fmt.Fprintf(out, "process: running pid=%d\n", pid)
			} else {
				_, _ = fmt.Fprintln(out, "process: not running")
			}
			return nil
		},
	}
}

func stopCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop a running local proxy daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			pid, running := readRunningPID()
			if !running {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "coagent-proxy is not running")
				return nil
			}
			proc, err := os.FindProcess(pid)
			if err != nil {
				return err
			}
			if err := proc.Signal(syscall.SIGTERM); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "sent SIGTERM to pid %d\n", pid)
			return nil
		},
	}
}

func logsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "logs",
		Short: "Print recent local proxy daemon logs",
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := logPath()
			if err != nil {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			const max = 64 << 10
			if len(raw) > max {
				raw = raw[len(raw)-max:]
			}
			_, err = cmd.OutOrStdout().Write(raw)
			return err
		},
	}
}

func loadStartConfig(path string) (proxydaemon.Config, error) {
	cfg, err := proxydaemon.LoadConfig(path)
	if err == nil {
		return cfg, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return proxydaemon.Config{}.Normalize(), nil
	}
	return proxydaemon.Config{}, err
}

func parseActorList(raw string) []actor.ActorID {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]actor.ActorID, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, actor.ActorID(p))
		}
	}
	return out
}

func actorListString(ids []actor.ActorID) string {
	if len(ids) == 0 {
		return ""
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = string(id)
	}
	return strings.Join(parts, ",")
}

func stateDir() (string, error) {
	cfgPath, err := proxydaemon.DefaultConfigPath()
	if err != nil {
		return "", err
	}
	return filepath.Dir(cfgPath), nil
}

func pidPath() (string, error) {
	dir, err := stateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "coagent-proxy.pid"), nil
}

func logPath() (string, error) {
	dir, err := stateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "proxy.log"), nil
}

func writePIDFile() (string, error) {
	path, err := pidPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func readRunningPID() (int, bool) {
	path, err := pidPath()
	if err != nil {
		return 0, false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return pid, false
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		return pid, false
	}
	return pid, true
}

func newProxyLogger() (*log.Logger, func(), error) {
	path, err := logPath()
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	writer := io.MultiWriter(os.Stderr, f)
	return log.New(writer, "", log.LstdFlags), func() { _ = f.Close() }, nil
}
