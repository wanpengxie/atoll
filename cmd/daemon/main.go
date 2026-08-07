// Command daemon runs one authenticated device carrier and a compartment for
// every bound channel. Cloud and user/proxy daemons are the same binary.
//
// What the daemon RUNS is NOT "one of every compiled class" — it is exactly the
// sets the server assigns each channel (channel composition
// placement='daemon'), pulled over that channel's lane. Two
// orthogonal axes: compiled-in (availability — actors/all + agent/all are linked
// so the daemon CAN build any tool/looper/device) vs run (the pulled assignment
// decides). NOTHING auto-runs. tool / looper / device are uniform — all just
// rows in the assignment; workspace-backed engines run with the user's local setup.
//
// Identity is the home's, not the run's (home 模型: 身份=持有凭据): --server and
// --key are FIRST-RUN registration (or an explicit rebind) and persist into the
// device home's identity file; every later run starts bare — the home is the
// logical device, so `atoll-daemon` with no flags resumes being exactly the
// device it was. One home, one live process (homelock).
//
// The carrier body itself lives in drivers/devicehost (shared with
// `atoll up`'s in-process local device). Adding an in-tree actor/engine = a new
// package with an init() + one blank import in actors/all (tools/devices) or
// agent/all (engines); this file is NEVER edited.
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/cmd/internal/homelock"
	"github.com/wanpengxie/atoll/drivers/devicehost"

	// Availability (NOT auto-run): blank-import every in-tree actor + engine so the
	// daemon CAN build any class the server assigns. actors/all = tools/devices;
	// agent/all = the agent engine classes (codex / script). What actually runs is
	// the pulled assignment, never "one of each".
	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	_ "github.com/wanpengxie/atoll/drivers/tools/all"
)

// shutdownGrace bounds the whole graceful teardown after the first signal.
// Twice the single-compartment join budget: a teardown still running past that
// is not finishing, it is wedged on something cancellation cannot reach.
const shutdownGrace = 60 * time.Second

// defaultServerWS is the fallback when neither --server nor the home's
// identity names one.
const defaultServerWS = "ws://localhost:8080/compute"

func main() {
	ws := flag.String("server", "", "server WS url; first run / rebind — persisted in home (default: the home's registered server)")
	key := flag.String("key", "", "api key; first run / rebind — persisted in home (default: the home's registered key)")
	name := flag.String("name", "", "device display name; default: hostname")
	home := flag.String("home", defaultDeviceHome(), "device home: identity + channel workspaces + resource trees (home 模型: 一个 home=一个逻辑设备)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// One home, one live process — a second carrier presenting the same
	// identity is the double-device accident, refused at the door.
	release, err := homelock.Acquire(*home, "device")
	if err != nil {
		log.Fatalf("daemon: %v", err)
	}
	defer release()

	// Resolve identity: explicit flags register/rebind and persist; a bare
	// start resumes the home's persisted identity.
	ident, _, err := devicehost.LoadIdentity(*home)
	if err != nil {
		log.Fatalf("daemon: %v", err)
	}
	// server and key are an INSEPARABLE pair: a key is a credential minted BY
	// one server, so pointing --server somewhere new while silently reusing
	// the stored key would send the old server's bearer credential to an
	// arbitrary new origin. Changing servers therefore requires the new
	// server's --key in the same run (or a fresh --home).
	if *ws != "" && *ws != ident.ServerWS && *key == "" && ident.APIKey != "" {
		log.Fatalf("daemon: --server points this device somewhere new (%q; home %s is registered to %q) — a key is bound to the server that minted it, so pass the new server's --key alongside --server (or use a different --home)",
			*ws, *home, ident.ServerWS)
	}
	serverWS := *ws
	if serverWS == "" {
		serverWS = ident.ServerWS
	}
	if serverWS == "" {
		serverWS = defaultServerWS
	}
	credential := *key
	if credential == "" {
		credential = ident.APIKey
	}
	if credential == "" {
		log.Fatalf("daemon: home %s holds no identity yet — first run needs --key (mint one via POST /api/daemons, or let `atoll up` provision the local device)", *home)
	}
	if serverWS != ident.ServerWS || credential != ident.APIKey {
		// A rebind invalidates the remembered row id along with the old pair —
		// the new server's attach verdict (OnAttached below) fills in the id
		// this credential actually IS.
		ident.ServerWS, ident.APIKey, ident.DaemonID = serverWS, credential, ""
		if err := devicehost.SaveIdentity(*home, ident); err != nil {
			log.Fatalf("daemon: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// A device daemon is a user-run process with no supervisor behind it, so
	// both escape hatches a supervised server takes for granted are built here:
	// the first signal starts the graceful teardown and immediately restores
	// default signal handling, so a second Ctrl-C hard-kills a wedged teardown;
	// and if the teardown itself exceeds the grace period — the shape is a lane
	// answer parked inside an uncancellable storage syscall — the process exits
	// on its own. Reconciliation is crash-safe, so exiting here is exactly one
	// crash, not corruption.
	go func() {
		<-ctx.Done()
		stop()
		time.Sleep(shutdownGrace)
		slog.Error("daemon: graceful shutdown exceeded its grace period; exiting")
		os.Exit(1)
	}()

	// Display name resolves last — an assigned device actor derives its id
	// from DeviceName; loopers' situation facts derive from the home.
	deviceName := *name
	if deviceName == "" {
		host, err := os.Hostname()
		if err != nil {
			log.Fatalf("daemon: hostname: %v", err)
		}
		deviceName = host
	}

	if err := devicehost.Run(ctx, devicehost.Config{
		ServerWS:   serverWS,
		Credential: credential,
		DeviceName: deviceName,
		AtollHome:  *home,
		Logger:     logger,
		// The attach verdict is the moment this home learns which daemons row
		// it IS — persist it so the identity triple {daemon_id, api_key,
		// server_ws} is complete and a later `atoll up` on this home claims
		// the SAME row instead of minting a double.
		OnAttached: func(daemonID string) {
			if ident.DaemonID == daemonID {
				return
			}
			persisted := ident
			persisted.DaemonID = daemonID
			if err := devicehost.SaveIdentity(*home, persisted); err != nil {
				logger.Error("daemon: persisting attach identity", "err", err.Error())
				return
			}
			ident = persisted
		},
	}); err != nil {
		log.Fatalf("daemon: %v", err)
	}
}

func defaultDeviceHome() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".atoll-device"
	}
	return filepath.Join(home, ".atoll", "device")
}
