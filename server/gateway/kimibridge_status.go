package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// kimiBridgeDaemonURL is where the local kimi-webbridge daemon listens
// after install (see ~/.kimi-webbridge/bin/kimi-webbridge status).
// v1 hard-codes the convention — composition-root override can come
// later if multi-host deployments need it.
const kimiBridgeDaemonURL = "http://127.0.0.1:10086"

// kimiBridgeStatusReply mirrors the daemon's /status JSON so the
// proxy can decode + re-encode (instead of streaming bytes) — gives
// us a typed surface for the UI without coupling to the daemon's
// version string.
type kimiBridgeStatusReply struct {
	Running            bool   `json:"running"`
	Version            string `json:"version"`
	Port               int    `json:"port"`
	UptimeSeconds      int64  `json:"uptime_seconds"`
	ExtensionConnected bool   `json:"extension_connected"`
	ExtensionID        string `json:"extension_id"`
	ExtensionVersion   string `json:"extension_version"`
}

// handleKimiBridgeStatus is GET /api/kimibridge/status. Always returns
// 200 with a JSON body so the UI can render a consistent indicator.
// The `available` field is the UI's single signal — true iff the
// local daemon answered + reported running. `extension_connected`
// flips when the Chrome extension is paired.
func (a *App) handleKimiBridgeStatus(c *gin.Context) {
	resp, err := a.kimiBridgeStatusFetcher()
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"available":  false,
			"reason":     "daemon_unreachable",
			"detail":     err.Error(),
			"daemon_url": kimiBridgeDaemonURL,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"available":           resp.Running,
		"version":             resp.Version,
		"port":                resp.Port,
		"uptime_seconds":      resp.UptimeSeconds,
		"extension_connected": resp.ExtensionConnected,
		"extension_id":        resp.ExtensionID,
		"extension_version":   resp.ExtensionVersion,
		"daemon_url":          kimiBridgeDaemonURL,
		// Chrome web store link so the UI can show an "install
		// extension" button when extension_connected is false.
		"extension_install_url": "https://chromewebstore.google.com/detail/kimi-webbridge/fldmhceldgbpfpkbgopacenieobmligc",
		// Daemon install command, in case daemon itself isn't running.
		"daemon_install_command": "curl -fsSL https://cdn.kimi.com/webbridge/install.sh | bash",
	})
}

// kimiBridgeStatusFetcher is the one HTTP call to the local daemon.
// Carved out so tests can stub it without spinning a real daemon.
func (a *App) kimiBridgeStatusFetcher() (kimiBridgeStatusReply, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(kimiBridgeDaemonURL + "/status")
	if err != nil {
		return kimiBridgeStatusReply{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return kimiBridgeStatusReply{}, err
	}
	var out kimiBridgeStatusReply
	if jerr := json.Unmarshal(body, &out); jerr != nil {
		return kimiBridgeStatusReply{}, jerr
	}
	return out, nil
}
