// Package gateway — install endpoints.
//
// Two public routes are wired when InstallerDir is configured:
//
//	GET /install/proxy.sh
//	    Returns scripts/install-proxy.sh with __SERVER_ORIGIN__ substituted
//	    for the request's resolved origin (Host header, X-Forwarded-Proto/
//	    -Host honored when present). The script downloads the matching
//	    coagent-proxy_<os>_<arch> binary from /install/<filename> on the
//	    same origin, writes the config via `coagent-proxy install`, then
//	    execs `coagent-proxy start` to bring the daemon up.
//
//	GET /install/coagent-proxy_<os>_<arch>
//	    Serves a single binary file from InstallerDir. Filename is
//	    constrained to coagent-proxy_<os>_<arch>(.exe)? — no traversal,
//	    no other names, no symlinks-following surprises.
//
// Both routes are public (no auth) so the one-line `curl | sh` flow works
// before the user has any cookie. The api-key is the only secret in the
// flow and is supplied to install.sh on the command line.

package gateway

import (
	_ "embed"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
)

//go:embed install_proxy_template.sh
var installProxyTemplate string

// installerFilenamePattern matches coagent-proxy_<os>_<arch> with an
// optional .exe suffix. Restricting at the regex level removes any path-
// traversal risk before filepath.Join.
var installerFilenamePattern = regexp.MustCompile(`^coagent-proxy_[a-z0-9]+_[a-z0-9]+(\.exe)?$`)

// registerInstallRoutes wires /install/proxy.sh and /install/:filename when
// InstallerDir is set. Called from gateway.buildEngine before the SPA
// fallback so /install/* never falls into the catch-all UI handler.
func (a *App) registerInstallRoutes(r *gin.Engine) {
	if strings.TrimSpace(a.cfg.InstallerDir) == "" {
		return
	}
	r.GET("/install/proxy.sh", a.handleInstallScript)
	r.GET("/install/:filename", a.handleInstallBinary)
}

func (a *App) handleInstallScript(c *gin.Context) {
	origin := resolveServerOrigin(c)
	// The template ships with `SERVER_ORIGIN=""` (empty default). We rewrite
	// only that one assignment line in-place — avoiding sentinel collisions
	// inside the body, which were a real footgun in an earlier iteration
	// (the marker check itself got substituted, masking failures).
	body := strings.Replace(installProxyTemplate,
		`SERVER_ORIGIN=""`,
		`SERVER_ORIGIN="`+origin+`"`,
		1)
	c.Header("Content-Type", "text/x-shellscript; charset=utf-8")
	c.Header("Cache-Control", "no-store")
	c.String(http.StatusOK, body)
}

func (a *App) handleInstallBinary(c *gin.Context) {
	name := c.Param("filename")
	if !installerFilenamePattern.MatchString(name) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
		return
	}
	path := filepath.Join(a.cfg.InstallerDir, name)
	// filepath.Join already cleans the path; we still verify the result
	// stays inside InstallerDir to refuse any odd cases.
	abs, err := filepath.Abs(path)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "installer_path"})
		return
	}
	root, err := filepath.Abs(a.cfg.InstallerDir)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "installer_root"})
		return
	}
	if !strings.HasPrefix(abs, root+string(filepath.Separator)) && abs != root {
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found"})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("Content-Type", "application/octet-stream")
	c.File(abs)
}

// resolveServerOrigin returns the scheme://host[:port] callers should use
// when fetching binaries off this server. Honors X-Forwarded-Proto / Host
// when present (so installs work behind a reverse proxy) and falls back to
// the raw Host header otherwise.
func resolveServerOrigin(c *gin.Context) string {
	scheme := strings.ToLower(strings.TrimSpace(c.GetHeader("X-Forwarded-Proto")))
	if scheme == "" {
		if c.Request.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := strings.TrimSpace(c.GetHeader("X-Forwarded-Host"))
	if host == "" {
		host = c.Request.Host
	}
	return scheme + "://" + host
}
