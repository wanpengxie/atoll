// Package portal owns the human and device HTTP entrances. It translates wire
// values into the gateway/lagoon contracts and contains no registry judgement.
package portal

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/connector/web"
	"github.com/wanpengxie/atoll/platform/daemonhost"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"golang.org/x/crypto/bcrypt"
)

const sessionCookie = "atoll_session"
const sessionTTL = 30 * 24 * time.Hour

type Config struct {
	Registry        *lagoon.Registry
	Submitter       lagoon.Submitter
	Sessions        *gateway.SessionStore
	Gateway         *gateway.Gateway
	DaemonHost      *daemonhost.Host
	ContractVersion string
}
type Portal struct {
	cfg Config
	ws  *web.Connector
	mux *http.ServeMux
}

func New(cfg Config) *Portal {
	p := &Portal{cfg: cfg, ws: web.New(cfg.Gateway, cfg.ContractVersion), mux: http.NewServeMux()}
	p.mux.HandleFunc("POST /api/identity/register", p.register)
	p.mux.HandleFunc("POST /api/identity/login", p.login)
	p.mux.HandleFunc("POST /api/identity/logout", p.logout)
	p.mux.HandleFunc("GET /ws", p.serveWS)
	p.mux.HandleFunc("GET /compute", p.compute)
	p.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	p.mux.HandleFunc("/", p.fallback)
	return p
}
func (p *Portal) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mux.ServeHTTP(w, r)
}

func (p *Portal) fallback(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/identity/register", "/api/identity/login", "/api/identity/logout", "/ws", "/compute", "/healthz":
		writeError(w, http.StatusMethodNotAllowed, string(codeNotFound), "method not allowed")
	default:
		writeError(w, http.StatusNotFound, string(codeNotFound), "route not found")
	}
}

func (p *Portal) register(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID          string `json:"id"`
		Email       string `json:"email"`
		Password    string `json:"password"`
		DisplayName string `json:"display_name"`
	}
	if !readJSON(w, r, &in) {
		return
	}
	in.Email = strings.TrimSpace(in.Email)
	if in.Email == "" || in.Password == "" {
		writeError(w, 400, string(codeInvalidArgs), "email and password required")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		writeError(w, 500, string(codeInternalError), err.Error())
		return
	}
	reply, err := p.cfg.Submitter.SubmitApplication(r.Context(), lagoon.WordPrincipalRegister, lagoon.PrincipalRegister{ID: in.ID, Email: in.Email, SecretHash: string(hash), DisplayName: in.DisplayName})
	if err != nil {
		writeLagoonError(w, err)
		return
	}
	var principal lagoon.PrincipalRow
	if err := reply.DecodeValue(&principal); err != nil {
		writeError(w, 500, string(codeInternalError), "invalid registrar reply: "+err.Error())
		return
	}
	p.setSession(w, principal.ID)
	writeJSON(w, http.StatusCreated, principal)
}
func (p *Portal) login(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !readJSON(w, r, &in) {
		return
	}
	in.Email = strings.TrimSpace(in.Email)
	if in.Email == "" || in.Password == "" {
		writeError(w, 400, string(codeInvalidArgs), "email and password required")
		return
	}
	principal, ok, err := p.cfg.Registry.VerifyCredential(r.Context(), in.Email, in.Password)
	if err != nil {
		writeError(w, 503, string(codeUnavailable), err.Error())
		return
	}
	if !ok {
		writeError(w, 401, string(codeInvalidCredentials), "invalid credentials")
		return
	}
	p.setSession(w, principal)
	writeJSON(w, 200, map[string]string{"id": principal})
}
func (p *Portal) logout(w http.ResponseWriter, r *http.Request) {
	if token := requestToken(r); token != "" {
		p.cfg.Sessions.Revoke(token)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true})
	writeJSON(w, 200, map[string]bool{"ok": true})
}
func (p *Portal) serveWS(w http.ResponseWriter, r *http.Request) {
	principal, ok := p.authenticate(r)
	if !ok {
		writeError(w, 401, string(codeNotAuthenticated), "invalid session")
		return
	}
	p.ws.ServeWeb(w, r, principal)
}
func (p *Portal) compute(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		writeError(w, 400, string(codeBadPayload), "malformed bearer authorization")
		return
	}
	id, ok, err := p.cfg.Registry.ResolveDeviceKey(r.Context(), strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")))
	if err != nil {
		writeError(w, 503, string(codeUnavailable), err.Error())
		return
	}
	if !ok {
		writeError(w, 401, string(codeNotAuthenticated), "invalid device credential")
		return
	}
	p.cfg.DaemonHost.Serve(w, r, id)
}
func (p *Portal) setSession(w http.ResponseWriter, principal string) {
	token := p.cfg.Sessions.Mint(principal, sessionTTL)
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: "/", MaxAge: int(sessionTTL.Seconds()), HttpOnly: true, SameSite: http.SameSiteLaxMode})
}
func (p *Portal) authenticate(r *http.Request) (string, bool) {
	token := requestToken(r)
	return p.cfg.Sessions.Verify(token)
}
func requestToken(r *http.Request) string {
	if c, err := r.Cookie(sessionCookie); err == nil && c.Value != "" {
		return c.Value
	}
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	}
	return ""
}
func readJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	d := json.NewDecoder(r.Body)
	if err := d.Decode(out); err != nil {
		writeError(w, 400, string(codeInvalidArgs), err.Error())
		return false
	}
	var trailing any
	if err := d.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(w, 400, string(codeInvalidArgs), "request body must contain one JSON value")
		return false
	}
	return true
}
func writeLagoonError(w http.ResponseWriter, err error) {
	var le *lagoon.Error
	if errors.As(err, &le) {
		status := 400
		if le.Code == lagoon.CodeConflictExists {
			status = 409
		} else if le.Code == lagoon.CodePermissionDenied {
			status = 403
		} else if le.Code == lagoon.CodeResultUnknown {
			status = 504
		}
		code, ok := mapLagoonCode(le.Code)
		if !ok {
			writeError(w, 500, string(codeInternalError), le.Detail)
			return
		}
		writeError(w, status, string(code), le.Detail)
		return
	}
	writeError(w, 500, string(codeInternalError), err.Error())
}
func writeError(w http.ResponseWriter, status int, code, detail string) {
	writeJSON(w, status, map[string]string{"code": code, "detail": detail})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
