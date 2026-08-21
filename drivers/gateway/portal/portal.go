// Package portal owns the human and device HTTP entrances. It translates wire
// values into the gateway/lagoon contracts and contains no registry judgement.
package portal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/connector/web"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/daemonhost"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/obs"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"golang.org/x/crypto/bcrypt"
)

const sessionCookie = "atoll_session"
const sessionTTL = 30 * 24 * time.Hour

type Config struct {
	Registry        *lagoon.Registry
	Lobby           func(context.Context) (channelhost.Bundle, error)
	Sessions        *gateway.SessionStore
	Gateway         *gateway.Gateway
	DaemonHost      *daemonhost.Host
	DataPlane       dataplane.Redeemer
	ContractVersion string
	Obs             ObsPlane
	// Web is the browser UI, served from this same origin because the UI
	// addresses the node over relative paths. Nil serves no UI; the entrance
	// then answers unknown paths the way it always has.
	Web fs.FS
}

type ObsPlane interface {
	Pull(ctx context.Context, principal, escapedPath, rawQuery string) (obs.Observation, error)
}
type Portal struct {
	cfg Config
	ws  *web.Connector
	mux *http.ServeMux
	ui  http.Handler
}

func New(cfg Config) *Portal {
	p := &Portal{cfg: cfg, ws: web.New(cfg.Gateway, cfg.ContractVersion), mux: http.NewServeMux()}
	if cfg.Web != nil {
		p.ui = http.FileServerFS(cfg.Web)
	}
	p.mux.HandleFunc("POST /api/identity/register", p.register)
	p.mux.HandleFunc("POST /api/identity/login", p.login)
	p.mux.HandleFunc("POST /api/identity/logout", p.logout)
	p.mux.HandleFunc("GET /ws", p.serveWS)
	p.mux.HandleFunc("GET /compute", p.compute)
	p.mux.HandleFunc("GET /files", p.files)
	p.mux.HandleFunc("PUT /files", p.files)
	p.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	p.mux.HandleFunc("/", p.fallback)
	return p
}

// files is the byte half of a resource operation the access door already
// decided, and it is the outside edge of the business domain. What arrives here
// is a session, which names a principal — an identity an outside party can
// claim and therefore one that has to be authenticated. What the door decided
// for is an actor, which the runtime mints and nobody asserts. So this entrance
// does the one translation that edge exists for: authenticate the principal,
// look up which actor that principal is in the named channel, and hand the
// plane the actor. Every other frame from a browser crosses the same way.
//
// The channel is named by the request, as it is on every other business frame,
// because a ticket's scope is (channel, actor) and a scope neither side states
// is not checked. Nothing else is named: the machine, the path and the
// direction were fixed when the ticket was issued.
func (p *Portal) files(w http.ResponseWriter, r *http.Request) {
	if p.cfg.DataPlane == nil || p.cfg.Gateway == nil {
		writeError(w, http.StatusServiceUnavailable, string(codeUnavailable), "data plane unavailable")
		return
	}
	principal, ok := p.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, string(codeNotAuthenticated), "invalid session")
		return
	}
	chID := channel.ID(strings.TrimSpace(r.URL.Query().Get("channel_id")))
	ticket := r.URL.Query().Get("t")
	if chID == "" || ticket == "" {
		writeError(w, http.StatusBadRequest, string(codeInvalidArgs), "channel_id and a file ticket are required")
		return
	}
	caller, member, err := p.cfg.Gateway.SubjectIn(r.Context(), principal, chID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, string(codeUnavailable), "channel eligibility unavailable — retry")
		return
	}
	if !member {
		writeError(w, http.StatusForbidden, string(codePermissionDenied), "no eligibility for channel")
		return
	}
	mode := access.OpRead
	if r.Method == http.MethodPut {
		mode = access.OpWrite
	} else if filename, named := p.cfg.DataPlane.TicketFile(chID, caller, ticket); named {
		// Naming the download is presentation, and the name has to be on the
		// wire before the first byte — so it is asked for up front. It grants
		// nothing: the ticket is still redeemed below.
		w.Header().Set("Content-Disposition", `attachment; filename*=UTF-8''`+url.PathEscape(filename))
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	tracked := &trackingResponseWriter{ResponseWriter: w}
	if err := p.cfg.DataPlane.ServeHTTP(r.Context(), chID, caller, ticket, mode, tracked, r.Body); err != nil {
		if tracked.wrote {
			panic(http.ErrAbortHandler)
		}
		status := http.StatusBadGateway
		if errors.Is(err, dataplane.ErrInvalidTicket) {
			status = http.StatusForbidden
		} else if errors.Is(err, dataplane.ErrHostOffline) {
			status = http.StatusServiceUnavailable
			var offline *dataplane.HostOfflineError
			if errors.As(err, &offline) {
				err = accessdoor.NewHostOfflineError(offline.Host)
			}
		}
		writeError(w, status, string(codeUnavailable), err.Error())
		return
	}
	if r.Method == http.MethodPut && !tracked.wrote {
		w.WriteHeader(http.StatusNoContent)
	}
}

type trackingResponseWriter struct {
	http.ResponseWriter
	wrote bool
}

func (w *trackingResponseWriter) Write(p []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(p)
}

func (w *trackingResponseWriter) WriteHeader(status int) {
	w.wrote = true
	w.ResponseWriter.WriteHeader(status)
}
func (p *Portal) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.EscapedPath(), "/obs/") {
		if r.Method == http.MethodGet {
			p.observe(w, r)
		} else {
			writeError(w, http.StatusMethodNotAllowed, string(codeNotFound), "method not allowed")
		}
		return
	}
	if r.URL.EscapedPath() == "/files" {
		if r.Method == http.MethodGet || r.Method == http.MethodPut {
			p.files(w, r)
		} else {
			writeError(w, http.StatusMethodNotAllowed, string(codeNotFound), "method not allowed")
		}
		return
	}
	p.mux.ServeHTTP(w, r)
}

func (p *Portal) observe(w http.ResponseWriter, r *http.Request) {
	principal, ok := p.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, string(codeNotAuthenticated), "invalid session")
		return
	}
	if p.cfg.Obs == nil {
		writeError(w, http.StatusServiceUnavailable, string(codeUnavailable), "observation plane unavailable")
		return
	}
	answer, err := p.cfg.Obs.Pull(r.Context(), principal, r.URL.EscapedPath(), r.URL.RawQuery)
	if err == nil {
		writeJSON(w, http.StatusOK, answer)
		return
	}
	var observationErr *obs.Error
	if !errors.As(err, &observationErr) {
		writeError(w, http.StatusInternalServerError, string(codeInternalError), err.Error())
		return
	}
	detail := observationErr.Detail
	if detail == "" {
		detail = observationErr.Error()
	}
	switch observationErr.Kind {
	case obs.ErrBadAddress, obs.ErrUnknownKind, obs.ErrBadQuery:
		writeError(w, http.StatusBadRequest, string(codeInvalidArgs), detail)
	case obs.ErrUnauthed:
		writeError(w, http.StatusUnauthorized, string(codeNotAuthenticated), detail)
	case obs.ErrForbidden:
		writeError(w, http.StatusForbidden, string(codePermissionDenied), detail)
	case obs.ErrNotServing, obs.ErrTimeout, obs.ErrOverloaded:
		writeError(w, http.StatusServiceUnavailable, string(codeUnavailable), detail)
	case obs.ErrCanceled:
		return
	default:
		writeError(w, http.StatusInternalServerError, string(codeInternalError), detail)
	}
}

func (p *Portal) fallback(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/identity/register", "/api/identity/login", "/api/identity/logout", "/ws", "/compute", "/healthz":
		writeError(w, http.StatusMethodNotAllowed, string(codeNotFound), "method not allowed")
		return
	}
	// /api is the machine's namespace: an unknown path under it stays a JSON
	// error, so a caller expecting JSON never has to parse a page to find out
	// it asked for something that does not exist.
	if p.ui == nil || r.Method != http.MethodGet && r.Method != http.MethodHead ||
		strings.HasPrefix(r.URL.Path, "/api/") {
		writeError(w, http.StatusNotFound, string(codeNotFound), "route not found")
		return
	}
	p.serveUI(w, r)
}

// serveUI answers everything the node itself did not claim. A path naming a
// file that is not there is a missing file and says so; a path naming no file
// at all is one of the UI's own routes, which only the browser can resolve, so
// it gets the page and resolves it there.
func (p *Portal) serveUI(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if name == "" || name == "." {
		name = "index.html"
	}
	if _, err := fs.Stat(p.cfg.Web, name); err != nil {
		if path.Ext(name) != "" {
			writeError(w, http.StatusNotFound, string(codeNotFound), "route not found")
			return
		}
		r = r.Clone(r.Context())
		r.URL.Path = "/"
	}
	p.ui.ServeHTTP(w, r)
}

// qualifyLoginName reads a bare name as one of the accounts the node carved for
// itself. A principal is named by an email, and `root` is nobody's email — but
// it is the only thing the person installing a node ever calls that account, and
// making them type a domain the node invented for them is a toll charged for
// nothing.
//
// Only the accounts boot writes can be named this way, and that is why only
// logging in qualifies: registration insists on a real address (see register),
// so a bare name can never be a stored one. Qualify both sides instead and the
// two spellings drift — an account registered as "alice" would be looked up as
// "alice@atoll.local" and could never be reached at all.
//
// A name that already carries a domain is left exactly as it is; a node is not
// the only place a principal can come from.
func qualifyLoginName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.Contains(s, "@") {
		return s
	}
	return s + "@" + channelspec.LocalEmailDomain
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
	// Registering asks for a real address, and a bare name is refused rather
	// than completed: the node's own domain is not this account's to claim, and
	// an account stored under a bare name could never be logged into — logging
	// in reads a bare name as that domain (see qualifyLoginName), and would look
	// for an address the store does not have.
	if !strings.Contains(in.Email, "@") {
		writeError(w, 400, string(codeInvalidArgs), "email must be a full address, e.g. you@example.com")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		writeError(w, 500, string(codeInternalError), err.Error())
		return
	}
	callCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	reply, err := p.callViaLobby(callCtx, lagoon.WordPrincipalCreate, lagoon.PrincipalRegister{ID: in.ID, Email: in.Email, SecretHash: string(hash), DisplayName: in.DisplayName})
	if err != nil {
		// Registration closed = c0 exposes no such endpoint to the lobby: the
		// svcactor answers endpoint_not_found. That is node policy, not an error.
		var le *lagoon.Error
		if errors.As(err, &le) && le.Code == "endpoint_not_found" {
			writeError(w, http.StatusForbidden, string(codeRegistrationClosed), "registration is closed on this node")
			return
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeError(w, http.StatusGatewayTimeout, string(codeUnavailable), err.Error())
			return
		}
		writeLagoonError(w, err)
		return
	}
	var registered lagoon.PrincipalRegisterReply
	if err := reply.DecodeValue(&registered); err != nil {
		writeError(w, 500, string(codeInternalError), "invalid registrar reply: "+err.Error())
		return
	}
	p.setSession(w, registered.PrincipalID)
	writeJSON(w, http.StatusCreated, registered)
}

// callViaLobby speaks one of the two lobby doors as the guest. The request is
// addressed to the lobby membrane, which frames it to c0, and the terminal is
// read back from the lobby.
func (p *Portal) callViaLobby(ctx context.Context, word lagoon.Word, in any) (lagoon.Reply, error) {
	if p.cfg.Lobby == nil {
		return lagoon.Reply{}, errors.New("registration lobby unavailable")
	}
	bundle, err := p.cfg.Lobby(ctx)
	if err != nil {
		return lagoon.Reply{}, err
	}
	guest, found, err := bundle.View().ResolvePrincipal(ctx, channelspec.GuestPrincipalID)
	if err != nil {
		return lagoon.Reply{}, err
	}
	if !found {
		return lagoon.Reply{}, errors.New("guest cell unavailable")
	}
	slot, ok := bundle.Gateway().SubjectSlotFor(guest)
	if !ok {
		return lagoon.Reply{}, errors.New("guest subject slot unavailable")
	}
	requestID := message.ID(uuid.NewString())
	raw, _ := json.Marshal(in)
	frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, uuid.NewString(), subjectgate.SubmitPayload{ChannelID: string(channelspec.LobbyChannelID), ID: string(requestID), MsgType: string(word), Kind: string(message.KindRequest), Audience: []string{string(actor.SystemActorID)}, Visibility: string(message.VisibilityPublic), Payload: raw})
	if err != nil {
		return lagoon.Reply{}, err
	}
	if _, err := slot.Deliver(ctx, frame); err != nil {
		return lagoon.Reply{}, err
	}
	reader := gateway.Reader{ActorID: guest, Mode: gateway.ReaderMember}
	var cursor int64
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		active, err := bundle.View().IsActive(ctx, reader.ActorID)
		if err != nil || !active {
			return lagoon.Reply{}, errors.New("guest cell unavailable")
		}
		rows, next, err := bundle.View().ReadVisibleAfterSeq(ctx, cursor, 256)
		if err != nil {
			return lagoon.Reply{}, err
		}
		cursor = next
		for _, row := range rows {
			if !row.IsTerminal || row.Envelope.ParentID != requestID {
				continue
			}
			var terminal struct {
				Status    string `json:"status"`
				ErrorCode string `json:"error_code"`
				Detail    string `json:"detail"`
			}
			if json.Unmarshal(row.Envelope.Payload, &terminal) != nil {
				return lagoon.Reply{}, errors.New("invalid registration terminal")
			}
			if terminal.Status == message.StatusFailed {
				return lagoon.Reply{}, &lagoon.Error{Code: lagoon.ErrorCode(terminal.ErrorCode), Detail: terminal.Detail}
			}
			var reply lagoon.Reply
			if err := json.Unmarshal(row.Envelope.Payload, &reply); err != nil {
				return lagoon.Reply{}, err
			}
			return reply, nil
		}
		select {
		case <-ctx.Done():
			return lagoon.Reply{}, ctx.Err()
		case <-ticker.C:
		}
	}
}
func (p *Portal) login(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !readJSON(w, r, &in) {
		return
	}
	in.Email = qualifyLoginName(in.Email)
	if in.Email == "" || in.Password == "" {
		writeError(w, 400, string(codeInvalidArgs), "email and password required")
		return
	}
	callCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	reply, err := p.callViaLobby(callCtx, lagoon.WordPrincipalLogin, lagoon.PrincipalLogin{Email: in.Email, Password: in.Password})
	if err != nil {
		var le *lagoon.Error
		if errors.As(err, &le) && le.Code == lagoon.CodeInvalidCredentials {
			writeError(w, 401, string(codeInvalidCredentials), "invalid credentials")
			return
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeError(w, http.StatusGatewayTimeout, string(codeUnavailable), err.Error())
			return
		}
		writeLagoonError(w, err)
		return
	}
	var login lagoon.PrincipalLoginReply
	if err := reply.DecodeValue(&login); err != nil || login.PrincipalID == "" {
		writeError(w, 500, string(codeInternalError), "invalid registrar reply")
		return
	}
	p.setSession(w, login.PrincipalID)
	writeJSON(w, 200, map[string]string{"id": login.PrincipalID})
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
