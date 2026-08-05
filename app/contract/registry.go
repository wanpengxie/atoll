package contract

import "strings"

// Auth describes the credential requirement of a method.
type Auth string

const (
	AuthNone    Auth = "none"
	AuthSession Auth = "session"
)

// Method is one endpoint in the shell contract registry. Path, Query and Body
// are independent because absence of a body is itself a fail-closed rule.
type Method struct {
	Method      string   `json:"method"`
	Path        string   `json:"path"`
	Auth        Auth     `json:"auth"`
	PathSchema  string   `json:"path_schema"`
	QuerySchema string   `json:"query_schema"`
	BodySchema  string   `json:"body_schema"`
	Response    string   `json:"response_schema"`
	Errors      []string `json:"error_schemas"`
	// Since is the contract version that INTRODUCED the endpoint — a
	// write-time literal snapshot, never a reference to the live Version
	// constant (that would rewrite every entry's history on a version bump).
	Since        string `json:"since"`
	Experimental bool   `json:"experimental"`
}

const (
	NoSchema    = "none"
	errorSchema = "Error"
)

// since10 pins the day-1 endpoints to the version that introduced them.
// Endpoints added in a later contract version must NOT reuse this helper's
// default — declare their own literal (add a since parameter then).
const since10 = "1.0"

func method(verb, path string, auth Auth, pathSchema, querySchema, bodySchema, response string) Method {
	return Method{Method: verb, Path: path, Auth: auth, PathSchema: pathSchema,
		QuerySchema: querySchema, BodySchema: bodySchema, Response: response,
		Errors: []string{errorSchema}, Since: since10}
}

func experimentalMethod(verb, path string, auth Auth, pathSchema, querySchema, bodySchema, response string) Method {
	if !strings.HasPrefix(path, "/api/experimental/") {
		panic("experimental contract method must use /api/experimental prefix: " + path)
	}
	value := method(verb, path, auth, pathSchema, querySchema, bodySchema, response)
	value.Experimental = true
	return value
}

var methods = [...]Method{
	method("GET", "/ws", AuthSession, NoSchema, NoSchema, NoSchema, "SubjectgateFrameStream"),
	method("GET", "/api/meta", AuthNone, NoSchema, NoSchema, NoSchema, "Meta"),
	method("POST", "/api/identity/register", AuthNone, NoSchema, NoSchema, "RegisterRequest", "Principal"),
	method("POST", "/api/identity/login", AuthNone, NoSchema, NoSchema, "LoginRequest", "Principal"),
	method("POST", "/api/identity/logout", AuthNone, NoSchema, NoSchema, NoSchema, "OK"),
	method("GET", "/api/identity/me", AuthSession, NoSchema, NoSchema, NoSchema, "Principal"),
	method("POST", "/api/identity/verification/issue", AuthNone, NoSchema, NoSchema, NoSchema, "OK"),
	method("GET", "/api/channels", AuthSession, NoSchema, "ChannelListQuery", NoSchema, "ChannelList"),
	method("POST", "/api/channels", AuthSession, NoSchema, NoSchema, "CreateChannelRequest", "Channel"),
	method("GET", "/api/channels/:chID", AuthSession, "ChannelPath", NoSchema, NoSchema, "Channel"),
	method("DELETE", "/api/channels/:chID", AuthSession, "ChannelPath", NoSchema, NoSchema, "ChannelDeletion"),
	method("POST", "/api/channels/:chID/join", AuthSession, "ChannelPath", NoSchema, NoSchema, "Membership"),
	method("GET", "/api/channels/:chID/observe", AuthSession, "ChannelPath", "MessagePageQuery", NoSchema, "EventStream"),
	experimentalMethod("GET", "/api/experimental/channels/:chID/observe", AuthSession, "ChannelPath", "MessagePageQuery", NoSchema, "EventStream"),
	method("GET", "/api/channels/:chID/messages", AuthSession, "ChannelPath", "MessagePageQuery", NoSchema, "MessagePage"),
	method("GET", "/api/channels/:chID/resources", AuthSession, "ChannelPath", "ResourceListQuery", NoSchema, "ResourcePage"),
	method("GET", "/api/channels/:chID/resources/:rid", AuthSession, "ChannelResourcePath", NoSchema, NoSchema, "ResourceMeta"),
	method("GET", "/api/channels/:chID/resources/:rid/bytes", AuthSession, "ChannelResourcePath", NoSchema, NoSchema, "Binary"),
	method("POST", "/api/channels/:chID/actors", AuthSession, "ChannelPath", NoSchema, "IntroduceActorRequest", "IntroduceActorResponse"),
	method("DELETE", "/api/channels/:chID/actors/:actorID", AuthSession, "ChannelActorPath", NoSchema, NoSchema, "RemoveActorResponse"),
	method("PUT", "/api/channels/:chID/decls/:declID/config", AuthSession, "ChannelDeclarationPath", NoSchema, "DeclarationOverlayRequest", "DeclarationOverlay"),
	method("DELETE", "/api/channels/:chID/decls/:declID/config", AuthSession, "ChannelDeclarationPath", NoSchema, NoSchema, "DeclarationOverlay"),
	method("GET", "/api/channels/:chID/candidates", AuthSession, "ChannelPath", NoSchema, NoSchema, "CandidateList"),
	method("GET", "/api/actor-decls", AuthSession, NoSchema, NoSchema, NoSchema, "DeclarationList"),
	method("POST", "/api/actor-decls", AuthSession, NoSchema, NoSchema, "DeclarationCreateRequest", "Declaration"),
	method("PATCH", "/api/actor-decls/:declID", AuthSession, "DeclarationPath", NoSchema, "DeclarationUpdateRequest", "DeclarationMutation"),
	method("DELETE", "/api/actor-decls/:declID", AuthSession, "DeclarationPath", NoSchema, NoSchema, "DeclarationMutation"),
	method("GET", "/api/daemons", AuthSession, NoSchema, NoSchema, NoSchema, "DaemonList"),
	method("POST", "/api/daemons", AuthSession, NoSchema, NoSchema, "CreateDaemonRequest", "Daemon"),
	method("DELETE", "/api/daemons/:id", AuthSession, "DaemonPath", NoSchema, NoSchema, "DaemonDeletion"),
	method("GET", "/api/channels/:chID/daemons", AuthSession, "ChannelPath", NoSchema, NoSchema, "DaemonList"),
	method("POST", "/api/channels/:chID/daemons", AuthSession, "ChannelPath", NoSchema, "AttachDaemonRequest", "DaemonBinding"),
	method("DELETE", "/api/channels/:chID/daemons/:id", AuthSession, "ChannelDaemonPath", NoSchema, NoSchema, "DaemonBinding"),
}

func (m Method) HasBody() bool { return m.BodySchema != NoSchema }

// Methods returns a copy of the method registry in declaration order.
func Methods() []Method {
	out := make([]Method, len(methods))
	copy(out, methods[:])
	return out
}
