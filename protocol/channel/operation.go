package channel

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/protocol/actor"
)

var ErrDeclarationNotFound = errors.New("channel: declaration not found")

type DaemonFacts struct {
	Deleted bool
}

type DeclarationFacts struct {
	OwnerPrincipal string
	Visibility     string
	Class          string
	Config         json.RawMessage
}

// PlacementKind is the wire-level placement discriminator carried by a rendered
// declaration snapshot. DesiredHost is meaningful only for daemon placement.
type PlacementKind string

const (
	PlacementServer PlacementKind = "server"
	PlacementDaemon PlacementKind = "daemon"
)

type Placement struct {
	Kind        PlacementKind `json:"kind"`
	DesiredHost string        `json:"desired_host,omitempty"`
}

func (p Placement) Validate() error {
	switch p.Kind {
	case PlacementServer:
		if p.DesiredHost != "" {
			return ErrInvalidPlacement
		}
	case PlacementDaemon:
		// An empty host is allowed on an introduction request: the in-channel
		// admission segment resolves it from the bound-daemon set.
	default:
		return ErrInvalidPlacement
	}
	return nil
}

// RenderedSnapshot is the complete, already-resolved declaration value accepted
// by a channel. Channel storage has no global/local merge semantics.
type RenderedSnapshot struct {
	Class     string          `json:"class"`
	Config    json.RawMessage `json:"config,omitempty"`
	Placement Placement       `json:"placement"`
	TIdleMS   int64           `json:"t_idle_ms"`
	Digest    string          `json:"digest"`
}

func (s RenderedSnapshot) Validate() error {
	if strings.TrimSpace(s.Class) == "" {
		return ErrInvalidRequest
	}
	if err := s.Placement.Validate(); err != nil {
		return err
	}
	want, err := s.ContentDigest()
	if err != nil {
		return err
	}
	if s.Digest != want {
		return ErrDigestMismatch
	}
	return nil
}

// ContentDigest covers the rendered value only. Digest is deliberately
// excluded so equal values can be detected across local declaration versions.
func (s RenderedSnapshot) ContentDigest() (string, error) {
	payload := struct {
		Class     string          `json:"class"`
		Config    json.RawMessage `json:"config,omitempty"`
		Placement Placement       `json:"placement"`
		TIdleMS   int64           `json:"t_idle_ms"`
	}{s.Class, s.Config, s.Placement, s.TIdleMS}
	return Digest(payload)
}

// Seal computes and installs the content digest.
func (s RenderedSnapshot) Seal() (RenderedSnapshot, error) {
	digest, err := s.ContentDigest()
	if err != nil {
		return RenderedSnapshot{}, err
	}
	s.Digest = digest
	return s, nil
}

// Digest returns the v1 RFC-8785/JCS digest used by operation requests.
func Digest(v any) (string, error) {
	canonical, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "v1:" + hex.EncodeToString(sum[:]), nil
}

// CanonicalJSON implements the RFC-8785 data-model subset accepted by this
// protocol: JSON objects/arrays/strings/bools/null and finite IEEE-754 numbers.
// encoding/json already supplies the RFC-required UTF-8 string escaping and
// lexicographic map-key order; this routine additionally normalizes every
// number through binary64 and disables HTML escaping.
func CanonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := appendCanonical(&out, value); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func appendCanonical(out *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if v {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case string:
		if err := appendJSONString(out, v); err != nil {
			return err
		}
	case json.Number:
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return ErrInvalidRequest
		}
		out.WriteString(formatJCSNumber(f))
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := appendCanonical(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		slicesSort(keys)
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := appendJSONString(out, key); err != nil {
				return err
			}
			out.WriteByte(':')
			if err := appendCanonical(out, v[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("channel: unsupported canonical JSON value %T", value)
	}
	return nil
}

func slicesSort(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && jcsStringLess(values[j], values[j-1]); j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func jcsStringLess(a, b string) bool {
	aa := utf16.Encode([]rune(a))
	bb := utf16.Encode([]rune(b))
	for i := 0; i < len(aa) && i < len(bb); i++ {
		if aa[i] != bb[i] {
			return aa[i] < bb[i]
		}
	}
	return len(aa) < len(bb)
}

func appendJSONString(out *bytes.Buffer, value string) error {
	if !utf8.ValidString(value) {
		return ErrInvalidRequest
	}
	var encoded bytes.Buffer
	enc := json.NewEncoder(&encoded)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(value)
	b := bytes.ReplaceAll(encoded.Bytes(), []byte(`\u2028`), []byte("\u2028"))
	b = bytes.ReplaceAll(b, []byte(`\u2029`), []byte("\u2029"))
	out.Write(b[:len(b)-1]) // trim Encoder's newline
	return nil
}

func formatJCSNumber(value float64) string {
	if value == 0 {
		return "0"
	}
	s := strconv.FormatFloat(value, 'g', -1, 64)
	// ECMAScript/JCS omits the exponent for [1e-6, 1e21).
	abs := math.Abs(value)
	if abs >= 1e-6 && abs < 1e21 {
		s = strconv.FormatFloat(value, 'f', -1, 64)
	}
	if i := strings.IndexByte(s, 'e'); i >= 0 {
		exp := s[i+1:]
		sign := "+"
		if strings.HasPrefix(exp, "-") {
			sign = "-"
		}
		exp = strings.TrimPrefix(strings.TrimPrefix(exp, "+"), "-")
		exp = strings.TrimLeft(exp, "0")
		if exp == "" {
			exp = "0"
		}
		s = s[:i] + "e" + sign + exp
	}
	return s
}

func appendLengthPrefixed(dst []byte, value string) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len([]byte(value))))
	dst = append(dst, size[:]...)
	return append(dst, value...)
}

func DerivedRealmToolRef(channelID ID, requestID string) string {
	payload := appendLengthPrefixed(nil, string(channelID))
	payload = appendLengthPrefixed(payload, requestID)
	sum := sha256.Sum256(payload)
	return "adm:rt:v1:" + hex.EncodeToString(sum[:])
}

func RefCorrelation(ref string) string {
	sum := sha256.Sum256([]byte(ref))
	return "op:ref:v1:" + hex.EncodeToString(sum[:])
}

func MessageCorrelation(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "op:msg:v1:" + hex.EncodeToString(sum[:])
}

type OperationErrorCode string

const (
	ErrCodeBadPayload           OperationErrorCode = "bad_payload"
	ErrCodeChannelUnavailable   OperationErrorCode = "channel_unavailable"
	ErrCodeInvalidDesiredHost   OperationErrorCode = "invalid_desired_host"
	ErrCodeDeclNotFound         OperationErrorCode = "decl_not_found"
	ErrCodeForbidden            OperationErrorCode = "forbidden"
	ErrCodeInvalidPlacement     OperationErrorCode = "invalid_placement"
	ErrCodeUnknownClass         OperationErrorCode = "unknown_class"
	ErrCodeProtectedActor       OperationErrorCode = "protected_actor"
	ErrCodeNotInComposition     OperationErrorCode = "not_in_composition"
	ErrCodeRebuildFailed        OperationErrorCode = "rebuild_failed"
	ErrCodeUnauthorizedSender   OperationErrorCode = "unauthorized_sender"
	ErrCodeInternal             OperationErrorCode = "internal_error"
	ErrCodeNotAcceptedSource    OperationErrorCode = "not_accepted_source"
	ErrCodeMemberInactive       OperationErrorCode = "member_inactive"
	ErrCodeAuthorityUnavailable OperationErrorCode = "authority_unavailable"
	ErrCodeRefConflict          OperationErrorCode = "ref_conflict"
)

var AllOperationErrorCodes = []OperationErrorCode{
	ErrCodeBadPayload, ErrCodeChannelUnavailable, ErrCodeInvalidDesiredHost,
	ErrCodeDeclNotFound, ErrCodeForbidden, ErrCodeInvalidPlacement,
	ErrCodeUnknownClass, ErrCodeProtectedActor, ErrCodeNotInComposition,
	ErrCodeRebuildFailed, ErrCodeUnauthorizedSender, ErrCodeInternal,
	ErrCodeNotAcceptedSource, ErrCodeMemberInactive, ErrCodeAuthorityUnavailable,
}

type OperationError struct {
	Code      OperationErrorCode
	Detail    string
	Retryable bool
}

func (e *OperationError) Error() string {
	if e.Detail == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Detail
}

var (
	ErrInvalidPlacement = errors.New("channel: invalid placement")
	ErrInvalidRequest   = errors.New("channel: invalid request")
	ErrDigestMismatch   = errors.New("channel: snapshot digest mismatch")
)

type AdmitRequest struct {
	Ref       string `json:"ref"`
	Principal string `json:"principal"`
}

type AdmitResult struct {
	ActorID actor.ActorID `json:"actor_id"`
	Created bool          `json:"created"`
}

type IntroduceRequest struct {
	Ref              string        `json:"ref"`
	DeclID           string        `json:"decl_id"`
	InitiatorActorID actor.ActorID `json:"initiator_actor_id"`
}

type IntroduceResult = AdmitResult

type RemoveRequest struct {
	Ref              string        `json:"ref"`
	Target           actor.ActorID `json:"target"`
	InitiatorActorID actor.ActorID `json:"initiator_actor_id"`
}

type RemoveResult struct {
	Removed []actor.ActorID `json:"removed"`
}

type DaemonRequest struct {
	Ref      string `json:"ref"`
	DaemonID string `json:"daemon_id"`
}

type BindingResult struct {
	Bound            bool            `json:"bound"`
	ClearedInstances []actor.ActorID `json:"cleared_instances,omitempty"`
}
