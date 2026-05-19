package message

// canonical.go implements the RFC 8785 (JCS) canonical JSON
// serialization + SHA-256 (hex, lowercase) hash mandated by L2
// §1.4.10.2 for the v4 protocol baseline.
//
// Three call sites share this hash function (L1 §10.2.2):
//
//  1. Message-Write Harness step 0.5 / step 8 catch — content compare.
//  2. action_ledger.ledger_key — caller-side idempotency key derivation
//     (L2 §1.4.10.1). See kernel/ledger/key.go for the derivation rule.
//  3. Adapter deterministic response id — `response:<request_id>:<hash>`
//     (L2 §1.4.10.2 + L2 §8.5).
//
// Algorithm choice rationale + spec text: L2 §1.4.10.2. RFC 8785
// reference: https://www.rfc-editor.org/rfc/rfc8785
//
// Implementation contract:
//
//   - Number formatting follows ECMAScript Number→String per RFC 8785
//     §3.2.2.3 (shortest round-trip; fixed for 1e-6 ≤ |x| < 1e21;
//     scientific otherwise with `e+N` / `e-N`, no leading zero in
//     exponent).
//   - Object keys sort by UTF-16 code-unit lexicographic order
//     (RFC 8785 §3.2.3).
//   - Arrays preserve their original ordering.
//   - Strings escape only `"` / `\` / U+0000..U+001F (with shorthand
//     `\b/\t/\n/\f/\r` where defined). Forward slash is NOT escaped.
//   - JSON numbers are parsed via encoding/json's `UseNumber()` so
//     integer payloads survive without float64 round-trip precision
//     loss.
//
// The function rejects NaN / ±Inf (RFC 8785 §3.2.2.3 forbids them).

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// CanonicalHash returns SHA-256 (hex, lowercase) over the RFC 8785
// canonicalization of the 14 hash-input fields of `e` (L2 §1.4.10.2).
//
// Excluded by L1 §10.2.2: ts_received, is_terminal, the four delivery
// metadata fields, and seq. Hash inputs use the post-normalize value
// domain — callers MUST call the harness normalize pass before hashing
// (T7); CanonicalHash itself does not fill defaults.
//
// For each optional field, the key is always present in the canonical
// output (value `null` when absent) to avoid the "omit key" vs "null
// key" canonicalization ambiguity.
func CanonicalHash(e Envelope) (string, error) {
	m, err := envelopeHashInput(e)
	if err != nil {
		return "", err
	}
	buf, err := canonicalizeValue(nil, m)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), nil
}

// CanonicalHashPayload returns SHA-256 (hex, lowercase) over the RFC
// 8785 canonicalization of `payload`. `payload` MUST be valid JSON
// (object, array, or scalar). Used by adapter deterministic response id
// derivation per L2 §1.4.10.2.
func CanonicalHashPayload(payload []byte) (string, error) {
	v, err := decodeJSON(payload)
	if err != nil {
		return "", fmt.Errorf("canonical: parse payload: %w", err)
	}
	buf, err := canonicalizeValue(nil, v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), nil
}

// CanonicalizeJSON returns the RFC 8785 canonical byte sequence for the
// JSON value encoded in `raw`. Exported so tests and step 0.5 callers
// can inspect the canonical form without re-hashing.
func CanonicalizeJSON(raw []byte) ([]byte, error) {
	v, err := decodeJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("canonical: parse: %w", err)
	}
	return canonicalizeValue(nil, v)
}

// envelopeHashInput builds the 14-key map L2 §1.4.10.2 specifies as
// the hash input domain.
func envelopeHashInput(e Envelope) (map[string]any, error) {
	// payload: parse raw JSON via UseNumber so integers retain precision.
	if len(e.Payload) == 0 {
		// L0 §2.2: payload non-null but {} legal. Caller's normalize
		// pass should have substituted `{}` already; refuse otherwise.
		return nil, errors.New("canonical: envelope.payload is empty; normalize must run first")
	}
	payload, err := decodeJSON(e.Payload)
	if err != nil {
		return nil, fmt.Errorf("canonical: parse envelope payload: %w", err)
	}

	// audience: copy slice to []any (canonicalizeArray accepts []any only).
	audience := make([]any, len(e.Audience))
	for i, a := range e.Audience {
		audience[i] = a
	}

	// doc_refs: tri-state per L0 §2.1.
	//   nil pointer → null
	//   pointer to slice (even empty) → array
	var docRefs any
	if e.DocRefs != nil {
		refs := make([]any, len(*e.DocRefs))
		for i, r := range *e.DocRefs {
			refs[i] = r
		}
		docRefs = refs
	}

	sender := map[string]any{
		"kind": string(e.Sender.Kind),
		"id":   string(e.Sender.ID),
		"name": e.Sender.Name,
	}

	return map[string]any{
		"id":             e.ID,
		"ts":             jsonNumberFromInt64(e.TS),
		"channel_id":     e.ChannelID,
		"sender":         sender,
		"kind":           string(e.Kind),
		"type":           e.Type,
		"payload":        payload,
		"parent_id":      nullableString(e.ParentID),
		"correlation_id": nullableString(e.CorrelationID),
		"doc_refs":       docRefs,
		"visibility":     string(e.Visibility),
		"audience":       audience,
		"not_before":     nullableInt64Ptr(e.NotBefore),
		"expires_at":     nullableInt64Ptr(e.ExpiresAt),
	}, nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableInt64Ptr(p *int64) any {
	if p == nil {
		return nil
	}
	return jsonNumberFromInt64(*p)
}

func jsonNumberFromInt64(v int64) json.Number {
	return json.Number(strconv.FormatInt(v, 10))
}

// decodeJSON wraps json.Decoder with UseNumber so numeric precision is
// preserved up to the canonicalize step.
func decodeJSON(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("trailing JSON data")
	}
	return v, nil
}

// ---------------------------------------------------------------------------
// RFC 8785 canonical serialization
// ---------------------------------------------------------------------------

func canonicalizeValue(buf []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(buf, 'n', 'u', 'l', 'l'), nil
	case bool:
		if x {
			return append(buf, 't', 'r', 'u', 'e'), nil
		}
		return append(buf, 'f', 'a', 'l', 's', 'e'), nil
	case string:
		return appendCanonicalString(buf, x), nil
	case json.Number:
		s, err := formatJSONNumber(x)
		if err != nil {
			return nil, err
		}
		return append(buf, s...), nil
	case float64:
		s, err := formatFloatES(x)
		if err != nil {
			return nil, err
		}
		return append(buf, s...), nil
	case int:
		return strconv.AppendInt(buf, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(buf, x, 10), nil
	case []any:
		return canonicalizeArray(buf, x)
	case map[string]any:
		return canonicalizeObject(buf, x)
	}
	return nil, fmt.Errorf("canonical: unsupported value type %T", v)
}

func canonicalizeArray(buf []byte, arr []any) ([]byte, error) {
	buf = append(buf, '[')
	for i, item := range arr {
		if i > 0 {
			buf = append(buf, ',')
		}
		var err error
		buf, err = canonicalizeValue(buf, item)
		if err != nil {
			return nil, err
		}
	}
	return append(buf, ']'), nil
}

func canonicalizeObject(buf []byte, m map[string]any) ([]byte, error) {
	if len(m) == 0 {
		return append(buf, '{', '}'), nil
	}

	// Sort keys by UTF-16 code-unit lexicographic order (RFC 8785 §3.2.3).
	type kv struct {
		k     string
		units []uint16
	}
	entries := make([]kv, 0, len(m))
	for k := range m {
		entries = append(entries, kv{k: k, units: utf16.Encode([]rune(k))})
	}
	sort.Slice(entries, func(i, j int) bool {
		return ltUTF16Units(entries[i].units, entries[j].units)
	})

	buf = append(buf, '{')
	for i, e := range entries {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendCanonicalString(buf, e.k)
		buf = append(buf, ':')
		var err error
		buf, err = canonicalizeValue(buf, m[e.k])
		if err != nil {
			return nil, err
		}
	}
	return append(buf, '}'), nil
}

func ltUTF16Units(a, b []uint16) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// ---------------------------------------------------------------------------
// String escaping (RFC 8785 §3.2.2.2)
// ---------------------------------------------------------------------------

func appendCanonicalString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			// Invalid UTF-8: emit a literal replacement char's bytes.
			buf = append(buf, 0xEF, 0xBF, 0xBD)
			i++
			continue
		}
		switch r {
		case '"':
			buf = append(buf, '\\', '"')
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\b':
			buf = append(buf, '\\', 'b')
		case '\t':
			buf = append(buf, '\\', 't')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\f':
			buf = append(buf, '\\', 'f')
		case '\r':
			buf = append(buf, '\\', 'r')
		default:
			if r < 0x20 {
				buf = append(buf, '\\', 'u', '0', '0')
				const hexd = "0123456789abcdef"
				buf = append(buf, hexd[(r>>4)&0xF], hexd[r&0xF])
			} else {
				buf = append(buf, s[i:i+size]...)
			}
		}
		i += size
	}
	return append(buf, '"')
}

// ---------------------------------------------------------------------------
// Number formatting (RFC 8785 §3.2.2.3 / ECMAScript Number→String)
// ---------------------------------------------------------------------------

func formatJSONNumber(n json.Number) (string, error) {
	s := string(n)
	if s == "" {
		return "", errors.New("canonical: empty json.Number")
	}
	if isIntegerLiteral(s) {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return strconv.FormatInt(v, 10), nil
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return "", fmt.Errorf("canonical: invalid number %q: %w", s, err)
	}
	return formatFloatES(f)
}

func isIntegerLiteral(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	if s[0] == '-' || s[0] == '+' {
		i++
	}
	if i >= len(s) {
		return false
	}
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func formatFloatES(f float64) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("canonical: %v not representable in JSON", f)
	}
	if f == 0 {
		return "0", nil
	}
	s := strconv.FormatFloat(f, 'e', -1, 64)
	sign := ""
	idx := 0
	if s[0] == '-' {
		sign = "-"
		idx = 1
	}
	eIdx := strings.IndexByte(s, 'e')
	if eIdx < 0 {
		return "", fmt.Errorf("canonical: strconv produced %q without exponent", s)
	}
	mant := s[idx:eIdx]
	expStr := s[eIdx+1:]
	exp, err := strconv.Atoi(expStr)
	if err != nil {
		return "", fmt.Errorf("canonical: bad exponent in %q: %w", s, err)
	}
	var digits string
	if dot := strings.IndexByte(mant, '.'); dot >= 0 {
		digits = mant[:dot] + mant[dot+1:]
	} else {
		digits = mant
	}
	for len(digits) > 1 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
		exp++
	}
	esExp := exp
	if esExp >= -6 && esExp < 21 {
		return sign + renderFixed(digits, esExp), nil
	}
	mantOut := digits
	if len(digits) > 1 {
		mantOut = digits[:1] + "." + digits[1:]
	}
	expSign := "+"
	expAbs := esExp
	if expAbs < 0 {
		expSign = "-"
		expAbs = -expAbs
	}
	return sign + mantOut + "e" + expSign + strconv.Itoa(expAbs), nil
}

func renderFixed(digits string, esExp int) string {
	intLen := esExp + 1
	if intLen >= len(digits) {
		return digits + strings.Repeat("0", intLen-len(digits))
	}
	if intLen <= 0 {
		return "0." + strings.Repeat("0", -intLen) + digits
	}
	return digits[:intLen] + "." + digits[intLen:]
}
