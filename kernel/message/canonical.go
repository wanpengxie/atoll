package message

// canonical.go implements the RFC 8785 (JCS) canonical JSON
// serialization mandated by L2 for the v4 protocol baseline.
//
// Sole remaining consumer: action_ledger.ledger_key — caller-side
// idempotency key derivation (L2 §1.4.10.1). See kernel/ledger/key.go
// for the derivation rule (CanonicalizeJSON over the ledger key domain).
//
// (The v1 message-dedupe hash + adapter deterministic response-id hash
// were retired with the dedup machinery: message.id is now a random uuid
// correlation anchor and seq is the store-allocated truth identity.)
//
// RFC 8785 reference: https://www.rfc-editor.org/rfc/rfc8785
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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

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
	return canonicalizeReflectValue(buf, v)
}

func canonicalizeReflectValue(buf []byte, v any) ([]byte, error) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return append(buf, 'n', 'u', 'l', 'l'), nil
	}
	switch rv.Kind() {
	case reflect.String:
		return appendCanonicalString(buf, rv.String()), nil
	case reflect.Bool:
		if rv.Bool() {
			return append(buf, 't', 'r', 'u', 'e'), nil
		}
		return append(buf, 'f', 'a', 'l', 's', 'e'), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.AppendUint(buf, rv.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		s, err := formatFloatES(rv.Float())
		if err != nil {
			return nil, err
		}
		return append(buf, s...), nil
	case reflect.Slice, reflect.Array:
		buf = append(buf, '[')
		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				buf = append(buf, ',')
			}
			var err error
			buf, err = canonicalizeValue(buf, rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
		}
		return append(buf, ']'), nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			break
		}
		m := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			m[iter.Key().String()] = iter.Value().Interface()
		}
		return canonicalizeObject(buf, m)
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return append(buf, 'n', 'u', 'l', 'l'), nil
		}
		return canonicalizeValue(buf, rv.Elem().Interface())
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
