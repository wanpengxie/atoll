package framework

import (
	"errors"
	"strings"
	"testing"
)

func sampleSpec() TokenSpec {
	return TokenSpec{
		SessionID: "sess-1",
		ChannelID: "channel-1",
		DeviceID:  "device-1",
		IssuedAt:  1_000_000,
		ExpiresAt: 2_000_000,
	}
}

// TestTokenIssueParseRoundTrip verifies a signed token decodes back to
// the same spec and produces a deterministic fingerprint.
func TestTokenIssueParseRoundTrip(t *testing.T) {
	secret := []byte("super-secret")
	spec := sampleSpec()
	token, fp, err := Issue(secret, spec)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if !strings.Contains(token, ".") {
		t.Fatalf("token must contain separator: %q", token)
	}
	if len(fp) != FingerprintLength {
		t.Errorf("fingerprint length = %d want %d", len(fp), FingerprintLength)
	}

	got, err := Parse(secret, token)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got != spec {
		t.Errorf("round-trip mismatch:\n got=%+v\nwant=%+v", got, spec)
	}

	// Fingerprint helper agrees with the Issue return value.
	if Fingerprint(token) != fp {
		t.Errorf("Fingerprint() = %q want %q", Fingerprint(token), fp)
	}
}

// TestTokenSignatureSensitive verifies a bit-flip in the body makes
// Parse reject the token.
func TestTokenSignatureSensitive(t *testing.T) {
	secret := []byte("secret")
	token, _, err := Issue(secret, sampleSpec())
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	// Replace one body byte (first character).
	tampered := "x" + token[1:]
	if _, err := Parse(secret, tampered); !errors.Is(err, ErrTokenSignatureInvalid) {
		t.Errorf("tampered body should yield signature-invalid; got %v", err)
	}
}

// TestTokenWrongSecret verifies a different secret rejects the token.
func TestTokenWrongSecret(t *testing.T) {
	token, _, err := Issue([]byte("secret-A"), sampleSpec())
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := Parse([]byte("secret-B"), token); !errors.Is(err, ErrTokenSignatureInvalid) {
		t.Errorf("wrong secret should yield signature-invalid; got %v", err)
	}
}

// TestTokenMalformed covers the explicit malformed-shape sentinels.
func TestTokenMalformed(t *testing.T) {
	cases := []string{
		"",
		"only-one-part",
		".body-empty",
		"body-only.",
	}
	for _, in := range cases {
		if _, err := Parse([]byte("s"), in); !errors.Is(err, ErrTokenMalformed) {
			t.Errorf("Parse(%q) should be malformed; got %v", in, err)
		}
	}
}

// TestTokenSpecValidate covers each required-field rule + IssuedAt /
// ExpiresAt ordering.
func TestTokenSpecValidate(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*TokenSpec)
		want string
	}{
		{"missing-session", func(s *TokenSpec) { s.SessionID = "" }, "SessionID"},
		{"missing-channel", func(s *TokenSpec) { s.ChannelID = "" }, "ChannelID"},
		{"missing-device", func(s *TokenSpec) { s.DeviceID = "" }, "DeviceID"},
		{"iat-zero", func(s *TokenSpec) { s.IssuedAt = 0 }, "IssuedAt"},
		{"exp-before-iat", func(s *TokenSpec) { s.ExpiresAt = s.IssuedAt - 1 }, "ExpiresAt"},
		{"exp-eq-iat", func(s *TokenSpec) { s.ExpiresAt = s.IssuedAt }, "ExpiresAt"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := sampleSpec()
			c.mod(&spec)
			err := spec.Validate()
			if err == nil {
				t.Fatalf("expected error mentioning %q", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q should mention %q", err, c.want)
			}
		})
	}
}

// TestTokenIsExpired covers the millisecond-epoch boundary.
func TestTokenIsExpired(t *testing.T) {
	spec := TokenSpec{IssuedAt: 100, ExpiresAt: 200}
	if spec.IsExpired(199) {
		t.Error("should not be expired before ExpiresAt")
	}
	if !spec.IsExpired(200) {
		t.Error("should be expired at ExpiresAt boundary")
	}
	if !spec.IsExpired(201) {
		t.Error("should be expired after ExpiresAt boundary")
	}
}

// TestIssueRejectsEmptySecret covers the up-front secret check.
func TestIssueRejectsEmptySecret(t *testing.T) {
	if _, _, err := Issue(nil, sampleSpec()); err == nil {
		t.Error("Issue with nil secret should fail")
	}
}

// TestIssueRejectsInvalidSpec covers the up-front spec validate.
func TestIssueRejectsInvalidSpec(t *testing.T) {
	bad := sampleSpec()
	bad.SessionID = ""
	if _, _, err := Issue([]byte("s"), bad); err == nil {
		t.Error("Issue with invalid spec should fail")
	}
}

// TestFingerprintMalformed: best-effort Fingerprint helper returns ""
// instead of panicking on garbage.
func TestFingerprintMalformed(t *testing.T) {
	if Fingerprint("") != "" {
		t.Error("empty token -> empty fingerprint")
	}
	if Fingerprint("no-separator") != "" {
		t.Error("malformed token -> empty fingerprint")
	}
	if Fingerprint("body.!@#$") != "" {
		t.Error("malformed sig -> empty fingerprint")
	}
}
