package identity_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/coagent-ai/coagent/server/identity"
	"github.com/coagent-ai/coagent/server/store"
)

// testService opens a fresh in-tempdir sqlite DB, applies migrations,
// returns the identity Service. The notify hook is captured into a
// channel so tests can read the demo verification code.
func testService(t *testing.T) (*identity.Service, chan string) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "id.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	codeCh := make(chan string, 4)
	svc := identity.NewService(db, identity.Config{
		SessionSecret:   "test-secret",
		BcryptCost:      4,
		SessionTTL:      time.Hour,
		VerificationTTL: time.Hour,
		NotifyCode: func(email, code string, purpose identity.VerificationPurpose) {
			codeCh <- code
		},
	})
	return svc, codeCh
}

func TestRegisterRequiresValidInputs(t *testing.T) {
	t.Parallel()
	svc, _ := testService(t)
	ctx := context.Background()

	cases := []struct {
		name string
		in   identity.RegisterInput
		want error
	}{
		{"empty email", identity.RegisterInput{Password: "password123", Code: "123456"}, identity.ErrEmailRequired},
		{"empty password", identity.RegisterInput{Email: "a@b.com", Code: "123456"}, identity.ErrPasswordRequired},
		{"short password", identity.RegisterInput{Email: "a@b.com", Password: "short", Code: "123456"}, identity.ErrPasswordTooShort},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := svc.Register(ctx, tc.in); err != tc.want {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRegisterLoginRoundtrip(t *testing.T) {
	t.Parallel()
	svc, codeCh := testService(t)
	ctx := context.Background()

	if _, err := svc.IssueCode(ctx, "Alice@Example.com", identity.PurposeRegister); err != nil {
		t.Fatalf("IssueCode: %v", err)
	}
	code := <-codeCh

	user, err := svc.Register(ctx, identity.RegisterInput{
		Email:    "alice@example.com",
		Password: "topsecret123",
		Code:     code,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if user.Email != "alice@example.com" {
		t.Errorf("email = %q, want alice@example.com (lowercase)", user.Email)
	}
	if !user.EmailVerified {
		t.Errorf("EmailVerified = false, want true (code consumed)")
	}

	res, err := svc.Login(ctx, identity.LoginInput{
		Email:    "alice@example.com",
		Password: "topsecret123",
	})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.User.ID != user.ID {
		t.Errorf("login user id = %q, want %q", res.User.ID, user.ID)
	}
	if res.Token == "" {
		t.Errorf("token empty")
	}

	resolved, err := svc.Authenticate(ctx, res.Token)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if resolved.ID != user.ID {
		t.Errorf("authenticate id = %q, want %q", resolved.ID, user.ID)
	}

	// Logout invalidates the token.
	if err := svc.Logout(ctx, res.Token); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if _, err := svc.Authenticate(ctx, res.Token); err != identity.ErrSessionInvalid {
		t.Errorf("post-logout auth err = %v, want ErrSessionInvalid", err)
	}
}

func TestRegisterRejectsBadCode(t *testing.T) {
	t.Parallel()
	svc, _ := testService(t)
	ctx := context.Background()

	if _, err := svc.IssueCode(ctx, "bob@example.com", identity.PurposeRegister); err != nil {
		t.Fatalf("IssueCode: %v", err)
	}
	_, err := svc.Register(ctx, identity.RegisterInput{
		Email: "bob@example.com", Password: "topsecret123", Code: "999999",
	})
	if err != identity.ErrCodeInvalid {
		t.Errorf("err = %v, want ErrCodeInvalid", err)
	}
}

func TestDuplicateEmail(t *testing.T) {
	t.Parallel()
	svc, codeCh := testService(t)
	ctx := context.Background()

	if _, err := svc.IssueCode(ctx, "carol@example.com", identity.PurposeRegister); err != nil {
		t.Fatalf("IssueCode: %v", err)
	}
	code := <-codeCh
	if _, err := svc.Register(ctx, identity.RegisterInput{
		Email: "carol@example.com", Password: "topsecret123", Code: code,
	}); err != nil {
		t.Fatalf("register #1: %v", err)
	}

	if _, err := svc.IssueCode(ctx, "carol@example.com", identity.PurposeRegister); err != nil {
		t.Fatalf("IssueCode #2: %v", err)
	}
	code2 := <-codeCh
	_, err := svc.Register(ctx, identity.RegisterInput{
		Email: "carol@example.com", Password: "anotherpwd", Code: code2,
	})
	if err != identity.ErrEmailAlreadyExists {
		t.Errorf("err = %v, want ErrEmailAlreadyExists", err)
	}
}

func TestLoginWrongPassword(t *testing.T) {
	t.Parallel()
	svc, codeCh := testService(t)
	ctx := context.Background()

	if _, err := svc.IssueCode(ctx, "dave@example.com", identity.PurposeRegister); err != nil {
		t.Fatalf("IssueCode: %v", err)
	}
	code := <-codeCh
	if _, err := svc.Register(ctx, identity.RegisterInput{
		Email: "dave@example.com", Password: "topsecret123", Code: code,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err := svc.Login(ctx, identity.LoginInput{Email: "dave@example.com", Password: "wrong-pwd"})
	if err != identity.ErrInvalidCredentials {
		t.Errorf("err = %v, want ErrInvalidCredentials", err)
	}
}

func TestExpiredCodeRejected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	codeCh := make(chan string, 1)

	now := time.Unix(1_700_000_000, 0)
	clock := &fakeClock{now: now}
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "exp.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := identity.NewService(db, identity.Config{
		SessionSecret:   "test",
		BcryptCost:      4,
		SessionTTL:      time.Hour,
		VerificationTTL: time.Minute,
		Now:             clock.Now,
		NotifyCode: func(email, code string, purpose identity.VerificationPurpose) {
			codeCh <- code
		},
	})

	if _, err := svc.IssueCode(ctx, "exp@example.com", identity.PurposeRegister); err != nil {
		t.Fatalf("IssueCode: %v", err)
	}
	code := <-codeCh

	// Advance past TTL.
	clock.now = clock.now.Add(2 * time.Minute)

	_, err = svc.Register(ctx, identity.RegisterInput{
		Email: "exp@example.com", Password: "topsecret123", Code: code,
	})
	if err != identity.ErrCodeInvalid {
		t.Errorf("err = %v, want ErrCodeInvalid (expired)", err)
	}
}

type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }
