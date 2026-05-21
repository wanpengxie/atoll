package identity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/server/store"
)

func newRateLimitedRouter(t *testing.T) (*Service, http.Handler) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "id-routes.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := NewService(db, Config{
		SessionSecret:       "test-secret",
		BcryptCost:          4,
		SessionTTL:          time.Hour,
		VerificationTTL:     time.Hour,
		AuthRateLimitWindow: time.Hour,
		AuthRateLimitMax:    1,
		NotifyCode:          func(email, code string, purpose VerificationPurpose) {},
	})
	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api")
	svc.RegisterPublicRoutes(api)
	return svc, r
}

func TestAuthRoutesRateLimitPerEmail(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		body       string
		firstWant  int
		prepareSvc func(t *testing.T, svc *Service)
	}{
		{
			name:      "issue code",
			path:      "/api/identity/verification/issue",
			body:      `{"email":"limited@example.com"}`,
			firstWant: http.StatusAccepted,
		},
		{
			name:      "register",
			path:      "/api/identity/register",
			body:      `{"email":"limited@example.com","password":"topsecret123"}`,
			firstWant: http.StatusAccepted,
		},
		{
			name:      "login",
			path:      "/api/identity/login",
			body:      `{"email":"limited@example.com","password":"topsecret123"}`,
			firstWant: http.StatusOK,
			prepareSvc: func(t *testing.T, svc *Service) {
				t.Helper()
				if _, err := svc.Register(context.Background(), RegisterInput{
					Email: "limited@example.com", Password: "topsecret123",
				}); err != nil {
					t.Fatalf("seed user: %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, router := newRateLimitedRouter(t)
			if tc.prepareSvc != nil {
				tc.prepareSvc(t, svc)
			}
			first := postJSONFrom(t, router, tc.path, tc.body, "10.0.0.1:1000")
			if first.Code != tc.firstWant {
				t.Fatalf("first status=%d want %d body=%s", first.Code, tc.firstWant, first.Body.String())
			}
			second := postJSONFrom(t, router, tc.path, tc.body, "10.0.0.2:1000")
			if second.Code != http.StatusTooManyRequests {
				t.Fatalf("second status=%d want 429 body=%s", second.Code, second.Body.String())
			}
			if !strings.Contains(second.Body.String(), "rate_limited") {
				t.Fatalf("rate-limit body should be generic, got %s", second.Body.String())
			}
		})
	}
}

func TestAuthRoutesRateLimitPerRemoteAddr(t *testing.T) {
	_, router := newRateLimitedRouter(t)
	first := postJSONFrom(t, router, "/api/identity/verification/issue",
		`{"email":"first@example.com"}`, "10.0.0.9:1000")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d want 202 body=%s", first.Code, first.Body.String())
	}
	second := postJSONFrom(t, router, "/api/identity/verification/issue",
		`{"email":"second@example.com"}`, "10.0.0.9:1001")
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status=%d want 429 body=%s", second.Code, second.Body.String())
	}
}

func postJSONFrom(t *testing.T, h http.Handler, path, body, remoteAddr string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = remoteAddr
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}
