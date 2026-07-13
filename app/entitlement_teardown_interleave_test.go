package app_test

// DoD-7⑥ app-side teardown barriers. Each subtest parks a real Home teardown after
// its handle has been detached but before Home.Close returns, then runs the real
// EntitlementSnapshot concurrently. A snapshot that returns while the failpoint is
// held proves App.Close, channel deletion, and create rollback do not hold a.mu across
// Home.Close (a context timeout cannot rescue a Go mutex wait).

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestEntitlementEnumerationConcurrentWithHomeTeardown(t *testing.T) {
	t.Run("AppClose", func(t *testing.T) {
		env := setupTestApp(t)
		s := fullSetup(t, env)
		entered, release := homeCloseBarrier(t, env.app, "app-close")

		closed := make(chan error, 1)
		go func() { closed <- env.app.Close() }()
		awaitHomeCloseBarrier(t, entered, "App.Close")
		assertEntitlementReturnsWhileBlocked(t, env.app, s.userID, "App.Close")
		release()
		select {
		case err := <-closed:
			if err != nil {
				t.Fatalf("App.Close: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("App.Close did not finish after Home.Close failpoint release")
		}
	})

	t.Run("DeleteChannel", func(t *testing.T) {
		env := setupTestApp(t)
		s := fullSetup(t, env)
		entered, release := homeCloseBarrier(t, env.app, "delete-channel")

		w, done := startAppRequest(env, http.MethodDelete, "/api/channels/"+s.chID, nil, s.cookies)
		awaitHomeCloseBarrier(t, entered, "delete channel")
		assertEntitlementReturnsWhileBlocked(t, env.app, s.userID, "delete channel")
		release()
		select {
		case <-done:
			assertStatus(t, w, http.StatusOK)
		case <-time.After(3 * time.Second):
			t.Fatal("delete channel did not finish after Home.Close failpoint release")
		}
	})

	t.Run("CreateRollback", func(t *testing.T) {
		env := setupTestApp(t)
		reg, cookies := register(t, env, "teardown-rollback@example.com", "secret123", "Rollback")
		principal := reg["id"].(string)
		ws, cookies := createWorkspace(t, env, cookies, "rollback-ws")
		wsID := ws["id"].(string)
		env.app.SetSeedAdmitFailForTest(true)
		defer env.app.SetSeedAdmitFailForTest(false)
		entered, release := homeCloseBarrier(t, env.app, "create-rollback")

		w, done := startAppRequest(env, http.MethodPost, "/api/workspaces/"+wsID+"/channels",
			map[string]any{"name": "doomed"}, cookies)
		awaitHomeCloseBarrier(t, entered, "create rollback")
		assertEntitlementReturnsWhileBlocked(t, env.app, principal, "create rollback")
		release()
		select {
		case <-done:
			assertStatus(t, w, http.StatusInternalServerError)
		case <-time.After(3 * time.Second):
			t.Fatal("create rollback did not finish after Home.Close failpoint release")
		}
	})
}

func homeCloseBarrier(t *testing.T, a *app.App, wantOp string) (<-chan channel.ID, func()) {
	t.Helper()
	entered := make(chan channel.ID, 1)
	releaseCh := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	a.SetHomeCloseHookForTest(func(op string, chID channel.ID) {
		if op != wantOp {
			return
		}
		enterOnce.Do(func() {
			entered <- chID
			<-releaseCh
		})
	})
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	t.Cleanup(func() {
		release()
		a.SetHomeCloseHookForTest(nil)
	})
	return entered, release
}

func awaitHomeCloseBarrier(t *testing.T, entered <-chan channel.ID, op string) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s never reached the lock-outside Home.Close failpoint", op)
	}
}

func assertEntitlementReturnsWhileBlocked(t *testing.T, a *app.App, principal, op string) {
	t.Helper()
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _, err := a.EntitlementSnapshot(ctx, principal)
		done <- result{err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("EntitlementSnapshot concurrent with %s: %v", op, got.err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("EntitlementSnapshot blocked behind a.mu while %s was parked in Home.Close", op)
	}
}

func startAppRequest(env *testEnv, method, path string, body any, cookies []*http.Cookie) (*httptest.ResponseRecorder, <-chan struct{}) {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		env.handler.ServeHTTP(w, req)
		close(done)
	}()
	return w, done
}
