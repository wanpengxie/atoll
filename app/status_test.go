package app

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/lib/introspect"
)

func TestProjectActorStatusPresenceStates(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name       string
		err        error
		known      bool
		val        []byte
		wantStatus int
		wantBody   string
	}{
		{name: "no testimony", wantStatus: http.StatusOK, wantBody: `{"known":false}`},
		{name: "testimony", known: true, val: introspect.MarshalDevicePresence(true), wantStatus: http.StatusOK, wantBody: `{"age_ms":250,"known":true,"online":true}`},
		{name: "snapshot error", err: errors.New("registry unavailable"), wantStatus: http.StatusInternalServerError, wantBody: `{"error":"presence unavailable"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			projectActorStatus(c, test.err, true, test.known, test.val, 1_000, func(receivedAt int64) int64 {
				return 1_250 - receivedAt
			})
			if w.Code != test.wantStatus || w.Body.String() != test.wantBody {
				t.Fatalf("status/body = %d %s, want %d %s", w.Code, w.Body.String(), test.wantStatus, test.wantBody)
			}
		})
	}
}
