package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

func TestRequestIDMiddlewareEchoesIncomingAndContext(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(requestIDMiddleware())
	r.GET("/", func(c *gin.Context) {
		if got := requestctx.RequestID(c.Request.Context()); got != "req-123" {
			t.Fatalf("request context id=%q want req-123", got)
		}
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(requestctx.Header, " req-123 ")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if got := rec.Header().Get(requestctx.Header); got != "req-123" {
		t.Fatalf("response request id=%q want req-123", got)
	}
}

func TestRequestIDMiddlewareMintsMissingID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(requestIDMiddleware())
	r.GET("/", func(c *gin.Context) {
		if got := requestctx.RequestID(c.Request.Context()); got == "" {
			t.Fatal("request context id empty")
		}
		c.Status(http.StatusNoContent)
	})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := rec.Header().Get(requestctx.Header); got == "" {
		t.Fatal("response request id empty")
	}
}
