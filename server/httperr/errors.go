package httperr

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// MaxBodyBytes caps request bodies before JSON binding/decoding. The
// ContentLength precheck gives callers a clean 413 for normal requests;
// http.MaxBytesReader still enforces the limit for chunked bodies.
func MaxBodyBytes(limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if limit > 0 && c.Request.ContentLength > limit {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body too large"})
			return
		}
		if limit > 0 && c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
		}
		c.Next()
	}
}

// Respond returns caller-fixable errors verbatim, but keeps 5xx details
// out of HTTP bodies.
func Respond(c *gin.Context, component string, status int, err error) {
	if status >= http.StatusInternalServerError {
		Internal(c, component, err)
		return
	}
	if err == nil {
		c.JSON(status, gin.H{"error": http.StatusText(status)})
		return
	}
	c.JSON(status, gin.H{"error": err.Error()})
}

// Internal emits an opaque 5xx response and logs the original error with
// a caller-visible error_id for correlation.
func Internal(c *gin.Context, component string, err error) {
	errorID := newErrorID()
	if err == nil {
		err = fmt.Errorf("internal server error")
	}
	log.Printf("component=%s error_id=%s method=%s path=%s error=%v", component, errorID, c.Request.Method, c.Request.URL.Path, err)
	c.JSON(http.StatusInternalServerError, gin.H{
		"error":    "internal",
		"error_id": errorID,
	})
}

func newErrorID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
