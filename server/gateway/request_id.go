package gateway

import (
	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := requestctx.Normalize(c.GetHeader(requestctx.Header))
		if id == "" {
			id = requestctx.NewID()
		}
		c.Header(requestctx.Header, id)
		c.Request = c.Request.WithContext(requestctx.WithRequestID(c.Request.Context(), id))
		c.Next()
	}
}
