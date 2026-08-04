package app

import (
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/contract"
)

func writeAPIError(c *gin.Context, status int, code contract.ErrorCode, message string) {
	c.JSON(status, contract.Error{Code: contract.NormalizeErrorCode(code), Message: message})
}

func writeAPIErrorDetails(c *gin.Context, status int, code contract.ErrorCode, message string, details any) {
	c.JSON(status, contract.Error{Code: contract.NormalizeErrorCode(code), Message: message, Details: details})
}

func writeRetryingAPIError(c *gin.Context, status int, code contract.ErrorCode, message string) {
	yes := true
	c.JSON(status, contract.Error{Code: contract.NormalizeErrorCode(code), Message: message, WillRetry: &yes})
}

func rejectRequestBody(c *gin.Context) {
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, 1))
	if err != nil || len(raw) != 0 {
		writeAPIError(c, http.StatusBadRequest, contract.CodeBadPayload, "request body is not allowed")
		c.Abort()
		return
	}
	c.Next()
}

func decodeRequest(c *gin.Context, out any) bool {
	if err := contract.DecodeRequest(c.Request.Body, out); err != nil {
		writeAPIErrorDetails(c, http.StatusBadRequest, contract.CodeBadPayload, "invalid JSON request", map[string]any{"cause": err.Error()})
		return false
	}
	return true
}
