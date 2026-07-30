package app

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const sysopCodeDaemonNotFound channelspec.OperationErrorCode = "daemon_not_found"

type sysopUnknownError struct{ cause error }

func (e *sysopUnknownError) Error() string {
	if e.cause == nil {
		return "sysop result unknown"
	}
	return "sysop result unknown: " + e.cause.Error()
}

func (e *sysopUnknownError) Unwrap() error { return e.cause }

type sysopGateError struct {
	Status int
	Code   string
	Detail string
}

func (e *sysopGateError) Error() string {
	if e.Detail != "" {
		return e.Detail
	}
	return e.Code
}

type sysopOutcome[T any] struct {
	Value   T
	Changed bool
}

type sysopForward[T any] struct {
	Predicate func(channelhost.Bundle) (T, bool, error)
	Qualify   func(channelhost.Bundle) error
	Invoke    func(channelhost.SysOp, string) (T, error)
	Changed   func(T) bool
}

// forwardSysop is the one predicate → qualification → membrane-forward
// skeleton for all five structural words and both HTTP/RealmOps entry families.
func forwardSysop[T any](ctx context.Context, a *App, chID channel.ID, call sysopForward[T]) (sysopOutcome[T], error) {
	var zero sysopOutcome[T]
	release := a.channelLocks.lock(string(chID))
	defer release()

	exists, err := a.channelExists(ctx, string(chID))
	if err != nil {
		return zero, &sysopUnknownError{cause: err}
	}
	if !exists {
		return zero, &sysopGateError{Status: http.StatusNotFound, Code: "channel_not_found", Detail: "channel not found"}
	}
	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return zero, &sysopUnknownError{cause: errChannelUnavailable}
	}
	if call.Predicate != nil {
		value, achieved, err := call.Predicate(bundle)
		if err != nil {
			return zero, &sysopUnknownError{cause: err}
		}
		if achieved {
			return sysopOutcome[T]{Value: value, Changed: false}, nil
		}
	}
	if call.Qualify != nil {
		if err := call.Qualify(bundle); err != nil {
			return zero, err
		}
	}
	value, err := call.Invoke(bundle.SysOp(), uuid.NewString())
	if err != nil {
		var operationErr *channelspec.OperationError
		if errors.As(err, &operationErr) && !operationErr.Retryable {
			return zero, operationErr
		}
		return zero, &sysopUnknownError{cause: err}
	}
	changed := true
	if call.Changed != nil {
		changed = call.Changed(value)
	}
	return sysopOutcome[T]{Value: value, Changed: changed}, nil
}

type sysopErrorClass uint8

const (
	sysopBadRequest sysopErrorClass = iota
	sysopForbidden
	sysopNotFound
	sysopConflict
)

func classifySysopError(code string) sysopErrorClass {
	switch channelspec.OperationErrorCode(code) {
	case channelspec.ErrCodeBadPayload, channelspec.ErrCodeUnknownClass, channelspec.ErrCodeInvalidDesiredHost:
		return sysopBadRequest
	case channelspec.ErrCodeForbidden, channelspec.ErrCodeNotAcceptedSource:
		return sysopForbidden
	case channelspec.ErrCodeDeclNotFound, sysopCodeDaemonNotFound:
		return sysopNotFound
	default:
		return sysopConflict
	}
}

// sysopErrorHTTP maps a membrane operate code to its HTTP status.
func sysopErrorHTTP(code string) int {
	switch classifySysopError(code) {
	case sysopBadRequest:
		return http.StatusBadRequest
	case sysopForbidden:
		return http.StatusForbidden
	case sysopNotFound:
		return http.StatusNotFound
	default:
		return http.StatusConflict
	}
}

func writeSysopError(c *gin.Context, err error) {
	var unknown *sysopUnknownError
	if errors.As(err, &unknown) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "result unknown", "retry": "safe"})
		return
	}
	var gate *sysopGateError
	if errors.As(err, &gate) {
		c.JSON(gate.Status, gin.H{"error": gate.Code})
		return
	}
	var operationErr *channelspec.OperationError
	if errors.As(err, &operationErr) {
		c.JSON(sysopErrorHTTP(string(operationErr.Code)), gin.H{
			"error": operationErr.Detail, "error_code": operationErr.Code,
		})
		return
	}
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": "result unknown", "retry": "safe"})
}

func sysopRealmErrorCode(code string) channelspec.RealmErrorCode {
	switch classifySysopError(code) {
	case sysopBadRequest:
		return channelspec.RealmInvalidRequest
	case sysopForbidden:
		return channelspec.RealmForbidden
	case sysopNotFound:
		if channelspec.OperationErrorCode(code) == channelspec.ErrCodeDeclNotFound {
			return channelspec.RealmDeclNotFound
		}
		return channelspec.RealmUnavailable
	default:
		if channelspec.OperationErrorCode(code) == channelspec.ErrCodeChannelUnavailable {
			return channelspec.RealmChannelUnavailable
		}
		return channelspec.RealmConflict
	}
}

func memberGate(ctx context.Context, bundle channelhost.Bundle, principal string) error {
	_, found, err := resolveMember(ctx, bundle, principal)
	if err != nil {
		return &sysopUnknownError{cause: err}
	}
	if !found {
		return &sysopGateError{Status: http.StatusForbidden, Code: "forbidden", Detail: "active channel membership required"}
	}
	return nil
}
