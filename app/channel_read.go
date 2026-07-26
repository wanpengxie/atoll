package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
)

type observeReason string

const (
	observeAllowed         observeReason = ""
	observeNowMember       observeReason = "now_member"
	observeUnavailable     observeReason = "capability_unavailable"
	observeHostUnavailable observeReason = "channel_unavailable"
	observeChannelAbsent   observeReason = "channel_not_found"
	observeChannelRetired  observeReason = "channel_retired"
)

// canObserve is the single realm policy gate for observer Readers. P1 is
// realm-public: authentication is established by the HTTP membrane or by a
// trusted in-channel requester before this method is called. The source
// channel must exist, be serving, retain its realm-tool member, and the
// principal must not already be an active human member.
func (a *App) canObserve(ctx context.Context, chID channel.ID, principal string) (channelhost.Bundle, channel.Reader, observeReason, error) {
	if principal == "" {
		return nil, channel.Reader{}, observeUnavailable, nil
	}
	bundle, err := a.acquireBundle(ctx, chID)
	if err != nil {
		return nil, channel.Reader{}, observeReasonForBundleError(err), nil
	}
	reader, reason, err := a.readerForPrincipal(ctx, bundle, principal, false)
	return bundle, reader, reason, err
}

func (a *App) readSubject(ctx context.Context, chID channel.ID, principal string) (channelhost.Bundle, channel.Reader, observeReason, error) {
	bundle, err := a.acquireBundle(ctx, chID)
	if err != nil {
		return nil, channel.Reader{}, observeReasonForBundleError(err), nil
	}
	reader, reason, err := a.readerForPrincipal(ctx, bundle, principal, true)
	return bundle, reader, reason, err
}

func observeReasonForBundleError(err error) observeReason {
	if errors.Is(err, errChannelNotFound) {
		return observeChannelAbsent
	}
	return observeHostUnavailable
}

// readerForPrincipal layers read authorization on one already-acquired Bundle.
// Acquisition and directory/serving classification belong exclusively to
// acquireBundle; this function decides only member-vs-observer policy.
func (a *App) readerForPrincipal(ctx context.Context, bundle channelhost.Bundle, principal string, membersAllowed bool) (channel.Reader, observeReason, error) {
	if principal == "" {
		return channel.Reader{}, observeUnavailable, nil
	}
	memberID, found, err := bundle.View().ResolvePrincipal(ctx, actor.KindHuman, principal)
	if err != nil {
		return channel.Reader{}, observeUnavailable, err
	}
	if found {
		if membersAllowed {
			return channel.Reader{ActorID: memberID, Mode: channel.ReaderMember}, observeAllowed, nil
		}
		return channel.Reader{}, observeNowMember, nil
	}
	hasTool, err := bundle.View().HasDeclaredInstance(ctx, realmToolDeclID)
	if err != nil {
		return channel.Reader{}, observeUnavailable, err
	}
	if !hasTool {
		return channel.Reader{}, observeUnavailable, nil
	}
	return channel.Reader{Principal: principal, Mode: channel.ReaderObserver}, observeAllowed, nil
}

func writeReadFailure(c *gin.Context, reason observeReason, err error) {
	switch reason {
	case observeChannelAbsent:
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
	case observeNowMember:
		c.JSON(http.StatusConflict, gin.H{"code": string(observeNowMember)})
	case observeHostUnavailable:
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": string(observeHostUnavailable)})
	default:
		if err != nil {
			c.Error(err) // keep the outward contract closed while retaining diagnostics
		}
		c.JSON(http.StatusConflict, gin.H{"code": string(observeUnavailable)})
	}
}

func parsePage(c *gin.Context) (int64, int, bool) {
	after := int64(0)
	if raw := c.Query("after_seq"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid after_seq"})
			return 0, 0, false
		}
		after = value
	}
	limit := 100
	if raw := c.Query("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 || value > 500 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
			return 0, 0, false
		}
		limit = value
	}
	return after, limit, true
}

func (a *App) handleListMessages(c *gin.Context) {
	after, limit, ok := parsePage(c)
	if !ok {
		return
	}
	bundle, reader, reason, err := a.readSubject(c.Request.Context(), channel.ID(c.Param("chID")), middleware.UserID(c))
	if reason != observeAllowed || err != nil {
		writeReadFailure(c, reason, err)
		return
	}
	rows, scanned, err := bundle.View().ReadVisibleAfterSeq(c.Request.Context(), reader, after, limit)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"messages": rows, "scanned_through_seq": scanned})
}

func writeSSE(w http.ResponseWriter, event string, value any) error {
	if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", mustJSON(value)); err != nil {
		return err
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func mustJSON(value any) []byte {
	raw, _ := jsonMarshal(value)
	return raw
}

// jsonMarshal is a variable-free seam kept tiny so SSE formatting has one
// error-free call site; all values supplied here are protocol DTOs.
func jsonMarshal(value any) ([]byte, error) { return json.Marshal(value) }

func (a *App) handleObserveChannel(c *gin.Context) {
	chID := channel.ID(c.Param("chID"))
	principal := middleware.UserID(c)
	bundle, reader, reason, err := a.canObserve(c.Request.Context(), chID, principal)
	if reason != observeAllowed || err != nil {
		writeReadFailure(c, reason, err)
		return
	}
	after, _, ok := parsePage(c)
	if !ok {
		return
	}
	w := c.Writer
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	wake, cancel := bundle.Gateway().Subscribe()
	defer func() { cancel() }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		current, currentReader, currentReason, gateErr := a.canObserve(c.Request.Context(), chID, principal)
		if currentReason != observeAllowed || gateErr != nil {
			terminal := currentReason
			if terminal == observeChannelAbsent {
				terminal = observeChannelRetired
			} else if terminal == observeAllowed || terminal == observeHostUnavailable {
				terminal = observeUnavailable
			}
			_ = writeSSE(w, "terminated", gin.H{"type": "terminated", "code": string(terminal)})
			return
		}
		if current.Generation() != bundle.Generation() {
			cancel()
			bundle = current
			wake, cancel = bundle.Gateway().Subscribe()
		}
		reader = currentReader
		rows, scanned, readErr := bundle.View().ReadVisibleAfterSeq(c.Request.Context(), reader, after, 100)
		if readErr != nil {
			_ = writeSSE(w, "terminated", gin.H{"type": "terminated", "code": string(observeUnavailable)})
			return
		}
		for _, row := range rows {
			if err := writeSSE(w, "message", row); err != nil {
				return
			}
		}
		after = scanned
		if len(rows) == 100 {
			continue
		}
		select {
		case <-c.Request.Context().Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

func resourceListQuery(c *gin.Context) (channel.ResourceListQuery, bool) {
	q := channel.ResourceListQuery{Prefix: c.Query("prefix"), Cursor: c.Query("cursor"), Limit: 100}
	if raw := c.Query("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 || limit > 200 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
			return q, false
		}
		q.Limit = limit
	}
	return q, true
}

func (a *App) resourceSubject(c *gin.Context) (channelhost.Bundle, channel.Reader, bool) {
	bundle, reader, reason, err := a.readSubject(c.Request.Context(), channel.ID(c.Param("chID")), middleware.UserID(c))
	if reason != observeAllowed || err != nil {
		writeReadFailure(c, reason, err)
		return nil, channel.Reader{}, false
	}
	return bundle, reader, true
}

func writeRealmFailure(c *gin.Context, err error) {
	var realmErr *channel.RealmError
	if errors.As(err, &realmErr) {
		switch realmErr.Code {
		case channel.RealmResourceNotFound:
			c.JSON(http.StatusNotFound, gin.H{"error": string(realmErr.Code)})
		case channel.RealmForbidden:
			c.JSON(http.StatusForbidden, gin.H{"error": string(realmErr.Code)})
		case channel.RealmInvalidRequest:
			c.JSON(http.StatusBadRequest, gin.H{"error": string(realmErr.Code)})
		case channel.RealmCapabilityUnavailable:
			c.JSON(http.StatusConflict, gin.H{"code": string(realmErr.Code)})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": string(realmErr.Code)})
		}
		return
	}
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
}

func (a *App) handleListResources(c *gin.Context) {
	bundle, reader, ok := a.resourceSubject(c)
	if !ok {
		return
	}
	query, ok := resourceListQuery(c)
	if !ok {
		return
	}
	page, err := bundle.View().Resources().List(c.Request.Context(), reader, query)
	if err != nil {
		writeRealmFailure(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

func (a *App) handleStatResource(c *gin.Context) {
	bundle, reader, ok := a.resourceSubject(c)
	if !ok {
		return
	}
	meta, err := bundle.View().Resources().Stat(c.Request.Context(), reader, resource.ResourceID(c.Param("rid")))
	if err != nil {
		writeRealmFailure(c, err)
		return
	}
	c.JSON(http.StatusOK, meta)
}

func (a *App) handleFetchResource(c *gin.Context) {
	bundle, reader, ok := a.resourceSubject(c)
	if !ok {
		return
	}
	fetch, err := bundle.View().Resources().Fetch(c.Request.Context(), reader, resource.ResourceID(c.Param("rid")))
	if err != nil {
		writeRealmFailure(c, err)
		return
	}
	defer fetch.Body.Close()
	c.Header("Content-Type", "application/octet-stream")
	c.Header("X-Resource-ID", string(fetch.Meta.ID))
	buffer := make([]byte, 32*1024)
	for {
		if reader.Mode == channel.ReaderObserver {
			_, _, reason, gateErr := a.canObserve(c.Request.Context(), channel.ID(c.Param("chID")), reader.Principal)
			if reason != observeAllowed || gateErr != nil {
				return
			}
		}
		n, readErr := fetch.Body.Read(buffer)
		if n > 0 {
			if _, err := c.Writer.Write(buffer[:n]); err != nil {
				return
			}
			c.Writer.Flush()
		}
		if errors.Is(readErr, io.EOF) {
			return
		}
		if readErr != nil {
			return
		}
	}
}
