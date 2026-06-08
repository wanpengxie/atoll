// Package app is the product application layer: identity, workspace, channel
// lifecycle, daemon management, and HTTP API. It sits above platform (which
// owns per-channel truth) and below cmd (which wires concrete config).
package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"golang.org/x/crypto/bcrypt"

	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// App is the product application server.
type App struct {
	db     *sql.DB
	logger *slog.Logger
	engine *gin.Engine

	mu    sync.RWMutex
	homes map[channel.ID]*platform.ChannelHome

	channelDBDir string
}

// Config configures the App.
type Config struct {
	DB           *sql.DB
	Logger       *slog.Logger
	ChannelDBDir string // e.g. "/tmp/coagent-dev/channels"
}

// New assembles the App: gin engine, routes, and loads existing channels.
func New(cfg Config) (*App, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	if err := os.MkdirAll(cfg.ChannelDBDir, 0o755); err != nil {
		return nil, fmt.Errorf("app: mkdir channel db dir: %w", err)
	}

	a := &App{
		db:           cfg.DB,
		logger:       logger,
		homes:        make(map[channel.ID]*platform.ChannelHome),
		channelDBDir: cfg.ChannelDBDir,
	}

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	// CORS (allow all for dev).
	engine.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type,Authorization")
		c.Header("Access-Control-Allow-Credentials", "true")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	a.engine = engine
	a.registerRoutes()

	// Load existing channels from DB.
	if err := a.loadChannels(); err != nil {
		return nil, fmt.Errorf("app: load channels: %w", err)
	}

	return a, nil
}

// Run starts the HTTP server.
func (a *App) Run(addr string) error {
	return a.engine.Run(addr)
}

// Close tears down all channel homes.
func (a *App) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var firstErr error
	for id, home := range a.homes {
		if err := home.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(a.homes, id)
	}
	return firstErr
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

func (a *App) registerRoutes() {
	// Identity (no auth required).
	identity := a.engine.Group("/api/identity")
	{
		identity.POST("/register", a.handleRegister)
		identity.POST("/login", a.handleLogin)
		identity.POST("/logout", a.handleLogout)
		identity.GET("/me", a.handleMe)
		identity.POST("/verification/issue", a.handleVerificationIssue)
	}

	// Authenticated API routes.
	api := a.engine.Group("/api")
	api.Use(a.authMiddleware())
	{
		api.GET("/workspaces", a.handleListWorkspaces)
		api.POST("/workspaces", a.handleCreateWorkspace)

		api.GET("/workspaces/:wsID/channels", a.handleListChannels)
		api.POST("/workspaces/:wsID/channels", a.handleCreateChannel)
		api.POST("/workspaces/:wsID/channels/:chID/bind", a.handleBindChannel)

		api.GET("/channels/:chID", a.handleGetChannel)
		api.GET("/channels/:chID/members", a.handleListChannelMembers)
		api.GET("/channels/:chID/actors", a.handleListActors)

		api.GET("/channels/:chID/cursor", a.handleCursor)
		api.GET("/channels/:chID/messages", a.handleListMessages)
		api.POST("/channels/:chID/messages", a.handleSendMessage)

		api.GET("/daemons", a.handleListDaemons)
		api.POST("/daemons", a.handleCreateDaemon)
		api.DELETE("/daemons/:id", a.handleDeleteDaemon)

		api.GET("/channels/:chID/daemons", a.handleListChannelDaemons)
		api.POST("/channels/:chID/daemons/attach", a.handleAttachDaemons)
		api.DELETE("/channels/:chID/daemons/:id/attach", a.handleDetachDaemon)
	}

	// Daemon version stub.
	a.engine.GET("/api/daemon/version", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"latest": "dev", "force": false})
	})

	// WebSocket endpoints.
	a.engine.GET("/ws", a.handleWS)
	a.engine.GET("/compute", a.handleCompute)

	// Static files.
	a.engine.Static("/assets", "web/ui/dist/assets")
	a.engine.StaticFile("/favicon.svg", "web/ui/dist/favicon.svg")
	a.engine.NoRoute(func(c *gin.Context) {
		// For non-API, non-asset paths, serve index.html (SPA fallback).
		if !strings.HasPrefix(c.Request.URL.Path, "/api/") &&
			!strings.HasPrefix(c.Request.URL.Path, "/ws") &&
			!strings.HasPrefix(c.Request.URL.Path, "/compute") {
			c.File("web/ui/dist/index.html")
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
	})
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

const (
	sessionCookieName = "coagent_session"
	sessionDuration   = 30 * 24 * time.Hour
	ctxKeyUserID      = "user_id"
)

func (a *App) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token, err := c.Cookie(sessionCookieName)
		if err != nil || token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
			return
		}
		var userID string
		var expiresAt int64
		err = a.db.QueryRowContext(c.Request.Context(),
			`SELECT user_id, expires_at FROM sessions WHERE token = ?`, token,
		).Scan(&userID, &expiresAt)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid session"})
			return
		}
		if time.Now().UnixMilli() > expiresAt {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "session expired"})
			return
		}
		c.Set(ctxKeyUserID, userID)
		c.Next()
	}
}

func getUserID(c *gin.Context) string {
	v, _ := c.Get(ctxKeyUserID)
	s, _ := v.(string)
	return s
}

// ---------------------------------------------------------------------------
// Identity handlers
// ---------------------------------------------------------------------------

func (a *App) handleRegister(c *gin.Context) {
	var req struct {
		Email       string `json:"email"`
		Password    string `json:"password"`
		DisplayName string `json:"display_name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if req.Email == "" || req.Password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email and password required"})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "hash failed"})
		return
	}

	userID := uuid.NewString()
	now := time.Now().UnixMilli()

	_, err = a.db.ExecContext(c.Request.Context(),
		`INSERT INTO users (id, email, password, display_name, created_at) VALUES (?,?,?,?,?)`,
		userID, req.Email, string(hash), req.DisplayName, now,
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			c.JSON(http.StatusConflict, gin.H{"error": "email already registered"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create user failed"})
		return
	}

	// Auto-login.
	a.setSession(c, userID)
	c.JSON(http.StatusCreated, gin.H{
		"id":           userID,
		"email":        req.Email,
		"display_name": req.DisplayName,
	})
}

func (a *App) handleLogin(c *gin.Context) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	var userID, hash, displayName string
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT id, password, display_name FROM users WHERE email = ?`, req.Email,
	).Scan(&userID, &hash, &displayName)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	a.setSession(c, userID)
	c.JSON(http.StatusOK, gin.H{
		"id":           userID,
		"email":        req.Email,
		"display_name": displayName,
	})
}

func (a *App) handleLogout(c *gin.Context) {
	token, err := c.Cookie(sessionCookieName)
	if err == nil && token != "" {
		_, _ = a.db.ExecContext(c.Request.Context(),
			`DELETE FROM sessions WHERE token = ?`, token,
		)
	}
	c.SetCookie(sessionCookieName, "", -1, "/", "", false, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) handleMe(c *gin.Context) {
	token, err := c.Cookie(sessionCookieName)
	if err != nil || token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}

	var userID string
	var expiresAt int64
	err = a.db.QueryRowContext(c.Request.Context(),
		`SELECT user_id, expires_at FROM sessions WHERE token = ?`, token,
	).Scan(&userID, &expiresAt)
	if err != nil || time.Now().UnixMilli() > expiresAt {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid session"})
		return
	}

	var email, displayName string
	err = a.db.QueryRowContext(c.Request.Context(),
		`SELECT email, display_name FROM users WHERE id = ?`, userID,
	).Scan(&email, &displayName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "user not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":           userID,
		"email":        email,
		"display_name": displayName,
	})
}

func (a *App) handleVerificationIssue(c *gin.Context) {
	// Stub: v1 skips email verification.
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) setSession(c *gin.Context, userID string) {
	token := uuid.NewString()
	now := time.Now().UnixMilli()
	expiresAt := now + sessionDuration.Milliseconds()

	_, _ = a.db.ExecContext(c.Request.Context(),
		`INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?,?,?,?)`,
		token, userID, now, expiresAt,
	)
	c.SetCookie(sessionCookieName, token, int(sessionDuration.Seconds()), "/", "", false, true)
}

// ---------------------------------------------------------------------------
// Workspace handlers
// ---------------------------------------------------------------------------

func (a *App) handleListWorkspaces(c *gin.Context) {
	userID := getUserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT w.id, w.owner_id, w.name, w.created_at
		 FROM workspaces w
		 JOIN workspace_members wm ON w.id = wm.workspace_id
		 WHERE wm.user_id = ?`, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, ownerID, name string
		var createdAt int64
		if err := rows.Scan(&id, &ownerID, &name, &createdAt); err != nil {
			continue
		}
		result = append(result, gin.H{
			"id": id, "owner_id": ownerID, "name": name, "created_at": createdAt,
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, result)
}

func (a *App) handleCreateWorkspace(c *gin.Context) {
	userID := getUserID(c)
	var req struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}

	wsID := uuid.NewString()
	now := time.Now().UnixMilli()

	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "tx failed"})
		return
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(c.Request.Context(),
		`INSERT INTO workspaces (id, owner_id, name, created_at) VALUES (?,?,?,?)`,
		wsID, userID, req.Name, now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create workspace failed"})
		return
	}

	_, err = tx.ExecContext(c.Request.Context(),
		`INSERT INTO workspace_members (workspace_id, user_id, role) VALUES (?,?,?)`,
		wsID, userID, "owner",
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "add member failed"})
		return
	}

	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id": wsID, "owner_id": userID, "name": req.Name, "created_at": now,
	})
}

// ---------------------------------------------------------------------------
// Channel handlers
// ---------------------------------------------------------------------------

func (a *App) handleListChannels(c *gin.Context) {
	wsID := c.Param("wsID")
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, workspace_id, name, type, created_at FROM channels WHERE workspace_id = ?`, wsID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, workspaceID, name, chType string
		var createdAt int64
		if err := rows.Scan(&id, &workspaceID, &name, &chType, &createdAt); err != nil {
			continue
		}
		result = append(result, gin.H{
			"id": id, "workspace_id": workspaceID, "name": name,
			"type": chType, "created_at": createdAt,
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, result)
}

func (a *App) handleCreateChannel(c *gin.Context) {
	wsID := c.Param("wsID")
	var req struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}
	if req.Type == "" {
		req.Type = "group"
	}

	chID := uuid.NewString()
	dbPath := filepath.Join(a.channelDBDir, chID+".db")
	now := time.Now().UnixMilli()

	_, err := a.db.ExecContext(c.Request.Context(),
		`INSERT INTO channels (id, workspace_id, name, type, db_path, created_at) VALUES (?,?,?,?,?,?)`,
		chID, wsID, req.Name, req.Type, dbPath, now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create channel failed"})
		return
	}

	home, err := a.createHome(channel.ID(chID), dbPath)
	if err != nil {
		// Roll back: delete the orphaned channel row.
		_, _ = a.db.ExecContext(c.Request.Context(),
			`DELETE FROM channels WHERE id = ?`, chID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "init channel home failed: " + err.Error()})
		return
	}

	userID := getUserID(c)
	actorID := actor.ActorID("user:" + userID)
	if mErr := home.Membership().Insert(c.Request.Context(), newRecord(actorID, actor.KindHuman)); mErr != nil {
		a.logger.Warn("app: channel membership insert failed", "channel", chID, "err", mErr.Error())
	}

	c.JSON(http.StatusCreated, gin.H{
		"id": chID, "workspace_id": wsID, "name": req.Name,
		"type": req.Type, "created_at": now,
	})
}

func (a *App) handleBindChannel(c *gin.Context) {
	chID := c.Param("chID")
	var req struct {
		DaemonID string `json:"daemon_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.DaemonID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "daemon_id required"})
		return
	}

	_, err := a.db.ExecContext(c.Request.Context(),
		`INSERT OR IGNORE INTO daemon_channels (daemon_id, channel_id) VALUES (?,?)`,
		req.DaemonID, chID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "bind failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) handleGetChannel(c *gin.Context) {
	chID := c.Param("chID")
	var id, workspaceID, name, chType string
	var createdAt int64
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT id, workspace_id, name, type, created_at FROM channels WHERE id = ?`, chID,
	).Scan(&id, &workspaceID, &name, &chType, &createdAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"id": id, "workspace_id": workspaceID, "name": name,
		"type": chType, "created_at": createdAt,
	})
}

func (a *App) handleListChannelMembers(c *gin.Context) {
	chID := c.Param("chID")
	// Get the workspace for this channel, then list workspace_members.
	var wsID string
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT workspace_id FROM channels WHERE id = ?`, chID,
	).Scan(&wsID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}

	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT wm.user_id, wm.role, u.email, u.display_name
		 FROM workspace_members wm
		 JOIN users u ON u.id = wm.user_id
		 WHERE wm.workspace_id = ?`, wsID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var userID, role, email, displayName string
		if err := rows.Scan(&userID, &role, &email, &displayName); err != nil {
			continue
		}
		result = append(result, gin.H{
			"user_id": userID, "role": role, "email": email, "display_name": displayName,
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"members": result})
}

func (a *App) handleListActors(c *gin.Context) {
	chID := c.Param("chID")
	home := a.getHome(channel.ID(chID))
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}

	actors, err := home.Gateway().ListActors(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var result []gin.H
	for _, rec := range actors {
		result = append(result, gin.H{
			"id": string(rec.ID), "kind": string(rec.Kind),
			"binding": string(rec.Binding), "created_at": rec.CreatedAt,
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"channel_id": chID, "actors": result})
}

// ---------------------------------------------------------------------------
// Message handlers
// ---------------------------------------------------------------------------

func (a *App) handleCursor(c *gin.Context) {
	chID := c.Param("chID")
	home := a.getHome(channel.ID(chID))
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}
	seq, err := home.Gateway().MaxSeq(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"last_received_seq": seq})
}

func (a *App) handleListMessages(c *gin.Context) {
	chID := c.Param("chID")
	home := a.getHome(channel.ID(chID))
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}

	afterStr := c.DefaultQuery("after", "0")
	after, _ := strconv.ParseInt(afterStr, 10, 64)
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := home.Gateway().ListMessages(c.Request.Context(), after, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	type storedEnvelope struct {
		Seq        int64            `json:"seq"`
		IsTerminal bool             `json:"is_terminal"`
		Envelope   message.Envelope `json:"envelope"`
	}
	result := make([]storedEnvelope, 0, len(rows))
	for _, r := range rows {
		result = append(result, storedEnvelope{
			Seq: r.Seq, IsTerminal: r.IsTerminal, Envelope: r.Envelope,
		})
	}
	c.JSON(http.StatusOK, result)
}

func (a *App) handleSendMessage(c *gin.Context) {
	chID := c.Param("chID")
	home := a.getHome(channel.ID(chID))
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "read body failed"})
		return
	}

	var req struct {
		ID         string          `json:"id"`
		Type       string          `json:"type"`
		Kind       string          `json:"kind"`
		Payload    json.RawMessage `json:"payload"`
		Audience   []string        `json:"audience"`
		Visibility string          `json:"visibility"`
		ParentID   string          `json:"parent_id"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}

	userID := getUserID(c)
	senderID := actor.ActorID("user:" + userID)

	audience := make([]actor.ActorID, 0, len(req.Audience))
	for _, a := range req.Audience {
		audience = append(audience, actor.ActorID(a))
	}

	gw := home.Gateway()
	env := gw.NewClientEnvelope(
		senderID,
		req.ID,
		req.Type,
		message.Kind(req.Kind),
		req.Payload,
		audience,
	)
	if req.Visibility != "" {
		env.Visibility = message.Visibility(req.Visibility)
	}
	if req.ParentID != "" {
		env.ParentID = message.ID(req.ParentID)
	}

	ctx := harness.CtxWithCaller(c.Request.Context(), gw.CallerContext(senderID))
	res, err := gw.SendMessage(ctx, env)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !res.Accepted() {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error":  string(res.RejectReason),
			"detail": res.RejectDetail,
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message_id": string(res.MessageID),
		"seq":        res.Seq,
	})
}

// ---------------------------------------------------------------------------
// Daemon handlers
// ---------------------------------------------------------------------------

func (a *App) handleListDaemons(c *gin.Context) {
	userID := getUserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, name, status, hostname, platform, last_heartbeat, created_at
		 FROM daemons WHERE owner_id = ?`, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, name, status string
		var hostname, plat sql.NullString
		var lastHB sql.NullInt64
		var createdAt int64
		if err := rows.Scan(&id, &name, &status, &hostname, &plat, &lastHB, &createdAt); err != nil {
			continue
		}
		d := gin.H{
			"id": id, "name": name, "status": status, "created_at": createdAt,
		}
		if hostname.Valid {
			d["hostname"] = hostname.String
		}
		if plat.Valid {
			d["platform"] = plat.String
		}
		if lastHB.Valid {
			d["last_heartbeat"] = lastHB.Int64
		}
		result = append(result, d)
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"daemons": result})
}

func (a *App) handleCreateDaemon(c *gin.Context) {
	userID := getUserID(c)
	var req struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}

	daemonID := uuid.NewString()
	apiKey := uuid.NewString() // plaintext, returned once
	keyHash := hashAPIKey(apiKey)
	now := time.Now().UnixMilli()

	_, err := a.db.ExecContext(c.Request.Context(),
		`INSERT INTO daemons (id, owner_id, name, api_key_hash, created_at) VALUES (?,?,?,?,?)`,
		daemonID, userID, req.Name, keyHash, now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create daemon failed"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":      daemonID,
		"name":    req.Name,
		"api_key": apiKey, // returned only once
	})
}

func (a *App) handleDeleteDaemon(c *gin.Context) {
	daemonID := c.Param("id")
	userID := getUserID(c)

	res, err := a.db.ExecContext(c.Request.Context(),
		`DELETE FROM daemons WHERE id = ? AND owner_id = ?`, daemonID, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "daemon not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) handleListChannelDaemons(c *gin.Context) {
	chID := c.Param("chID")
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT d.id, d.name, d.status, d.created_at
		 FROM daemons d
		 JOIN daemon_channels dc ON d.id = dc.daemon_id
		 WHERE dc.channel_id = ?`, chID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, name, status string
		var createdAt int64
		if err := rows.Scan(&id, &name, &status, &createdAt); err != nil {
			continue
		}
		result = append(result, gin.H{
			"id": id, "name": name, "status": status, "created_at": createdAt,
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"daemons": result})
}

func (a *App) handleAttachDaemons(c *gin.Context) {
	chID := c.Param("chID")
	var req struct {
		DaemonIDs []string `json:"daemon_ids"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || len(req.DaemonIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "daemon_ids required"})
		return
	}

	for _, did := range req.DaemonIDs {
		_, _ = a.db.ExecContext(c.Request.Context(),
			`INSERT OR IGNORE INTO daemon_channels (daemon_id, channel_id) VALUES (?,?)`,
			did, chID,
		)
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) handleDetachDaemon(c *gin.Context) {
	chID := c.Param("chID")
	daemonID := c.Param("id")

	_, err := a.db.ExecContext(c.Request.Context(),
		`DELETE FROM daemon_channels WHERE daemon_id = ? AND channel_id = ?`,
		daemonID, chID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "detach failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ---------------------------------------------------------------------------
// WebSocket: client message tail (/ws)
// ---------------------------------------------------------------------------

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func (a *App) handleWS(c *gin.Context) {
	// Auth via cookie.
	token, err := c.Cookie(sessionCookieName)
	if err != nil || token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}
	var userID string
	var expiresAt int64
	err = a.db.QueryRowContext(c.Request.Context(),
		`SELECT user_id, expires_at FROM sessions WHERE token = ?`, token,
	).Scan(&userID, &expiresAt)
	if err != nil || time.Now().UnixMilli() > expiresAt {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid session"})
		return
	}

	ws, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	// Read the subscribe message from client: {channel_id, after_seq}.
	var sub struct {
		ChannelID string `json:"channel_id"`
		AfterSeq  int64  `json:"after_seq"`
	}
	if err := ws.ReadJSON(&sub); err != nil {
		return
	}

	chID := channel.ID(sub.ChannelID)
	home := a.getHome(chID)
	if home == nil {
		_ = ws.WriteJSON(gin.H{"error": "channel not loaded"})
		return
	}

	// Subscribe to PushHub.
	notify, cancel := home.PushHub().Subscribe()
	defer cancel()

	cursor := sub.AfterSeq
	gw := home.Gateway()

	// Initial backfill.
	a.wsSendMessages(ws, gw, &cursor)

	// Tail loop.
	ctx := c.Request.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-notify:
			if !ok {
				return
			}
			a.wsSendMessages(ws, gw, &cursor)
		}
	}
}

func (a *App) wsSendMessages(ws *websocket.Conn, gw *platform.Gateway, cursor *int64) {
	rows, err := gw.ListMessages(context.Background(), *cursor, 100)
	if err != nil || len(rows) == 0 {
		return
	}
	for _, r := range rows {
		type wsMsg struct {
			Seq        int64            `json:"seq"`
			IsTerminal bool             `json:"is_terminal"`
			Envelope   message.Envelope `json:"envelope"`
		}
		msg := wsMsg{Seq: r.Seq, IsTerminal: r.IsTerminal, Envelope: r.Envelope}
		if err := ws.WriteJSON(msg); err != nil {
			return
		}
		if r.Seq > *cursor {
			*cursor = r.Seq
		}
	}
}

// ---------------------------------------------------------------------------
// WebSocket: compute attach (/compute)
// ---------------------------------------------------------------------------

func (a *App) handleCompute(c *gin.Context) {
	// The fleet's ServeWS handles the WS upgrade internally. We need to find
	// the right channel home. The channel is identified after the attach
	// handshake, but we need to route to a fleet before that.
	//
	// v1 approach: api-key in query param identifies the daemon, which tells
	// us the allowed channels. For v1, we accept one channel per compute
	// connection via a query param.
	apiKey := c.Query("key")
	chIDStr := c.Query("channel")

	if apiKey == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "api key required"})
		return
	}
	if chIDStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel query param required"})
		return
	}

	if _, err := a.authFunc(apiKey); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
		return
	}

	chID := channel.ID(chIDStr)
	home := a.getHome(chID)
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}

	// Delegate to the channel home's fleet.
	home.Fleet().ServeWS(c.Writer, c.Request)
}

// ---------------------------------------------------------------------------
// Auth helper for fleet
// ---------------------------------------------------------------------------

func (a *App) authFunc(apiKey string) (string, error) {
	keyHash := hashAPIKey(apiKey)
	var daemonID string
	err := a.db.QueryRow(
		`SELECT id FROM daemons WHERE api_key_hash = ?`, keyHash,
	).Scan(&daemonID)
	if err != nil {
		return "", fmt.Errorf("invalid api key")
	}
	return daemonID, nil
}

// ---------------------------------------------------------------------------
// Channel home management
// ---------------------------------------------------------------------------

func (a *App) getHome(chID channel.ID) *platform.ChannelHome {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.homes[chID]
}

func (a *App) createHome(chID channel.ID, dbPath string) (*platform.ChannelHome, error) {
	home, err := platform.NewChannelHome(platform.HomeConfig{
		ChannelID: chID,
		DBPath:    dbPath,
		AuthFunc:  a.authFunc,
		Logger:    a.logger,
	})
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	a.homes[chID] = home
	a.mu.Unlock()

	return home, nil
}

func (a *App) loadChannels() error {
	rows, err := a.db.Query(`SELECT id, db_path FROM channels`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id, dbPath string
		if err := rows.Scan(&id, &dbPath); err != nil {
			continue
		}
		if _, err := a.createHome(channel.ID(id), dbPath); err != nil {
			a.logger.Warn("app: load channel failed", "channel", id, "err", err.Error())
			// Continue loading other channels.
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func hashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

func newRecord(id actor.ActorID, kind actor.Kind) storespec.Record {
	return storespec.Record{
		ID:        id,
		Kind:      kind,
		CreatedAt: time.Now().UnixMilli(),
	}
}
