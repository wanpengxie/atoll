package identity

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

// RegisterPublicRoutes mounts the unauthenticated endpoints on the
// supplied router group (typically /api).
func (s *Service) RegisterPublicRoutes(g *gin.RouterGroup) {
	g.POST("/identity/verification/issue", s.handleIssueCode)
	g.POST("/identity/register", s.handleRegister)
	g.POST("/identity/login", s.handleLogin)
}

// RegisterAuthRoutes mounts the authenticated endpoints (must be
// wrapped by AuthMiddleware upstream).
func (s *Service) RegisterAuthRoutes(g *gin.RouterGroup) {
	g.GET("/identity/me", s.handleMe)
	g.POST("/identity/logout", s.handleLogout)
}

type issueCodeReq struct {
	Email   string              `json:"email"   binding:"required"`
	Purpose VerificationPurpose `json:"purpose"`
}

func (s *Service) handleIssueCode(c *gin.Context) {
	var req issueCodeReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	purpose := req.Purpose
	if purpose == "" {
		purpose = PurposeRegister
	}
	if _, err := s.IssueCode(c.Request.Context(), req.Email, purpose); err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "issued"})
}

type registerReq struct {
	Email       string `json:"email"        binding:"required"`
	Password    string `json:"password"     binding:"required"`
	Code        string `json:"code"`
	DisplayName string `json:"display_name"`
}

func (s *Service) handleRegister(c *gin.Context) {
	var req registerReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	user, err := s.Register(c.Request.Context(), RegisterInput(req))
	if err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"id":           user.ID,
		"email":        user.Email,
		"display_name": user.DisplayName,
		"created_at":   user.CreatedAt,
	})
}

type loginReq struct {
	Email    string `json:"email"    binding:"required"`
	Password string `json:"password" binding:"required"`
}

func (s *Service) handleLogin(c *gin.Context) {
	var req loginReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	res, err := s.Login(c.Request.Context(), LoginInput(req))
	if err != nil {
		c.JSON(httpStatusFor(err), gin.H{"error": err.Error()})
		return
	}
	SetCookie(c, res.Token, res.Expires)
	c.JSON(http.StatusOK, gin.H{
		"user": gin.H{
			"id":           res.User.ID,
			"email":        res.User.Email,
			"display_name": res.User.DisplayName,
		},
		"expires_at": res.Expires,
	})
}

func (s *Service) handleMe(c *gin.Context) {
	u := UserFrom(c)
	c.JSON(http.StatusOK, gin.H{
		"id":           u.ID,
		"email":        u.Email,
		"display_name": u.DisplayName,
		"created_at":   u.CreatedAt,
	})
}

func (s *Service) handleLogout(c *gin.Context) {
	raw := ExtractTokenFromRequest(c.Request)
	if err := s.Logout(c.Request.Context(), raw); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ClearCookie(c)
	c.JSON(http.StatusOK, gin.H{"status": "logged_out"})
}

// httpStatusFor maps service errors to HTTP status codes. Anything
// not enumerated falls back to 500.
func httpStatusFor(err error) int {
	switch {
	case errors.Is(err, ErrEmailRequired),
		errors.Is(err, ErrPasswordRequired),
		errors.Is(err, ErrPasswordTooShort),
		errors.Is(err, ErrCodeRequired):
		return http.StatusBadRequest
	case errors.Is(err, ErrEmailAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, ErrInvalidCredentials),
		errors.Is(err, ErrSessionInvalid),
		errors.Is(err, ErrCodeInvalid):
		return http.StatusUnauthorized
	case errors.Is(err, ErrUserNotFound):
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}
