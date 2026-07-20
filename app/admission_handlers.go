package app

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func (a *App) handleJoinChannel(c *gin.Context) {
	chID := channel.ID(c.Param("chID"))
	if !a.channelExists(c.Request.Context(), string(chID)) {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}
	caller := middleware.UserID(c)
	intent := struct {
		Principal string `json:"principal"`
	}{caller}
	record, _, err := a.admission.submit(c.Request.Context(), admissionCommand{
		ChannelID: chID, Op: "join", RequestedBy: caller, IdempotencyKey: c.GetHeader("Idempotency-Key"), Intent: intent,
		BuildRequest: func(ref string) any { return channel.AdmitRequest{Ref: ref, Principal: caller} },
	})
	if err != nil || record.Status != "done" {
		respondAdmissionRecord(c, record, err, http.StatusCreated)
		return
	}
	var result channel.AdmitResult
	if err := json.Unmarshal([]byte(record.ResultJSON.String), &result); err != nil {
		c.JSON(500, gin.H{"error": "invalid terminal result"})
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	c.JSON(status, gin.H{"operation_id": record.OperationID, "status": "done", "actor_id": result.ActorID, "created": result.Created})
}

func (a *App) handleIntroduceActor(c *gin.Context) {
	chID := channel.ID(c.Param("chID"))
	if !a.channelExists(c.Request.Context(), string(chID)) {
		c.JSON(404, gin.H{"error": "channel not found"})
		return
	}
	var input struct {
		DeclID string `json:"decl_id"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || strings.TrimSpace(input.DeclID) == "" {
		c.JSON(400, gin.H{"error": "decl_id required"})
		return
	}
	input.DeclID = strings.TrimSpace(input.DeclID)
	caller := middleware.UserID(c)
	intent := struct {
		DeclID string `json:"decl_id"`
	}{input.DeclID}
	record, _, err := a.admission.submit(c.Request.Context(), admissionCommand{
		ChannelID: chID, Op: "introduce", RequestedBy: caller, IdempotencyKey: c.GetHeader("Idempotency-Key"), Intent: intent,
		BuildRequest: func(ref string) any {
			return channel.IntroduceRequest{Ref: ref, DeclID: input.DeclID, InitiatorPrincipal: caller}
		},
	})
	if err != nil || record.Status != "done" {
		respondAdmissionRecord(c, record, err, http.StatusCreated)
		return
	}
	var result channel.IntroduceResult
	if err := json.Unmarshal([]byte(record.ResultJSON.String), &result); err != nil {
		c.JSON(500, gin.H{"error": "invalid terminal result"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"operation_id": record.OperationID, "status": "done", "actor_id": result.ActorID, "created": result.Created})
}

func (a *App) handleEditActorConfig(c *gin.Context) {
	chID, ok := a.requireChannelMember(c)
	if !ok {
		return
	}
	var input struct {
		Config json.RawMessage `json:"config"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || !isJSONObject(input.Config) {
		c.JSON(400, gin.H{"error": "config must be a JSON object"})
		return
	}
	record, err := a.admission.submitEdit(c.Request.Context(), channel.ID(chID), c.Param("actorID"), middleware.UserID(c), input.Config, c.GetHeader("Idempotency-Key"))
	respondAdmissionRecord(c, record, err, http.StatusOK)
}
