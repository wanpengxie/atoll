package app

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
)

func (a *App) handleGetOperation(c *gin.Context) {
	ref := c.Param("ref")
	caller := middleware.UserID(c)
	var admission admissionRecord
	err := a.db.QueryRowContext(c.Request.Context(), `SELECT operation_id,channel_id,op,requested_by,request_json,request_digest,status,result_json,error_code,created_at,done_at FROM channel_admission_operations WHERE operation_id=?`, ref).
		Scan(&admission.OperationID, &admission.ChannelID, &admission.Op, &admission.RequestedBy, &admission.RequestJSON, &admission.RequestDigest, &admission.Status, &admission.ResultJSON, &admission.ErrorCode, &admission.CreatedAt, &admission.DoneAt)
	if err == nil {
		if admission.RequestedBy != caller {
			c.JSON(http.StatusForbidden, gin.H{"error": "operation owner required"})
			return
		}
		view := gin.H{"ref": ref, "family": "admission", "status": admission.Status, "op": admission.Op, "created_at": admission.CreatedAt}
		if admission.ResultJSON.Valid {
			view["result_json"] = json.RawMessage(admission.ResultJSON.String)
		}
		if admission.ErrorCode.Valid {
			view["error_code"] = admission.ErrorCode.String
		}
		if admission.DoneAt.Valid {
			view["done_at"] = admission.DoneAt.Int64
		}
		c.JSON(http.StatusOK, view)
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	var requested, status string
	var result, code sql.NullString
	var created int64
	var done, dead, published sql.NullInt64
	err = a.db.QueryRowContext(c.Request.Context(), `SELECT requested_by,receipt_json,error_code,created_at,done_at,dead_at,published_at FROM channel_provision_jobs WHERE operation_id=?`, ref).
		Scan(&requested, &result, &code, &created, &done, &dead, &published)
	if err == nil {
		if requested != caller {
			c.JSON(http.StatusForbidden, gin.H{"error": "operation owner required"})
			return
		}
		status = "provisioning"
		if published.Valid {
			status = "opening"
		}
		if done.Valid {
			status = "done"
		}
		if dead.Valid {
			status = "dead"
		}
		view := gin.H{"ref": ref, "family": "lifecycle", "status": status, "created_at": created}
		if result.Valid {
			view["result_json"] = json.RawMessage(result.String)
		}
		if code.Valid {
			view["error_code"] = code.String
		}
		if done.Valid {
			view["done_at"] = done.Int64
		}
		if dead.Valid {
			view["done_at"] = dead.Int64
		}
		c.JSON(200, view)
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		c.JSON(500, gin.H{"error": "query failed"})
		return
	}
	result = sql.NullString{}
	code = sql.NullString{}
	done = sql.NullInt64{}
	dead = sql.NullInt64{}
	err = a.db.QueryRowContext(c.Request.Context(), `SELECT requested_by,error_code,created_at,done_at,dead_at FROM channel_destroy_jobs WHERE operation_id=?`, ref).Scan(&requested, &code, &created, &done, &dead)
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(404, gin.H{"error": "operation not found"})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{"error": "query failed"})
		return
	}
	if requested != caller {
		c.JSON(403, gin.H{"error": "operation owner required"})
		return
	}
	status = "destroying"
	if done.Valid {
		status = "done"
	}
	if dead.Valid {
		status = "dead"
	}
	view := gin.H{"ref": ref, "family": "lifecycle", "status": status, "created_at": created}
	if code.Valid {
		view["error_code"] = code.String
	}
	if done.Valid {
		view["done_at"] = done.Int64
	}
	if dead.Valid {
		view["done_at"] = dead.Int64
	}
	c.JSON(200, view)
}
