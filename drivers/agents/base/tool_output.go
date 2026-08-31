package base

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/boundedjson"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

const (
	toolOutputInlineBytes     = 10 << 20
	toolOutputProjectionBytes = 10 << 10
	toolOutputDirectory       = ".atoll/outputs"
)

type externalJSONRecord struct {
	Stored        bool   `json:"stored"`
	ResourceID    string `json:"resource_id,omitempty"`
	Path          string `json:"path,omitempty"`
	MediaType     string `json:"media_type"`
	OriginalBytes int    `json:"original_bytes"`
	SHA256        string `json:"sha256"`
	Reason        string `json:"reason,omitempty"`
}

func (l *agentLoop) prepareToolOutput(raw json.RawMessage) any {
	if len(raw) <= toolOutputInlineBytes {
		return raw
	}
	value, err := prepareOversizedToolOutput(l.sys.Resource(), l.def.cfg.OutputDeviceName, l.def.cfg.OutputWorkspace, raw, toolOutputProjectionBytes)
	if err != nil {
		l.logger.Warn("agent oversized tool output degraded", "original_bytes", len(raw), "error", err)
	}
	return value
}

func prepareOversizedToolOutput(resources actorbase.ResourceHandle, deviceName, workspace string, raw json.RawMessage, projectionBudget int) (any, error) {
	projection, meta, projectionErr := boundedjson.Project(raw, projectionBudget)
	if projectionErr != nil {
		projection = json.RawMessage(`{"$atoll_cut":{"type":"json","reason":"projection_failed"}}`)
	}
	record := externalJSONRecord{
		MediaType: "application/json", OriginalBytes: len(raw), SHA256: meta.SHA256,
	}
	address, relative, err := writeToolOutputFile(resources, deviceName, workspace, raw)
	if err != nil {
		record.Reason = "channel_file_write_failed"
	} else {
		record.Stored, record.ResourceID, record.Path = true, address, relative
	}
	return map[string]any{"external_json": record, "projection": projection}, errors.Join(projectionErr, err)
}

func writeToolOutputFile(resources actorbase.ResourceHandle, deviceName, workspace string, raw []byte) (string, string, error) {
	if resources == nil || deviceName == "" || workspace == "" {
		return "", "", errors.New("tool output channel storage unavailable")
	}
	channelName := filepath.Base(filepath.Clean(workspace))
	if channelName == "." || channelName == string(filepath.Separator) || channelName == "" {
		return "", "", errors.New("tool output channel name unavailable")
	}
	for _, directory := range []string{".atoll", toolOutputDirectory} {
		address, err := toolOutputAddress(deviceName, channelName, directory)
		if err != nil {
			return "", "", err
		}
		out, err := resources.CreateDirectory(address)
		if err != nil {
			return "", "", err
		}
		if !out.Accepted() && out.RejectReason != access.AlreadyExists {
			return "", "", fmt.Errorf("create output directory rejected: %s", out.RejectReason)
		}
	}

	relative := toolOutputDirectory + "/" + uuid.NewString() + ".json"
	address, err := toolOutputAddress(deviceName, channelName, relative)
	if err != nil {
		return "", "", err
	}
	file, out, err := resources.CreateFile(address, true)
	if err != nil {
		return "", "", err
	}
	if !out.Accepted() {
		return "", "", fmt.Errorf("create output file rejected: %s", out.RejectReason)
	}
	writer, ok := file.Writer()
	if !ok {
		return "", "", errors.New("tool output file writer unavailable")
	}
	if _, err := writer.Write(raw); err != nil {
		_ = writer.Abort()
		return "", "", err
	}
	if err := writer.Commit(); err != nil {
		_ = writer.Abort()
		return "", "", err
	}
	return string(address), relative, nil
}

func toolOutputAddress(deviceName, channelName, relative string) (resource.ResourceID, error) {
	return accessdoor.FormatFileAddress(deviceName, channelName, relative)
}
