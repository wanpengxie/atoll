package native

import (
	"encoding/json"
	"path"
	"strings"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type catalogModel struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Provider       string   `json:"provider"`
	ThinkingLevels []string `json:"thinking_levels"`
}

func (c *controller) modelCatalog(sys actorbase.Sys) ([]catalogModel, error) {
	pending, err := sys.Call(message.Root(), harness.Context{}, actorID(c.cfg.LLMActor), llmproto.TypeModels, llmproto.ModelsRequest{})
	if err != nil {
		return nil, err
	}
	terminal, err := pending.Wait(sys.Life(), 0)
	if err != nil {
		_ = pending.Cancel()
		return nil, err
	}
	var status struct {
		Status string `json:"status"`
		message.Failure
		Models []json.RawMessage `json:"models"`
	}
	if json.Unmarshal(terminal.Payload, &status) != nil || status.Status != "completed" {
		return nil, &modelCatalogError{code: status.ErrorCode, detail: status.Detail}
	}
	var out []catalogModel
	for _, raw := range status.Models {
		var model catalogModel
		if json.Unmarshal(raw, &model) == nil && model.ID != "" {
			if c.modelEnabled(model) {
				out = append(out, model)
			}
		}
	}
	return out, nil
}

type modelCatalogError struct{ code, detail string }

func (e *modelCatalogError) Error() string {
	if e.code == "" {
		return "model catalog unavailable"
	}
	return e.code + ": " + e.detail
}

func (c *controller) modelEnabled(model catalogModel) bool {
	if len(c.cfg.Models) == 0 {
		return true
	}
	full := model.Provider + "/" + model.ID
	for _, pattern := range c.cfg.Models {
		modelPattern := pattern
		if colon := strings.LastIndex(pattern, ":"); colon > strings.LastIndex(pattern, "/") {
			modelPattern = pattern[:colon]
			effort := pattern[colon+1:]
			found := false
			for _, level := range model.ThinkingLevels {
				found = found || level == effort
			}
			if !found {
				continue
			}
		}
		for _, candidate := range []string{full, model.ID} {
			if ok, _ := path.Match(modelPattern, candidate); ok {
				return true
			}
		}
	}
	return false
}

func modelValue(m catalogModel) string {
	if m.Provider == "" {
		return m.ID
	}
	return m.Provider + "/" + m.ID
}

func acceptsSelection(models []catalogModel, selection runtimeproto.TurnOptions) bool {
	for _, model := range models {
		if selection.Model != model.ID && selection.Model != modelValue(model) {
			continue
		}
		if selection.Effort == "" {
			return true
		}
		for _, effort := range model.ThinkingLevels {
			if effort == selection.Effort {
				return true
			}
		}
	}
	return false
}

func (c *controller) loadSelection(sys actorbase.Sys, models []catalogModel) runtimeproto.TurnOptions {
	if c.selection != nil && acceptsSelection(models, *c.selection) {
		return *c.selection
	}
	if out, err := sys.State().Get(resource.ResourceID(agentbase.SelectionKey)); err == nil && out.Accepted() && out.Found {
		var saved runtimeproto.TurnOptions
		if json.Unmarshal(out.Value, &saved) == nil && acceptsSelection(models, saved) {
			c.selection = &saved
			return saved
		}
	}
	want := runtimeproto.TurnOptions{Model: c.cfg.DefaultModel, Effort: c.cfg.DefaultThinkingLevel}
	if acceptsSelection(models, want) {
		c.selection = &want
		return want
	}
	if len(models) > 0 {
		want.Model = modelValue(models[0])
		want.Effort = c.cfg.DefaultThinkingLevel
		if !acceptsSelection(models, want) {
			want.Effort = ""
		}
		c.selection = &want
		return want
	}
	return runtimeproto.TurnOptions{Model: c.cfg.DefaultModel, Effort: c.cfg.DefaultThinkingLevel}
}

func (c *controller) selected(sys actorbase.Sys) runtimeproto.TurnOptions {
	models, err := c.modelCatalog(sys)
	if err != nil {
		return runtimeproto.TurnOptions{Model: c.cfg.DefaultModel, Effort: c.cfg.DefaultThinkingLevel}
	}
	return c.loadSelection(sys, models)
}

func (c *controller) handleOptions(sys actorbase.Sys, msg actorbase.Msg) {
	var empty struct{}
	if actorbase.DecodeStrict(msg.Payload, &empty) != nil {
		_, _ = sys.Fail(msg, "invalid_args", "agent.options payload must be empty")
		return
	}
	models, err := c.modelCatalog(sys)
	if err != nil {
		_, _ = sys.Fail(msg, "provider_failed", err.Error())
		return
	}
	current := c.loadSelection(sys, models)
	options := make([]driverproto.ModelOption, 0, len(models))
	for _, model := range models {
		entry := driverproto.ModelOption{Value: modelValue(model), Label: model.Name}
		for _, level := range model.ThinkingLevels {
			entry.Efforts = append(entry.Efforts, driverproto.EffortOption{Value: level})
		}
		options = append(options, entry)
	}
	_, _ = sys.Reply(msg, driverproto.OptionsSnapshot{Provider: "pi", Source: driverproto.OptionsSourceNative, Models: options, Default: driverproto.TurnOptions{Model: c.cfg.DefaultModel, Effort: c.cfg.DefaultThinkingLevel}, Current: driverproto.TurnOptions{Model: current.Model, Effort: current.Effort}, Client: driverproto.ClientInfo{Name: "pi", UpdateStatus: driverproto.UpdateUnknown}})
}

func (c *controller) handleSelect(sys actorbase.Sys, msg actorbase.Msg) {
	var req runtimeproto.TurnOptions
	if actorbase.DecodeStrict(msg.Payload, &req) != nil || req.Model == "" {
		_, _ = sys.Fail(msg, "invalid_args", "model is required")
		return
	}
	models, err := c.modelCatalog(sys)
	if err != nil {
		_, _ = sys.Fail(msg, "provider_failed", err.Error())
		return
	}
	if !acceptsSelection(models, req) {
		_, _ = sys.Fail(msg, "invalid_args", "model/effort is not offered by agent.options")
		return
	}
	raw, _ := json.Marshal(req)
	out, err := sys.State().Put(resource.ResourceID(agentbase.SelectionKey), raw)
	if err != nil || !out.Accepted() {
		_, _ = sys.Fail(msg, "ledger_unavailable", "selection could not be persisted")
		return
	}
	c.selection = &req
	_, _ = sys.Reply(msg, map[string]any{"disposition": "selected", "model": req.Model, "effort": req.Effort})
}
