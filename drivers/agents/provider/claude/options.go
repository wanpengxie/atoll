package claude

import (
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type claudeInitialize struct {
	Models []struct {
		Value                 string   `json:"value"`
		ResolvedModel         string   `json:"resolvedModel"`
		DisplayName           string   `json:"displayName"`
		Description           string   `json:"description"`
		SupportsEffort        bool     `json:"supportsEffort"`
		SupportedEffortLevels []string `json:"supportedEffortLevels"`
	} `json:"models"`
}

func (w *worker) optionsFromInitialize(raw json.RawMessage, current driverproto.TurnOptions) driverproto.OptionsSnapshot {
	version := ""
	if w.cfg.versionProbe != nil {
		version = w.cfg.versionProbe(w.cfg.Binary)
	}
	fallback := driverproto.FallbackOptions(Class, w.cfg.Selections, w.cfg.SelectionTitles, w.cfg.Default, current)
	fallback.Client = driverproto.ClientInfo{Name: "claude", Current: version, UpdateStatus: driverproto.UpdateUnknown}
	latest := ""
	if w.cfg.latestProbe != nil {
		latest = w.cfg.latestProbe()
	}
	applyClaudeUpdateSignal(&fallback.Client, latest)
	var initialized claudeInitialize
	if json.Unmarshal(raw, &initialized) != nil || len(initialized.Models) == 0 {
		return fallback
	}
	snapshot := driverproto.OptionsSnapshot{
		Provider: Class, Source: driverproto.OptionsSourceNative, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Client: driverproto.ClientInfo{Name: "claude", Current: version, UpdateStatus: driverproto.UpdateUnknown},
	}
	applyClaudeUpdateSignal(&snapshot.Client, latest)
	for _, item := range initialized.Models {
		value := strings.TrimSpace(item.Value)
		if value == "" {
			continue
		}
		model := driverproto.ModelOption{Value: value, Label: item.DisplayName, Description: item.Description}
		if item.SupportsEffort {
			for _, nativeEffort := range item.SupportedEffortLevels {
				if effort := strings.TrimSpace(nativeEffort); effort != "" {
					model.Efforts = append(model.Efforts, driverproto.EffortOption{Value: effort})
				}
			}
		}
		snapshot.Models = append(snapshot.Models, model)
	}
	if len(snapshot.Models) == 0 {
		return fallback
	}
	snapshot.Default = driverproto.TurnOptions{Model: snapshot.Models[0].Value}
	snapshot.Current = matchClaudeCurrent(snapshot, current)
	return snapshot
}

func probeLatestClaude() string {
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("https://registry.npmjs.org/@anthropic-ai%2fclaude-code/latest")
	if err != nil {
		return ""
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return ""
	}
	var value struct {
		Version string `json:"version"`
	}
	if json.Unmarshal(body, &value) != nil {
		return ""
	}
	return strings.TrimSpace(value.Version)
}

func applyClaudeUpdateSignal(client *driverproto.ClientInfo, latest string) {
	client.Latest = strings.TrimSpace(latest)
	client.UpdateStatus = driverproto.UpdateUnknown
	if client.Current == "" || client.Latest == "" {
		return
	}
	parse := func(value string) ([]int, bool) {
		match := regexp.MustCompile(`\d+(?:\.\d+){1,3}`).FindString(value)
		if match == "" {
			return nil, false
		}
		parts := strings.Split(match, ".")
		out := make([]int, len(parts))
		for i, part := range parts {
			n, err := strconv.Atoi(part)
			if err != nil {
				return nil, false
			}
			out[i] = n
		}
		return out, true
	}
	a, okA := parse(client.Current)
	b, okB := parse(client.Latest)
	if !okA || !okB {
		return
	}
	for len(a) < len(b) {
		a = append(a, 0)
	}
	for len(b) < len(a) {
		b = append(b, 0)
	}
	for i := range a {
		if a[i] < b[i] {
			client.UpdateStatus = driverproto.UpdateAvailable
			return
		}
		if a[i] > b[i] {
			client.UpdateStatus = driverproto.UpdateCurrent
			return
		}
	}
	client.UpdateStatus = driverproto.UpdateCurrent
}

func matchClaudeCurrent(snapshot driverproto.OptionsSnapshot, current driverproto.TurnOptions) driverproto.TurnOptions {
	if snapshot.Accepts(current) {
		return current
	}
	want := strings.ToLower(strings.TrimSpace(current.Model))
	if want != "" {
		for _, model := range snapshot.Models {
			candidate := strings.ToLower(model.Value)
			if candidate == want || strings.Contains(candidate, want) {
				mapped := driverproto.TurnOptions{Model: model.Value, Effort: current.Effort}
				if len(model.Efforts) == 0 {
					mapped.Effort = ""
				}
				if snapshot.Accepts(mapped) {
					return mapped
				}
			}
		}
	}
	return snapshot.Default
}

// claudeSelectionForCatalog retains model-only selection for models that
// accept effort, while clearing an old effort when the selected native model
// explicitly has no effort control.
func claudeSelectionForCatalog(current, requested driverproto.TurnOptions, catalog driverproto.OptionsSnapshot) driverproto.TurnOptions {
	selected := requested
	if selected.Model == "" {
		selected.Model = current.Model
	}
	for _, model := range catalog.Models {
		if model.Value != selected.Model {
			continue
		}
		if len(model.Efforts) == 0 {
			selected.Effort = ""
		} else if selected.Effort == "" {
			selected.Effort = current.Effort
		}
		return selected
	}
	if selected.Effort == "" {
		selected.Effort = current.Effort
	}
	return selected
}
