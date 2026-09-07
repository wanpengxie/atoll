package codex

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

type modelListResponse struct {
	Data       []codexModel `json:"data"`
	NextCursor string       `json:"nextCursor"`
}

type codexModel struct {
	ID                        string `json:"id"`
	Model                     string `json:"model"`
	DisplayName               string `json:"displayName"`
	Description               string `json:"description"`
	Hidden                    bool   `json:"hidden"`
	IsDefault                 bool   `json:"isDefault"`
	DefaultReasoningEffort    string `json:"defaultReasoningEffort"`
	SupportedReasoningEfforts []struct {
		ReasoningEffort string `json:"reasoningEffort"`
		Description     string `json:"description"`
	} `json:"supportedReasoningEfforts"`
}

var codexVersionRE = regexp.MustCompile(`(?i)(?:^|\s)[^/\s]+/([0-9]+(?:\.[0-9]+){1,3}(?:[-+][^\s()]+)?)`)

func codexClientVersion(userAgent string) string {
	match := codexVersionRE.FindStringSubmatch(strings.TrimSpace(userAgent))
	if len(match) == 2 {
		return match[1]
	}
	return ""
}

func (w *worker) fallbackOptions(current driverproto.TurnOptions, version string) driverproto.OptionsSnapshot {
	snapshot := driverproto.FallbackOptions(Class, w.cfg.Selections, w.cfg.SelectionTitles, w.cfg.Default, current)
	snapshot.Client.Name, snapshot.Client.Current = "codex", version
	return snapshot
}

func probeLatestCodex() string {
	return npmLatestVersion("https://registry.npmjs.org/@openai%2fcodex/latest")
}

func npmLatestVersion(endpoint string) string {
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(endpoint)
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

func applyUpdateSignal(client *driverproto.ClientInfo, latest string) {
	client.Latest = strings.TrimSpace(latest)
	client.UpdateStatus = driverproto.UpdateUnknown
	if client.Current == "" || client.Latest == "" {
		return
	}
	comparison, ok := compareVersions(client.Current, client.Latest)
	if !ok {
		return
	}
	if comparison < 0 {
		client.UpdateStatus = driverproto.UpdateAvailable
	} else {
		client.UpdateStatus = driverproto.UpdateCurrent
	}
}

func compareVersions(left, right string) (int, bool) {
	parts := func(value string) ([]int, bool) {
		match := regexp.MustCompile(`\d+(?:\.\d+){1,3}`).FindString(value)
		if match == "" {
			return nil, false
		}
		pieces := strings.Split(match, ".")
		out := make([]int, len(pieces))
		for i, piece := range pieces {
			n, err := strconv.Atoi(piece)
			if err != nil {
				return nil, false
			}
			out[i] = n
		}
		return out, true
	}
	a, okA := parts(left)
	b, okB := parts(right)
	if !okA || !okB {
		return 0, false
	}
	for len(a) < len(b) {
		a = append(a, 0)
	}
	for len(b) < len(a) {
		b = append(b, 0)
	}
	for i := range a {
		if a[i] < b[i] {
			return -1, true
		}
		if a[i] > b[i] {
			return 1, true
		}
	}
	return 0, true
}

func nativeOptions(models []codexModel, current driverproto.TurnOptions, version string) (driverproto.OptionsSnapshot, bool) {
	snapshot := driverproto.OptionsSnapshot{
		Provider: Class, Source: driverproto.OptionsSourceNative, GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Client: driverproto.ClientInfo{Name: "codex", Current: version, UpdateStatus: driverproto.UpdateUnknown},
	}
	for _, item := range models {
		if item.Hidden {
			continue
		}
		value := strings.TrimSpace(item.Model)
		if value == "" {
			value = strings.TrimSpace(item.ID)
		}
		if value == "" {
			continue
		}
		model := driverproto.ModelOption{Value: value, Label: item.DisplayName, Description: item.Description}
		for _, nativeEffort := range item.SupportedReasoningEfforts {
			effort := strings.TrimSpace(nativeEffort.ReasoningEffort)
			if effort != "" {
				model.Efforts = append(model.Efforts, driverproto.EffortOption{Value: effort, Description: nativeEffort.Description})
			}
		}
		snapshot.Models = append(snapshot.Models, model)
		if item.IsDefault {
			snapshot.Default = driverproto.TurnOptions{Model: value, Effort: item.DefaultReasoningEffort}
		}
	}
	if len(snapshot.Models) == 0 {
		return driverproto.OptionsSnapshot{}, false
	}
	if snapshot.Default.Model == "" {
		snapshot.Default.Model = snapshot.Models[0].Value
		if len(snapshot.Models[0].Efforts) > 0 {
			snapshot.Default.Effort = snapshot.Models[0].Efforts[0].Value
		}
	}
	current = codexSelectionForCatalog(current, current, snapshot)
	if !snapshot.Accepts(current) {
		current = snapshot.Default
	}
	snapshot.Current = current
	return snapshot, true
}

// codexSelectionForCatalog retains the established model-only selection
// behavior for effort-capable models, but never carries an old effort into a
// model whose native catalog says effort is not a parameter.
func codexSelectionForCatalog(current, requested driverproto.TurnOptions, catalog driverproto.OptionsSnapshot) driverproto.TurnOptions {
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

func decodeModelList(raw json.RawMessage) (modelListResponse, bool) {
	var response modelListResponse
	if json.Unmarshal(raw, &response) != nil || len(response.Data) == 0 {
		return modelListResponse{}, false
	}
	return response, true
}
