package agentlooper

import (
	"bytes"
	"encoding/json"
	"errors"

	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
)

func (a *assignment) acceptInput(req agentloop.InputRequest) (agentloop.ControlResult, error) {
	inputs := req.Inputs
	if len(inputs) == 0 {
		inputs = []agentloop.Input{req.Input}
	}
	rawInputs, _ := json.Marshal(inputs)
	inputs = nil
	_ = json.Unmarshal(rawInputs, &inputs)
	id := req.ControlID
	if id == "" {
		id = req.Input.ID
	}
	if id == "" {
		return agentloop.ControlResult{}, errors.New("control_id required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, old := range a.controls {
		if old.ControlID == id {
			left, _ := json.Marshal(old.Inputs)
			right, _ := json.Marshal(inputs)
			if !bytes.Equal(left, right) {
				return agentloop.ControlResult{}, errors.New("control_id reused with different input")
			}
			return old, nil
		}
	}
	decision := agentloop.ControlResult{ControlID: id, Disposition: "accepted", Inputs: inputs}
	if a.closed {
		decision.Disposition = "target_gone"
		return decision, nil
	}
	if len(a.controls) >= 256 || len(a.inputs)+len(inputs) > 1024 {
		return decision, errors.New("execution control capacity exceeded")
	}
	seen := map[string]bool{}
	var seq int64
	for _, input := range a.inputs {
		seen[input.ID] = true
		if input.Seq > seq {
			seq = input.Seq
		}
	}
	for _, input := range inputs {
		if input.ID == "" || seen[input.ID] || input.Seq <= seq {
			return decision, errors.New("input identity or sequence conflict")
		}
		seen[input.ID] = true
		seq = input.Seq
	}
	combined := append(append([]agentloop.Input(nil), a.inputs...), inputs...)
	raw, _ := json.Marshal(combined)
	if len(raw) > agentloop.MaxControlBytes {
		return decision, errors.New("execution input byte limit exceeded")
	}
	a.inputs = combined
	a.controls = append(a.controls, decision)
	return decision, nil
}

// pendingInputs takes a stable prefix; arrivals during context construction are
// left for the next model call. Consumption is recorded only after that build.
func (a *assignment) pendingInputs() []agentloop.Input {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]agentloop.Input(nil), a.inputs[a.consumed:]...)
}

// seal competes with acceptInput under the same lock. A normally completed
// assistant may not close an execution whose accepted input is still pending.
func (a *assignment) seal() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.inputs) > a.consumed && !a.closed {
		return false
	}
	a.closed = true
	return true
}
