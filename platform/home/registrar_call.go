package home

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/platform/lagoon"
)

type registrarCaller struct{ home *Home }

func RegistrarCaller(h *Home) lagoon.C0Caller { return registrarCaller{home: h} }

func (c registrarCaller) CallRegistrar(ctx context.Context, word lagoon.Word, payload any) (json.RawMessage, error) {
	if c.home == nil || c.home.callPort == nil || c.home.closed.Load() {
		return nil, ErrClosed
	}
	ids, err := c.home.controller.DeclaredInstances(lagoon.RegistrarSeatDeclID)
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, errors.New("lagoon: registrar seat unavailable")
	}
	msg, err := c.home.callPort.Call(ctx, ids[0], string(word), payload)
	if err != nil {
		return nil, err
	}
	var terminal struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(msg.Payload, &terminal)
	if terminal.Status == "failed" {
		var failure struct {
			Status    string `json:"status"`
			ErrorCode string `json:"error_code"`
			Detail    string `json:"detail"`
		}
		if json.Unmarshal(msg.Payload, &failure) == nil && failure.ErrorCode != "" {
			return nil, &lagoon.Error{Code: lagoon.ErrorCode(failure.ErrorCode), Detail: failure.Detail}
		}
		return nil, errors.New("lagoon: registrar call failed")
	}
	return append(json.RawMessage(nil), msg.Payload...), nil
}

var _ lagoon.C0Caller = registrarCaller{}
