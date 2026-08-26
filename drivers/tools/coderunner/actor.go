// Package coderunner adapts one short-lived JavaScript program into an Atoll
// tool request. Every SDK call made by the program remains an ordinary child
// request on the channel ledger; the Node process is only an execution vessel.
package coderunner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

const (
	TypeRun             = "code.run"
	defaultCallDeadline = 30 * time.Second
)

type coderunnerActor struct {
	cfg      Config
	deps     registry.Deps
	inflight sync.WaitGroup
}

func Def(cfg Config, deps registry.Deps) actorbase.Def {
	return actorbase.Def{Manifest: manifest(), New: func() (actorbase.Proc, error) {
		return (&coderunnerActor{cfg: cfg, deps: deps}).run, nil
	}}
}

func (a *coderunnerActor) run(sys actorbase.Sys) error {
	defer a.inflight.Wait()
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		switch msg.Type {
		case TypeRun, TypeValidate:
		default:
			_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("coderunner does not answer %q", msg.Type))
			continue
		}
		a.inflight.Add(1)
		go func(msg actorbase.Msg) {
			defer a.inflight.Done()
			if msg.Type == TypeValidate {
				a.handleValidate(sys, msg)
				return
			}
			a.handle(sys, msg)
		}(msg)
	}
}

type runPayload struct {
	Program  string          `json:"program,omitempty"`
	Requires []string        `json:"requires,omitempty"`
	Args     json.RawMessage `json:"args,omitempty"`
}

type runSpec struct {
	program  string
	requires []string
	args     json.RawMessage
	forward  bool
}

func (a *coderunnerActor) decodeRun(raw json.RawMessage) (runSpec, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return runSpec{}, errors.New("code.run input must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var payload runPayload
	if err := dec.Decode(&payload); err != nil {
		return runSpec{}, fmt.Errorf("invalid code.run input: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return runSpec{}, errors.New("invalid code.run input: multiple JSON values")
	}
	_, hasProgram := fields["program"]
	_, hasRequires := fields["requires"]
	if hasProgram {
		var value string
		if json.Unmarshal(fields["program"], &value) != nil {
			return runSpec{}, errors.New("program must be a string")
		}
	}
	if hasRequires {
		var value []string
		if json.Unmarshal(fields["requires"], &value) != nil || value == nil {
			return runSpec{}, errors.New("requires must be an array of strings")
		}
	}
	args := json.RawMessage("null")
	if value, ok := fields["args"]; ok {
		args = append(json.RawMessage(nil), value...)
	}
	if a.cfg.Program == "" {
		if !hasProgram || strings.TrimSpace(payload.Program) == "" {
			return runSpec{}, errors.New("mode-one coderunner requires a non-blank program")
		}
		if err := validateRequires(payload.Requires); err != nil {
			return runSpec{}, err
		}
		return runSpec{program: payload.Program, requires: payload.Requires, args: args, forward: true}, nil
	}
	if hasProgram || hasRequires {
		return runSpec{}, errors.New("fixed-program coderunner forbids program and requires in code.run")
	}
	return runSpec{program: a.cfg.Program, requires: a.cfg.Requires, args: args}, nil
}

func (a *coderunnerActor) handle(sys actorbase.Sys, msg actorbase.Msg) {
	spec, err := a.decodeRun(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_input", err.Error())
		return
	}
	resolved, missing, err := resolveRequirements(sys, msg, spec.requires, a.logger())
	if err != nil {
		_, _ = sys.Fail(msg, "dependency_missing", err.Error())
		return
	}
	if len(missing) > 0 {
		failWithFields(sys, msg, "dependency_missing", "required actors are not present", dependencyFailure{Missing: missing})
		return
	}
	a.execute(sys, msg, spec, resolved)
}

func (a *coderunnerActor) logger() *slog.Logger {
	if a.deps.Logger != nil {
		return a.deps.Logger
	}
	return slog.Default()
}

func failWithFields(sys actorbase.Sys, msg actorbase.Msg, code, detail string, fields any) {
	body := map[string]any{}
	raw, err := json.Marshal(fields)
	if err == nil {
		var extra map[string]any
		if json.Unmarshal(raw, &extra) == nil {
			for key, value := range extra {
				body[key] = value
			}
		}
	}
	_, _ = sys.Fail(msg, code, detail, body)
}

type declarationRow struct {
	Name         string `json:"name"`
	DefaultClass string `json:"default_class"`
}

// resolution is one requires list resolved against the channel: what each
// requirement resolved to, what was absent, and every present candidate per
// requirement (more than one = ambiguous; the first sorted one is used).
type resolution struct {
	resolved   map[string]actor.ActorID
	missing    []string
	candidates map[string][]string
}

func (r resolution) ambiguous() map[string][]string {
	var out map[string][]string
	for name, ids := range r.candidates {
		if len(ids) > 1 {
			if out == nil {
				out = map[string][]string{}
			}
			out[name] = append([]string(nil), ids...)
		}
	}
	return out
}

func resolveRequirements(sys actorbase.Sys, msg actorbase.Msg, requires []string, logger *slog.Logger) (map[string]actor.ActorID, []string, error) {
	res, err := resolveRequirementsDetailed(sys, msg, requires, logger)
	if err != nil {
		return nil, nil, err
	}
	return res.resolved, res.missing, nil
}

func resolveRequirementsDetailed(sys actorbase.Sys, msg actorbase.Msg, requires []string, logger *slog.Logger) (resolution, error) {
	resolved := make(map[string]actor.ActorID, len(requires))
	if len(requires) == 0 {
		return resolution{resolved: resolved}, nil
	}
	needDirectory := false
	for _, requirement := range requires {
		if requirement == "system" {
			resolved[requirement] = actor.SystemActorID
		} else {
			needDirectory = true
		}
	}
	if !needDirectory {
		return resolution{resolved: resolved}, nil
	}

	membersMsg, err := callAndWait(sys, msg, actor.SystemActorID, message.TypeSystemMemberList, struct{}{})
	if err != nil {
		return resolution{}, fmt.Errorf("list channel members: %w", err)
	}
	var catalog introspect.Catalog
	if err := json.Unmarshal(membersMsg.Payload, &catalog); err != nil {
		return resolution{}, fmt.Errorf("decode member directory: %w", err)
	}
	templatesMsg, err := callAndWait(sys, msg, actor.SystemActorID, message.TypeSystemActorTemplateList, struct{}{})
	if err != nil {
		return resolution{}, fmt.Errorf("list actor templates: %w", err)
	}
	var declarations []declarationRow
	if err := json.Unmarshal(templatesMsg.Payload, &declarations); err != nil {
		var wrapped struct {
			Value []declarationRow `json:"value"`
		}
		if wrappedErr := json.Unmarshal(templatesMsg.Payload, &wrapped); wrappedErr != nil || wrapped.Value == nil {
			return resolution{}, fmt.Errorf("decode actor templates: %w", err)
		}
		declarations = wrapped.Value
	}
	return resolveDirectoryDetailed(requires, catalog, declarations, logger), nil
}

func resolveDirectory(requires []string, catalog introspect.Catalog, declarations []declarationRow, logger *slog.Logger) (map[string]actor.ActorID, []string) {
	res := resolveDirectoryDetailed(requires, catalog, declarations, logger)
	return res.resolved, res.missing
}

func resolveDirectoryDetailed(requires []string, catalog introspect.Catalog, declarations []declarationRow, logger *slog.Logger) resolution {
	resolved := make(map[string]actor.ActorID, len(requires))
	allCandidates := make(map[string][]string, len(requires))
	classes := make(map[string]string, len(declarations))
	for _, declaration := range declarations {
		classes[declaration.Name] = declaration.DefaultClass
	}

	var missing []string
	for _, requirement := range requires {
		if requirement == "system" {
			resolved[requirement] = actor.SystemActorID
			continue
		}
		class, declaration, hasDeclaration := strings.Cut(requirement, ":")
		var candidates []string
		for _, member := range catalog.Actors {
			if !member.Present || classes[member.Name] != class {
				continue
			}
			if hasDeclaration && member.Name != declaration {
				continue
			}
			candidates = append(candidates, member.ID)
		}
		if len(candidates) == 0 {
			missing = append(missing, requirement)
			continue
		}
		sort.Strings(candidates)
		resolved[requirement] = actor.ActorID(candidates[0])
		allCandidates[requirement] = candidates
		logger.Info(fmt.Sprintf("resolved %s -> %s (%d candidates)", requirement, candidates[0], len(candidates)))
	}
	return resolution{resolved: resolved, missing: missing, candidates: allCandidates}
}

func callAndWait(sys actorbase.Sys, msg actorbase.Msg, target actor.ActorID, typ string, payload any) (actorbase.Msg, error) {
	pending, err := sys.Call(msg.Cause(), target, typ, payload)
	if err != nil {
		return actorbase.Msg{}, err
	}
	response, err := pending.Wait(msg.Ctx(), boundedWait(msg.Ctx(), defaultCallDeadline))
	if err != nil {
		_ = pending.Cancel()
		return actorbase.Msg{}, err
	}
	var outcome struct {
		Status string `json:"status"`
		message.Failure
	}
	_ = json.Unmarshal(response.Payload, &outcome)
	if outcome.Status != message.StatusCompleted {
		failure := outcome.Failure
		return actorbase.Msg{}, fmt.Errorf("%s: %s", failure.ErrorCode, failure.Detail)
	}
	return response, nil
}

func boundedWait(ctx context.Context, requested time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < requested {
			if remaining < 0 {
				return time.Nanosecond
			}
			return remaining
		}
	}
	return requested
}
