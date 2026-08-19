package actorbase

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
)

const ManifestStateKey resource.ResourceID = "_manifest"

func (e *engine) ProjectManifest(ctx context.Context) (introspect.Describe, error) {
	manifest := e.def.Manifest
	words := introspect.CloneWords(manifest.Words)
	if words == nil {
		words = map[string]introspect.WordSpec{}
	}
	dynamic, err := e.dynamicWords(ctx, manifest)
	if err != nil {
		return introspect.Describe{}, err
	}
	validateDynamic := introspect.ValidateDynamicWords
	if manifest.Dynamic != nil {
		validateDynamic = introspect.ValidateProjectedWords
	}
	if err := validateDynamic(manifest.Words, dynamic); err != nil {
		return introspect.Describe{}, err
	}
	for name, spec := range dynamic {
		words[name] = spec
	}
	interfaces := append([]string(nil), manifest.Interfaces...)
	if interfaces == nil {
		interfaces = []string{}
	}
	capabilities := make(map[string]bool, len(manifest.Capabilities))
	for name, enabled := range manifest.Capabilities {
		capabilities[name] = enabled
	}
	return introspect.Describe{
		Class: manifest.Class, Interfaces: interfaces,
		Capabilities: capabilities, Words: words,
	}, nil
}

func (e *engine) dynamicWords(ctx context.Context, manifest introspect.Manifest) (map[string]introspect.WordSpec, error) {
	if manifest.Dynamic != nil {
		return manifest.Dynamic(ctx)
	}
	if e.state == nil {
		return nil, nil
	}
	out, err := e.state.Invoke(ctx, access.OpRead, ManifestStateKey, nil)
	if err != nil {
		return nil, err
	}
	if out.RejectReason == access.ResourceNotFound || !out.Found || len(out.Value) == 0 {
		return nil, nil
	}
	if !out.Accepted() {
		return nil, fmt.Errorf("actorbase: dynamic manifest read rejected: %s", out.RejectReason)
	}
	var words map[string]introspect.WordSpec
	if err := json.Unmarshal(out.Value, &words); err != nil {
		return nil, err
	}
	return words, nil
}

func (e *engine) validateStatePut(id resource.ResourceID, raw []byte) error {
	if id != ManifestStateKey {
		return nil
	}
	var words map[string]introspect.WordSpec
	if err := json.Unmarshal(raw, &words); err != nil {
		return fmt.Errorf("actorbase: invalid dynamic manifest: %w", err)
	}
	return introspect.ValidateDynamicWords(e.def.Manifest.Words, words)
}

func (e *engine) respondManifest(env *message.Envelope) {
	msg := NewMsg(OriginMailbox, e.lifeCtx, *env)
	var req introspect.DescribeRequest
	err := DecodeStrictEmpty(msg.Payload, &req)
	if err != nil {
		_, _ = e.Fail(msg, "invalid_args", err.Error())
		return
	}
	describe, err := e.ProjectManifest(msg.Ctx())
	if err != nil {
		_, _ = e.Fail(msg, "internal_error", err.Error())
		return
	}
	answer, ok := introspect.AnswerDescribe(describe, req)
	if !ok {
		_, _ = e.Fail(msg, "invalid_args", "unknown manifest word")
		return
	}
	_, _ = e.Reply(msg, answer)
}
