# Actor Channel v3.3 implementation handoff

Scope: message-plane refactoring on top of `0a4cc9fe` (batch A). No access-plane implementation, runtime extension, running-node restart, or live registry migration.

## Implemented

- Separate seat `{body}` and handle `{host, words, drivers?}` configurations. `words` carries schema and an internal declaration target; the seat exposes only the public word projection.
- Body target resolution reuses Home's `MemberOfDeclaration`. Handle driver admission uses the authenticated envelope Sender; caller attribution is business metadata only.
- Call/Post/Emit carry ordinary message envelope fields, including audience, visibility and request deadline. Local endpoint control distinguishes Call from Post. Replies/progress/cancellation use the ordinary pending request lifecycle.
- H-side actions use S's own identity. The existing full-spec `CallSpecFor` API is supplied S itself, not a foreign caller, to preserve deadline and visibility without adding a Sys/runtime method.
- Events cross into A; unknown request words are refused before entering A. Transport remains the in-process Hub, not a deployed cross-process seam.
- Channel member identity is the existing body Channel ID, resolved from the published seat address by the registry and used as the member seed. Home compares existing member identities at introduction; it does not parse implementation classes or business config. Only channel-member introductions take the admission mutex. Config updates do not retarget that identity. Genesis uses the same ID. Ordinary peer/singleton behavior is unchanged; no runtime changes or relation database.
- Each new channel publishes separate peer and seat declarations. Recipe entries explicitly opt into creation-input binding (bindings.host=parent_channel_id); no class selects a handle or causes all host fields to be overwritten. No handle is synthesized from svc_agent. c0 control peer and parent relation are composed separately, merging the group/c0 case. seated reports membership admission, not connected transport.
- Channel creation observes member-create results and returns partial-creation receipts. Duplicate names return conflict_exists. Retirement revokes both published declarations; unrelated host seats remain for manual removal.
- Native looper exposes channel_call/channel_post/channel_emit using audience rather than target.

## Verification

- Follow-up identity refactor: full `go test ./... -timeout 180s` passed; targeted Home/lagoon/engineboot race acceptance passed, and concurrent same-body admission was repeated ten times under the race detector.
- Tests cover class-independent same-ID admission, absent bodies, alias refusal, implementation-config retarget refusal, membership without a connected Handle, and explicit parent binding leaving another Handle untouched.

- `go test ./... -timeout 180s` passed during this change.
- Targeted Home/channelmember race tests passed: bidirectional request/progress, incoming event identity, undeclared-word refusal, Sender restrictions, local call authority, targeted Post/Emit, declaration projection, and concurrent relationship uniqueness.
- Relevant Home/lagoon/engineboot/channelmember package regression passed after receipt and configuration updates.
- `git diff 6518628c -- runtime` is empty; the pre-existing uncommitted runtime reversal is preserved.

## Deployment boundary

Do not restart an existing installation with the new binary before its legacy Seat/Handle declarations and genesis configurations have been inventoried and migrated. The new parsers intentionally do not silently reinterpret `{host_channel, body_channel, receiver}`. Startup declaration materialization is fail-closed and may reject these old declarations.

Migration requires a separate authorized operation:

1. Read current published declarations, channel recipes/genesis, overlays and live memberships; redact credentials from the report.
2. Restore each body's peer declaration and add its separate `seat:<body>` declaration.
3. Replace legacy handles with body-authored words/targets; update overlays and the native template, keeping each existing body and ledger.
4. Resolve old seat declaration references and duplicate relationships before restarting; do not bulk-retire bodies or erase histories.
5. After restart, verify live host identity, reverse system calls, events, cancellation, and removal behavior. Local tests are not evidence of a live deployment.

Access/resource mounting and Space bodies remain deferred per v3.3. This handoff does not claim these are implemented.
