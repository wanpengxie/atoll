# coagentsdk

Minimal Go SDK for calling an actor inside a coagent channel.

```go
client := &coagentsdk.Client{
	BaseURL:      "http://127.0.0.1:8832",
	SessionToken: "raw-coagent-session-cookie-value",
}

res, err := client.Call(ctx, coagentsdk.CallRequest{
	ChannelID: "ch_123",
	ActorID:   "tool:xhs",
	Type:      "xhs.publish",
	Payload:   json.RawMessage(`{"title":"hello","content":"world"}`),
	Timeout:   30 * time.Second,
})
if err != nil {
	return err
}
if !res.OK {
	return fmt.Errorf("call_actor failed: %s: %s", res.Error.Code, res.Error.Message)
}
fmt.Printf("response payload: %s\n", res.Data)
```

`ListActors` is a display/catalog projection. For current readiness of one
actor, use the reserved `actor.status` request:

```go
actors, err := client.ListActors(ctx, "ch_123")
if err != nil {
	return err
}
for _, a := range actors {
	fmt.Printf("%s projected_ready=%v reason=%s\n", a.ActorID, a.Ready, a.ReadyReason)
}

status, err := client.ActorStatus(ctx, "ch_123", "tool:kimi")
if err != nil {
	return err
}
fmt.Printf("kimi proxy available=%v reason=%s\n", status.Available, status.Reason)
```

Default `Call` timeout is 30s. Pass `CallRequest.Timeout` only when a
specific type has an explicit longer `max_pending_ms` budget.

`SessionToken` is the raw value of the `coagent_session` cookie. The SDK sends
it as a cookie on both HTTP and WebSocket requests.

## Streaming: `Submit` + `Watch` + `Await`

`Call` is the convenience sync wrap. The first-class async surface is
`Submit` + `Watch` / `Await`, which exposes provisional responses
(`payload.status` ∈ `received` / `queued` / `processing` / `deferred` /
`unavailable` plus Layer 3 namespace extensions like `xhs.login_queued`)
without collapsing them to RPC.

Prefer the one-call sugar `SubmitAndWatch` / `SubmitAndAwait`: they emit
the request and open the watch in a single step, threading the submit-time
cursor into the subscription automatically so a fast final (emitted before
the WS subscribe completes) is never lost.

```go
client := &coagentsdk.Client{BaseURL: "http://127.0.0.1:8832", SessionToken: tok}

// SubmitAndWatch emits the request AND opens the stream in one call,
// auto-threading the submit-time cursor into the watch's since_seq.
watch, err := client.SubmitAndWatch(ctx, coagentsdk.SubmitRequest{
    ChannelID: "ch_123",
    ActorID:   "tool:xhs",
    Type:      "xhs.publish",
    Payload:   json.RawMessage(`{"title":"hello"}`),
})
if err != nil {
    return err
}
defer watch.Close()

for ev := range watch.Events() {
    if ev.Err != nil {
        return ev.Err
    }
    if ev.Envelope == nil {
        continue
    }
    // Inspect payload.status for provisional progress.
    fmt.Printf("envelope %s final=%v payload=%s\n",
        ev.Envelope.ID, ev.IsFinal, string(ev.Envelope.Payload))
    if ev.IsFinal {
        break
    }
}
```

`SubmitAndAwait(ctx, req, timeout)` is the blocking-on-final counterpart:
it emits the request and returns the final `CallResult`, threading the
submit-time cursor automatically. Provisional responses are silently
dropped. Timeout failure does NOT cancel substrate state — the daemon may
still emit the final response later; reconnect with `Watch` to observe it.

The lower-level `Submit` + `Watch` / `Await` API is still available for
callers that need to split the two steps (e.g. fan-in across many in-flight
requests). When using it manually you MUST pass
`WithSinceSeq(res.SinceSeq)` to `Watch` / `Await` to avoid losing a fast
final — `SubmitAndWatch` / `SubmitAndAwait` exist precisely to remove that
footgun.

Layer 3 status names follow `<adapter>.<name>` (e.g. `xhs.login_queued`).
The namespace must match the sender actor's local name; the harness rejects
spoofed names at write time so SDK consumers can trust them.
