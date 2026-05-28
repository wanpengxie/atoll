# coagentsdk

Minimal Go SDK for calling an actor inside a coagent channel.

```go
client := &coagentsdk.Client{
	BaseURL:      "http://127.0.0.1:8832",
	SessionToken: "raw-coagent-session-cookie-value",
}

res, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
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

Default `CallActor` timeout is 30s. Pass `CallActorRequest.Timeout` only when a
specific type has an explicit longer `max_pending_ms` budget.

`SessionToken` is the raw value of the `coagent_session` cookie. The SDK sends
it as a cookie on both HTTP and WebSocket requests.

## Streaming: `Submit` + `Watch` + `Await`

`CallActor` is the convenience sync wrap. The first-class async surface is
`Submit` + `Watch` / `Await`, which exposes provisional responses
(`payload.status` ∈ `received` / `queued` / `processing` / `deferred` /
`unavailable` plus Layer 3 namespace extensions like `xhs.login_queued`)
without collapsing them to RPC.

```go
client := &coagentsdk.Client{BaseURL: "http://127.0.0.1:8832", SessionToken: tok}

// Submit returns immediately with the envelope id assigned to the request.
res, err := client.Submit(ctx, coagentsdk.SubmitRequest{
    ChannelID: "ch_123",
    ActorID:   "tool:xhs",
    Type:      "xhs.publish",
    Payload:   json.RawMessage(`{"title":"hello"}`),
})
if err != nil {
    return err
}

// Watch streams every envelope (provisional + final) whose parent_id
// matches the request id. The stream closes after the final response or
// when the caller calls Close().
watch, err := client.Watch(ctx, "ch_123", res.RequestID)
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

`Await(ctx, channelID, requestID, timeout)` is sugar around `Watch` that
filters to the final response (`payload.status ∈ {completed, failed}`) and
returns a `CallActorResult`. Provisional responses are silently dropped.
Timeout failure does NOT cancel substrate state — the daemon may still emit
the final response later; reconnect with `Watch` to observe it.

Layer 3 status names follow `<adapter>.<name>` (e.g. `xhs.login_queued`).
The namespace must match the sender actor's local name; the harness rejects
spoofed names at write time so SDK consumers can trust them.
