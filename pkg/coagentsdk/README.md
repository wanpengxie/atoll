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
