# coagentsdk

Minimal Go SDK for calling an actor inside a coagent channel.

```go
client := &coagentsdk.Client{
	BaseURL:      "http://127.0.0.1:8832",
	SessionToken: "raw-coagent-session-cookie-value",
}

res, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
	ChannelID: "ch_123",
	ActorID:   "tool:xhs-adapter",
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

`SessionToken` is the raw value of the `coagent_session` cookie. The SDK sends
it as a cookie on both HTTP and WebSocket requests.
