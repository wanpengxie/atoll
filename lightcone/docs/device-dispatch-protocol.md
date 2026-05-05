# Device Dispatch Protocol

Device and external workers receive `dispatch.start` messages from a channel and should answer on the same `correlation_id`.

## Message Flow

1. Agent emits `dispatch.start` with `payload.body.type` and `payload.body.params`.
2. Device responds with one of:
   - `dispatch.accepted`
   - `dispatch.rejected`
   - `dispatch.completed`
   - `dispatch.failed`
3. Terminal messages are `dispatch.rejected`, `dispatch.completed`, and `dispatch.failed`.

All messages in the chain must keep the original `correlation_id`.

## HTTP Bridge

Devices can report through:

```http
POST /api/device/result
Authorization: Bearer <machine_api_key>
Content-Type: application/json
```

Body:

```json
{
  "channelId": "channel-a",
  "correlationId": "corr-a",
  "status": "completed",
  "deviceId": "chrome-ext",
  "result": { "url": "https://example.test/note" }
}
```

`status` maps to `payload.type`:

| status | payload.type |
|---|---|
| `accepted` | `dispatch.accepted` |
| `rejected` | `dispatch.rejected` |
| `completed` | `dispatch.completed` |
| `failed` | `dispatch.failed` |

The server forwards this to the owning channel daemon as `channel:message.send`. The daemon writes the channel sqlite truth first, then syncs the server view cache. The HTTP bridge does not write MySQL directly.
