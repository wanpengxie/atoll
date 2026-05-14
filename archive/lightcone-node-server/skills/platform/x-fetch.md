---
name: x-fetch
description: Fetch latest tweets from a specified X (Twitter) account using X API v2
tags: ["x", "twitter", "social-media", "fetch"]
---

# Fetching Tweets from X (Twitter)

## Prerequisites

You need a Bearer Token granted to this agent. Retrieve it at the start of every session:

```
get_credential(platform="x-readonly")
# Returns: X_BEARER_TOKEN=<token value>
```

If `get_credential` returns "No credential found", ask the human to go to **设置 → 连接外部账号**, connect the **X (只读 / Bearer Token)** account, and grant it to this agent.

## Step 1: Resolve username to user ID

```bash
curl -s \
  -H "Authorization: Bearer <X_BEARER_TOKEN>" \
  "https://api.x.com/2/users/by/username/{username}?user.fields=id,name,username"
```

Replace `{username}` with the target account handle (without `@`). Example: `OpenAI`, `elonmusk`.

Response shape:
```json
{
  "data": {
    "id": "123456789",
    "name": "OpenAI",
    "username": "OpenAI"
  }
}
```

Extract `data.id` as `user_id` for the next step.

## Step 2: Fetch latest tweets

```bash
curl -s \
  -H "Authorization: Bearer <X_BEARER_TOKEN>" \
  "https://api.x.com/2/users/{user_id}/tweets?max_results=10&tweet.fields=created_at,public_metrics,conversation_id&exclude=replies,retweets"
```

- `{user_id}`: from Step 1
- `max_results`: 5–100 (default 10)
- `exclude=replies,retweets`: omit replies and reposts; remove if you want them

Response shape:
```json
{
  "data": [
    {
      "id": "1234567890123456789",
      "text": "Tweet content here",
      "created_at": "2026-04-12T08:00:00.000Z",
      "public_metrics": {
        "retweet_count": 10,
        "like_count": 100
      }
    }
  ]
}
```

Each item in `data` is one tweet. `id` can be used to construct the tweet URL:
`https://x.com/{username}/status/{id}`

## Tips

- The user ID is stable; cache it to avoid repeating Step 1 on subsequent calls.
- If `data` is missing or empty, the account may have no recent public tweets.
- Rate limits apply. X API v2 free tier allows limited requests per 15-minute window.
