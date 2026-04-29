---
name: youtube-fetch
description: Fetch latest videos from a specified YouTube channel using YouTube Data API v3
tags: ["youtube", "video", "social-media", "fetch"]
---

# Fetching Latest Videos from a YouTube Channel

## Prerequisites

The agent must have `YOUTUBE_API_KEY` set in its environment variables.

## Step 1: Resolve channel handle to channel ID and uploads playlist ID

```bash
# By channel handle (e.g. "GoogleDevelopers", without "@")
curl -s \
  "https://www.googleapis.com/youtube/v3/channels?part=snippet,contentDetails&forHandle={channel_handle}&key=$YOUTUBE_API_KEY"

# Or by channel ID directly (starts with "UC")
curl -s \
  "https://www.googleapis.com/youtube/v3/channels?part=snippet,contentDetails&id={channel_id}&key=$YOUTUBE_API_KEY"
```

Response shape:
```json
{
  "items": [
    {
      "id": "UC295-Dw0tDd-y_RCKQRHFhg",
      "snippet": { "title": "Google for Developers" },
      "contentDetails": {
        "relatedPlaylists": {
          "uploads": "UU295-Dw0tDd-y_RCKQRHFhg"
        }
      }
    }
  ]
}
```

Extract:
- `items[0].id` → `channel_id`
- `items[0].contentDetails.relatedPlaylists.uploads` → `uploads_playlist_id`

Both values are stable; cache them to skip this step on subsequent calls.

## Step 2: Fetch latest video IDs from uploads playlist

```bash
curl -s \
  "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet,contentDetails&playlistId={uploads_playlist_id}&maxResults=5&key=$YOUTUBE_API_KEY"
```

- `{uploads_playlist_id}`: from Step 1
- `maxResults`: 1–50 (default 5)

Response shape:
```json
{
  "items": [
    {
      "contentDetails": { "videoId": "dQw4w9WgXcQ", "videoPublishedAt": "2026-04-10T12:00:00Z" },
      "snippet": { "title": "Video Title" }
    }
  ]
}
```

Collect all `contentDetails.videoId` values into a comma-separated list.

## Step 3: Fetch video details

```bash
curl -s \
  "https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&id={video_ids}&key=$YOUTUBE_API_KEY"
```

- `{video_ids}`: comma-separated IDs from Step 2, e.g. `id1,id2,id3`

Response shape:
```json
{
  "items": [
    {
      "id": "dQw4w9WgXcQ",
      "snippet": {
        "title": "Video Title",
        "description": "...",
        "publishedAt": "2026-04-10T12:00:00Z",
        "thumbnails": { "high": { "url": "https://..." } }
      },
      "contentDetails": { "duration": "PT4M33S" },
      "statistics": { "viewCount": "50000", "likeCount": "3000" }
    }
  ]
}
```

Video URL: `https://www.youtube.com/watch?v={id}`

## Tips

- Steps 1 is only needed once per channel. Cache `channel_id` and `uploads_playlist_id` in memory for subsequent fetches.
- Step 3 is optional if you only need titles and video IDs from Step 2.
- `duration` is in ISO 8601 format, e.g. `PT4M33S` = 4 minutes 33 seconds.
- If `items` is empty in Step 1, double-check the handle spelling (case-insensitive but must be exact).
