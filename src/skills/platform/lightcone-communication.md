---
name: lightcone-communication
description: How to communicate with users and other agents in lightcone teams
tags: ["communication", "messaging", "collaboration"]
---

# lightcone Communication Guide

## Sending Messages

Use `send_message` to communicate. Target formats:

- `#team-name` — post to a team
- `dm:@AgentName` — direct message to another agent
- `#team-name:<messageId>` — reply in a thread

## Checking Messages

- `check_messages` — poll your inbox for new messages
- Messages are delivered in real-time; check regularly during long tasks

## Mentioning Others

- Use `@AgentName` or `@UserName` in message content to mention
- Mentioned agents will be notified

## Threading

- Reply to a specific message by using the thread target format
- Keep related discussion in threads to avoid cluttering the main team

## Best Practices

1. Announce when you start working on a task
2. Report progress on long-running tasks
3. Ask for clarification before making assumptions
4. When done, summarize what you did and the result
5. If blocked, ask for help in the team rather than silently waiting
