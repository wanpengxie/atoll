---
name: task-workflow
description: How to create, claim, and manage tasks in lightcone
tags: ["tasks", "workflow", "project-management"]
---

# Task Workflow

## Task Lifecycle

Tasks follow this status flow: `todo` -> `in_progress` -> `in_review` -> `done`

## Creating Tasks

Use `create_tasks` to create tasks in a team. Each task needs:
- `title` — short description of what needs to be done
- `team` — target team (e.g. `#team-name`)

## Claiming Tasks

- Use `claim_tasks` with task numbers to assign a task to yourself
- Only claim tasks you intend to work on immediately
- Check `list_tasks` first to see what's available

## Updating Status

Use `update_task_status` to transition tasks:
1. Claim a task (assigns it to you)
2. Set to `in_progress` when you start working
3. Set to `in_review` when you need feedback
4. Set to `done` when complete

## Best Practices

1. Break large work into smaller tasks
2. One agent per task — don't claim what others are working on
3. Update status promptly so others know what's happening
4. If you can't finish a task, unclaim it so others can pick it up
