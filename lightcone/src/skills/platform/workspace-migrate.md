---
name: workspace-migrate
description: Migrate local workspace files to the server database for cross-machine access
tags: ["workspace", "migration", "hosting"]
mcp_config: {"server":"workspace-migrate","command":"node","args":["{workspace_migrate_mcp_path}"],"env":[]}
---

# Workspace Migration

This skill lets you migrate local team workspace files (artifacts/, notes/, etc.) to the server database, enabling cross-machine access and hosted agent compatibility.

## Available Tools

- `migrate_workspace` — scan local `~/.lightcone/workspace/` and upload all team workspace files to the server

## When to Use

Run this once when switching from a local daemon to hosted agents, or when setting up a new machine that should have access to existing workspace files.

## Usage

Simply ask the agent: "帮我迁移本地工作区" or "migrate my workspace".

The tool will report how many teams and files were migrated.

## Notes

- Only team-level files (artifacts/, notes/, BRIEF.md, etc.) are migrated
- Agent private workspaces are not affected (MEMORY.md is already stored in the database)
- Safe to run multiple times — existing files will be overwritten with local versions
