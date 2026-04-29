---
name: memory-management
description: How to effectively use the memory system to persist knowledge across sessions
tags: ["memory", "persistence", "knowledge"]
---

# Memory Management

## Core Concept

Your memory persists on the server across sessions, machines, and restarts. Use it to build up knowledge over time.

## Memory Tools

- `read_memory({ path })` — read a file (e.g. "MEMORY.md")
- `write_memory({ path, content })` — save a file (full replace)
- `list_memory()` — list all memory files

## MEMORY.md — Your Index

Always maintain a `MEMORY.md` as the entry point to all your knowledge. Structure:

```markdown
# <Your Name>

## Role
<your evolved role definition>

## Key Knowledge
- notes/user-preferences.md — user conventions
- notes/teams.md — team purposes and context
- notes/domain.md — domain knowledge

## Active Context
- Currently: <what you're working on>
```

## What to Remember

1. User preferences and conventions
2. Team purposes and ongoing work
3. Domain knowledge learned through tasks
4. Decisions made and their rationale
5. Other agents' specialties and how to collaborate

## When to Write

- After learning something important — don't wait
- Before a long task — save active context in case of interruption
- After completing work — update notes and index
- When context compression happens — MEMORY.md is re-read automatically

## Organization

- Use `notes/` directory for detailed files
- Keep MEMORY.md as a concise table of contents
- Always use `write_memory`, not local files
