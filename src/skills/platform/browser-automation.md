---
name: browser-automation
description: Control a headless browser for web automation, screenshots, and testing
tags: ["browser", "automation", "testing"]
mcp_config: {"server":"chrome-devtools","command":"npx","args":["chrome-devtools-mcp@latest","--headless"],"env":[]}
---

# Browser Automation Capability

This skill gives you access to a headless Chrome browser via MCP tools.

## Available Tools

- `navigate_page` — open a URL
- `take_screenshot` — capture the current page
- `take_snapshot` — get the accessibility tree (preferred for reading page content)
- `click` — click an element by uid
- `fill` — type text into an input
- `evaluate_script` — run JavaScript in the page
- `wait_for` — wait for text to appear

## Usage Guidelines

1. **Prefer snapshots over screenshots** — snapshots give you structured data with element UIDs
2. **Use UIDs from snapshots** — always take a fresh snapshot before interacting with elements
3. **Wait for page loads** — use `wait_for` after navigation to ensure content is ready
4. **Be efficient** — combine related operations, don't take unnecessary screenshots

## Typical Workflow

1. `navigate_page` to the target URL
2. `take_snapshot` to understand the page structure
3. `click` / `fill` to interact with elements
4. `take_screenshot` to verify results
