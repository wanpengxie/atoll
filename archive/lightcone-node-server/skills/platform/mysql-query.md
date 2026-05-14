---
name: mysql-query
description: Query MySQL databases using SQL via MCP tools
tags: ["mysql", "database", "sql"]
mcp_config: {"server":"mysql","command":"node","args":["{mysql_mcp_path}"],"env":["DB_HOST","DB_PORT","DB_USER","DB_PASSWORD","DB_NAME"]}
---

# MySQL Query Capability

This skill gives you access to a MySQL database via MCP tools.

## Available Tools

- `query` — execute a SELECT query and return results
- `execute` — execute an INSERT/UPDATE/DELETE statement

## Usage Guidelines

1. **Read-first**: always use SELECT to understand the schema before modifying data
2. **Add LIMIT**: use `LIMIT` on large tables to avoid overwhelming results
3. **Be careful with writes**: confirm with the user before running UPDATE/DELETE
4. **Use transactions**: wrap multi-step writes in transactions when possible

## Discovering Schema

```sql
SHOW TABLES;
DESCRIBE table_name;
SELECT * FROM table_name LIMIT 5;
```

## Required Environment Variables

The following must be set in the Agent's environment variables:
- `DB_HOST` — database host
- `DB_PORT` — database port (default: 3306)
- `DB_USER` — database username
- `DB_PASSWORD` — database password
- `DB_NAME` — database name
