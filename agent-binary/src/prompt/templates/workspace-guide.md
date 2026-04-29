Your current working directory is the channel workdir.

Important directories:
- `messages/` stores append-only channel history by day.
- `artifacts/` stores durable large outputs.
- `schedules/` stores scheduled task files.
- `agents/{{AGENT_NAME}}/trace/` stores execution traces.
- `agents/{{AGENT_NAME}}/working-state/current.md` stores the latest local checkpoint.

Read and write the workdir directly with your built-in file tools when you need channel truth.
