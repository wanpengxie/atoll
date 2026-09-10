# Looper lifecycle

Looper startup creates an empty active-assignment table and receives commands.
There is no startup ledger scan, pre-command attach, historical closed-turn
cache or synthetic report for another process's execution. The current process
keeps its normal capacity checks, session exclusion and bounded finished cache.

When the process exits, it cancels its own execution contexts and waits for its
goroutines. Exit does not cancel remote calls or send terminal reports. Normal
explicit stop, task timeout and completion still use their normal report paths.

A new start builds context from ledger history on demand. It does not trust the
shared context KV, which may contain an interrupted turn's intermediate writes.
Only accepted turns with valid terminal boundaries enter the assembled history.
Unclosed accepted turns, missing required inputs, malformed snapshots, invalid
tool pairing, parent/base cycles and reused historical turn IDs cause rejection.
Main merge summaries and explicit fork boundaries remain part of context.

All ledger reads use `sys.View()`, not `system.log.query`. View returns complete
structured envelopes at one fixed head, within 4,096 scanned messages (including
unrelated rows), 16 MiB of payload and 10 seconds. Ancestor expansion belongs to
View's snapshot `Session` builder; Looper renders the returned ancestor chain.
All session/base reads for a command share that snapshot. Each input insertion
batch shares a fresh snapshot so later steering inputs can be read without
rescanning once per input. Model context is also limited to 4,096 messages.
A partial or failed read is never treated as complete history.

When a new start cannot establish consistent context within these limits it
returns `session_context_unavailable`, with advice to open a new session. It
does not accept the assignment, emit an old report, or fall back to empty/KV
history. Controller closes that current work with the same error and advice,
rather than requeueing it. Receipt callers see the failure in result/status;
waiting callers receive the terminal work result. Existing other assignments
continue independently.

Tests cover no startup/command recovery, refusal of unfinished history, normal
closed-history reconstruction, missing input, main summaries, local session
exclusion, shared recursive/sparse read budgets, the 4,096-message boundary,
process exit without remote cancellation, and Controller refusal propagation.
