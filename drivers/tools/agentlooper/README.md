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

All session and recursive-base reads for a command share one ledger head, a
4,096-message budget and a request-local cache. Resulting model context is also
limited to 4,096 messages. Sparse scans additionally have a 4,096-exchange budget
(reserving the log API's maximum 512-exchange page before each call). Full-text
continuations share a 512-query, 16 MiB response and 10-second budget. A read may
fail conservatively before exhausting its message budget. A partial read is
never treated as complete history.

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
