# Native Controller lifecycle

The Controller starts with an empty in-memory work table and waiter map. It
projects branch/main relationships from the ledger, arms its periodic timer,
and receives messages. Startup does not contact a Looper or replay old work.

Each relation projection reads at most 1,000 historical message rows, counting
unrelated messages and responses too. Full-body reads are restricted to relation
messages. The whole read also has a shared 128-query, 4 MiB returned-text and
5-second budget. Reaching a limit before completing history aborts the read;
message/query/byte limits report `relation_history_limit_exceeded`. It never
commits a partial projection. Startup fails on that error; session queries keep
the previous projection and return the error. Relation checkpoints are not yet
implemented, so a history beyond this bound cannot be fully projected at startup.
The cap is not a promise that SQLite examines exactly 1,000 physical rows: the
log API batches and prefetches exchanges, but Controller pagination is bounded.

The relation projection retains session holders, fork boundaries, archive facts,
merge policy, merged/skipped boundaries and sync progress. Historical starts and
reports never populate or clear the current `Execution`, `Owner`, `Buffer`,
pending control or hold state. Session list/get refresh this projection without
reconstructing work or changing current execution.

Work receipts, submission/operation deduplication, queues, results and request
associations last only for the current Controller process. There is no work
State snapshot, migration, report recovery, reattachment or queue replay.
Existing legacy snapshots are ignored. Model/thinking selection remains a
separate persistent user setting.

After a restart, lookup of an old work returns `work_not_found`; an unmatched
report returns `stale_assignment` before any history lookup or control-slot
mutation. Callers submit new requests. The Controller neither settles old
requests nor stops or probes their Loopers; runtime owns request lifecycle.
Exiting start/input waiters do not cancel remote requests when Controller life
ends, and failed control delivery does not trigger `loop.inspect`.

New requests route commands directly. Work is submitted in `_context.session`;
`related_work_id` can reference a work still known to this process. Current
queues, capacity scheduling, steering, ownership transfer, hold/unhold,
replacement, explicit interrupt and report acceptance continue to operate in
memory. A terminal report must match a current assignment and have a preceding
accepted start in the ledger.

The timer continues normal queue/hold/waiter maintenance. Automatic idle archive
requires a completed assignment handled by this process and no remaining local
work. A historical session with no local execution is not evidence that its
independent Looper is idle, so the timer does not stop it. Explicit session
commands still route normally when requested.

`lifecycle_test.go` covers startup with legacy State, unmatched reports,
relationship reads, historical-holder isolation, normal idle archive, control
failure without probing, and process exit without remote cancellation. Existing
native tests cover new work, capacity, steering, report-before-ack, ownership,
control rollback and request completion.
