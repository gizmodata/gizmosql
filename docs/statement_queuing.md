# Statement Queuing

> **Enterprise feature.** Statement queuing requires a valid GizmoSQL Enterprise license that includes the `statement_queue` feature. If you configure any statement-queue flag (`--max-concurrent-statements`, `--max-queued-statements`, `--max-queue-wait`, or their `GIZMOSQL_*` env vars) **without** that license, the server **exits at startup with a clear error** rather than silently running with the concurrency cap unenforced — a silent fail-open would strip the very protection you asked for. To run an unlicensed server, leave the statement-queue flags at their defaults (concurrency `0` = queue disabled / unlimited). The runtime `SET GLOBAL gizmosql.max_concurrent_statements` path is likewise rejected without the license. *(Behavior changed in v1.27.1; prior versions silently fixed open.)*

Statement queuing caps how many SQL statements **execute concurrently** on a GizmoSQL server. Statements submitted beyond the limit **queue** (block) until a slot frees. To JDBC/ADBC/ODBC clients a queued statement is indistinguishable from a slow‑running one, so **no client changes are required**.

This protects a shared server from being overwhelmed by many simultaneous heavy queries: a single DuckDB instance has **one** shared memory budget (`memory_limit`) and **one** shared thread pool, with no per‑query resource governor. Bounding concurrency keeps the number of simultaneous memory‑hungry queries within that budget.

## Quick start

Allow at most 10 statements to execute at once; the 11th and beyond queue:

```bash
gizmosql_server --database-filename mydb.db \
    --license-key-file /path/to/license.jwt \
    --max-concurrent-statements 10
```

Or via environment variables (ideal for containers):

```bash
export GIZMOSQL_LICENSE_KEY_FILE=/path/to/license.jwt
export GIZMOSQL_MAX_CONCURRENT_STATEMENTS=10
gizmosql_server --database-filename mydb.db
```

## Configuration

All settings are available as CLI flags, environment variables, and `RunFlightSQLServer()` library parameters.

| Setting | CLI flag | Environment variable | Default | Scope |
|---------|----------|----------------------|---------|-------|
| Concurrency limit | `--max-concurrent-statements` | `GIZMOSQL_MAX_CONCURRENT_STATEMENTS` | `0` (unlimited / disabled) | server (runtime: `SET GLOBAL`, admin) |
| Waiter bound | `--max-queued-statements` | `GIZMOSQL_MAX_QUEUED_STATEMENTS` | `-1` → auto `8 ×` concurrency limit (`0` = unbounded) | server (runtime: `SET GLOBAL`, admin) |
| Queue wait | `--max-queue-wait` | `GIZMOSQL_MAX_QUEUE_WAIT` | `-1` → `300` seconds (`0` = wait forever) | server default + per‑session override |
| Admin bypass default | `--admin-bypass-queue-default` | `GIZMOSQL_ADMIN_BYPASS_QUEUE_DEFAULT` | `true` | server |
| Memory limit (partner knob) | `--memory-limit` | `GIZMOSQL_MEMORY_LIMIT` | unset → DuckDB default (80% RAM) | server |

- **Concurrency limit (`N`)** — execution slots. `0` disables the queue entirely (unlimited concurrency). Internal/metadata introspection queries (`GetTables`, `GetCatalogs`, …) are **exempt** and never consume a slot.
- **Admission order** — strictly **FIFO**. When a slot frees it goes to the statement that has waited longest, and a newly‑arriving statement never barges ahead of one already queued. This guarantees fairness and bounds worst‑case wait — no statement can be starved by a steady stream of later arrivals.
- **Waiter bound (`M`)** — how many statements may *wait* for a slot at once. Beyond it, a statement is **rejected** with a retriable Flight `UNAVAILABLE` error rather than queued. This protects the gRPC handler thread pool. The default auto‑sizes to `8 × N`.
- **Queue wait** — how long a statement may wait before being rejected with a retriable error. `0` waits indefinitely. Override per session with `SET SESSION gizmosql.max_queue_wait = <seconds>`.
- **Memory limit** — not part of the queue itself, but its natural partner: set `memory_limit`, then size `max_concurrent_statements` so that *N × (typical peak query memory)* fits the budget.

> **Why not just lower DuckDB `threads`?** DuckDB's `threads` is a **global** instance‑wide pool, not a per‑session limit — lowering it throttles *every* query (even a lone one) rather than isolating sessions. Statement queuing is the right lever for bounding concurrency; keep it independent of `threads`.

## Tuning a running server

Admins can change the limits live, without a restart:

```sql
SET GLOBAL gizmosql.max_concurrent_statements = 4;
SET GLOBAL gizmosql.max_queued_statements    = 32;
SET GLOBAL gizmosql.max_queue_wait           = 60;   -- seconds

SET GLOBAL gizmosql.max_concurrent_statements = 0;   -- disable the queue (unlimited)
```

`SET GLOBAL` requires the `admin` role. Changes are **in‑memory** and revert to the configured/env value on restart. A session may shorten its own wait with `SET SESSION gizmosql.max_queue_wait = <seconds>`.

## Admin bypass

So that operators are never locked out of a saturated server, **admin‑role sessions bypass the queue by default** (`--admin-bypass-queue-default`, default `true`). This means `KILL SESSION` and diagnostic queries always run, even when every slot is occupied.

Any session can toggle its own bypass (only admins may *enable* it):

```sql
SET SESSION gizmosql.bypass_queue = true;    -- skip the queue (admin only)
SET SESSION gizmosql.bypass_queue = false;   -- opt back into the queue
```

An admin running a heavy analytical query may want to `SET SESSION gizmosql.bypass_queue = false` so it participates in admission control rather than competing for the shared memory budget unbounded.

## Client behavior

A queued statement simply takes longer to return — the client sees normal latency, not a special status. Two things to keep in mind:

- **Query‑timeout budget.** The wait counts against client‑side RPC deadlines. Size client deadlines for `max_queue_wait` + expected execution time. The server's own `gizmosql.query_timeout` clock starts when the statement *begins executing*, not while it waits.
- **Rejections are retriable.** When the waiter bound or queue‑wait limit is exceeded, the server returns a Flight `UNAVAILABLE` error — clients should back off and retry.

## Observability

When session instrumentation is enabled, the queue is fully visible in the audit tables:

- `sql_executions.status` includes `queued` (waiting for a slot) and `executing`; `KILL SESSION`‑interrupted statements are recorded as `cancelled` — including a statement killed *while still queued*, which is cancelled immediately (it abandons the queue without waiting for a slot) rather than running to completion.
- `sql_executions.enqueue_time` and the computed `execution_details.queue_wait_ms` show when a statement entered the queue and how long it waited.

```sql
-- Currently queued or executing statements, with wait time
SELECT session_id, status, queue_wait_ms, sql_text
FROM   _gizmosql_instr.execution_details
WHERE  status IN ('queued', 'executing')
ORDER  BY enqueue_time;
```

See [Session Instrumentation](session_instrumentation.md) for the full schema.

### Inspecting settings

The `gizmosql_settings()` table function lists every `gizmosql.*` setting (the GizmoSQL analog of DuckDB's `duckdb_settings()`) with its current effective value, scope, default, and environment variable. It is composable like any relation:

```sql
SELECT name, value, scope, env_var
FROM   gizmosql_settings()
WHERE  name LIKE 'gizmosql.max%';
```

## See also

- [SET Commands](set_commands.md) — the `gizmosql.*` settings reference
- [Session Instrumentation](session_instrumentation.md) — the audit tables the queue is recorded in
- [Editions](editions.md) — Core vs. Enterprise
