# Statement Queuing — Design Doc

**Status:** Phases 1–3 implemented (`[Unreleased]`, branch `feature/statement-queuing`); Phase 4 (React admin console) planned
**Edition:** Enterprise (runtime license-gated)
**Author:** GizmoData
**Related:** `docs/session_instrumentation.md`, `docs/websocket-migration-plan.md`, `gizmosql-rbac-design.md`

> **Implementation status — Phases 1–3 done, on `feature/statement-queuing`:**
> - **Phase 1** — admission gate (`max_concurrent_statements`), internal-query
>   exemption, runtime license check that fails open.
> - **Phase 2** — waiter bound (`max_queued_statements`), `max_queue_wait` with
>   Flight `UNAVAILABLE` rejections, `admin_bypass_queue_default` +
>   `SET SESSION gizmosql.bypass_queue`.
> - **Phase 3** — `--memory-limit` (passthrough to DuckDB, allowlist-validated) +
>   gRPC server keepalive; the **settings registry** (one descriptor per
>   `gizmosql.*` setting; centralized license/scope/admin checks) with runtime
>   `SET GLOBAL` of the queue limits; the `gizmosql_settings()` table function
>   (bind-parameterized); and `queued`/`cancelled` instrumentation (`enqueue_time`,
>   `queue_wait_ms`, honest queued→executing transition, `KILL`→`cancelled`).
>
> Tested: `tests/integration/test_admission_controller.cpp` (9 unit tests) and
> `tests/integration/test_statement_queue.cpp` (enterprise E2E, incl. deterministic
> concurrency via `sleep_ms`). Side-improvement: `SET gizmosql.*` accepts unquoted
> boolean keywords (`= true`/`= false`). **Remaining (Phase 4 / follow-ups):** the
> React SQL-monitor admin console, a live `queue_position` view, and the separable
> `bypassed` audit column.

> File/line references in this document reflect the code at the time of writing
> and may drift; treat them as starting points, not guarantees. The architectural
> claims about DuckDB threading were verified empirically against DuckDB v1.5.3 and
> the v1.5.2 source GizmoSQL pins.

---

## 1. Summary

Add a server-side **admission-control queue** that caps the number of *concurrently
executing* SQL statements to a configurable limit `N`. Statements beyond `N` block
(queue) until a slot frees. To JDBC/ADBC/ODBC clients a queued statement is
indistinguishable from a slow-executing one — **no client changes are required**.

The feature is **Enterprise-gated via a runtime license check** and **fails open**:
without a valid license the cap is unenforced (today's unlimited-concurrency
behavior), so a license lapse can never take the data plane down.

This doc also specifies the admin escape hatches (internal-query exemption,
admin-default bypass, `SET SESSION gizmosql.bypass_queue`) and the instrumentation
changes that make a future SQL-Monitor admin console show an honest
`queued → executing → completed/cancelled` lifecycle.

---

## 2. Motivation

- **Customer request.** A customer wants to bound how many statements run at once
  (e.g. 10 "slots") and queue the rest, to protect a shared server from being
  overwhelmed by many concurrent heavy queries.
- **It's a correctness/stability lever, not just a nicety.** GizmoSQL runs **one
  shared DuckDB instance** with **one DuckDB connection per session**
  (`src/duckdb/duckdb_server.cpp:2103`, `:934`). DuckDB has **one global
  `TaskScheduler` thread pool and one global `memory_limit`** for the whole
  instance — both are `GLOBAL`-scope only and **cannot** be set per
  connection/session (verified: `SET SESSION threads` → *"option \"threads\"
  cannot be set locally"*). There is **no per-query or per-session resource
  governor** in DuckDB. So K concurrent memory-hungry queries all draw from the
  *same* memory budget; the failure mode is spill-to-disk (slow) or OOM
  cancellation. An admission limiter is the **only in-process lever** that
  protects that shared budget.
- **Differentiation.** Combined with the existing enterprise instrumentation and a
  planned admin console, this gives GizmoSQL an Oracle-Enterprise-Manager-style
  "SQL Monitor" story in a single self-contained binary — something Starlake's
  "Quack on Demand" does not have (its model is reject-at-capacity across a node
  pool, with no FIFO queue and no graceful drain).

---

## 3. Background — current execution model

Understanding *where* execution happens is the crux of the design.

- **Lazy execution lands on `DoGet`.** `GetFlightInfoStatement` only computes the
  Arrow schema (binds/plans, no rows). `DoGetStatement` returns a
  `RecordBatchStream`; the real query doesn't run until the **first `ReadNext()`
  pull** in `DuckDBStatementBatchReader::ReadNext`
  (`src/duckdb/duckdb_statement_batch_reader.cpp:71`), which calls
  `DuckDBStatement::Execute()`.
- **`Execute()` is the single chokepoint.** Reads reach it via
  `ReadNext → Execute()`; updates/ingest via `ExecuteUpdate() → Execute()`. Inside
  `Execute()` the query body runs on a one-shot `std::async` worker
  (`src/duckdb/duckdb_statement.cpp:1360`) purely so the gRPC handler thread can
  implement timeout-based interruption; the handler thread then **blocks** on the
  future (`future.wait()` / `future.wait_for()`).
- **Admin/control commands short-circuit before that.** `KILL SESSION` is detected
  and fully handled at statement *creation* (`duckdb_statement.cpp:818`,
  `IsKillSessionCommand` → `HandleKillSession`), returning a synthetic result with
  `is_gizmosql_admin_ = true`. In `Execute()`, the `if (is_gizmosql_admin_)` branch
  (`:1317`) returns synthetic results (`:1328`) and runs `SET gizmosql.*` via
  `HandleGizmoSQLSet()` (`:1330`) — **all before the `std::async` launch at
  `:1360`**.
- **No concurrency control exists today.** A repo-wide search for semaphore /
  max_concurrent / thread-pool / connection-limit constructs finds nothing in
  product code. Existing mutexes (`sessions_mutex_`, per-session
  `statements_mutex`) protect shared state; they do not throttle execution. The
  Flight gRPC server uses the default `grpc::ServerBuilder` threading — **no**
  configured server thread count, max-concurrent-streams, or keepalive (the only
  `builder_hook` tuning sets `GRPC_ARG_MAX_METADATA_SIZE`,
  `src/common/gizmosql_library.cpp:917`).

**Consequence for placement:** the admission gate belongs at `Execute()` just before
the `std::async` launch (`:1360`), released after `future.get()` / on the
timeout-interrupt branch. This is the one point every *real* query passes through,
and `KILL`/`SET` short-circuit before it for free.

---

## 4. Goals / Non-goals

### Goals
- Cap concurrently executing statements to a configurable `N`; queue the excess.
- Transparent to all Flight SQL clients (queued ≈ slow-executing). No driver changes.
- Decouple the concurrency knob from DuckDB `threads` (see §6).
- Never strand an admin: control commands and admin diagnostics always run.
- Never break client connectivity: internal/metadata introspection is never queued.
- Enterprise-gated, fail-open.
- Make the queue state observable via instrumentation (for the admin console).

### Non-goals (for v1)
- Per-session / per-tenant resource *isolation* (impossible in-process with one
  DuckDB instance — would require separate instances/processes; explicitly out of
  scope).
- Priority queues / weighted fair scheduling / resource consumer groups.
- `PollFlightInfo`-based async queued-status signaling (future; see §11).
- Per-statement SQL hint (`/*+ ... */`) parity with Oracle (future; see §11).

---

## 5. Design overview

```
                         ┌───────────────────────────────────────────────┐
  client query  ─DoGet─▶ │ DuckDBStatement::Execute()                     │
                         │                                                │
                         │  bypass?  (internal | admin-session | SET/KILL)│──► run now (no slot)
                         │     │ no                                       │
                         │     ▼                                          │
                         │  license enforced & N>0 ?                      │──► no ──► run now (no slot)
                         │     │ yes                                      │
                         │     ▼                                          │
                         │  try acquire 1 of N execution slots            │
                         │     ├─ slot free ───────────────► acquire ────┐│
                         │     └─ none free:                              ││
                         │          waiters < M ? ── no ──► REJECT (busy) ││
                         │              │ yes                             ││
                         │              ▼  block (status=queued)          ││
                         │          slot frees ──────────────► acquire ───┤│
                         │                                                ▼│
                         │     std::async launch (status=executing) ──► …  │
                         │     release slot on completion / timeout / kill │
                         └───────────────────────────────────────────────┘
```

### 5.1 The admission gate

A **CV-guarded counter** (`std::mutex` + `std::condition_variable` + `int active_` +
`int limit_`) of depth `N = max_concurrent_statements` — **not** a
`std::counting_semaphore`. The counter form is mandated for two reasons: (1) it is
introspectable for live `queue_position`, and (2) its `limit_` is **runtime-resizable**
(see §5.6), which `std::counting_semaphore` cannot do (its maximum is fixed at
construction). Acquired immediately before the `std::async` launch
(`duckdb_statement.cpp:~1360`), released after `future.get()` and on every exit path
(timeout-interrupt branch ~`:1480–1508`, exceptions). RAII guard strongly preferred
so the slot is released on any unwind.

### 5.2 Waiter bound (protects the handler-thread pool)

Because a blocked statement **holds its gRPC handler thread** while waiting (handler
blocks on the future / the semaphore), an unbounded wait queue can exhaust the Flight
gRPC handler pool — at which point even a no-slot-needed `KILL SESSION` can't get a
thread to run on. So we bound **how many statements may wait**:

- `max_queued_statements = M`. When all `N` slots are busy **and** `M` statements are
  already waiting, the next statement is **rejected** immediately with a
  `FlightStatusCode` mapping to a resource-exhausted / "server busy, try again"
  error (Trino's `maxQueued` model). This keeps handler-thread headroom for control
  commands and admin diagnostics.

### 5.3 What is exempt from the gate

Exemptions are checked **before** attempting to acquire a slot. A statement runs
immediately, consuming no slot and no waiter budget, if **any** of:

1. **Internal / metadata queries** (`is_internal == true`). These are the cheap
   schema-introspection queries that Flight SQL clients fire on connect
   (`GetTables`, `GetCatalogs`, `GetDbSchemas`, …). They are *not* the heavy
   analytical workload we are protecting `memory_limit` from, and queueing them
   would stall every client at connect time. **Never queue them.** (This also
   resolves the long-standing "should `DoGetTables` consume a slot?" question — no.)
2. **`SET` and `KILL SESSION` commands.** Already short-circuit before `:1360`; no
   change needed — documented here for completeness.
3. **Admin-bypass sessions.** See §5.4.

### 5.4 Admin bypass (chicken-and-egg fix)

The recovery workflow is: *connect → query `active_sessions`/`execution_details` to
find the clog → `KILL SESSION '<uuid>'`*. The middle step is a **real query** that
would otherwise hit the gate — so without a bypass, an admin cannot see what to kill
when the queue is jammed. Worse, admin GUI tools auto-fire metadata queries on
connect before the admin can type anything.

Resolution:

- **`admin_bypass_queue_default`** (server param, default **`true`**). At session
  creation, any session with `role == "admin"` gets its session-level
  `bypass_queue` seeded to this value. With the default, an admin's *entire* session
  — tool metadata queries, diagnostic `SELECT`s, and `KILL SESSION` — sails through
  with zero ceremony.
- **`SET SESSION gizmosql.bypass_queue = <bool>`** — per-session override:
  - **Admin** may set it `true` *or* `false`. Setting it `false` lets an admin opt
    *into* the queue for a genuinely heavy analytical query (so they don't blow the
    shared memory budget while doing real work rather than firefighting).
  - **Non-admin** sessions may **not** set it `true` (rejected, mirroring the role
    gate used by `query_log_level` GLOBAL and `KILL SESSION`). Whether a non-admin
    may set it `false` is moot (they're already in the queue).

> **Bypass semantics = true bypass, not queue-jump.** A bypassed statement skips the
> semaphore entirely — it does not consume a slot and does not wait. (You cannot wait
> for a slot when all slots are wedged.) The trade-off, which is documented for
> operators: a bypassed heavy query is *not* protected by the concurrency cap and can
> add to memory pressure. That is the right default for trusted admins doing recovery.

### 5.5 Why the gate is decoupled from DuckDB `threads`

An earlier idea was an "easy" `number_of_concurrent_sessions` knob that also set
`threads = cores / N`. **We are deliberately not doing this.** Because `threads` is
**global, not per-session**:

- `threads = cores/N` does **not** isolate sessions — it permanently shrinks the one
  shared pool that *every* query draws from. A lone session running at 3am could no
  longer burst to all cores; it would be pinned to `1/N` of the machine while the
  rest sits idle. That is a global throttle, strictly worse than the feared
  "single-session cap."
- It still wouldn't give fairness between concurrent sessions (the scheduler
  interleaves everyone's morsels into one task queue regardless of the `threads`
  value).

So the concurrency knob (`max_concurrent_statements`) is **independent** of
`threads`. `threads` stays at DuckDB's default (self-tunes to core count). Operators
who want to cap parallelism can still `SET threads` via `--init-sql-commands`
(existing path) as a separate, advanced, machine-level decision.

**The real partner knob is `memory_limit`, not `threads`.** Concurrency is bounded to
keep simultaneous memory-hungry queries within the single shared budget. We therefore
also promote `memory_limit` to a first-class server flag (today it's only reachable
via init SQL). Operator framing:

> *Set `memory_limit`, then set `max_concurrent_statements` so that
> N × (typical peak query memory) fits the budget.*

### 5.6 Runtime reconfiguration via `SET` / `SET GLOBAL`

All queue knobs are adjustable on a **running instance** — no restart required —
reusing the existing `HandleGizmoSQLSet()` machinery (`duckdb_statement.cpp:1069`),
which already parses the scope (`SET SESSION` vs `SET GLOBAL`) and already has the
admin-gated server-side setter pattern (`server->SetQueryTimeout` /
`SetQueryLogLevel`). We add new `else if` branches. **Scope differs per parameter,
because it follows what the knob actually governs:**

| Parameter | `SET SESSION` | `SET GLOBAL` | Notes |
|---|---|---|---|
| `gizmosql.max_concurrent_statements` | ✗ (rejected) | ✓ admin-only | Governs the single shared semaphore — no per-session meaning. Mutates the live `AdmissionController`. |
| `gizmosql.max_queued_statements` | ✗ (rejected) | ✓ admin-only | Same — shared waiter bound. |
| `gizmosql.max_queue_wait` | ✓ | ✓ admin-only (server default) | Per-statement patience; session value falls back to server default — exactly like `query_timeout`. |
| `gizmosql.bypass_queue` | ✓ (admin may set `true`/`false`; non-admin may not set `true`) | — | Per-session. |

**Live-resize semantics (the `AdmissionController`'s `SetLimit(n)` / `SetMaxQueued(m)`):**
because the gate is a CV-guarded counter (§5.1), resizing is just mutating `limit_`
under the lock + `notify_all()`:

- **Raise `N`:** waiting statements wake and proceed up to the new limit.
- **Lower `N` below `active_`:** running queries are **not** preempted; new
  acquisitions wait until `active_` drains below the new limit. Graceful.
- **`N = 0` at runtime:** disables the cap live (unlimited) — an operational
  kill-switch to instantly relieve a jam without a restart.
- **Lower `M`:** existing waiters are grandfathered; only *new* arrivals beyond the
  new bound are rejected.

**Persistence:** runtime `SET GLOBAL` changes are **in-memory only** and revert to the
env-var/CLI value on restart (same semantics as DuckDB's own `SET GLOBAL` and today's
`gizmosql.query_log_level` GLOBAL). For a permanent change, set the env var / flag.

**Licensing:** the explicit `SET` path (session *or* global) **requires the Enterprise
license** and is rejected otherwise with `GetLicenseRequiredError(...)`, consistent
with the `session_tag`/`query_tag` branches. (This is distinct from the data-plane
gate, which fails *open* — see §7. An admin explicitly tuning the cap on an unlicensed
instance gets a clear error; queries are never blocked by licensing.)

### 5.7 Settings discoverability — `gizmosql_settings()`

As the GizmoSQL-specific parameter surface grows, operators need a single place to see
"what settings exist, what are their current values, and how do I set them" — the
analog of DuckDB's `duckdb_settings()`. We expose a **`gizmosql_settings()` table
function** returning one row per `gizmosql.*` setting:

| Column | Meaning |
|---|---|
| `name` | e.g. `gizmosql.max_concurrent_statements` |
| `value` | current **effective** value for this session (session override if set, else the global/server value) |
| `session_value` | the session-level override, or `NULL` if unset |
| `global_value` | the server-wide value |
| `scope` | `GLOBAL`, `SESSION`, or `SESSION_OR_GLOBAL` — where it may be set |
| `input_type` | `INTEGER` / `BOOLEAN` / `VARCHAR` |
| `default_value` | the built-in default |
| `env_var` | the env-var name (GizmoSQL-specific, container-friendly) |
| `enterprise` | `BOOLEAN` — whether the setting is Enterprise-gated |
| `description` | human-readable help |

Settings live in C++ server/session state (not in DuckDB's own settings store), so the
function is **session-aware**. Crucially, GizmoSQL's existing `GIZMOSQL_*` "functions"
are **not** registered DuckDB UDFs — they are implemented by **textual rewriting** of
the SQL string before execution (`duckdb_statement.cpp:384-435` scans for, e.g.,
`GIZMOSQL_CURRENT_SESSION()` and substitutes the literal value + an `AS "…"` alias).
`gizmosql_settings()` follows the **same mechanism**, just rewriting into a table
instead of a scalar:

- **(Recommended) Inline-`VALUES` rewrite.** Intercept `gizmosql_settings()` in the
  FROM clause and substitute a parenthesized, session-materialized table:
  ```text
  (VALUES
     ('gizmosql.max_concurrent_statements','10',NULL,'10','GLOBAL','INTEGER','0',
      'GIZMOSQL_MAX_CONCURRENT_STATEMENTS', true, 'Max concurrent executing statements'),
     ('gizmosql.query_timeout','30','30','0','SESSION_OR_GLOBAL','INTEGER','0',
      'GIZMOSQL_QUERY_TIMEOUT', false, 'Per-statement timeout in seconds'),
     ...
  ) AS gizmosql_settings(name, value, session_value, global_value, scope,
                         input_type, default_value, env_var, enterprise, description)
  ```
  Because the result is real SQL, it is **composable** —
  `SELECT * FROM gizmosql_settings() WHERE scope='GLOBAL'` and JOINs work, matching
  `duckdb_settings()` ergonomics — and it is built from the calling session + server
  state at rewrite time, so it's session-aware. Value columns are `VARCHAR`
  (stringified), like `duckdb_settings().value`; `enterprise` is `BOOLEAN`.
- **(Fallback) Synthetic-result interception** like `KILL SESSION` — simpler, but not
  composable in SQL. Only if the rewrite proves awkward.

> **Central settings registry (do this now — you're adding parameters).** Rather than
> hand-maintaining the `HandleGizmoSQLSet()` if/else chain, the `gizmosql_settings()`
> rows, the env-var fallbacks, and the docs separately (and letting them drift),
> introduce a single **settings registry** in the library: one entry per setting
> describing `{name, input_type, scope, env_var, default, enterprise, description,
> get_session(session), get_global(server), set(session/server, value, scope)}`. Then:
> `HandleGizmoSQLSet()` dispatches through the registry (no growing if/else),
> `gizmosql_settings()` enumerates it, and adding a parameter is one registry entry +
> its plumbing. This is the scalable answer to "we are getting more parameters," and it
> gives the admin console a single source of truth for a "Settings" pane.

---

## 6. Client-visible behavior

- **Transparent by default.** A queued statement blocks inside the client's
  `DoGet` call exactly like a slow query. No JDBC/ADBC/ODBC changes.
- **Query-timeout semantics.** The execution timeout (`query_timeout`) clock starts
  at **slot acquisition** (execution start), *not* at enqueue — queue wait does not
  consume the execution-timeout budget. A separate optional **`max_queue_wait`**
  (seconds; `0` = wait indefinitely) bounds how long a statement may sit queued
  before it is rejected, so clients don't hang forever behind a wedged queue.
  - Caveat to document: client-side RPC deadlines (driver defaults) and load-balancer
    idle timeouts still apply to the *whole* call (queue wait + execution). A deep
    queue can surface as `DEADLINE_EXCEEDED` on the client. Size client deadlines for
    `max_queue_wait + max_execution_time`.
- **gRPC keepalive (new, required for long waits).** Configure server keepalive in
  the `builder_hook` (`gizmosql_library.cpp:917`) so a long-queued, silent stream is
  not killed by proxy/LB idle timeouts (AWS NLB ~350s, Azure ~4min). Set
  `PERMIT_KEEPALIVE_TIME` / `PERMIT_KEEPALIVE_WITHOUT_CALLS` so well-behaved clients
  that ping aren't GOAWAY'd. Document recommended client keepalive settings.
- **Rejection.** When the waiter bound `M` is exceeded (or `max_queue_wait` elapses),
  the client receives a clear retriable error (resource-exhausted / "server busy").

---

## 7. Enterprise gating

- New feature constant `kFeatureStatementQueue = "statement_queue"` in
  `src/enterprise/enterprise_features.h` (alongside `kFeatureKillSession`,
  `kFeatureInstrumentation`, …), plus a display-name entry in
  `src/enterprise/license_mgr/license_manager.cpp`.
- The gate consults `EnterpriseFeatures::Instance().IsFeatureAvailable(kFeatureStatementQueue)`
  at the admission point.
- **Fail open.** If the feature is **not** licensed, the gate is a no-op
  (unlimited concurrency = today's behavior). No statement is ever blocked or
  rejected due to licensing. This differs from `KILL SESSION` (which *rejects*
  without a license) on purpose: queuing sits on the hot path of every query, so a
  license problem must degrade to "no limit," never to "no service."
- Behavior with a license but `max_concurrent_statements == 0`: also unlimited
  (`0` = disabled sentinel). The feature is therefore opt-in even when licensed.

---

## 8. Instrumentation changes

The enterprise instrumentation schema (`kSchemaSQLTemplate`,
`src/enterprise/instrumentation/instrumentation_manager.cpp:47`) already
half-anticipates this: `ExecutionInstrumentation::status_` defaults to `"executing"`
(`instrumentation_records.h:146`) and a `SetCancelled()` setter exists
(`instrumentation_records.cpp:239`) but is **never called** today. We make the queue
state honest:

**Schema (additive migration — `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, mirroring
the tag-column migration):**

- `sql_executions.enqueue_time TIMESTAMPTZ` — when the statement entered the queue.
- Extend the `status` value domain to include `'queued'` (and start using
  `'cancelled'`). Status is a `VARCHAR` with comment-only constraints, so this is a
  documentation/codepath change, not a hard schema change.
- `sql_executions.bypassed BOOLEAN` — true if the statement skipped the gate
  (internal, admin-bypass, etc.). Lets the SQL Monitor visibly flag queries that ran
  outside the queue (audit).

**Record lifecycle (`instrumentation_records.cpp`):**

- Today the INSERT hardcodes `'executing'` (`:206`,
  `VALUES ($1, $2, $3, now(), 'executing', 0)`). Split this:
  - On **enqueue**: INSERT with `status='queued', enqueue_time=now()`.
  - Add **`SetRunning()`**: flip to `status='executing'`, set
    `execution_start_time=now()` at the *running* transition (capture
    `start_timestamp_` here, not at construction).
- Wire the existing **`SetCancelled()`** into the `KILL SESSION` path
  (`kill_session_handler.cpp`) and the timeout-interrupt branch, so a killed/aborted
  query records `'cancelled'` instead of surfacing as `'error'`. (Near-free win,
  independent of the queue.)
- Exempt/bypassed statements: INSERT directly at `status='executing'` (or with
  `bypassed=true`), skipping the `'queued'` phase.

**Views (`kSchemaSQLTemplate`):**

- `execution_details`: add `queue_wait_ms` = `execution_start_time - enqueue_time`.
- New `queued_statements` view (or extend `execution_details`) computing **live
  queue position**: `ROW_NUMBER() OVER (ORDER BY enqueue_time) WHERE
  status='queued'`. Compute it live rather than storing a position-at-enqueue value,
  which goes stale the instant anything ahead dequeues.
- `session_stats`: add a `queued_executions` count (mirror the existing
  `SUM(CASE … )` pattern for timeout/cancelled).

The stale-instance recovery sweep that marks orphaned `'executing'` rows as `'error'`
(`instrumentation_manager.cpp:585`) should also sweep orphaned `'queued'` rows.

---

## 9. Configuration parameters

All follow the existing plumbing conventions (see §10). Booleans use the
`std::optional<bool>` + `resolve_bool_env` pattern; integers use the
`max_metadata_size` int + inline env-fallback pattern.

| Parameter | CLI flag | Env var | Default | Scope | Meaning |
|---|---|---|---|---|---|
| Max concurrent statements | `--max-concurrent-statements` | `GIZMOSQL_MAX_CONCURRENT_STATEMENTS` | `0` (disabled/unlimited) | server; GLOBAL admin override only | Execution slots `N`. |
| Max queued statements | `--max-queued-statements` | `GIZMOSQL_MAX_QUEUED_STATEMENTS` | `8 × N` (finite; `0` = unbounded) | server; GLOBAL admin override only | Waiter bound `M`; reject beyond. |
| Max queue wait (s) | `--max-queue-wait` | `GIZMOSQL_MAX_QUEUE_WAIT` | `300` (`0` = wait indefinitely) | server; session-overridable | Reject a statement queued longer than this. |
| Memory limit | `--memory-limit` | `GIZMOSQL_MEMORY_LIMIT` | unset (DuckDB default) | server (global, applied via DuckDB) | Partner knob; promoted to first-class flag. |
| Admin bypass default | `--admin-bypass-queue-default` | `GIZMOSQL_ADMIN_BYPASS_QUEUE_DEFAULT` | `true` | server | Seeds `bypass_queue` for admin sessions. |
| Per-session bypass | — | — | (role-derived) | `SET SESSION gizmosql.bypass_queue = <bool>` | Admin-settable `true`/`false`; non-admin cannot set `true`. |

> `max_concurrent_statements` and `max_queued_statements` must **not** be freely
> session-settable by non-admins — a non-admin raising their own cap is a
> privilege-escalation footgun. Make runtime changes admin/GLOBAL only (mirror the
> `role != "admin"` gate on `SET GLOBAL gizmosql.query_log_level`).

> **Container-first / env-var parity (required).** GizmoSQL is most often run in
> containers (Docker/Kubernetes) where CLI flags are awkward but env vars are
> idiomatic, so **every server-level setting above MUST have an env-var
> equivalent** — there are no CLI-only server settings. Per the project design
> principle (CLAUDE.md), the env-var fallback logic lives **in the library**
> (`RunFlightSQLServer()` in `src/common/gizmosql_library.cpp`), *not* in the CLI
> executable — `pick(cli_val, "ENV_NAME", default)` for strings, the
> `max_metadata_size` `<=0` inline pattern for ints, and
> `resolve_bool_env(val, "ENV_NAME")` (with `std::optional<bool>` defaulting to
> `std::nullopt`) for booleans, so explicit CLI `true`/`false` always wins over the
> env var. This guarantees C-API callers of `RunFlightSQLServer()` get identical
> env-var behavior to the CLI. The **only** non-env setting is the per-session
> `SET SESSION gizmosql.bypass_queue`, which is a runtime SQL override issued
> per-connection (not a startup setting); its container-level counterpart is
> `GIZMOSQL_ADMIN_BYPASS_QUEUE_DEFAULT`. At implementation time also add each new
> env var to the header comment tables in `scripts/start_gizmosql.sh` and
> `scripts/start_gizmosql_slim.sh` (per CLAUDE.md).

---

## 10. Implementation plan

> **Design tenet — library-first (do everything in the library, not the CLI).**
> Every part of this feature — the parameter *defaults*, the env-var *fallback*, the
> `AdmissionController`, the bypass logic, the role gating, and the instrumentation —
> lives in the **library** (`src/common/gizmosql_library.cpp` + `src/duckdb/` +
> `src/enterprise/`), reachable through `RunFlightSQLServer()` /
> `CreateFlightSQLServer()`. The CLI executable (`src/gizmosql_server.cpp`) is a
> **thin pass-through**: it only parses Boost.ProgramOptions flags and forwards them
> — **no `std::getenv`, no defaulting, no business logic there.** This is mandatory
> (CLAUDE.md): it guarantees that **embedders / C-API callers** who launch GizmoSQL
> via the library (Docker entrypoints calling the library, the iOS edition, language
> bindings, tests) get **identical** behavior — same env vars, same defaults, same
> queue semantics — as someone running the CLI. If a behavior only works when started
> via the CLI, it's in the wrong layer.

### 10.1 Parameter plumbing (per CLAUDE.md's recipe, for each new param)

1. `src/common/include/gizmosql_library.h` — add to `RunFlightSQLServer()` signature
   + docstring (booleans as `std::optional<bool> = std::nullopt`; ints with a default).
2. `src/gizmosql_server.cpp` — add the Boost.ProgramOptions flag(s) only (no
   `std::getenv`). For booleans, pass `std::nullopt` when `vm["…"].defaulted()`.
3. `src/common/gizmosql_library.cpp` — env-var fallback in `RunFlightSQLServer()`
   (`resolve_bool_env` for bools; copy the `max_metadata_size` `<=0` inline pattern,
   ~`:1479`, for ints), then thread through `CreateFlightSQLServer()` and into the
   builder / `DuckDBFlightSqlServer::Create`. `memory_limit` is applied to DuckDB via
   `DBConfig` or an init `SET` at instance creation (`duckdb_server.cpp:2087`).
4. `tests/integration/test_server_fixture.h` — add to `TestServerConfig`.

### 10.2 The gate

- New `AdmissionController` (owned by `DuckDBFlightSqlServer`, accessible from
  `DuckDBStatement` via the existing `GetServer()` weak-ptr path): holds the counter,
  the CV/semaphore, the waiter count, and the license/`N`/`M` config. Provides an RAII
  `AdmissionSlot acquire(session, statement)` that:
  - returns an *exempt* (no-op) slot if licensed-off, `N==0`, `is_internal`, or
    `session.bypass_queue` is true;
  - else checks the waiter bound (`reject` if exceeded), records `enqueue_time` +
    `status='queued'`, blocks until a slot frees or `max_queue_wait` elapses, then
    flips instrumentation to `SetRunning()`.
- Wire `acquire()` into `DuckDBStatement::Execute()` just before the `std::async`
  launch (`:1360`); the RAII slot releases on scope exit (covers `future.get()`,
  timeout, exceptions).

### 10.3 Session field + SET handler

- `src/common/include/detail/session_context.h` — add
  `std::optional<bool> bypass_queue = std::nullopt;` to `ClientSession` (nullopt =
  inherit role-derived default). Seed at session creation in `GetClientSession()`
  from `admin_bypass_queue_default` when `role == "admin"`.
- `src/duckdb/duckdb_statement.cpp` `HandleGizmoSQLSet()` (`:1069`/`:1099`) — add an
  `else if (name == "gizmosql.bypass_queue")` branch: `parse_bool`, enforce the role
  gate (non-admin cannot set `true`), store on the session. Enterprise-license-check
  like the `session_tag`/`query_tag` branches.

### 10.4 Instrumentation — see §8.

### 10.5 gRPC keepalive — add args in the `builder_hook` (`gizmosql_library.cpp:917`).

### 10.6 Phasing

1. **Phase 1 (core mechanism):** `AdmissionController` + `max_concurrent_statements`
   + internal-query exemption + RAII slot + license gate (fail-open). Memory_limit
   flag. Tests.
2. **Phase 2 (safety + admin):** waiter bound `M` + `max_queue_wait` + reject path;
   `admin_bypass_queue_default` + `SET … bypass_queue`; gRPC keepalive.
3. **Phase 3 (observability):** `'queued'` state, `enqueue_time`, `queue_position`
   view, `queue_wait_ms`, `bypassed`, wire `SetCancelled()`/`SetRunning()`.
4. **Phase 4 (console):** the SQL-Monitor admin console reads these over Flight SQL
   (separate doc / RBAC track).

---

## 11. Future work

- **`PollFlightInfo` async signaling.** Replace the blocking `GetFlightInfo`/`DoGet`
  with `PollFlightInfo` for clients that support it: respond immediately on first
  call (enqueue + ack), report `progress = 0.0` while queued, advance once running,
  use `PollInfo.expiration` as a built-in queue timeout, and let `CancelFlightInfo`
  cancel a queued statement. Each RPC is short, so client deadlines and idle-LB drops
  evaporate. Caveat: `PollFlightInfo` is newer and JDBC/ADBC driver support is
  uneven — ship the blocking shape first, offer poll as an upgrade.
- **Per-statement hint** (`/*+ gizmosql_bypass_queue */`) for Oracle
  `NO_STATEMENT_QUEUING` parity — finer-grained than `SET SESSION`, but requires
  SQL-text scanning; optional sugar.
- **RBAC integration.** Once the `gizmosql-rbac-design.md` users/roles/grants land,
  gate `bypass_queue` and the runtime concurrency knobs on a capability/privilege
  rather than the coarse `role == "admin"` string.
- **Admin SQL-Monitor console.** Build as "just another Flight SQL client" that
  authenticates as `admin` and runs SQL against the (admin-only) instrumentation
  catalog — keeping the single-self-contained-binary differentiation.

---

## 12. Decisions

**Resolved (2026-06, owner-approved):**

1. **`admin_bypass_queue_default` = `true`.** Admin recovery "just works" —
   diagnostics and `KILL SESSION` are never stranded behind a jam. Admins can still
   `SET SESSION gizmosql.bypass_queue = false` to opt *into* the queue for heavy
   analytical work.
2. **`max_queue_wait` default = `300` seconds.** Wedged clients fail predictably
   instead of hanging forever; `0` remains available to mean "wait indefinitely."
   Sized to sit comfortably below common client/LB deadlines.
3. **`max_queued_statements` default = finite, scaled to the cap.** Concrete default
   `8 × max_concurrent_statements`, so the waiter bound grows with the cap and never
   lets blocked statements starve the gRPC handler pool; `0` = unbounded (opt-in).
   (When `N = 0` the queue is disabled, so `M` is moot.)

**Still open (have leanings, not blockers):**

4. **Reject error code** — which `FlightStatusCode` best signals "busy, retriable" to
   JDBC/ADBC/ODBC drivers? Leaning `Unavailable` (maps to gRPC `UNAVAILABLE`, which
   drivers generally treat as retriable).
5. **Bypassed-query visibility** — surface bypassed admin queries separately in
   `session_stats` via the `bypassed` column? Leaning **yes** (cheap, good for audit).

---

## 13. Testing

- Licensed vs unlicensed: cap enforced/queues vs cap ignored (use
  `license_keys/license_GizmoData_LLC.txt`).
- `N` slots: `N+k` concurrent statements → exactly `N` execute, `k` queue, all
  eventually complete; FIFO-ish ordering.
- Internal-query exemption: metadata RPCs never block even when slots are full.
- Admin bypass: with slots full, an admin session's `SELECT` + `KILL SESSION` run
  immediately; non-admin `SET bypass_queue=true` is rejected.
- Waiter bound: beyond `M`, statements are rejected with the chosen code.
- `max_queue_wait`: a statement queued past the limit is rejected.
- Instrumentation: a queued statement shows `queued → executing → completed`;
  `enqueue_time`/`queue_wait_ms` populated; killed query records `cancelled`.
- Follow the CRTP fixture pattern in `tests/integration/test_server_fixture.h`;
  register in `tests/CMakeLists.txt`; unique ports.

> Per CLAUDE.md: at implementation time, update `CHANGELOG.md` (`## [Unreleased]`),
> the CLI help text in `src/gizmosql_server.cpp`, the env-var tables in
> `scripts/start_gizmosql*.sh`, and the relevant `docs/` pages.

---

## Appendix A — Settings registry (C++ sketch)

Lives in the **library** layer (e.g. `src/common/settings_registry.{h,cpp}`), per the
§10 library-first tenet, so embedders/C-API callers get it too. It is the single
source of truth consumed by three call sites: `HandleGizmoSQLSet()` (runtime `SET`),
`gizmosql_settings()` (introspection), and the startup env-var fallback. Values are
carried as canonical **strings** (matching the `SET` value, env vars, and
`duckdb_settings().value`); the per-setting hooks own the typed conversion into the
settings' natural homes (`ClientSession` fields, server globals, the
`AdmissionController`). The registry holds **no storage of its own** — it's a façade.

```cpp
enum class SettingScope { kSessionOnly, kGlobalOnly, kSessionOrGlobal };
enum class SettingType  { kInteger, kBoolean, kVarchar };

// One entry per gizmosql.* setting. Hooks are null where they don't apply
// (e.g. get_session/set_session are null for a kGlobalOnly setting).
struct SettingDescriptor {
  std::string  name;             // "gizmosql.max_concurrent_statements"
  SettingType  input_type;
  SettingScope scope;
  std::string  env_var;          // "GIZMOSQL_…"  ("" if none)
  std::string  default_value;    // canonical string form
  bool         enterprise = false;
  const char*  enterprise_feature = nullptr;  // e.g. enterprise::kFeatureStatementQueue
  std::string  description;

  // Read current values as canonical strings (for gizmosql_settings()).
  std::function<std::optional<std::string>(const ClientSession&)>      get_session;  // nullopt = unset
  std::function<std::string(const ddb::DuckDBFlightSqlServer&)>         get_global;

  // Write hooks: parse + validate + store. The registry calls these ONLY after it has
  // verified scope is permitted, the license is present, and (for GLOBAL) the caller
  // is admin — so each hook stays tiny and free of cross-cutting concerns.
  std::function<arrow::Status(ClientSession&, const std::string&)>                            set_session;
  std::function<arrow::Status(ddb::DuckDBFlightSqlServer&, ClientSession&, const std::string&)> set_global;
};

class SettingsRegistry {
 public:
  static const SettingsRegistry& Instance();                  // built once, immutable
  const SettingDescriptor* Find(std::string_view name) const; // nullptr if unknown
  const std::vector<SettingDescriptor>& All() const { return settings_; }

  // The single dispatch used by HandleGizmoSQLSet(). Centralizes every cross-cutting
  // check, then delegates to the descriptor's set_session/set_global.
  arrow::Status Apply(ClientSession& session, ddb::DuckDBFlightSqlServer* server,
                      std::string_view name, duckdb::SetScope requested,
                      const std::string& value) const;
 private:
  SettingsRegistry();                                         // registers all settings
  std::vector<SettingDescriptor> settings_;
  std::unordered_map<std::string, size_t> by_name_;           // name -> index
};
```

### Registration (in `SettingsRegistry::SettingsRegistry()`)

```cpp
// Session-or-global, non-enterprise — existing setting, retrofitted.
settings_.push_back({
  .name = "gizmosql.query_timeout", .input_type = SettingType::kInteger,
  .scope = SettingScope::kSessionOrGlobal, .env_var = "GIZMOSQL_QUERY_TIMEOUT",
  .default_value = "0", .description = "Per-statement timeout in seconds (0 = none).",
  .get_session = [](const ClientSession& s) -> std::optional<std::string> {
      return s.query_timeout ? std::optional(std::to_string(*s.query_timeout)) : std::nullopt; },
  .get_global  = [](const ddb::DuckDBFlightSqlServer& srv) { return std::to_string(srv.GetQueryTimeout()); },
  .set_session = [](ClientSession& s, const std::string& v) -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(int n, ParseNonNegInt(v, "query_timeout")); s.query_timeout = n;
      return arrow::Status::OK(); },
  .set_global  = [](ddb::DuckDBFlightSqlServer& srv, ClientSession& s, const std::string& v) -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(int n, ParseNonNegInt(v, "query_timeout"));
      return srv.SetQueryTimeout(s, n); },   // server setter already admin-gates
});

// Global-only, enterprise — the new queue knob; SET GLOBAL live-resizes the controller.
settings_.push_back({
  .name = "gizmosql.max_concurrent_statements", .input_type = SettingType::kInteger,
  .scope = SettingScope::kGlobalOnly, .env_var = "GIZMOSQL_MAX_CONCURRENT_STATEMENTS",
  .default_value = "0", .enterprise = true,
  .enterprise_feature = enterprise::kFeatureStatementQueue,
  .description = "Max concurrently executing statements (0 = unlimited).",
  .get_session = nullptr, .set_session = nullptr,            // not per-session
  .get_global  = [](const ddb::DuckDBFlightSqlServer& srv) {
      return std::to_string(srv.GetAdmissionController().Limit()); },
  .set_global  = [](ddb::DuckDBFlightSqlServer& srv, ClientSession&, const std::string& v) -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(int n, ParseNonNegInt(v, "max_concurrent_statements"));
      srv.GetAdmissionController().SetLimit(n);               // live resize (§5.6)
      return arrow::Status::OK(); },
});

// Session bool — admin may set true/false; non-admin cannot set true (enforced in set_session).
settings_.push_back({
  .name = "gizmosql.bypass_queue", .input_type = SettingType::kBoolean,
  .scope = SettingScope::kSessionOnly, .env_var = "",        // per-session SQL only; see admin_bypass_queue_default
  .default_value = "false", .enterprise = true,
  .enterprise_feature = enterprise::kFeatureStatementQueue,
  .description = "Skip the statement queue for this session (admin only).",
  .get_session = [](const ClientSession& s) -> std::optional<std::string> {
      return s.bypass_queue ? std::optional(*s.bypass_queue ? "true" : "false") : std::nullopt; },
  .get_global  = nullptr,
  .set_session = [](ClientSession& s, const std::string& v) -> arrow::Status {
      ARROW_ASSIGN_OR_RAISE(bool b, ParseBool(v, "bypass_queue"));
      if (b && s.role != "admin")
        return arrow::Status::Invalid("Only admin users may set gizmosql.bypass_queue = true");
      s.bypass_queue = b; return arrow::Status::OK(); },
  .set_global  = nullptr,
});
```

### Consumer 1 — `HandleGizmoSQLSet()` collapses to one call

The growing `if (name == …) else if …` chain (`duckdb_statement.cpp:1099+`) becomes:

```cpp
// after parsing name/scope/value out of the SET statement (unchanged):
return SettingsRegistry::Instance().Apply(*session, GetServer(*session).get(), name, scope, val);
```

with `Apply()` performing the cross-cutting checks once:

```cpp
arrow::Status SettingsRegistry::Apply(ClientSession& session, ddb::DuckDBFlightSqlServer* server,
                                      std::string_view name, duckdb::SetScope requested,
                                      const std::string& value) const {
  const SettingDescriptor* d = Find(name);
  if (!d) return arrow::Status::Invalid("Unknown GizmoSQL configuration parameter: ", name);

  // Enterprise SET requires the license (data plane still fails open — see §7).
#ifdef GIZMOSQL_ENTERPRISE
  if (d->enterprise && !enterprise::EnterpriseFeatures::Instance().IsFeatureAvailable(d->enterprise_feature))
    return arrow::Status::Invalid(enterprise::EnterpriseFeatures::GetLicenseRequiredError(d->name));
#else
  if (d->enterprise) return arrow::Status::Invalid(d->name + " is a licensed enterprise feature. …");
#endif

  // Resolve effective scope: GLOBAL keyword -> global; SESSION/LOCAL/AUTOMATIC ->
  // session, except a kGlobalOnly setting treats a bare SET as GLOBAL.
  const bool to_global = (requested == duckdb::SetScope::GLOBAL) ||
                         (requested != duckdb::SetScope::GLOBAL && d->scope == SettingScope::kGlobalOnly &&
                          requested == duckdb::SetScope::AUTOMATIC);

  if (to_global && d->scope == SettingScope::kSessionOnly)
    return arrow::Status::Invalid(d->name + " can only be set at SESSION scope");
  if (!to_global && d->scope == SettingScope::kGlobalOnly)
    return arrow::Status::Invalid(d->name + " can only be set with SET GLOBAL");

  if (to_global) {
    if (session.role != "admin") return arrow::Status::Invalid("Only admin users can SET GLOBAL " + d->name);
    if (!server) return arrow::Status::Invalid("No server context for SET GLOBAL " + d->name);
    return d->set_global(*server, session, value);
  }
  return d->set_session(session, value);
}
```

### Consumer 2 — `gizmosql_settings()` enumerates the registry

The textual rewrite (§5.7) builds its inline `VALUES` straight from `All()`:

```cpp
std::string BuildGizmoSqlSettingsValues(const ClientSession& s, const ddb::DuckDBFlightSqlServer& srv) {
  std::string rows;
  for (const auto& d : SettingsRegistry::Instance().All()) {
    std::optional<std::string> sess = d.get_session ? d.get_session(s) : std::nullopt;
    std::string global = d.get_global ? d.get_global(srv) : std::string{};
    std::string effective = sess.value_or(global);
    // Append one tuple, SQL-quoting strings and emitting NULL where sess is unset:
    //   (name, value, session_value, global_value, scope, input_type,
    //    default_value, env_var, enterprise, description)
    rows += MakeRow(d, effective, sess, global);   // helper handles quoting/NULL/commas
  }
  return "(VALUES " + rows + ") AS gizmosql_settings(name, value, session_value, global_value, "
         "scope, input_type, default_value, env_var, enterprise, description)";
}
```

### Consumer 3 — startup env-var fallback

At startup the server object doesn't exist yet, so the typed values still flow through
the `RunFlightSQLServer()` signature (library-first tenet). The registry remains the
**source of truth for the `env_var` name + `default_value`**, so the startup resolution
and `gizmosql_settings()` can never disagree on names/defaults. Two acceptable shapes:

- **Pragmatic:** keep the existing `pick(...)` / `resolve_bool_env(...)` /
  `max_metadata_size`-style calls in `RunFlightSQLServer()`, but have them read the
  env-var name and default *from the registry* (`Find("gizmosql.x")->env_var`) rather
  than re-hardcoding string literals.
- **Fuller:** give each descriptor an optional `apply_startup(Builder&, std::optional<cli>)`
  hook and loop the registry once during configuration; the CLI still only forwards
  flags.

Either way the rule from §9 holds: **every server-level setting has an env var, and the
fallback lives in the library.** Adding a parameter becomes: one registry entry + its
typed home + (for startup) one signature param — and `SET`, `SET GLOBAL`,
`gizmosql_settings()`, and the env-var fallback all pick it up automatically.
