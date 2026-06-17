# Graceful Shutdown

By default, GizmoSQL stops **immediately** when it receives `SIGINT` (Ctrl-C) or
`SIGTERM` (the signal Kubernetes sends to terminate a pod): any in-flight queries
are interrupted and their clients get an error.

**Graceful shutdown** changes that. When enabled, the first `SIGINT`/`SIGTERM`
puts the server into a *draining* state instead of stopping right away:

- **Already-running queries and their in-progress result fetches are allowed to
  finish** (or hit their per-query timeout).
- **New sessions and new statements are rejected** with a retriable
  `UNAVAILABLE` error — `"GizmoSQL instance is shutting down; not accepting new
  statements. Retry against another instance."`
- Once all in-flight work has drained, the server shuts down on its own.

This is the behavior you want for **rolling deployments** on Kubernetes (or any
container orchestrator): when a pod is being replaced, its in-flight analytical
queries complete cleanly instead of failing, while the load balancer routes new
work to other replicas.

## Enabling it

Graceful shutdown is **opt-in**. Enable it with the CLI flag or the environment
variable:

| Setting | CLI flag | Environment variable | Default |
|---------|----------|----------------------|---------|
| Enable graceful shutdown | `--graceful-shutdown` | `GIZMOSQL_GRACEFUL_SHUTDOWN` | `false` (immediate stop) |
| Maximum drain time | `--shutdown-grace-period-seconds` | `GIZMOSQL_SHUTDOWN_GRACE_PERIOD_SECONDS` | `300` (`0` = wait indefinitely) |

```bash
gizmosql_server \
  --database-filename=./data.db \
  --graceful-shutdown=true \
  --shutdown-grace-period-seconds=300
```

Or via environment variables (e.g. in a container):

```bash
export GIZMOSQL_GRACEFUL_SHUTDOWN=true
export GIZMOSQL_SHUTDOWN_GRACE_PERIOD_SECONDS=300
```

## How it works

1. **First signal → drain.** On the first `SIGINT`/`SIGTERM`, the server marks
   itself as draining. New `Execute`, prepared-statement creation/execution, and
   update calls return `UNAVAILABLE`; new client sessions are refused. Statements
   and result fetches that were *already* running continue.
2. **Wait for in-flight work.** The server waits until every in-flight query
   execution and result-stream fetch has completed.
3. **Grace-period cap.** If draining takes longer than
   `--shutdown-grace-period-seconds`, any remaining queries are interrupted and
   the server stops anyway. This guarantees the process always terminates — for
   example when a query has no timeout (`query_timeout = 0`) and runs forever.
   Set the cap to `0` to wait indefinitely and rely solely on per-query timeouts.
4. **Second signal → force.** Sending a second `SIGINT`/`SIGTERM` while draining
   aborts the drain and stops the server immediately (the "press Ctrl-C again to
   force quit" pattern).

## Kubernetes configuration

The kubelet sends `SIGTERM`, waits up to the pod's
`terminationGracePeriodSeconds`, and then sends `SIGKILL`. For the drain to
complete, the pod's grace period **must be at least as long as** GizmoSQL's
shutdown grace period (plus a little slack):

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      # Give the kubelet enough time for GizmoSQL to drain before SIGKILL.
      terminationGracePeriodSeconds: 330   # >= shutdown-grace-period-seconds
      containers:
        - name: gizmosql
          image: gizmodata/gizmosql:latest
          env:
            - name: GIZMOSQL_GRACEFUL_SHUTDOWN
              value: "true"
            - name: GIZMOSQL_SHUTDOWN_GRACE_PERIOD_SECONDS
              value: "300"
```

!!! warning "Match the two grace periods"
    If `terminationGracePeriodSeconds` is **shorter** than
    `GIZMOSQL_SHUTDOWN_GRACE_PERIOD_SECONDS`, the kubelet's `SIGKILL` will
    preempt the drain and in-flight queries will still be killed. Always set the
    pod grace period to the GizmoSQL grace period plus a buffer.

During the drain the server's gRPC health check stops reporting healthy when the
process finally exits; combined with the `UNAVAILABLE` errors on new statements,
this lets a Kubernetes `Service` / load balancer stop routing new connections to
the terminating pod while existing queries finish.

## Library (C API)

Embedders running a server via the GizmoSQL library can trigger the same drain
programmatically — the in-process equivalent of delivering one `SIGINT`/`SIGTERM`:

```cpp
#include "gizmosql_library.h"

// ... RunFlightSQLServer(..., /*graceful_shutdown=*/true, /*grace=*/300) on a thread ...

RequestGracefulShutdown();   // begin draining; call again to force immediate stop
```

When graceful shutdown is **not** enabled, `RequestGracefulShutdown()` falls back
to an immediate stop (equivalent to `ShutdownFlightServer()`).
