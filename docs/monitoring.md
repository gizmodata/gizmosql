# Monitoring with Prometheus (Enterprise)

Available in GizmoSQL **1.39.0 and later**, using the **DuckDB backend**.
Runtime metrics require an Enterprise license with the **`metrics`** feature.
Metrics are disabled by default. GizmoSQL collects counters in memory and samples
DuckDB/process resources on a dedicated background connection every five seconds.
The HTTP endpoint and `gizmosql_metrics()` use the same registry.

```sh
GIZMOSQL_ENABLE_METRICS=true \
GIZMOSQL_METRICS_PORT=9091 \
GIZMOSQL_METRICS_BIND_ADDRESS=0.0.0.0 \
gizmosql_server --license-key-file /path/to/license.jwt
```

Supply your usual server password and database settings as well. A license
containing only another Enterprise entitlement does not enable metrics.
Session instrumentation and metrics are separate features; metrics does not
require enabling the instrumentation database.

To verify the endpoint from the server itself:

```sh
curl --fail http://127.0.0.1:9091/metrics
```

Enabling metrics without the entitlement fails server startup with an explicit
licensing error. Calling `gizmosql_metrics()` without the entitlement also fails.
With a valid entitlement but collection disabled, SQL reports that metrics is
disabled. An expired license denies subsequent HTTP scrapes with status 403 and
denies SQL metrics access.

| CLI option | Environment variable | Default |
|---|---|---|
| `--enable-metrics` | `GIZMOSQL_ENABLE_METRICS` | `false` |
| `--metrics-port` | `GIZMOSQL_METRICS_PORT` | `9091` |
| `--metrics-bind-address` | `GIZMOSQL_METRICS_BIND_ADDRESS` | `0.0.0.0` |

Configuration is resolved by `RunFlightSQLServer()` in the library. Explicit CLI
true/false overrides the environment. Boolean environment values accept
`on/off`, `yes/no`, `true/false`, and `1/0`. These are startup settings.
`--enable-metrics false` disables collection and HTTP/SQL access. With metrics
enabled, `--metrics-port 0` disables the listener while preserving SQL metrics.

```sql
SELECT name, value, settable, cli_flag
FROM gizmosql_settings()
WHERE name = 'gizmosql.enable_metrics';

SELECT name, kind, labels, value
FROM gizmosql_metrics()
WHERE name LIKE 'gizmosql_queue%';
```

The HTTP listener serves `GET /metrics` as Prometheus text format 0.0.4 and
`GET /` as a short pointer. Other paths return 404. It is plain HTTP without
authentication: expose it only on a trusted monitoring network. It is independent
of the Flight SQL port and the **gRPC** health port. Do not add the metrics port
to an external LoadBalancer service. License validation applies to each scrape
and SQL execution, including prepared SQL statements.

Counters count executions, including each reuse of a prepared statement.
Downloading an already completed eager execution's ticket does not increment its
execution count. Histograms use cumulative buckets in seconds:
`0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10 30 60 300 +Inf`.
SQL exposes bucket, `_sum`, and `_count` rows individually. Two observations at
different times can differ; both surfaces use the same names, labels and values
from their respective registry snapshot.

## Port and bind address

The bind address selects which local network interfaces accept connections.
`127.0.0.1` accepts connections from the same host; `0.0.0.0` listens on all IPv4
interfaces, allowing a separate Prometheus instance to connect when network rules
permit it. The port defaults to **9091** and is independent of the Flight SQL port.
Choose another free port with `--metrics-port` or `GIZMOSQL_METRICS_PORT`.

In Kubernetes, use the pod's metrics port on the cluster network and restrict it
to the monitoring namespace as appropriate. Binding to localhost inside a pod
prevents an ordinary Prometheus pod from reaching it.

## Metric reference

Counters are cumulative since process startup unless specified otherwise.
Gauges describe the current sampled value. Statement and queue histograms expose
`_bucket`, `_sum`, and `_count` series. A zero session or concurrency limit means
unlimited.

| Metric family | Kind | Meaning / fixed labels |
|---|---|---|
| `gizmosql_build_info` | Gauge | Value 1, with `version`, `duckdb_version`, and `edition` labels. |
| `gizmosql_up_seconds` | Gauge | Elapsed time since the runtime registry started. |
| `gizmosql_sessions` | Gauge | `state`: `active`, `idle`, `idle_in_transaction`, `idle_in_transaction_aborted`. |
| `gizmosql_sessions_opened_total` | Counter | Sessions created. |
| `gizmosql_sessions_reaped_total` | Counter | Session removals by `reason`; see limitations below. |
| `gizmosql_session_limit` | Gauge | Configured maximum sessions. |
| `gizmosql_statements_total` | Counter | Completed user SQL requests by `status`: `ok`, `error`, `cancelled`. |
| `gizmosql_statement_errors_total` | Counter | `class`: `out_of_memory`, `io`, `catalog`, `syntax`, `permission`, `cancelled`, `other`. |
| `gizmosql_statement_duration_seconds` | Histogram | Execution wall time by `kind`: `read`, `write`, `ddl`, `other`; includes admission wait, excludes client download time. |
| `gizmosql_statements_active` | Gauge | Observed execution/wait activity by `wait`: `none`, `queue`, `client_send`; see limitations. |
| `gizmosql_concurrency_limit` | Gauge | Configured admission limit. |
| `gizmosql_queue_depth` | Gauge | Statements awaiting admission. |
| `gizmosql_queue_wait_seconds` | Histogram | Time spent waiting for admission. |
| `gizmosql_queue_rejected_total` | Counter | `reason`: `full` or `timeout`. |
| `gizmosql_queue_admin_bypass_total` | Counter | Statements admitted using administrator bypass. |
| `gizmosql_duckdb_memory_used_bytes` | Gauge | DuckDB-tracked memory, rather than total process RSS. |
| `gizmosql_duckdb_memory_limit_bytes` | Gauge | Effective DuckDB memory limit. |
| `gizmosql_duckdb_threads` | Gauge | Effective DuckDB worker setting. |
| `gizmosql_duckdb_temp_storage_bytes` | Gauge | Temporary storage reported by `duckdb_memory()`. |
| `gizmosql_duckdb_spill_files`, `gizmosql_duckdb_spill_bytes` | Gauges | Live temporary spill files and their size. |
| `gizmosql_database_file_bytes`, `gizmosql_wal_bytes` | Gauges | Main local database and WAL sizes. |
| `gizmosql_disk_free_bytes` | Gauge | Available filesystem bytes; `path` is `database` or `temp`, never an actual filesystem path. |
| `process_resident_memory_bytes` | Gauge | Process resident memory. |
| `process_cpu_seconds_total` | Counter | Process user and system CPU time combined. |
| `process_start_time_seconds` | Gauge | Process start time as a Unix timestamp. |
| `process_threads` | Gauge | Operating-system process thread count. |
| `process_open_fds` | Gauge | Open file descriptors on Linux/macOS. |
| `process_open_handles` | Gauge | Open process handles on Windows. |
| `gizmosql_host_memory_bytes`, `gizmosql_host_cpus` | Gauges | Host physical memory and logical CPUs. |
| `gizmosql_cgroup_memory_limit_bytes`, `gizmosql_cgroup_cpu_quota_cores` | Gauges | Finite detected Linux container limits, when available. |
| `gizmosql_health_check_status` | Gauge | 1 when the cached internal health check is serving, otherwise 0. |
| `gizmosql_health_check_duration_seconds` | Gauge | Duration of the last internal health query. |
| `gizmosql_draining` | Gauge | 1 during graceful shutdown drain. |
| `gizmosql_license_expiry_seconds`, `gizmosql_tls_certificate_expiry_seconds` | Gauges | Expiry Unix timestamps, when applicable. |
| `gizmosql_last_exit_unclean` | Gauge | Whether the previous metrics-enabled run for this local database stopped uncleanly. |
| `gizmosql_unclean_exits_total` | Counter | Unclean exits detected in the persistent database-side marker. |
| `gizmosql_metrics_collection_success` | Gauge | 1 if the last local refresh succeeded. |
| `gizmosql_metrics_last_collection_success_seconds` | Gauge | Last successful local refresh timestamp. |
| `gizmosql_metrics_collection_duration_seconds` | Gauge | Duration of the last local refresh. |

## Sampling and limitations

Metrics do not contain SQL, usernames, session identifiers or user-supplied
catalog names as labels. Optional measurements that have not been obtained are
absent. An unavailable measurement must not be interpreted as zero.

Local resource and session gauges refresh every five seconds. Collection reads
local engine/process information; it does not issue health queries to attached
remote catalogs. Active PostgreSQL and DuckLake metastore probes are deferred
until their network operations can be reliably cancelled and bounded.

The current engine does not expose reliable per-query I/O or lock wait states:
`wait="io"` and `wait="lock"` are absent. `client_send` tracks live result streams,
not a measured network backpressure duration. These gauges are sampled and are
not a transactional snapshot of all concurrent activity.

Session removal reasons currently observed are `idle_timeout`, `client_close`,
and `server_shutdown`. The reserved `max_lifetime` and `client_abandoned` reasons
remain zero; their presence does not imply a maximum-lifetime policy or immediate
disconnection detection. Query counters cover the user SQL execution path;
internal metadata/health SQL is excluded. Bulk ingestion is a separate protocol
operation and is not a statement-counter substitute for ingested-row accounting.

Database/WAL and restart-marker measurements apply to the main local database,
not the total storage of every attached remote catalog. An in-memory database
has no persistent unclean-exit history. The marker is maintained only by a
server that opened the database read-write, since DuckDB allows one writer per
file; read-only instances sharing a file never write it and report no exit
history. Restart detection records process shutdown behavior; it is not a
guarantee of detecting every power-loss scenario.

Each collection section (server state, DuckDB memory, settings, spill files,
database files, process statistics) fails independently. A failing probe leaves
only its own gauges absent and sets `gizmosql_metrics_collection_success` to
zero; the session, queue and health gauges are sampled first.

Use `gizmosql_metrics_collection_success`,
`gizmosql_metrics_last_collection_success_seconds`, and
`gizmosql_metrics_collection_duration_seconds` to distinguish an unhealthy engine
from a stale collector. Cached values can remain at their last successful value
when collection fails.

## Prometheus and Grafana

A basic scrape configuration is:

```yaml
scrape_configs:
  - job_name: gizmosql
    scrape_interval: 15s
    static_configs:
      - targets: ['gizmosql:9091']
```

Replace `gizmosql` with the reachable service name or address. Prometheus scrapes
the cached endpoint; a scrape does not issue user SQL or synchronously contact
remote catalogs.

Example PodMonitor (requires the Prometheus Operator):

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: gizmosql
spec:
  selector:
    matchLabels:
      app: gizmosql
      gizmosql-metrics: enabled
  podMetricsEndpoints:
    - port: metrics
      path: /metrics
      interval: 15s
```

The pod must declare a container port named `metrics` at 9091, enable the feature,
and carry the selector labels. Configure the Prometheus instance to discover
this PodMonitor's namespace and labels. Prometheus supplies pod/namespace labels.

The [observability examples](https://github.com/gizmodata/gizmosql/tree/main/observability)
include a Grafana dashboard, alert rules, PodMonitor, and a Docker Compose stack.
From a repository checkout:

```sh
export GIZMOSQL_LICENSE_FILE=/absolute/path/to/metrics-enabled-license.jwt
export GIZMOSQL_PASSWORD='choose-a-local-test-password'
export GRAFANA_ADMIN_PASSWORD='choose-a-grafana-admin-password'
docker compose -f observability/compose.yml up -d
```

Grafana is available at `http://localhost:3000` (user `admin`), and Prometheus at
`http://localhost:9090`. The example keeps port 9091 inside the Compose network.
Its plaintext Flight SQL connection is intended for local evaluation. Adjust
TLS, authentication, network policy, and persistent storage for your deployment.

Example queries:

```promql
# Completed statements per second
sum by (instance, status) (rate(gizmosql_statements_total[5m]))

# Statement execution p95, including queue time
histogram_quantile(0.95, sum by (instance, le) (rate(gizmosql_statement_duration_seconds_bucket[5m])))

# CPU cores consumed by the process
rate(process_cpu_seconds_total[5m])
```

## Older servers and feature detection

Query `gizmosql_settings()` for `gizmosql.enable_metrics` before configuring a
scrape or displaying server metrics. If the setting is missing, false, or the
older server does not support the settings function, treat metrics as unavailable.
Do not assume that an Enterprise edition alone includes or enables this entitlement.
An enabled setting with port zero means SQL-only collection, so an HTTP scrape
also requires a reachable configured listener.
