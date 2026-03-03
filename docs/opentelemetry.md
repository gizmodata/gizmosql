# OpenTelemetry Integration

GizmoSQL supports optional OpenTelemetry instrumentation for tracing and metrics export over OTLP/HTTP.

## Overview

When OpenTelemetry is enabled, GizmoSQL provides:

- **Server spans** for Flight SQL RPC methods
- **W3C trace context propagation** from incoming request headers (`traceparent`/`tracestate`)
- **OTLP/HTTP export** of traces and metrics to your collector/backend
- **Log correlation fields** (`trace_id`, `span_id`) in both JSON and text logs

Telemetry is **disabled by default** and must be enabled at build time and runtime.

## Build with OpenTelemetry

OpenTelemetry support is optional and controlled by CMake:

```bash
cmake -S . -B build -G Ninja -DWITH_OPENTELEMETRY=ON
cmake --build build --target gizmosql_server
```

If built without this flag, GizmoSQL runs normally but telemetry remains disabled (even if runtime flags/env are set).

## Server Configuration

Use CLI flags or environment variables (CLI takes precedence when set):

| Parameter | CLI Flag | Env Var | Default | Description |
|-----------|----------|---------|---------|-------------|
| `otel_enabled` | `--otel-enabled` | `GIZMOSQL_OTEL_ENABLED` | `false` | Enable telemetry (`true`/`1` to enable) |
| `otel_exporter` | `--otel-exporter` | `GIZMOSQL_OTEL_EXPORTER` | `http` | Exporter type (`http`) |
| `otel_endpoint` | `--otel-endpoint` | `GIZMOSQL_OTEL_ENDPOINT` | `http://localhost:4318` | Base OTLP endpoint URL |
| `otel_service_name` | `--otel-service-name` | `GIZMOSQL_OTEL_SERVICE_NAME` | `gizmosql` | `service.name` resource attribute |
| `otel_headers` | `--otel-headers` | `GIZMOSQL_OTEL_HEADERS` | (empty) | OTLP HTTP headers (`key1=value1,key2=value2`) |
| `deployment_environment` | (none) | `GIZMOSQL_ENVIRONMENT` (fallback: `ENVIRONMENT`) | (empty) | `deployment.environment` resource attribute |

## Example: Export to Local OTLP Collector

```bash
GIZMOSQL_PASSWORD="gizmosql_password" \
GIZMOSQL_OTEL_ENABLED="true" \
GIZMOSQL_OTEL_EXPORTER="http" \
GIZMOSQL_OTEL_ENDPOINT="http://localhost:4318" \
GIZMOSQL_OTEL_SERVICE_NAME="gizmosql" \
gizmosql_server \
  --database-filename ./data/mydb.duckdb \
  --port 31337
```

Equivalent CLI-based configuration:

```bash
GIZMOSQL_PASSWORD="gizmosql_password" gizmosql_server \
  --database-filename ./data/mydb.duckdb \
  --port 31337 \
  --otel-enabled true \
  --otel-exporter http \
  --otel-endpoint http://localhost:4318 \
  --otel-service-name gizmosql
```

## Authentication Headers for OTLP

Use `otel_headers`/`GIZMOSQL_OTEL_HEADERS` when your collector requires auth headers:

```bash
export GIZMOSQL_OTEL_HEADERS="Authorization=Bearer <token>,X-Org-ID=my-org"
```

The value format is comma-separated `key=value` pairs.

## Telemetry Data Emitted

### Traces

GizmoSQL creates server spans per Flight SQL RPC and records attributes including:

- `rpc.system=grpc`
- `rpc.service=arrow.flight.protocol.FlightService`
- `rpc.method`
- `rpc.grpc.status_code`
- `duration_ms`

Incoming W3C trace context is extracted from request headers so GizmoSQL spans can join upstream traces.

### Metrics

The following metric instruments are emitted:

- `gizmosql.rpc.duration` (histogram, ms)
- `gizmosql.rpc.count` (counter)
- `gizmosql.query.duration` (histogram, ms)
- `gizmosql.query.count` (counter)
- `gizmosql.bytes.transferred` (counter, By)
- `gizmosql.rows.transferred` (counter)
- `gizmosql.connections.active` (up/down counter)

Common attributes:

- `gizmosql.rpc.duration` / `gizmosql.rpc.count`: `rpc.method`, `rpc.status`
- `gizmosql.query.duration` / `gizmosql.query.count`: `db.operation`, `db.status`
- `gizmosql.bytes.transferred` / `gizmosql.rows.transferred`: `direction` (`inbound`/`outbound`)

Metric semantics:

- `gizmosql.connections.active` tracks active GizmoSQL sessions (incremented on session create, decremented on close/kill/shutdown).
- `gizmosql.query.duration` and `gizmosql.query.count` are emitted once per executed SQL statement.
- `db.operation` is derived from the leading SQL keyword (for example `SELECT`, `INSERT`, `CALL`) and uses `ADMIN` for GizmoSQL admin commands.
- `db.status` records execution outcome (`OK`, `TIMEOUT`, or an Arrow status code string on failure).
- `direction=outbound` measures query result batches sent to clients; `direction=inbound` measures Arrow record batches received by ingest paths.

### Logs

When a span is active, log records include:

- `trace_id` (OpenTelemetry hex format)
- `span_id` (OpenTelemetry hex format)

This enables log-to-trace correlation in observability backends.

## Troubleshooting

- **No telemetry data exported**
  - Confirm binary was built with `-DWITH_OPENTELEMETRY=ON`
  - Confirm `GIZMOSQL_OTEL_ENABLED=true` (or `--otel-enabled true`)
  - Verify collector endpoint is reachable from GizmoSQL
- **Collector requires auth**
  - Set `GIZMOSQL_OTEL_HEADERS` with required headers
- **No parent/child linkage**
  - Verify upstream caller sends W3C `traceparent` (and optional `tracestate`) headers
