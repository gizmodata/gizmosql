#!/bin/bash
#
# Docker entrypoint for the slim GizmoSQL image (no TLS cert generation).
# TLS certs must be provided externally via TLS_CERT / TLS_KEY.
#
# ── Script-managed env vars (mapped to CLI flags here) ──────────────────────
#   DATABASE_FILENAME         --database-filename   (default: "" = in-memory)
#   DATABASE_BACKEND          --backend             (default: "duckdb")
#   PRINT_QUERIES             --print-queries       (default: "1")
#   TLS_ENABLED               --tls with TLS_CERT/TLS_KEY (default: "0")
#   TLS_CERT                  cert path for --tls
#   TLS_KEY                   key  path for --tls
#   READONLY                  --readonly            (default: "0")
#   QUERY_TIMEOUT             --query-timeout       (default: "0")
#   MTLS_CA_CERT_FILENAME     --mtls-ca-cert-filename
#   HEALTH_PORT               --health-port
#
# ── Passthrough env vars (read natively by gizmosql_server) ─────────────────
#   GIZMOSQL_PASSWORD         --password
#   GIZMOSQL_PORT             --port
#   GIZMOSQL_USERNAME         --username
#   SECRET_KEY                --secret-key
#   INIT_SQL_COMMANDS         --init-sql-commands
#   INIT_SQL_COMMANDS_FILE    --init-sql-commands-file
#   GIZMOSQL_LOG_LEVEL        --log-level
#   GIZMOSQL_LOG_FORMAT       --log-format
#   GIZMOSQL_ACCESS_LOG       --access-log
#   GIZMOSQL_LOG_FILE         --log-file
#   GIZMOSQL_QUERY_LOG_LEVEL  --query-log-level
#   GIZMOSQL_AUTH_LOG_LEVEL   --auth-log-level
#   GIZMOSQL_HEALTH_CHECK_QUERY      --health-check-query
#   GIZMOSQL_ENABLE_INSTRUMENTATION  --enable-instrumentation
#   GIZMOSQL_INSTRUMENTATION_DB_PATH --instrumentation-db-path
#   GIZMOSQL_INSTRUMENTATION_CATALOG --instrumentation-catalog
#   GIZMOSQL_INSTRUMENTATION_SCHEMA  --instrumentation-schema
#   GIZMOSQL_LICENSE_KEY_FILE        --license-key-file
#   GIZMOSQL_ALLOW_CROSS_INSTANCE_TOKENS --allow-cross-instance-tokens
#   GIZMOSQL_TOKEN_JWKS_URI              --token-jwks-uri
#   GIZMOSQL_TOKEN_DEFAULT_ROLE          --token-default-role
#   GIZMOSQL_TOKEN_AUTHORIZED_EMAILS     --token-authorized-emails
#
# ── Escape hatch ────────────────────────────────────────────────────────────
#   GIZMOSQL_EXTRA_ARGS       Appended verbatim to the command line.
#                             Supports quoted values, e.g.:
#                             GIZMOSQL_EXTRA_ARGS='--init-sql-commands "CREATE TABLE t(id INT)"'
#
# ── Positional args (backward compat) ──────────────────────────────────────
#   $1  DATABASE_FILENAME
#   $2  DATABASE_BACKEND
#   $3  PRINT_QUERIES
#   $4  TLS_ENABLED
#   $5  TLS_CERT
#   $6  TLS_KEY
#   $7  READONLY
#   $8  QUERY_TIMEOUT
#

set -e

L_DATABASE_FILENAME=${1:-${DATABASE_FILENAME:-""}}
L_DATABASE_BACKEND=${2:-${DATABASE_BACKEND:-"duckdb"}}
L_PRINT_QUERIES=${3:-${PRINT_QUERIES:-"1"}}
L_TLS_ENABLED=${4:-${TLS_ENABLED:-"0"}}
L_TLS_CERT=${5:-${TLS_CERT}}
L_TLS_KEY=${6:-${TLS_KEY}}
L_READONLY=${7:-${READONLY:-"0"}}
L_QUERY_TIMEOUT=${8:-${QUERY_TIMEOUT:-"0"}}

# Treat ":memory:" as in-memory (empty string)
if [ "${L_DATABASE_FILENAME}" = ":memory:" ]; then
  L_DATABASE_FILENAME=""
fi

# ── Build argument array ────────────────────────────────────────────────────
ARGS=()

ARGS+=(--backend="${L_DATABASE_BACKEND}")
ARGS+=(--database-filename="${L_DATABASE_FILENAME}")

# TLS: require cert and key when enabled
if [ "${L_TLS_ENABLED}" = "1" ]; then
  if [ -z "${L_TLS_CERT}" ] || [ -z "${L_TLS_KEY}" ]; then
    echo "TLS_CERT and TLS_KEY must be passed when TLS is enabled." >&2
    exit 1
  fi
  ARGS+=(--tls "${L_TLS_CERT}" "${L_TLS_KEY}")
fi

if [ "${L_PRINT_QUERIES}" = "1" ]; then
  ARGS+=(--print-queries)
fi

if [ "${L_READONLY}" = "1" ]; then
  ARGS+=(--readonly)
fi

ARGS+=(--query-timeout "${L_QUERY_TIMEOUT}")

if [ -n "${MTLS_CA_CERT_FILENAME}" ]; then
  ARGS+=(--mtls-ca-cert-filename "${MTLS_CA_CERT_FILENAME}")
fi

if [ -n "${HEALTH_PORT}" ]; then
  ARGS+=(--health-port "${HEALTH_PORT}")
fi

# ── Extra args escape hatch ─────────────────────────────────────────────────
if [ -n "${GIZMOSQL_EXTRA_ARGS}" ]; then
  eval "EXTRA=(${GIZMOSQL_EXTRA_ARGS})"
  ARGS+=("${EXTRA[@]}")
fi

exec gizmosql_server "${ARGS[@]}"
