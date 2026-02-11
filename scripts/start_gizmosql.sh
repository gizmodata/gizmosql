#!/bin/bash
#
# Docker entrypoint for the full GizmoSQL image (with TLS cert generation).
#
# ── Script-managed env vars (mapped to CLI flags here) ──────────────────────
#   DATABASE_BACKEND          --backend             (default: "duckdb")
#   DATABASE_FILENAME         --database-filename   (default: "" = in-memory)
#   TLS_ENABLED               generate & pass --tls (default: "1")
#   PRINT_QUERIES             --print-queries       (default: "1")
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
#   GIZMOSQL_OAUTH_CLIENT_ID             --oauth-client-id
#   GIZMOSQL_OAUTH_CLIENT_SECRET         --oauth-client-secret
#   GIZMOSQL_OAUTH_SCOPES                --oauth-scopes
#   GIZMOSQL_OAUTH_PORT                  --oauth-port
#   GIZMOSQL_OAUTH_REDIRECT_URI          --oauth-redirect-uri
#   GIZMOSQL_OAUTH_DISABLE_TLS           --oauth-disable-tls (localhost only!)
#
# ── Escape hatch ────────────────────────────────────────────────────────────
#   GIZMOSQL_EXTRA_ARGS       Appended verbatim to the command line.
#                             Supports quoted values, e.g.:
#                             GIZMOSQL_EXTRA_ARGS='--init-sql-commands "CREATE TABLE t(id INT)"'
#
# ── Positional args (backward compat) ──────────────────────────────────────
#   $1  DATABASE_BACKEND
#   $2  DATABASE_FILENAME
#   $3  TLS_ENABLED
#   $4  PRINT_QUERIES
#   $5  READONLY
#   $6  QUERY_TIMEOUT
#

set -e

SCRIPT_DIR=$(dirname "${0}")
TLS_DIR="${SCRIPT_DIR}/../tls"

L_DATABASE_BACKEND=${1:-${DATABASE_BACKEND:-"duckdb"}}
L_DATABASE_FILENAME=${2:-${DATABASE_FILENAME:-""}}
L_TLS_ENABLED=${3:-${TLS_ENABLED:-"1"}}
L_PRINT_QUERIES=${4:-${PRINT_QUERIES:-"1"}}
L_READONLY=${5:-${READONLY:-"0"}}
L_QUERY_TIMEOUT=${6:-${QUERY_TIMEOUT:-"0"}}

# Treat ":memory:" as in-memory (empty string)
if [ "${L_DATABASE_FILENAME}" = ":memory:" ]; then
  L_DATABASE_FILENAME=""
fi

# ── Build argument array ────────────────────────────────────────────────────
ARGS=()

ARGS+=(--backend="${L_DATABASE_BACKEND}")
ARGS+=(--database-filename="${L_DATABASE_FILENAME}")

# TLS: generate self-signed certs if needed
if [ "${L_TLS_ENABLED}" = "1" ]; then
  pushd "${TLS_DIR}" > /dev/null
  if [ ! -f ./cert0.pem ]; then
    echo "Generating TLS certs..."
    ./gen-certs.sh
  fi
  popd > /dev/null
  ARGS+=(--tls tls/cert0.pem tls/cert0.key)
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
