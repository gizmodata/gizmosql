#!/bin/bash

set -e

SCRIPT_DIR=$(dirname ${0})
TLS_DIR=${SCRIPT_DIR}/../tls

L_DATABASE_BACKEND=${1:-${DATABASE_BACKEND:-"duckdb"}}
L_DATABASE_FILENAME=${2:-${DATABASE_FILENAME:-"data/TPC-H-small.duckdb"}}
L_TLS_ENABLED=${3:-${TLS_ENABLED:-"1"}}
L_PRINT_QUERIES=${4:-${PRINT_QUERIES:-"1"}}
L_READONLY=${5:-${READONLY:-"0"}}
L_LOG_FORMAT=${6:-${LOG_FORMAT:-"json"}}

TLS_ARG=""
if [ "${L_TLS_ENABLED}" == "1" ]
then
  pushd ${TLS_DIR}
  if [ ! -f ./cert0.pem ]
  then
     echo -n "Generating TLS certs...\n"
     ./gen-certs.sh
  fi
  TLS_ARG="--tls tls/cert0.pem tls/cert0.key"
  popd
fi

# Setup the print_queries option
PRINT_QUERIES_FLAG=""
if [ "${L_PRINT_QUERIES}" == "1" ]
then
  PRINT_QUERIES_FLAG="--print-queries"
fi

# Setup the readonly option
READONLY_FLAG=""
if [ "${L_READONLY}" == "1" ]
then
  READONLY_FLAG="--readonly"
fi

# Setup the log format option
LOG_FORMAT_FLAG=""
if [ "${L_LOG_FORMAT}" == "json" ]
then
  LOG_FORMAT_FLAG="--log-format=json"
fi

# Try to find gizmosql_server in multiple locations
if [ -x "${SCRIPT_DIR}/../build/gizmosql_server" ]; then
  GIZMOSQL_SERVER="${SCRIPT_DIR}/../build/gizmosql_server"
elif [ -x "/usr/local/bin/gizmosql_server" ]; then
  GIZMOSQL_SERVER="/usr/local/bin/gizmosql_server"
elif command -v gizmosql_server >/dev/null 2>&1; then
  GIZMOSQL_SERVER="gizmosql_server"
else
  echo "Error: gizmosql_server not found in build directory, /usr/local/bin, or PATH"
  exit 1
fi

${GIZMOSQL_SERVER} --backend="${L_DATABASE_BACKEND}" --database-filename="${L_DATABASE_FILENAME}" ${TLS_ARG} ${PRINT_QUERIES_FLAG} ${READONLY_FLAG} ${LOG_FORMAT_FLAG}
