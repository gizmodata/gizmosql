#!/bin/bash

set -e

SCRIPT_DIR=$(dirname ${0})
TLS_DIR=${SCRIPT_DIR}/../tls

L_DATABASE_BACKEND=${1:-${DATABASE_BACKEND:-"duckdb"}}
L_DATABASE_FILENAME=${2:-${DATABASE_FILENAME:-"data/TPC-H-small.duckdb"}}
L_TLS_ENABLED=${3:-${TLS_ENABLED:-"1"}}
L_PRINT_QUERIES=${4:-${PRINT_QUERIES:-"1"}}
L_READONLY=${5:-${READONLY:-"0"}}

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

BINARY="gizmosql_server"

ARGS="--backend="${L_DATABASE_BACKEND}" --database-filename="${L_DATABASE_FILENAME}" ${TLS_ARG} ${PRINT_QUERIES_FLAG} ${READONLY_FLAG}"
LOG_DIR="/var/log/gizmosql"
mkdir -p "$LOG_DIR"

TIMESTAMP="$(date +'%Y%m%d_%H%M%S')"
LOG_FILE="$LOG_DIR/gizmosql_gdb_${TIMESTAMP}.log"

echo "[$(date)] Starting GizmoSQL under gdb..."
echo "Logs will be saved to: $LOG_FILE"
echo

gdb --batch-silent \
    -ex "set pagination off" \
    -ex "set logging file $LOG_FILE" \
    -ex "set logging enabled on" \
    -ex "run $ARGS" \
    -ex "echo \n\n----- BACKTRACE -----\n\n" \
    -ex "thread apply all bt full" \
    -ex "echo \n\n----- REGISTERS -----\n\n" \
    -ex "info registers" \
    -ex "echo \n\n----- SHARED LIBS -----\n\n" \
    -ex "info sharedlibrary" \
    -ex "set logging off" \
    -ex "quit" \
    --args "$BINARY" $ARGS

EXIT_CODE=$?

if [[ $EXIT_CODE -eq 137 ]]; then
    echo "[$(date)] Process killed by SIGKILL (OOM?). No full GDB trace available." | tee -a "$LOG_FILE"
elif [[ $EXIT_CODE -ne 0 ]]; then
    echo "[$(date)] GizmoSQL crashed with exit code $EXIT_CODE" | tee -a "$LOG_FILE"
else
    echo "[$(date)] GizmoSQL exited normally (code 0)" | tee -a "$LOG_FILE"
fi