#!/bin/bash

set -e

L_DATABASE_FILENAME=${1:-${DATABASE_FILENAME?"You must specify a database filename."}}
L_DATABASE_BACKEND=${2:-${DATABASE_BACKEND:-"duckdb"}}
L_PRINT_QUERIES=${3:-${PRINT_QUERIES:-"1"}}
L_TLS_ENABLED=${4:-${TLS_ENABLED:-"0"}}
L_TLS_CERT=${5:-${TLS_CERT}}
L_TLS_KEY=${6:-${TLS_KEY}}
L_READONLY=${7:-${READONLY:-"0"}}
L_QUERY_TIMEOUT=${8:-${QUERY_TIMEOUT:-"0"}}

TLS_ARG=""
if [ "${L_TLS_ENABLED}" == "1" ]
then
  # Make sure L_TLS_CERT and L_TLS_KEY were provided
  if [ -z "${L_TLS_CERT}" ] || [ -z "${L_TLS_KEY}" ]
  then
    echo "TLS_CERT and TLS_KEY must be passed when TLS is enabled."
    exit 1
  fi

  TLS_ARG="--tls ${L_TLS_CERT} ${L_TLS_KEY}"
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

gizmosql_server \
  --backend="${L_DATABASE_BACKEND}" \
  --database-filename="${L_DATABASE_FILENAME}" \
  ${TLS_ARG} \
  ${PRINT_QUERIES_FLAG} \
  ${READONLY_FLAG} \
  --query-timeout ${L_QUERY_TIMEOUT}
