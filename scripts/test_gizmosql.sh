#!/bin/bash

SCRIPT_DIR=$(dirname ${0})
TLS_DIR=${SCRIPT_DIR}/../tls

# Set a dummy password for the test...
export GIZMOSQL_PASSWORD="testing123"

# Use the TPC-H sample database (entrypoint defaults to in-memory)
export DATABASE_FILENAME="data/TPC-H-small.duckdb"

# Start the Flight SQL Server - in the background...
${SCRIPT_DIR}/start_gizmosql.sh &

# Set a timeout limit for waiting
timeout_limit=300
elapsed_time=0
interval=1  # seconds
started="0"

# Check if the process is running
while [ $elapsed_time -lt $timeout_limit ]; do
    # Check if the process is running
    if pgrep "gizmosql" > /dev/null; then
        echo "Flight SQL Server process started successfully!"
        started="1"
        # Sleep for a few more seconds...
        sleep 10
        break
    fi

    # Wait for a short interval before checking again
    sleep $interval
    elapsed_time=$((elapsed_time + interval))
done

# If the process didn't start within the timeout, exit
if [ "${started}" != "1" ]; then
    echo "The Flight SQL Server process did not start within the timeout period. Exiting."
    exit 1
fi

python "${SCRIPT_DIR}/test_gizmosql.py"

RC=$?

# Test the gizmosql_client
gizmosql_client \
  --command Execute \
  --host "localhost" \
  --port 31337 \
  --username "gizmosql_username" \
  --password "${GIZMOSQL_PASSWORD}" \
  --query "SELECT version()" \
  --use-tls \
  --tls-skip-verify

RC=$((RC + $?))

# Stop the server...
kill %1

# Remove temporary TLS cert files
pushd ${TLS_DIR}
rm -f ./*.csr \
      ./*.key \
      ./*.pkcs1 \
      ./*.pem \
      ./*.srl

popd

# Exit with the combined code of the python and gizmosql_client tests...
exit ${RC}
