#!/bin/bash

SCRIPT_DIR=$(dirname ${0})
TLS_DIR=${SCRIPT_DIR}/../tls

# Set a dummy password for the test...
export GIZMOSQL_PASSWORD="testing123"

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

# Exit with the code of the python test...
exit ${RC}
