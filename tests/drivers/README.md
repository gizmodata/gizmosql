# Driver compatibility checks

GizmoSQL ADBC remains the recommended ADBC driver, including its OAuth support
and `gizmosql://` URI scheme. These tests also verify interoperability with the
unmodified Apache Arrow Flight SQL drivers.

Use Python 3.12 or newer and JDK 17. Install the pinned test dependencies in a
virtual environment, then run the matrix against a compiled candidate:

```bash
python -m pip install -r tests/drivers/requirements.txt
python tests/drivers/run_compatibility.py \
  --server build/gizmosql_server --output build/driver-matrix
```

The runner starts an isolated server, downloads checksum-verified JDBC JARs from
Maven Central, and runs both ADBC packages and both JDBC drivers. It verifies
reads, DDL/DML, repeated parameter binding, batch writes, and RETURNING results.
CI runs this on macOS for both supported DuckDB channels and retains the logs.

The pinned Arrow Flight SQL JDBC 19.0.0 and GizmoSQL JDBC 1.7.0 releases have
client-side commit/rollback and batch-count limitations. The matrix explicitly
reports these: it checks every batch write through a database checksum, but
does not claim those released drivers have correct JDBC transaction behavior.
The proposed GizmoSQL JDBC maintenance release fixes both issues. To test that
JAR against a separately running candidate and a v1.38.x baseline, compile and
run `EagerJdbc` without either compatibility-limitation option:

```bash
javac -d build/driver-matrix tests/drivers/EagerJdbc.java
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp "build/driver-matrix:/path/to/candidate-gizmosql-jdbc.jar" \
  EagerJdbc 'jdbc:gizmosql://127.0.0.1:31582'
```

Use `;` instead of `:` in the classpath on Windows. The standalone harness
defaults to username `eager_test` and password `eager_test_password`; override
with `GIZMOSQL_TEST_USERNAME` and `GIZMOSQL_TEST_PASSWORD`. Use only isolated test
databases: these checks create and modify tables.

ODBC checks use the released driver library through pyodbc:

```bash
python -m pip install pyodbc==5.3.0
GIZMOSQL_TEST_ODBC_DRIVER=/path/to/driver-library \
GIZMOSQL_TEST_PORT=31582 python -m pytest -q tests/drivers/test_eager_odbc.py
```

Metrics startup/configuration tests use local license files:

```bash
GIZMOSQL_TEST_BINARY="$PWD/build/gizmosql_server" \
GIZMOSQL_TEST_METRICS_LICENSE=/private/path/metrics-license.txt \
GIZMOSQL_TEST_NON_METRICS_LICENSE=/private/path/other-features-license.txt \
python -m pytest -q tests/drivers/test_metrics_configuration.py
```

Never commit license files or signing keys. CI's encrypted
`GIZMOSQL_METRICS_TEST_LICENSE` secret contains a test license including metrics;
renew it before expiration. Signing keys remain local to the license generator.
