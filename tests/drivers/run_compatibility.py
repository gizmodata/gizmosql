#!/usr/bin/env python3
"""Test a local server with pinned, unmodified released ADBC and JDBC drivers."""

import argparse
import hashlib
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

from test_metrics_configuration import command, running

ROOT = Path(__file__).resolve().parents[2]
JARS = [
    (
        "arrow",
        "org/apache/arrow/flight-sql-jdbc-driver/19.0.0/flight-sql-jdbc-driver-19.0.0.jar",
        "d3beee43c613c457789825343368f652d570d76c08799dad38a43a10e569b57f",
        "jdbc:arrow-flight-sql",
    ),
    (
        "gizmosql",
        "com/gizmodata/gizmosql-jdbc-driver/1.7.0/gizmosql-jdbc-driver-1.7.0.jar",
        "f3c2ad1e238268f40cabecea2e144bff47e7661e3072e0160130406329cec54c",
        "jdbc:gizmosql",
    ),
]


def run_logged(args, env, output):
    with output.open("w") as log:
        result = subprocess.run(
            args, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=180
        )
    print(output.read_text(), flush=True)
    result.check_returncode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    os.environ["GIZMOSQL_TEST_BINARY"] = str(args.server.resolve())
    server_args, env, flight, _ = command(output)
    for name, resource, expected, _ in JARS:
        jar = output / f"{name}.jar"
        if not jar.exists():
            with urllib.request.urlopen(
                "https://repo.maven.apache.org/maven2/" + resource, timeout=60
            ) as response:
                jar.write_bytes(response.read())
        if hashlib.sha256(jar.read_bytes()).hexdigest() != expected:
            raise RuntimeError(f"Checksum mismatch: {jar}")
    subprocess.run(
        ["javac", "-d", str(output), str(ROOT / "tests/drivers/EagerJdbc.java")],
        check=True,
        timeout=60,
    )
    with running(server_args, env, flight, output):
        client_env = {
            **env,
            "GIZMOSQL_TEST_URI": f"grpc://127.0.0.1:{flight}",
            "GIZMOSQL_TEST_USERNAME": "tester",
            "GIZMOSQL_TEST_PASSWORD": "local_test_password",
        }
        run_logged(
            [sys.executable, "-m", "pytest", "-q", "tests/drivers/test_eager_adbc.py"],
            client_env,
            output / "adbc.log",
        )
        for name, resource, _, scheme in JARS:
            # These releases have known client-side transaction and batch-count
            # limitations. Still verify every batch write in the SQL ledger.
            # The fixed GizmoSQL driver is tested separately in its own repository.
            print(f"Testing released artifact: {resource}", flush=True)
            run_logged(
                [
                    "java",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "-cp",
                    str(output) + os.pathsep + str(output / f"{name}.jar"),
                    "EagerJdbc",
                    f"{scheme}://127.0.0.1:{flight}",
                    "--reads-writes-only",
                    "--allow-aggregate-batch-counts",
                ],
                client_env,
                output / f"{name}-jdbc.log",
            )


if __name__ == "__main__":
    main()
