"""Exercise the candidate image's spill path and timeout recovery over Flight SQL.

Run inside the v1.34.3 candidate container. This validates a release artifact;
the application and Chris Harrison's PR commit remain unmodified.
"""

import os
from pathlib import Path
import threading
import time

from adbc_driver_gizmosql import dbapi


def connect():
    for attempt in range(60):
        try:
            return dbapi.connect(
                "grpc://localhost:31337",
                username="gizmosql_user",
                password=os.environ["GIZMOSQL_PASSWORD"],
                autocommit=True,
            )
        except Exception:
            if attempt == 59:
                raise
            time.sleep(1)


def main():
    temp_directory = Path("/tmp/gizmosql-spill")
    stop = threading.Event()
    seen = set()
    peak = [0]

    def monitor():
        while not stop.wait(0.01):
            size = 0
            for path in temp_directory.glob("*"):
                try:
                    size += path.stat().st_size
                    seen.add(path.suffix)
                except FileNotFoundError:
                    pass
            peak[0] = max(peak[0], size)

    with connect() as connection, connection.cursor() as cursor:
        watcher = threading.Thread(target=monitor)
        watcher.start()
        try:
            # A scalar output keeps client memory small while sorting more data
            # than the server's 200 MB memory limit can hold.
            count = 30_000_000
            expected = count * (count - 1) // 2
            sql = (
                "SELECT sum(i)::BIGINT FROM "
                f"(SELECT i FROM range({count}) t(i) ORDER BY hash(i))"
            )
            for iteration in range(3):
                cursor.execute(sql)
                assert cursor.fetchall() == [(expected,)]
                print(f"Spill sort {iteration + 1}: correct result", flush=True)

            # Interrupt an actively spilling statement, then repeat the same
            # successful workload in the original session and server process.
            cursor.execute("SET gizmosql.query_timeout = 2")
            started = time.monotonic()
            try:
                cursor.execute(
                    "SELECT sum(i)::HUGEINT FROM "
                    "(SELECT i FROM range(1000000000) t(i) ORDER BY hash(i))"
                )
                cursor.fetchall()
            except Exception as error:
                message = str(error).lower()
                assert "timeout" in message or "timed out" in message, message
                assert time.monotonic() - started < 30
                print("Spilling query timed out as expected", flush=True)
            else:
                raise AssertionError("Expected the billion-row sort to time out")
            cursor.execute("SET gizmosql.query_timeout = 0")
            cursor.execute(sql)
            assert cursor.fetchall() == [(expected,)]
            print("Spill sort after interruption: correct result", flush=True)
            cursor.execute("SELECT version(), 1")
            assert cursor.fetchall() == [("v1.5.4", 1)]
        finally:
            stop.set()
            watcher.join()

    assert peak[0] > 10_000_000, f"No substantial disk spill observed: {peak[0]}"
    assert ".block" in seen or ".tmp" in seen, seen
    print(f"Observed spill files {sorted(seen)}, peak size {peak[0]} bytes", flush=True)
    print("Repeated spilling and timeout recovery passed", flush=True)


if __name__ == "__main__":
    main()
