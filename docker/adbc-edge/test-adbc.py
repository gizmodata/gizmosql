"""Run in a temporary test layer of an edge -adbc image (adapted from Hardcharger's
docker/test-adbc.py): load all 14 drivers, then use the four staged from GizmoData releases
through the driver manager, each by its manifest.

GizmoSQL runs a query against the image's own edge server and reports the driver version its
manifest names. Oracle, Db2 and Quack are clients with no server here: each must load through
its manifest's entrypoint and fail with its own connection error. Every driver is given its
options under the names the ADBC driver style guide asks for (`quack.auth.token`,
`oracle.connect_timeout_seconds`, ...): the drivers refuse unknown keys and print a deprecation
warning for an old name, so the test captures everything written to stderr and fails when any
of it says "deprecated".
"""
import ctypes
import os
import platform
from pathlib import Path
import socket
import sys
import time
import tempfile
import tomllib

from adbc_driver_manager import dbapi

# The drivers write warnings to file descriptor 2 from native code, so capture the descriptor
# itself, not sys.stderr, and give it back before reporting.
captured = tempfile.TemporaryFile()
saved_stderr = os.dup(2)
os.dup2(captured.fileno(), 2)


def restore_stderr():
    """Give descriptor 2 back and return what the drivers wrote to it."""
    os.dup2(saved_stderr, 2)
    captured.seek(0)
    return captured.read().decode(errors="replace")


def report_failure(kind, value, traceback):
    # A failed check must still show what the drivers printed, and its own traceback.
    sys.stdout.write(f"Driver stderr until the failure:\n{restore_stderr()}")
    sys.__excepthook__(kind, value, traceback)


sys.excepthook = report_failure

for attempt in range(60):
    try:
        with socket.create_connection(address=("127.0.0.1", 31337), timeout=1):
            break
    except OSError:
        time.sleep(1)
else:
    raise RuntimeError("GizmoSQL did not start")

DRIVERS = (
    "bigquery", "databricks", "exasol", "flightsql", "mssql", "mysql",
    "postgresql", "redshift", "snowflake", "trino", "gizmosql", "oracle",
    "db2", "quack",
)
QUACK_REFUSAL = "[quack] session: handshake: transport: HTTP connect failed for 127.0.0.1:1"
arch = {"x86_64": "amd64", "aarch64": "arm64"}[platform.machine()]
versions = {}
for name in DRIVERS:
    manifest = tomllib.loads(Path(f"/etc/adbc/drivers/{name}.toml").read_text())
    driver = manifest["Driver"]
    path = driver["shared"][f"linux_{arch}"]
    library = ctypes.CDLL(name=path)
    getattr(library, driver.get("entrypoint", "AdbcDriverInit"))
    versions[name] = manifest["version"]
    print(f"Loaded {name}: {manifest['version']}", flush=True)


def check_version(*, name, connection, vendor):
    """The library the manifest points at must be the release the manifest names."""
    info = connection.adbc_get_info()
    assert info["driver_version"].removeprefix("v") == versions[name], (name, info)
    assert info["vendor_name"] == vendor, (name, info)
    print(f"{name} reports driver {info['driver_version']}, server {vendor} {info['vendor_version']}", flush=True)


with dbapi.connect(driver="gizmosql", autocommit=True, db_kwargs={
    "uri": "grpc://127.0.0.1:31337", "username": "smoke", "password": "smoke",
}) as connection:
    check_version(name="gizmosql", connection=connection, vendor="gizmosql")
    # The edge channel's engine: this layer must sit on an edge base image.
    engine = connection.adbc_get_info()["vendor_version"]
    assert engine.startswith("duckdb v2."), engine
    with connection.cursor() as cursor:
        cursor.execute(operation="SELECT 42 AS answer")
        assert cursor.fetchone() == (42,)
print("GizmoSQL manifest discovery and SELECT 42 passed", flush=True)

# No server for these: nothing listens on port 1, so each driver must load through the
# driver manager (a manifest naming a missing entrypoint fails there) and refuse in its
# own words. Their versions are not read back; that needs a connection. Oracle and Db2 refuse
# an option they don't know, so reaching the connection error also shows the option's name is
# current.
REFUSALS = {
    "oracle": ("oracle://127.0.0.1:1/FREEPDB1",
               {"username": "smoke", "password": "smoke", "oracle.connect_timeout_seconds": "5"},
               "[oracle] oracle: cannot connect to 127.0.0.1:1"),
    "db2": ("db2://127.0.0.1:1/SAMPLE",
            {"username": "smoke", "password": "smoke", "db2.connect_timeout_seconds": "5"},
            "[db2] drda: connect 127.0.0.1:1"),
    "quack": ("quack://127.0.0.1:1", {"quack.auth.token": "smoke"}, QUACK_REFUSAL),
}
for name, (uri, options, refusal) in REFUSALS.items():
    try:
        with dbapi.connect(driver=name, db_kwargs={"uri": uri, **options}) as connection:
            # A driver that connects lazily refuses on its first statement instead.
            with connection.cursor() as cursor:
                cursor.execute(operation="SELECT 1")
        raise RuntimeError(f"{name} connected to a port nothing listens on")
    except dbapi.Error as error:
        assert refusal in str(error) and "127.0.0.1:1" in str(error), (name, str(error))
        print(f"{name} loaded by manifest and refused as expected: {error}", flush=True)

sys.excepthook = sys.__excepthook__
stderr = restore_stderr()
print(f"Driver stderr ({len(stderr.splitlines())} lines):", flush=True)
print(stderr, end="", flush=True)
assert "deprecat" not in stderr.lower(), "a driver printed a deprecation warning"
print("No deprecation warning on stderr", flush=True)
