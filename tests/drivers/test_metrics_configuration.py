"""Black-box metrics licensing and CLI/env precedence using released ADBC.

GIZMOSQL_TEST_BINARY, GIZMOSQL_TEST_METRICS_LICENSE and
GIZMOSQL_TEST_NON_METRICS_LICENSE name local artifacts, never committed licenses.
"""

import contextlib
import os
import socket
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest
from adbc_driver_gizmosql import dbapi


def artifact(name):
    value = os.getenv(name)
    if not value:
        pytest.skip(f"Set {name} to a local artifact")
    return str(Path(value).resolve())


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def command(tmp_path, license_path=None):
    flight, metrics = port(), port()
    args = [
        artifact("GIZMOSQL_TEST_BINARY"),
        "--hostname",
        "127.0.0.1",
        "--port",
        str(flight),
        "--health-port",
        "0",
        "--username",
        "tester",
        "--password",
        "local_test_password",
        "--enable-instrumentation",
        "false",
        "--database-filename",
        str(tmp_path / "test.duckdb"),
        "--metrics-port",
        str(metrics),
        "--metrics-bind-address",
        "127.0.0.1",
    ]
    if license_path:
        args += ["--license-key-file", license_path]
    # Environment inheritance must not accidentally license a negative test.
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIZMOSQL_")}
    for key in ("INIT_SQL_COMMANDS", "INIT_SQL_COMMANDS_FILE"):
        env.pop(key, None)
    return args, env, flight, metrics


@contextlib.contextmanager
def running(args, env, flight, tmp_path, expose_process=False):
    with (tmp_path / "server.log").open("wb") as log:
        proc = subprocess.Popen(args, env=env, stdout=log, stderr=log)
    conn = None
    try:
        deadline = time.monotonic() + 30
        while True:
            assert proc.poll() is None, "Server failed; inspect server.log"
            try:
                conn = dbapi.connect(
                    f"grpc://127.0.0.1:{flight}",
                    username="tester",
                    password="local_test_password",
                    autocommit=True,
                )
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    assert cursor.fetchone() == (1,)
                break
            except Exception:
                if conn:
                    conn.close()
                    conn = None
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        yield (conn, proc) if expose_process else conn
    finally:
        if conn and proc.poll() is None:
            conn.close()
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def test_unclean_exit_is_detected_and_clean_shutdown_clears_badge(tmp_path):
    args, env, flight, _ = command(tmp_path, artifact("GIZMOSQL_TEST_METRICS_LICENSE"))
    args += ["--enable-metrics", "true"]
    for expected_badge, expected_count, crash in [
        (0, 0, True),
        (1, 1, False),
        (0, 1, False),
    ]:
        with running(args, env, flight, tmp_path, expose_process=True) as (conn, proc):
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT value FROM gizmosql_metrics() WHERE name='gizmosql_last_exit_unclean'"
                )
                assert cursor.fetchone() == (expected_badge,)
                cursor.execute(
                    "SELECT value FROM gizmosql_metrics() WHERE name='gizmosql_unclean_exits_total'"
                )
                assert cursor.fetchone() == (expected_count,)
            if crash:
                conn.close()
                proc.kill()
                proc.wait(timeout=10)


@pytest.mark.parametrize("license_kind", ["none", "other_features"])
@pytest.mark.parametrize("enable_source", ["cli", "env"])
def test_unlicensed_enablement_fails(tmp_path, license_kind, enable_source):
    license_path = (
        artifact("GIZMOSQL_TEST_NON_METRICS_LICENSE")
        if license_kind == "other_features"
        else None
    )
    args, env, _, _ = command(tmp_path, license_path)
    if enable_source == "cli":
        args += ["--enable-metrics", "true"]
    else:
        env["GIZMOSQL_ENABLE_METRICS"] = "true"
    result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert "'metrics'" in result.stderr and "license" in result.stderr.lower()


@pytest.mark.parametrize("license_kind", ["none", "other_features", "metrics"])
def test_disabled_sql_returns_explicit_error(tmp_path, license_kind):
    license_path = (
        None
        if license_kind == "none"
        else artifact(
            "GIZMOSQL_TEST_METRICS_LICENSE"
            if license_kind == "metrics"
            else "GIZMOSQL_TEST_NON_METRICS_LICENSE"
        )
    )
    args, env, flight, metrics = command(tmp_path, license_path)
    args += ["--enable-metrics", "false"]
    env["GIZMOSQL_ENABLE_METRICS"] = "true"  # Explicit false wins.
    with running(args, env, flight, tmp_path) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT value FROM gizmosql_settings() WHERE name='gizmosql.enable_metrics'"
            )
            assert cursor.fetchone() == ("false",)
            with pytest.raises(
                Exception, match="disabled" if license_kind == "metrics" else "license"
            ):
                cursor.execute("SELECT * FROM gizmosql_metrics()")
        with socket.socket() as sock:
            assert sock.connect_ex(("127.0.0.1", metrics)) != 0


@pytest.mark.parametrize("http_enabled", [False, True])
def test_metrics_license_enables_sql_and_optional_http(tmp_path, http_enabled):
    args, env, flight, metrics = command(
        tmp_path, artifact("GIZMOSQL_TEST_METRICS_LICENSE")
    )
    if not http_enabled:
        args[args.index("--metrics-port") + 1] = "0"
    env["GIZMOSQL_ENABLE_METRICS"] = "yes"
    with running(args, env, flight, tmp_path) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT value FROM gizmosql_settings() WHERE name='gizmosql.enable_metrics'"
            )
            assert cursor.fetchone() == ("true",)
            cursor.execute(
                "SELECT value FROM gizmosql_metrics() WHERE name='gizmosql_session_limit'"
            )
            assert cursor.fetchone() == (0.0,)
        if http_enabled:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{metrics}/metrics", timeout=5
            ) as response:
                assert response.status == 200
                assert b"gizmosql_session_limit 0\n" in response.read()
        else:
            with socket.socket() as sock:
                assert sock.connect_ex(("127.0.0.1", metrics)) != 0
