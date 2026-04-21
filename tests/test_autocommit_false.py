#!/usr/bin/env python3
"""
Regression test for https://github.com/gizmodata/gizmosql/issues/155
and companion coverage for ADBC autocommit=False behavior.

Exercises basic DDL + DML under ``autocommit=False`` through both
commit paths that application code might take:

    1. ``conn.commit()`` — Flight SQL ``ActionEndTransaction`` (commit),
       routed through ``DoAction`` on the server.
    2. Explicit SQL ``COMMIT`` — issued via ``cursor.execute("COMMIT")``,
       routed through ``DoPutCommandStatementUpdate`` on the server.

Also exercises ``conn.rollback()`` so we verify that uncommitted writes
are discarded as expected.

Requirements:
    pip install adbc-driver-gizmosql pyarrow

Usage:
    # Start GizmoSQL server first, then:
    python tests/test_autocommit_false.py
"""

import os
import sys
import traceback
import uuid


def _connect(autocommit: bool):
    from adbc_driver_gizmosql import dbapi as gizmosql

    host = os.getenv("GIZMOSQL_HOST", "localhost")
    port = os.getenv("GIZMOSQL_PORT", "31337")
    username = os.getenv("GIZMOSQL_USERNAME", "gizmosql_user")
    password = os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password")
    use_tls = os.getenv("TLS_ENABLED", "0") == "1"

    uri = f"grpc+tls://{host}:{port}" if use_tls else f"grpc://{host}:{port}"

    kwargs = {"username": username, "password": password}
    if use_tls:
        kwargs["tls_skip_verify"] = True

    return gizmosql.connect(uri, autocommit=autocommit, **kwargs)


def _drop_if_exists(table: str) -> None:
    with _connect(autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")


def _count_rows(table: str) -> int:
    with _connect(autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def test_conn_commit_path() -> bool:
    """CREATE TABLE + INSERT under autocommit=False, committed via conn.commit()."""
    table = f"it155_conn_commit_{uuid.uuid4().hex[:8]}"
    print(f"\n[conn.commit() path] table: {table}")

    try:
        with _connect(autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {table} (id INTEGER, name VARCHAR)"
                )
                cur.execute(
                    f"INSERT INTO {table} VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')"
                )
            conn.commit()
            print("  conn.commit() succeeded")

        observed = _count_rows(table)
        print(f"  post-commit row count: {observed}")
        assert observed == 3, f"expected 3 rows after commit, got {observed}"
        return True
    finally:
        _drop_if_exists(table)


def test_sql_commit_path() -> bool:
    """CREATE TABLE + INSERT under autocommit=False, committed via SQL COMMIT."""
    table = f"it155_sql_commit_{uuid.uuid4().hex[:8]}"
    print(f"\n[SQL COMMIT path] table: {table}")

    try:
        with _connect(autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {table} (id INTEGER, name VARCHAR)"
                )
                cur.execute(
                    f"INSERT INTO {table} VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')"
                )
                cur.execute("COMMIT")
                print("  cursor.execute('COMMIT') succeeded")

        observed = _count_rows(table)
        print(f"  post-commit row count: {observed}")
        assert observed == 3, f"expected 3 rows after commit, got {observed}"
        return True
    finally:
        _drop_if_exists(table)


def test_rollback_path() -> bool:
    """CREATE TABLE + INSERT under autocommit=False, discarded via conn.rollback()."""
    table = f"it155_rollback_{uuid.uuid4().hex[:8]}"
    print(f"\n[rollback path] table: {table}")

    try:
        with _connect(autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {table} (id INTEGER, name VARCHAR)"
                )
                cur.execute(f"INSERT INTO {table} VALUES (1, 'alice')")
            conn.rollback()
            print("  conn.rollback() succeeded")

        # After rollback the table should not exist.
        from adbc_driver_manager import OperationalError

        try:
            observed = _count_rows(table)
        except OperationalError:
            print("  table does not exist post-rollback (expected)")
            return True

        raise AssertionError(
            f"expected table {table} to be rolled back, but it has {observed} rows"
        )
    finally:
        _drop_if_exists(table)


def main() -> int:
    print("=" * 60)
    print("autocommit=False regression test (issue #155)")
    print("=" * 60)

    checks = [
        ("conn.commit() path", test_conn_commit_path),
        ("SQL COMMIT path", test_sql_commit_path),
        ("rollback path", test_rollback_path),
    ]

    results: list[tuple[str, bool, str | None]] = []
    for name, fn in checks:
        try:
            results.append((name, fn(), None))
        except ImportError as e:
            print(f"\nMissing required package: {e}")
            print("  pip install adbc-driver-gizmosql pyarrow")
            return 1
        except Exception as e:
            traceback.print_exc()
            results.append((name, False, str(e)))

    print("\n" + "=" * 60)
    all_ok = True
    for name, ok, err in results:
        status = "PASS" if ok else "FAIL"
        suffix = f" — {err}" if err else ""
        print(f"  [{status}] {name}{suffix}")
        all_ok = all_ok and ok
    print("=" * 60)

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
