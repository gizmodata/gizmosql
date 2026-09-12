"""Exercise an unmodified ODBC driver through the platform driver manager."""

import os
import uuid
from decimal import Decimal

import pyodbc
import pytest


@pytest.fixture
def conn():
    driver = os.getenv("GIZMOSQL_TEST_ODBC_DRIVER")
    if not driver:
        pytest.skip("Set GIZMOSQL_TEST_ODBC_DRIVER to the released driver library")
    port = os.getenv("GIZMOSQL_TEST_PORT", "31582")
    with pyodbc.connect(
        f"Driver={{{driver}}};host=localhost;port={port};uid=eager_test;"
        "pwd=eager_test_password;useEncryption=false",
        autocommit=True,
        ansi=True,
        timeout=10,
    ) as connection:
        yield connection


@pytest.fixture
def ledger(conn):
    name = "odbc_ledger_" + uuid.uuid4().hex
    conn.execute(f"CREATE TABLE {name}(id INTEGER PRIMARY KEY, n BIGINT)")
    yield name
    conn.execute(f"DROP TABLE IF EXISTS {name}")


def test_no_fetch_and_repeated_bound_insert(conn, ledger):
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {ledger} VALUES (0, 0)")
    cursor.close()
    sql = f"INSERT INTO {ledger} VALUES (?, ?)"
    with conn.cursor() as cursor:
        for i in range(1, 21):
            cursor.execute(sql, i, i * 7)
            assert cursor.rowcount == 1
    assert tuple(conn.execute(f"SELECT count(*), sum(n) FROM {ledger}").fetchone()) == (
        21,
        1470,
    )


def test_update_delete_and_batch(conn, ledger):
    with conn.cursor() as cursor:
        cursor.executemany(
            f"INSERT INTO {ledger} VALUES (?, ?)", [(i, 0) for i in range(10)]
        )
        for _ in range(5):
            cursor.execute(f"UPDATE {ledger} SET n=n+1 WHERE id < ?", 4)
            assert cursor.rowcount == 4
        cursor.execute(f"DELETE FROM {ledger} WHERE id >= ?", 4)
        assert cursor.rowcount == 6
    assert tuple(conn.execute(f"SELECT count(*), sum(n) FROM {ledger}").fetchone()) == (
        4,
        20,
    )


def test_returning_and_repeated_reads(conn, ledger):
    assert tuple(
        conn.execute(f"INSERT INTO {ledger} VALUES (1, 42) RETURNING id, n").fetchone()
    ) == (1, 42)
    with conn.cursor() as cursor:
        sql = f"SELECT n FROM {ledger} WHERE id=?"
        for _ in range(20):
            assert cursor.execute(sql, 1).fetchone()[0] == 42
    assert conn.execute(f"SELECT count(*) FROM {ledger}").fetchone()[0] == 1


def test_ddl_and_basic_types(conn, ledger):
    conn.execute(f"ALTER TABLE {ledger} ADD COLUMN label VARCHAR")
    conn.execute(f"INSERT INTO {ledger} VALUES (1, 42, 'hello')")
    assert tuple(conn.execute(f"SELECT id,n,label FROM {ledger}").fetchone()) == (
        1,
        42,
        "hello",
    )
    row = conn.execute(
        "SELECT TRUE, NULL::INTEGER, 12.34::DECIMAL(8,2), DATE '2026-09-12'"
    ).fetchone()
    assert row[0] is True and row[1] is None and row[2] == Decimal("12.34")
    assert str(row[3]) == "2026-09-12"


def test_commit_and_rollback(conn, ledger):
    conn.autocommit = False
    conn.execute(f"INSERT INTO {ledger} VALUES (1, 11)")
    conn.rollback()
    assert conn.execute(f"SELECT count(*) FROM {ledger}").fetchone()[0] == 0
    conn.execute(f"INSERT INTO {ledger} VALUES (2, 22)")
    conn.commit()
    conn.autocommit = True
    assert conn.execute(f"SELECT sum(n) FROM {ledger}").fetchone()[0] == 22
