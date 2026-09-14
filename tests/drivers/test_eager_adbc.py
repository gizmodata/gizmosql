"""Run against the branch server; both ADBC packages exercise the same ledger.

GIZMOSQL_TEST_URI=grpc://127.0.0.1:31582 pytest tests/drivers/test_eager_adbc.py
"""

import importlib
import os
import uuid

import adbc_driver_manager
import pyarrow as pa
import pytest


@pytest.fixture(
    params=os.environ.get(
        "GIZMOSQL_TEST_ADBC_DRIVERS", "adbc_driver_flightsql,adbc_driver_gizmosql"
    ).split(",")
)
def conn(request):
    module = importlib.import_module(request.param + ".dbapi")
    uri = os.environ.get("GIZMOSQL_TEST_URI", "grpc://127.0.0.1:31582")
    auth = {
        "username": os.environ.get("GIZMOSQL_TEST_USERNAME", "eager_test"),
        "password": os.environ.get("GIZMOSQL_TEST_PASSWORD", "eager_test_password"),
    }
    kwargs = {"db_kwargs": auth} if request.param == "adbc_driver_flightsql" else auth
    with module.connect(uri, autocommit=True, **kwargs) as connection:
        yield connection


def scalar(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


@pytest.fixture
def ledger(conn):
    name = "eager_" + uuid.uuid4().hex
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {name} (token BIGINT PRIMARY KEY, n BIGINT)")
    yield name
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE {name}")


def test_close_without_fetch(conn, ledger):
    for token in range(10):
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {ledger} VALUES ({token}, {token * 7})")
    assert scalar(conn, f"SELECT count(*) FROM {ledger}") == 10
    assert scalar(conn, f"SELECT sum(n) FROM {ledger}") == 315


def test_bound_execute_without_fetch(conn, ledger):
    with conn.cursor() as cur:
        for token in range(10):
            cur.execute(f"INSERT INTO {ledger} VALUES (?, ?)", (token, token * 7))
    assert scalar(conn, f"SELECT count(*) FROM {ledger}") == 10
    assert scalar(conn, f"SELECT sum(n) FROM {ledger}") == 315


def test_one_prepare_many_bind_and_execute(conn, ledger):
    # Use the ADBC API directly to guarantee exactly ONE prepare, independent
    # of the DBAPI facade's automatic statement caching/routing decisions.
    with adbc_driver_manager.AdbcStatement(conn.adbc_connection) as stmt:
        stmt.set_sql_query(f"INSERT INTO {ledger} VALUES (?, ?)")
        stmt.prepare()
        for token in range(20):
            stmt.bind(pa.record_batch([[token], [token * 7]], names=["token", "n"]))
            stream, _ = stmt.execute_query()
            stream.release()  # deliberately never import/drain the Arrow stream
            assert scalar(conn, f"SELECT count(*) FROM {ledger}") == token + 1
    assert scalar(conn, f"SELECT sum(n) FROM {ledger}") == 1330


def test_executemany(conn, ledger):
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {ledger} VALUES (?, ?)", [(n, n * 7) for n in range(20)]
        )
    assert scalar(conn, f"SELECT count(*) FROM {ledger}") == 20
    assert scalar(conn, f"SELECT sum(n) FROM {ledger}") == 1330


def test_updates_and_deletes(conn, ledger):
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {ledger} VALUES (1, 0), (2, 0)")
        for _ in range(10):
            cur.execute(f"UPDATE {ledger} SET n = n + 1 WHERE token = ?", (1,))
        cur.execute(f"DELETE FROM {ledger} WHERE token = ?", (2,))
    assert scalar(conn, f"SELECT n FROM {ledger}") == 10


def test_returning(conn, ledger):
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {ledger} VALUES (1, 7) RETURNING token, n")
        assert cur.fetchall() == [(1, 7)]
        cur.execute(f"UPDATE {ledger} SET n = n + 1 RETURNING n")
        assert cur.fetchall() == [(8,)]
        cur.execute(f"DELETE FROM {ledger} RETURNING token")
        assert cur.fetchall() == [(1,)]
    assert scalar(conn, f"SELECT count(*) FROM {ledger}") == 0


def test_transaction_rollback(conn, ledger):
    conn.adbc_connection.set_autocommit(False)
    try:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {ledger} VALUES (?, ?)", (1, 7))
        conn.adbc_connection.rollback()
        assert scalar(conn, f"SELECT count(*) FROM {ledger}") == 0
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {ledger} VALUES (?, ?)", (2, 14))
        conn.adbc_connection.commit()
        assert scalar(conn, f"SELECT sum(n) FROM {ledger}") == 14
    finally:
        conn.adbc_connection.set_autocommit(True)


def test_large_result_streams_in_batches(conn):
    # 50,000 rows is many DuckDB vectors (2048 rows each); the driver must
    # deliver every batch, in order, and the row count must be exact.
    with conn.cursor() as cur:
        cur.execute("SELECT range AS i, 'row-' || range AS label FROM range(50000)")
        reader = cur.fetch_record_batch()
        batches = list(reader)
    assert len(batches) > 1, "result was not streamed in batches"
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 50000
    ids = table.column("i").to_pylist()
    assert ids == list(range(50000))
    labels = table.column("label")
    assert labels[0].as_py() == "row-0" and labels[49999].as_py() == "row-49999"
