#!/usr/bin/env python3
"""
Regression tests for v1.22.0 server changes.

Covers the server-side features added in v1.22.0:

1. The ``_gizmosql_system`` system catalog exposes a ``gizmosql_index_info``
   view whose schema matches JDBC's ``DatabaseMetaData.getIndexInfo()``.

2. Same catalog exposes ``gizmosql_view_definition`` for retrieving CREATE
   VIEW DDL text.

(Regression coverage for the DECIMAL prepared-statement parameter fix from
v1.22.0 lives in the GizmoSQL JDBC driver repo's integration tests —
``GizmoSqlIntegrationIT.testDecimalParameterRoundTrip`` — which exercises the
server's prepared-statement path through the Arrow Flight SQL JDBC client
against ``gizmodata/gizmosql:latest`` on every JDBC-driver CI run.)

Requirements:
    pip install adbc-driver-gizmosql pyarrow

Usage:
    # Start GizmoSQL server first (DuckDB backend), then:
    python tests/test_v1_22_features.py
"""

import os
import sys
import traceback


def _connect():
    from adbc_driver_gizmosql import dbapi as gizmosql

    host = os.getenv("GIZMOSQL_HOST", "localhost")
    port = os.getenv("GIZMOSQL_PORT", "31337")
    username = os.getenv("GIZMOSQL_USERNAME", "gizmosql_user")
    password = os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password")
    use_tls = os.getenv("TLS_ENABLED", "0") == "1"
    uri = f"grpc+tls://{host}:{port}" if use_tls else f"grpc://{host}:{port}"

    kwargs = {"username": username, "password": password, "autocommit": True}
    if use_tls:
        kwargs["tls_skip_verify"] = True
    return gizmosql.connect(uri, **kwargs)


def test_system_catalog_index_info_view():
    """The `_gizmosql_system.main.gizmosql_index_info` view must return JDBC-shaped rows."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS v122_ix CASCADE")
        cur.execute("CREATE TABLE v122_ix(a INT, b INT, c VARCHAR)")
        cur.execute("CREATE INDEX v122_ix_ab ON v122_ix(a, b)")
        cur.execute("CREATE UNIQUE INDEX v122_ix_c ON v122_ix(c)")

        cur.execute(
            'SELECT "TABLE_NAME", "NON_UNIQUE", "INDEX_NAME", "ORDINAL_POSITION", '
            '       "COLUMN_NAME", "ASC_OR_DESC", "TYPE" '
            'FROM _gizmosql_system.main.gizmosql_index_info '
            'WHERE "TABLE_NAME" = \'v122_ix\' '
            'ORDER BY "INDEX_NAME", "ORDINAL_POSITION"'
        )
        rows = cur.fetchall()

        # Expect: (ix_ab, a, 1), (ix_ab, b, 2), (ix_c, c, 1)
        assert len(rows) == 3, f"expected 3 rows, got {len(rows)}: {rows}"

        ix_ab_rows = [r for r in rows if r[2] == "v122_ix_ab"]
        ix_c_rows = [r for r in rows if r[2] == "v122_ix_c"]

        assert len(ix_ab_rows) == 2
        assert ix_ab_rows[0][4] == "a" and ix_ab_rows[0][3] == 1
        assert ix_ab_rows[1][4] == "b" and ix_ab_rows[1][3] == 2
        assert ix_ab_rows[0][1] is True, "compound index should have NON_UNIQUE=true"

        assert len(ix_c_rows) == 1
        assert ix_c_rows[0][4] == "c" and ix_c_rows[0][3] == 1
        assert ix_c_rows[0][1] is False, "unique index should have NON_UNIQUE=false"

        # TYPE should be tableIndexOther (3) per the JDBC contract.
        assert all(r[6] == 3 for r in rows), f"unexpected TYPE values: {[r[6] for r in rows]}"
        assert all(r[5] == "A" for r in rows), "ASC_OR_DESC should be 'A'"

        cur.execute("DROP TABLE v122_ix CASCADE")

    print("  OK: _gizmosql_system.main.gizmosql_index_info")


def test_system_catalog_view_definition():
    """`_gizmosql_system.main.gizmosql_view_definition` returns the DDL for a known view."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS v122_vt CASCADE")
        cur.execute("CREATE TABLE v122_vt(x INT)")
        cur.execute("CREATE OR REPLACE VIEW v122_vv AS SELECT x + 1 AS x1 FROM v122_vt")

        cur.execute(
            'SELECT "VIEW_DEFINITION" FROM _gizmosql_system.main.gizmosql_view_definition '
            'WHERE "TABLE_NAME" = \'v122_vv\''
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        ddl = rows[0][0]
        assert "CREATE" in ddl.upper(), f"DDL missing CREATE: {ddl!r}"
        assert "v122_vv" in ddl, f"DDL missing view name: {ddl!r}"

        cur.execute("DROP VIEW v122_vv")
        cur.execute("DROP TABLE v122_vt")

    print("  OK: _gizmosql_system.main.gizmosql_view_definition")


def main() -> int:
    failures = 0
    for fn in (
        test_system_catalog_index_info_view,
        test_system_catalog_view_definition,
    ):
        print(f"\n{fn.__name__}")
        try:
            fn()
        except Exception:
            failures += 1
            traceback.print_exc()
    print("\n" + ("FAILED" if failures else "PASSED"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
