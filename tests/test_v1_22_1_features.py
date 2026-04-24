#!/usr/bin/env python3
"""
Regression tests for v1.22.1 server changes.

Covers:

1. ``_gizmosql_system.main.gizmosql_index_info`` now UNIONs PRIMARY KEY and
   UNIQUE constraints (via duckdb_constraints()) on top of duckdb_indexes().
   Previously the view only surfaced user-created CREATE INDEX indexes, so
   PK-backing and UNIQUE-backing indexes were missing — breaking DBeaver's
   Data Editor, which needs a unique index to enable row-level editing.

2. The schema returned by Flight SQL ``GetTables`` with include_schema=true
   now carries real per-column metadata pulled from duckdb_columns():
       * Field.nullable  — mirrors the NOT NULL constraint
       * ARROW:FLIGHT:SQL:REMARKS         — column comment
       * ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT — "1"/"0" (nextval default)
       * GIZMOSQL:COLUMN_DEFAULT          — vendor-specific default expression

   End-to-end JDBC coverage for these lives in the gizmosql-jdbc-driver repo's
   ``GizmoSqlIntegrationIT.testGetColumnsEnrichment``. This script covers the
   server-level SQL/catalog contract directly.

Requirements:
    pip install adbc-driver-gizmosql pyarrow

Usage:
    # Start GizmoSQL server first (DuckDB backend), then:
    python tests/test_v1_22_1_features.py
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


def test_index_info_includes_primary_key_and_unique():
    """PK + UNIQUE constraints must surface in gizmosql_index_info as unique indexes."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS v1221_t CASCADE")
        cur.execute(
            "CREATE TABLE v1221_t("
            "  id INT PRIMARY KEY,"
            "  code VARCHAR UNIQUE,"
            "  label VARCHAR"
            ")"
        )
        cur.execute("CREATE INDEX v1221_t_label_idx ON v1221_t(label)")

        cur.execute(
            'SELECT "INDEX_NAME", "NON_UNIQUE", "COLUMN_NAME" '
            'FROM _gizmosql_system.main.gizmosql_index_info '
            'WHERE "TABLE_NAME" = \'v1221_t\' '
            'ORDER BY "INDEX_NAME", "ORDINAL_POSITION"'
        )
        rows = cur.fetchall()

        # Must include: the PK (unique), the UNIQUE constraint (unique), and the
        # user index (non-unique). Constraint names are DuckDB defaults.
        pk_rows = [r for r in rows if r[1] is False and r[2] == "id"]
        uk_rows = [r for r in rows if r[1] is False and r[2] == "code"]
        # DuckDB quotes the expression as `"label"` because `label` is a reserved
        # word, so our str_split of `expressions` produces `'"label"'`. Accept both.
        ix_rows = [r for r in rows if r[1] is True and r[2].strip('"\'') == "label"]

        assert pk_rows, f"PK-backing unique index missing from view: {rows}"
        assert uk_rows, f"UNIQUE-backing unique index missing from view: {rows}"
        assert ix_rows, f"user CREATE INDEX row missing from view: {rows}"

        cur.execute("DROP TABLE v1221_t CASCADE")
    print("  OK: gizmosql_index_info unions PK + UNIQUE constraints")


def _get_field_metadata(conn, table_name):
    """Fetch per-field metadata (keys ARROW:FLIGHT:SQL:*, GIZMOSQL:*) from the table
    schema that GizmoSQL serializes into the Flight SQL GetTables reply.

    We piggy-back on the ADBC driver's adbc_get_table_schema(), which deserializes
    the same schema bytes the JDBC driver reads.
    """
    # Connection-level helper returning a pyarrow.Schema.
    pa_schema = conn.adbc_get_table_schema(table_name)
    return {f.name: (f.nullable, dict(f.metadata or {})) for f in pa_schema}


def test_table_schema_column_metadata():
    """GetTables.include_schema=true must carry real NOT NULL / comments / defaults."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS v1221_meta CASCADE")
        cur.execute("DROP SEQUENCE IF EXISTS v1221_meta_seq")
        cur.execute("CREATE SEQUENCE v1221_meta_seq START 1")
        cur.execute(
            "CREATE TABLE v1221_meta("
            "  id INT PRIMARY KEY DEFAULT nextval('v1221_meta_seq'),"
            "  name VARCHAR NOT NULL,"
            "  salary DECIMAL(10,2) DEFAULT 50000.00,"
            "  note VARCHAR"
            ")"
        )
        cur.execute("COMMENT ON COLUMN v1221_meta.name IS 'employee name'")

    # Force metadata read via a fresh connection (ADBC caches schemas per-statement).
    with _connect() as conn:
        fields = _get_field_metadata(conn, "v1221_meta")

    id_nullable, id_md = fields["id"]
    name_nullable, name_md = fields["name"]
    salary_nullable, salary_md = fields["salary"]
    note_nullable, note_md = fields["note"]

    # ------- nullable (Field.nullable) -------
    assert id_nullable is False, "PK column should be NOT NULL"
    assert name_nullable is False, "explicit NOT NULL column should be NOT NULL"
    assert salary_nullable is True, "unconstrained column should be nullable"
    assert note_nullable is True

    # ------- ARROW:FLIGHT:SQL:REMARKS -------
    # Metadata keys in pyarrow come back as bytes; compare after decoding.
    def _get(md, k):
        v = md.get(k) or md.get(k.encode()) or md.get(k, None)
        if isinstance(v, bytes):
            return v.decode("utf-8")
        return v

    assert _get(name_md, "ARROW:FLIGHT:SQL:REMARKS") == "employee name", (
        f"expected comment on 'name', got: {name_md}"
    )
    # Columns without a comment should NOT have the REMARKS key set.
    assert _get(note_md, "ARROW:FLIGHT:SQL:REMARKS") is None

    # ------- ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT -------
    assert _get(id_md, "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT") == "1", (
        f"id (nextval default) should be auto-increment: {id_md}"
    )
    assert _get(name_md, "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT") == "0"
    assert _get(salary_md, "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT") == "0"

    # ------- GIZMOSQL:COLUMN_DEFAULT -------
    # Only present when the column has a default.
    assert "nextval" in (_get(id_md, "GIZMOSQL:COLUMN_DEFAULT") or "").lower(), (
        f"id default should be visible: {id_md}"
    )
    assert "50000" in (_get(salary_md, "GIZMOSQL:COLUMN_DEFAULT") or ""), (
        f"salary default should be visible: {salary_md}"
    )
    assert _get(name_md, "GIZMOSQL:COLUMN_DEFAULT") is None, (
        f"name has no default, key should be absent: {name_md}"
    )
    assert _get(note_md, "GIZMOSQL:COLUMN_DEFAULT") is None

    with _connect() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE v1221_meta CASCADE")
        cur.execute("DROP SEQUENCE v1221_meta_seq")
    print("  OK: table schema carries nullable / REMARKS / IS_AUTO_INCREMENT / COLUMN_DEFAULT")


def main() -> int:
    failures = 0
    for fn in (
        test_index_info_includes_primary_key_and_unique,
        test_table_schema_column_metadata,
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
