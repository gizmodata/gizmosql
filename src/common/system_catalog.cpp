#include "system_catalog.h"

namespace gizmosql {

std::string GetSystemCatalogInitSql() {
  return
      "ATTACH ':memory:' AS _gizmosql_system;"

      // JDBC DatabaseMetaData.getIndexInfo() — one row per (index, column)
      // with the exact column names/types defined by the JDBC contract.
      //
      // Sources:
      //   * duckdb_indexes()     — user-defined CREATE INDEX indexes. Its
      //     `expressions` column is a VARCHAR rendering of a list (e.g. "[a, b]"),
      //     so we strip the brackets and split on ", ".
      //   * duckdb_constraints() — PRIMARY KEY + UNIQUE constraints. DuckDB
      //     backs these with unique indexes internally, but does NOT list them
      //     in duckdb_indexes(). UNION ALL so they show up in getIndexInfo
      //     (DBeaver's Data Editor needs a unique index to enable row-level
      //     editing). `constraint_column_names` is already VARCHAR[] here.
      "CREATE OR REPLACE VIEW _gizmosql_system.main.gizmosql_index_info AS "
      "WITH idx_src AS ("
      "  SELECT di.database_name, di.schema_name, di.table_name,"
      "         di.is_unique, di.index_name,"
      "         str_split(trim(BOTH '[]' FROM di.expressions), ', ') AS cols"
      "  FROM duckdb_indexes() di"
      "), constraint_src AS ("
      "  SELECT dc.database_name, dc.schema_name, dc.table_name,"
      "         TRUE AS is_unique,"
      "         dc.constraint_name AS index_name,"
      "         dc.constraint_column_names AS cols"
      "  FROM duckdb_constraints() dc"
      "  WHERE dc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')"
      "), all_src AS ("
      "  SELECT * FROM idx_src UNION ALL SELECT * FROM constraint_src"
      ") "
      "SELECT database_name           AS \"TABLE_CAT\","
      "       schema_name             AS \"TABLE_SCHEM\","
      "       table_name              AS \"TABLE_NAME\","
      "       NOT is_unique           AS \"NON_UNIQUE\","
      "       CAST(NULL AS VARCHAR)   AS \"INDEX_QUALIFIER\","
      "       index_name              AS \"INDEX_NAME\","
      "       CAST(3 AS SMALLINT)     AS \"TYPE\","       // tableIndexOther
      "       idx::SMALLINT           AS \"ORDINAL_POSITION\","
      "       cols[idx]               AS \"COLUMN_NAME\","
      "       CAST('A' AS VARCHAR)    AS \"ASC_OR_DESC\","
      "       CAST(NULL AS BIGINT)    AS \"CARDINALITY\","
      "       CAST(NULL AS BIGINT)    AS \"PAGES\","
      "       CAST(NULL AS VARCHAR)   AS \"FILTER_CONDITION\" "
      "FROM all_src, generate_series(1, len(cols)) AS t(idx);"

      // View DDL, keyed by (catalog, schema, view name).
      "CREATE OR REPLACE VIEW _gizmosql_system.main.gizmosql_view_definition AS "
      "SELECT database_name AS \"TABLE_CAT\","
      "       schema_name   AS \"TABLE_SCHEM\","
      "       view_name     AS \"TABLE_NAME\","
      "       sql           AS \"VIEW_DEFINITION\" "
      "FROM duckdb_views();";
}

}  // namespace gizmosql
