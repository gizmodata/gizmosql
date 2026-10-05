// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "duckdb_tables_schema_batch_reader.h"

#include <duckdb.h>

#include <set>
#include <sstream>
#include <unordered_map>

#include <arrow/array/builder_binary.h>
#include <arrow/flight/sql/column_metadata.h>
#include <arrow/flight/sql/server.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>

#include "duckdb_compat.h"
#include "flight_sql_fwd.h"
#include "gizmosql_logging.h"

using arrow::Status;

namespace gizmosql::ddb {

namespace {

// What DatabaseMetaData.getColumns() needs that `SELECT * ... LIMIT 0` lacks.
struct ColumnInfo {
  bool is_nullable = true;     // Default to nullable when unknown.
  std::string comment;         // Empty = no comment.
  std::string column_default;  // Empty = no default.
  bool has_default = false;
  std::string type_name;       // Base DuckDB type name (e.g. "DECIMAL", "DOUBLE").
};

// DuckDB's type string includes parameters / decorations ("DECIMAL(18,3)",
// "INTEGER[]", "STRUCT(a INTEGER)"). Strip it to the base type name so
// consumers (Power BI's ADBC connector matches on literals like "DECIMAL",
// "DOUBLE", "INTEGER", "VARCHAR") see the canonical name.
std::string BaseTypeName(std::string type) {
  size_t end = type.find_first_of("([");
  if (end == std::string::npos) end = type.find(' ');
  if (end != std::string::npos) type.resize(end);
  return type;
}

// Column metadata of ONE table or view, read from its own catalog entry with
// the same rules duckdb_columns() applies (its TableColumnHelper /
// ViewColumnHelper). duckdb_columns() itself enumerates every attached
// catalog before filtering, so with DuckLake / PostgreSQL catalogs attached a
// table browse cost one metadata-store round trip per table per catalog.
// Best-effort: on any error the schema simply goes out without the extras.
std::unordered_map<std::string, ColumnInfo> CollectColumnInfo(
    duckdb::ClientContext& context, const std::string& catalog_name,
    const std::string& schema_name, const std::string& table_name) {
  std::unordered_map<std::string, ColumnInfo> info_by_column;
  try {
    context.RunFunctionInTransaction([&]() {
      auto catalog = duckdb::Catalog::GetCatalogEntry(context, compat::Name(catalog_name));
      if (!catalog) return;
      auto entry = catalog->GetEntry(context, duckdb::CatalogType::TABLE_ENTRY,
                                     compat::Name(schema_name), compat::Name(table_name),
                                     duckdb::OnEntryNotFound::RETURN_NULL);
      if (!entry) {
        entry = catalog->GetEntry(context, duckdb::CatalogType::VIEW_ENTRY,
                                  compat::Name(schema_name), compat::Name(table_name),
                                  duckdb::OnEntryNotFound::RETURN_NULL);
      }
      if (!entry) return;

      if (entry->type == duckdb::CatalogType::TABLE_ENTRY) {
        auto& table = entry->Cast<duckdb::TableCatalogEntry>();
        std::set<duckdb::idx_t> not_null_columns;
        for (auto& constraint : table.GetConstraints()) {
          if (constraint->type == duckdb::ConstraintType::NOT_NULL) {
            not_null_columns.insert(
                constraint->Cast<duckdb::NotNullConstraint>().index.index);
          }
        }
        for (auto& column : table.GetColumns().Logical()) {
          ColumnInfo info;
          info.is_nullable = not_null_columns.count(column.Logical().index) == 0;
          if (!column.Comment().IsNull()) info.comment = column.Comment().ToString();
          if (column.Generated()) {
            info.column_default = column.GeneratedExpression().ToString();
            info.has_default = true;
          } else if (column.HasDefaultValue()) {
            info.column_default = column.DefaultValue().ToString();
            info.has_default = true;
          }
          info.type_name = BaseTypeName(column.Type().ToString());
          info_by_column.emplace(compat::Str(column.Name()), std::move(info));
        }
      } else if (entry->type == duckdb::CatalogType::VIEW_ENTRY) {
        auto& view = entry->Cast<duckdb::ViewCatalogEntry>();
        duckdb::vector<std::string> names;
        duckdb::vector<duckdb::LogicalType> types;
#if GIZMOSQL_DUCKDB_CHANNEL_LTS
        names = view.names;
        types = view.types;
#else
        try {
          view.BindView(context);
        } catch (std::exception&) {
        }
        auto columns = view.GetColumnInfo();
        if (!columns) return;  // unbound view: duckdb_columns() reports no columns either
        names = compat::Strs(columns->names);
        types = columns->types;
        duckdb::QueryResult::DeduplicateColumns(names);
#endif
        for (duckdb::idx_t col = 0; col < types.size(); ++col) {
          ColumnInfo info;  // views: nullable, no default
#if GIZMOSQL_DUCKDB_CHANNEL_LTS
          if (col < view.column_comments.size() && !view.column_comments[col].IsNull()) {
            info.comment = view.column_comments[col].ToString();
          }
#else
          const auto comment = view.GetColumnComment(col);
          if (!comment.IsNull()) info.comment = comment.ToString();
#endif
          info.type_name = BaseTypeName(types[col].ToString());
          const std::string name =
              col < view.aliases.size() ? compat::Str(view.aliases[col]) : names[col];
          info_by_column.emplace(name, std::move(info));
        }
      }
    });
  } catch (std::exception&) {
    info_by_column.clear();
  }
  return info_by_column;
}

}  // namespace
std::shared_ptr<arrow::Schema> DuckDBTablesWithSchemaBatchReader::schema() const {
  return flight::sql::SqlSchema::GetTablesSchemaWithIncludedSchema();
}

Status DuckDBTablesWithSchemaBatchReader::ReadNext(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (already_executed_) {
    *batch = NULLPTR;
    return Status::OK();
  } else {
    std::shared_ptr<arrow::RecordBatch> first_batch;

    ARROW_RETURN_NOT_OK(reader_->ReadNext(&first_batch));

    if (!first_batch) {
      *batch = NULLPTR;
      return Status::OK();
    }

    const std::shared_ptr<arrow::Array> catalog_name_array =
        first_batch->GetColumnByName("catalog_name");
    const std::shared_ptr<arrow::Array> schema_name_array =
        first_batch->GetColumnByName("db_schema_name");
    const std::shared_ptr<arrow::Array> table_name_array =
        first_batch->GetColumnByName("table_name");

    arrow::BinaryBuilder schema_builder;

    auto* catalog_name_string_array =
        reinterpret_cast<arrow::StringArray*>(catalog_name_array.get());
    auto* schema_name_string_array =
        reinterpret_cast<arrow::StringArray*>(schema_name_array.get());
    auto* table_name_string_array =
        reinterpret_cast<arrow::StringArray*>(table_name_array.get());

    for (int i = 0; i < table_name_array->length(); i++) {
      const std::string& catalog_name = catalog_name_string_array->GetString(i);
      const std::string& schema_name = schema_name_string_array->GetString(i);
      const std::string& table_name = table_name_string_array->GetString(i);

      // Just get the schema from a prepared statement
      std::shared_ptr<DuckDBStatement> table_schema_statement;
      ARROW_ASSIGN_OR_RAISE(
          table_schema_statement,
          DuckDBStatement::Create(client_session_,
                                  "SELECT * FROM \"" + catalog_name + "\".\"" +
                                      schema_name + "\".\"" + table_name + "\"" +
                                      " LIMIT 0",
                                  arrow::util::ArrowLogLevel::ARROW_DEBUG,
                                  false, nullptr, "DoGetTables", true));

      ARROW_ASSIGN_OR_RAISE(auto table_schema, table_schema_statement->GetSchema());

      // Enrich the schema with per-column metadata that DuckDB's `SELECT * FROM t LIMIT 0`
      // does *not* carry: the real NOT NULL constraint (Arrow Field.nullable describes the
      // query result, not the underlying column), column comments, default expressions,
      // and auto-increment detection. DBeaver and other JDBC clients surface these through
      // DatabaseMetaData.getColumns() — NULLABLE/IS_NULLABLE, REMARKS, COLUMN_DEF,
      // IS_AUTOINCREMENT. The Arrow Flight SQL JDBC driver maps the standard
      // ARROW:FLIGHT:SQL:* metadata keys for REMARKS and IS_AUTO_INCREMENT. For
      // COLUMN_DEF we use a GizmoSQL-prefixed key that the GizmoSQL JDBC driver reads.
      {
        const auto info_by_column =
            CollectColumnInfo(*client_session_->connection->Get().context, catalog_name,
                              schema_name, table_name);

        if (!info_by_column.empty()) {
          arrow::FieldVector corrected_fields;
          corrected_fields.reserve(table_schema->num_fields());
          bool any_changed = false;
          for (const auto& f : table_schema->fields()) {
            auto it = info_by_column.find(f->name());
            if (it == info_by_column.end()) {
              corrected_fields.emplace_back(f);
              continue;
            }
            const ColumnInfo& info = it->second;

            // Build the metadata map, preserving any existing field metadata.
            std::unordered_map<std::string, std::string> md;
            if (f->metadata()) {
              const auto& keys = f->metadata()->keys();
              const auto& vals = f->metadata()->values();
              for (size_t k = 0; k < keys.size(); ++k) {
                md[keys[k]] = vals[k];
              }
            }
            if (!info.comment.empty()) {
              md["ARROW:FLIGHT:SQL:REMARKS"] = info.comment;
            }
            // Standard Flight SQL key — read by ADBC clients (e.g. the Apache
            // FlightSQL ADBC driver populates xdbc_type_name from this).
            if (!info.type_name.empty()) {
              md["ARROW:FLIGHT:SQL:TYPE_NAME"] = info.type_name;
            }
            // DuckDB's auto-increment idiom is a DEFAULT of `nextval('...')`. Mark the
            // column as IS_AUTO_INCREMENT so the JDBC driver can surface it as "YES".
            {
              const std::string& d = info.column_default;
              bool is_auto_increment = false;
              if (!d.empty()) {
                // Skip leading whitespace, then case-insensitive match of "nextval(".
                size_t pos = d.find_first_not_of(" \t\n\r");
                if (pos != std::string::npos && pos + 8 <= d.size()) {
                  const char* nv = "nextval(";
                  bool matches = true;
                  for (size_t k = 0; k < 8; ++k) {
                    char c = d[pos + k];
                    if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + 32);
                    if (c != nv[k]) {
                      matches = false;
                      break;
                    }
                  }
                  is_auto_increment = matches;
                }
              }
              md["ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT"] = is_auto_increment ? "1" : "0";
            }
            // Vendor-prefixed key — consumed by the GizmoSQL JDBC driver's getColumns
            // override to populate the JDBC COLUMN_DEF column. Only set when there is a
            // default; absence of the key distinguishes "no default" from "empty default".
            if (info.has_default) {
              md["GIZMOSQL:COLUMN_DEFAULT"] = info.column_default;
            }

            auto new_metadata = arrow::key_value_metadata(md);
            auto new_field = arrow::field(f->name(), f->type(), info.is_nullable,
                                          std::move(new_metadata));
            corrected_fields.emplace_back(new_field);
            if (f->nullable() != info.is_nullable || !info.comment.empty() ||
                info.has_default || md.count("ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT") ||
                !info.type_name.empty()) {
              any_changed = true;
            }
          }
          if (any_changed) {
            table_schema = arrow::schema(corrected_fields, table_schema->metadata());
          }
        }
      }

      const arrow::Result<std::shared_ptr<arrow::Buffer>>& value =
          arrow::ipc::SerializeSchema(*table_schema);

      std::shared_ptr<arrow::Buffer> schema_buffer;
      ARROW_ASSIGN_OR_RAISE(schema_buffer, value);

      ARROW_RETURN_NOT_OK(schema_builder.Append(::std::string_view(*schema_buffer)));
    }

    std::shared_ptr<arrow::Array> schema_array;
    ARROW_RETURN_NOT_OK(schema_builder.Finish(&schema_array));

    ARROW_ASSIGN_OR_RAISE(*batch,
                          first_batch->AddColumn(4, "table_schema", schema_array));

    // Conform the batch to the expected schema using RecordBatch selection
    const auto& expected_schema = schema();

    std::vector<std::shared_ptr<arrow::Array>> conformed_arrays;
    conformed_arrays.reserve(expected_schema->num_fields());

    for (const auto& expected_field : expected_schema->fields()) {
      int column_index = (*batch)->schema()->GetFieldIndex(expected_field->name());

      if (column_index == -1) {
        return arrow::Status::Invalid("Expected column '", expected_field->name(),
                                      "' not found in result batch");
      }

      conformed_arrays.push_back((*batch)->column(column_index));
    }

    // Create new batch with columns in the correct order and with the expected schema
    *batch =
        arrow::RecordBatch::Make(expected_schema, (*batch)->num_rows(), conformed_arrays);

    already_executed_ = true;

    return Status::OK();
  }
}
}  // namespace gizmosql::ddb