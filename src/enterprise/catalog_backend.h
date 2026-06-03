// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

#include <duckdb.hpp>

#include <string>

#include <arrow/status.h>

namespace gizmosql {

// The storage backend an attached catalog resolves to. Shared by the
// instrumentation manager and the catalog log sink so both detect the backend
// and branch their schema dialect identically.
enum class CatalogBackend { kDuckDBFile, kPostgres, kDuckLake };

// RAII transaction guard for a dedicated catalog writer connection. BEGINs on
// construction (throwing if BEGIN fails) and, unless Commit() succeeds first,
// ROLLBACKs on destruction. This guarantees the writer connection is never
// parked inside an open transaction (which surfaces as a long-lived
// "idle in transaction" session on a DuckLake/PostgreSQL catalog) and that a
// failed write/batch is rolled back rather than relying on auto-commit.
class CatalogTxnGuard {
 public:
  explicit CatalogTxnGuard(duckdb::Connection& conn) : conn_(conn) {
    conn_.BeginTransaction();
    active_ = true;
  }

  ~CatalogTxnGuard() {
    if (active_) {
      try {
        conn_.Rollback();
      } catch (...) {
        // Nothing actionable from a destructor; the connection returns to
        // auto-commit on its next statement regardless.
      }
    }
  }

  CatalogTxnGuard(const CatalogTxnGuard&) = delete;
  CatalogTxnGuard& operator=(const CatalogTxnGuard&) = delete;

  void Commit() {
    conn_.Commit();
    active_ = false;
  }

 private:
  duckdb::Connection& conn_;
  bool active_{false};
};

// Resolve the storage backend of an (already-attached) catalog from
// duckdb_databases().type: 'postgres' => kPostgres, 'ducklake' => kDuckLake,
// anything else (incl. a file/memory 'duckdb' database) => kDuckDBFile (the safe
// default — full schema, no postgres_execute). Never throws.
inline CatalogBackend DetectCatalogBackend(duckdb::Connection& conn,
                                           const std::string& catalog) {
  try {
    auto stmt = conn.Prepare(
        "SELECT lower(type) FROM duckdb_databases() WHERE database_name = $1");
    if (!stmt || stmt->HasError()) {
      return CatalogBackend::kDuckDBFile;
    }
    duckdb::vector<duckdb::Value> args{duckdb::Value(catalog)};
    auto raw = stmt->Execute(args, /*allow_stream_result=*/false);
    if (!raw || raw->HasError()) {
      return CatalogBackend::kDuckDBFile;
    }
    auto& result = raw->Cast<duckdb::MaterializedQueryResult>();
    if (result.RowCount() == 0) {
      return CatalogBackend::kDuckDBFile;
    }
    const std::string type = result.GetValue(0, 0).ToString();
    if (type.find("postgres") != std::string::npos) {
      return CatalogBackend::kPostgres;
    }
    if (type.find("ducklake") != std::string::npos) {
      return CatalogBackend::kDuckLake;
    }
    return CatalogBackend::kDuckDBFile;
  } catch (const std::exception&) {
    return CatalogBackend::kDuckDBFile;
  }
}

// Run one native PostgreSQL DDL statement on the attached catalog via
// postgres_execute(), escaping single quotes in both SQL-string arguments (the
// catalog is an operator-supplied attach name). This bypasses DuckDB's DDL
// translation, which mistranslates view functions (epoch()/date_diff()) and
// rejects catalog-qualified references.
inline arrow::Status RunPostgresDDL(duckdb::Connection& conn,
                                    const std::string& catalog,
                                    const std::string& sql) {
  auto sql_quote = [](const std::string& s) {
    std::string out;
    out.reserve(s.size() + 16);
    for (char c : s) {
      out += c;
      if (c == '\'') out += '\'';
    }
    return out;
  };
  auto result = conn.Query("CALL postgres_execute('" + sql_quote(catalog) + "', '" +
                           sql_quote(sql) + "')");
  if (result->HasError()) {
    return arrow::Status::Invalid("Failed to execute PostgreSQL catalog DDL [",
                                  sql.substr(0, 60), "...]: ", result->GetError());
  }
  return arrow::Status::OK();
}

}  // namespace gizmosql
