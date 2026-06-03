// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "catalog_log_sink.h"

#include <cstdlib>
#include <filesystem>
#include <regex>
#include <stdexcept>
#include <utility>

#include "gizmosql_logging.h"

namespace fs = std::filesystem;

namespace gizmosql::enterprise {

namespace {

// The append-only INSERT used for every record. The timestamp arrives as an
// ISO8601 UTC string ("...Z") and is CAST to TIMESTAMPTZ in SQL (so the bound
// $1 stays a plain VARCHAR and the cast is explicit on every backend). JSON
// catch-all is bound as a string: DuckDB casts string->JSON on INSERT, and on
// PostgreSQL the column is VARCHAR (see BuildLogSchemaStatements), so a string
// binds directly.
constexpr const char* kInsertSQL =
    "INSERT INTO logs (log_time, level, instance_id, cluster_id, session_id, "
    "username, role, peer, component, trace_id, span_id, pid, tid, source_file, "
    "source_line, func, message, fields) VALUES "
    "(CAST($1 AS TIMESTAMP WITH TIME ZONE), $2, $3, $4, $5, $6, $7, $8, $9, $10, "
    "$11, $12, $13, $14, $15, $16, $17, $18)";

// A UUID literal (8-4-4-4-12 hex). The promoted id columns are typed UUID, so a
// value that is not a well-formed UUID is stored as NULL rather than poisoning
// the insert — a logging sink must never drop a record over a malformed id.
bool IsUuid(const std::string& s) {
  static const std::regex kUuidRe(
      "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  return std::regex_match(s, kUuidRe);
}

duckdb::Value StrOrNull(const std::string& s) {
  return s.empty() ? duckdb::Value() : duckdb::Value(s);
}

duckdb::Value UuidOrNull(const std::string& s) {
  return IsUuid(s) ? duckdb::Value::UUID(s) : duckdb::Value();
}

duckdb::vector<duckdb::Value> BindRecord(const gizmosql::LogRecord& r) {
  return {
      duckdb::Value(r.timestamp),
      duckdb::Value(gizmosql::log_level_arrow_log_level_to_string(r.level)),
      UuidOrNull(r.instance_id),
      UuidOrNull(r.cluster_id),
      UuidOrNull(r.session_id),
      StrOrNull(r.username),
      StrOrNull(r.role),
      StrOrNull(r.peer),
      StrOrNull(r.component),
      StrOrNull(r.trace_id),
      StrOrNull(r.span_id),
      duckdb::Value::INTEGER(r.pid),
      StrOrNull(r.tid),
      StrOrNull(r.source_file),
      duckdb::Value::INTEGER(r.source_line),
      StrOrNull(r.func),
      duckdb::Value(r.message),
      StrOrNull(r.fields_json),
  };
}

/// The log table schema, parameterized by qualification prefix and JSON column
/// type (single source of truth shared by the file/DuckLake and PostgreSQL
/// paths, mirroring the instrumentation manager).
///  - `p`: "" for DuckDB after USE, "<schema>." for native PostgreSQL DDL.
///  - `json_type`: native JSON for DuckDB; VARCHAR for PostgreSQL (DuckDB's
///    postgres extension binds JSON-valued strings into a VARCHAR column cleanly).
///  - `with_indexes`: emit indexes (file-based DuckDB + PostgreSQL); DuckLake
///    cannot enforce indexes, so it gets the bare table.
/// There is intentionally NO primary key (logs are append-only and a PK would
/// only add write cost). The columns mirror the JSON/text log fields: the
/// popular/queryable fields are promoted to typed columns and everything else
/// rides along in the `fields` JSON catch-all. Every timestamp is TIMESTAMPTZ
/// and is indexed so time-range queries and time-based retention deletes stay
/// fast.
std::vector<std::string> BuildLogSchemaStatements(const std::string& p,
                                                  const std::string& json_type,
                                                  bool with_indexes) {
  std::vector<std::string> stmts = {
      "CREATE TABLE IF NOT EXISTS " + p +
          "logs ("
          "log_time TIMESTAMP WITH TIME ZONE NOT NULL, "
          "level VARCHAR NOT NULL, "
          "instance_id UUID, "
          "cluster_id UUID, "
          "session_id UUID, "
          "username VARCHAR, "
          "role VARCHAR, "
          "peer VARCHAR, "
          "component VARCHAR, "
          "trace_id VARCHAR, "
          "span_id VARCHAR, "
          "pid INTEGER, "
          "tid VARCHAR, "
          "source_file VARCHAR, "
          "source_line INTEGER, "
          "func VARCHAR, "
          "message VARCHAR, "
          "fields JSON)",
  };
  if (with_indexes) {
    stmts.push_back("CREATE INDEX IF NOT EXISTS idx_logs_log_time ON " + p + "logs(log_time)");
    stmts.push_back("CREATE INDEX IF NOT EXISTS idx_logs_level ON " + p + "logs(level)");
    stmts.push_back("CREATE INDEX IF NOT EXISTS idx_logs_instance_id ON " + p + "logs(instance_id)");
    stmts.push_back("CREATE INDEX IF NOT EXISTS idx_logs_cluster_id ON " + p + "logs(cluster_id)");
    stmts.push_back("CREATE INDEX IF NOT EXISTS idx_logs_session_id ON " + p + "logs(session_id)");
    stmts.push_back("CREATE INDEX IF NOT EXISTS idx_logs_username ON " + p + "logs(username)");
    stmts.push_back("CREATE INDEX IF NOT EXISTS idx_logs_component ON " + p + "logs(component)");
    stmts.push_back("CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON " + p + "logs(trace_id)");
  }
  // Substitute the JSON column type. " JSON" appears only as the `fields` type
  // token here — never in a value or identifier.
  if (json_type != "JSON") {
    const std::string from = " JSON";
    const std::string to = " " + json_type;
    for (auto& stmt : stmts) {
      size_t pos = 0;
      while ((pos = stmt.find(from, pos)) != std::string::npos) {
        stmt.replace(pos, from.size(), to);
        pos += to.size();
      }
    }
  }
  return stmts;
}

}  // namespace

std::string CatalogLogSink::GetDefaultDbPath(const std::string& database_filename) {
  const char* env_path = std::getenv("GIZMOSQL_LOG_DB_PATH");
  if (env_path && env_path[0] != '\0') {
    return std::string(env_path);
  }

  fs::path base_dir = fs::current_path();
  if (!database_filename.empty()) {
    bool is_memory = (database_filename == ":memory:");
    bool is_extension_syntax = (database_filename.find(':') != std::string::npos &&
                                database_filename.find(':') > 0);
    if (!is_memory && !is_extension_syntax) {
      fs::path db_path(database_filename);
      if (db_path.has_parent_path()) {
        base_dir = db_path.parent_path();
      }
    }
  }
  return (base_dir / "gizmosql_logs.db").string();
}

CatalogLogSink::CatalogLogSink(
    std::string db_path, std::string catalog, std::string schema,
    bool use_external_catalog, size_t max_queue_depth,
    std::shared_ptr<duckdb::DuckDB> db_instance,
    std::unique_ptr<gizmosql::TrackedDuckDBConnection> writer_connection)
    : db_path_(std::move(db_path)),
      catalog_(std::move(catalog)),
      schema_(std::move(schema)),
      use_external_catalog_(use_external_catalog),
      max_queue_depth_(max_queue_depth == 0 ? 1 : max_queue_depth),
      db_instance_(std::move(db_instance)),
      writer_connection_(std::move(writer_connection)) {}

CatalogLogSink::~CatalogLogSink() { Shutdown(); }

arrow::Result<std::shared_ptr<CatalogLogSink>> CatalogLogSink::Create(
    std::shared_ptr<duckdb::DuckDB> db_instance, const std::string& db_path,
    const std::string& catalog, const std::string& schema,
    bool use_external_catalog, size_t max_queue_depth) {
  if (!db_instance) {
    return arrow::Status::Invalid("CatalogLogSink requires a valid DuckDB instance");
  }

  if (use_external_catalog) {
    GIZMOSQL_LOG(INFO) << "Catalog logging using external catalog: " << catalog << "."
                       << schema;
  } else {
    GIZMOSQL_LOG(INFO) << "Catalog logging database path: " << db_path;
  }

  try {
    // Dedicated writer connection on the shared instance (client sessions never
    // use it; their catalog-permission checks therefore never gate these writes).
    auto writer_connection = std::make_unique<gizmosql::TrackedDuckDBConnection>(*db_instance);

    if (!use_external_catalog) {
      // The library also ATTACHes this catalog via the server's init SQL (so
      // client connections can see it for admin reads); ATTACH is instance-wide,
      // so this is a defensive IF NOT EXISTS in case ordering ever changes.
      // (READ_WRITE) lets the sink record logs even when the main DB is read-only.
      auto attach_result = writer_connection->Get().Query(
          "ATTACH IF NOT EXISTS '" + db_path + "' AS " + catalog + " (READ_WRITE)");
      if (attach_result->HasError()) {
        return arrow::Status::Invalid("Failed to attach log catalog database: ",
                                      attach_result->GetError());
      }
    }

    auto sink = std::shared_ptr<CatalogLogSink>(
        new CatalogLogSink(db_path, catalog, schema, use_external_catalog,
                           max_queue_depth, std::move(db_instance),
                           std::move(writer_connection)));

    sink->backend_ = sink->DetectBackend();
    if (sink->backend_ == CatalogBackend::kDuckLake) {
      GIZMOSQL_LOG(INFO)
          << "Catalog logging to a DuckLake catalog: logs are append-only, so "
             "DuckLake is a good fit here (no concurrent-UPDATE hazard, unlike "
             "instrumentation).";
    }

    ARROW_RETURN_NOT_OK(sink->InitializeSchema());

    sink->writer_thread_ = std::thread(&CatalogLogSink::WriterThreadLoop, sink.get());

    GIZMOSQL_LOG(INFO) << "Catalog log sink initialized (" << catalog << "." << schema
                       << ".logs)";
    return sink;
  } catch (const duckdb::Exception& ex) {
    return arrow::Status::Invalid("Failed to create catalog log sink: ", ex.what());
  } catch (const std::exception& ex) {
    return arrow::Status::Invalid("Failed to create catalog log sink: ", ex.what());
  }
}

CatalogBackend CatalogLogSink::DetectBackend() {
  return DetectCatalogBackend(writer_connection_->Get(), catalog_);
}

arrow::Status CatalogLogSink::InitializeSchema() {
  try {
    auto json_result = writer_connection_->Get().Query("INSTALL json; LOAD json;");
    if (json_result->HasError()) {
      return arrow::Status::Invalid("Failed to load json extension: ",
                                    json_result->GetError());
    }

    if (backend_ == CatalogBackend::kPostgres) {
      return InitializePostgresSchema();
    }

    // File-based DuckDB gets the table + indexes; DuckLake gets the bare table
    // (it cannot enforce indexes). Both run as one multi-statement DuckDB script
    // after creating + selecting the target schema on the writer connection.
    const bool with_indexes = (backend_ == CatalogBackend::kDuckDBFile);
    std::string sql = "CREATE SCHEMA IF NOT EXISTS " + catalog_ + "." + schema_ + ";\n";
    sql += "USE " + catalog_ + "." + schema_ + ";\n";
    for (const auto& stmt :
         BuildLogSchemaStatements(/*prefix=*/"", /*json_type=*/"JSON", with_indexes)) {
      sql += stmt + ";\n";
    }
    auto result = writer_connection_->Get().Query(sql);
    if (result->HasError()) {
      return arrow::Status::Invalid("Failed to initialize log catalog schema: ",
                                    result->GetError());
    }
    return arrow::Status::OK();
  } catch (const duckdb::Exception& ex) {
    return arrow::Status::Invalid("Failed to initialize log catalog schema: ", ex.what());
  }
}

arrow::Status CatalogLogSink::InitializePostgresSchema() {
  auto& conn = writer_connection_->Get();

  // Create the schema and the table/indexes with native PostgreSQL SQL via
  // postgres_execute() so DuckDB never has to translate the DDL.
  ARROW_RETURN_NOT_OK(
      RunPostgresDDL(conn, catalog_, "CREATE SCHEMA IF NOT EXISTS " + schema_));
  for (const auto& stmt : BuildLogSchemaStatements(schema_ + ".", /*json_type=*/"VARCHAR",
                                                   /*with_indexes=*/true)) {
    ARROW_RETURN_NOT_OK(RunPostgresDDL(conn, catalog_, stmt));
  }

  // postgres_execute() created the objects directly in PostgreSQL, bypassing
  // DuckDB's catalog cache; refresh it so the USE below and the unqualified
  // INSERTs resolve them.
  auto clear_cache = conn.Query("CALL pg_clear_cache()");
  if (clear_cache->HasError()) {
    return arrow::Status::Invalid("Failed to refresh PostgreSQL catalog cache: ",
                                  clear_cache->GetError());
  }

  auto use_result = conn.Query("USE " + catalog_ + "." + schema_);
  if (use_result->HasError()) {
    return arrow::Status::Invalid("Failed to set default PostgreSQL log schema: ",
                                  use_result->GetError());
  }
  return arrow::Status::OK();
}

void CatalogLogSink::Enqueue(const gizmosql::LogRecord& record) {
  if (shutdown_requested_.load(std::memory_order_relaxed)) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (queue_.size() >= max_queue_depth_) {
      // Drop the newest record and count it; the writer thread reports the
      // running total (throttled) so a slow/unreachable catalog can't make
      // logging itself block the session threads.
      ++dropped_count_;
      return;
    }
    queue_.push_back(record);
  }
  queue_cv_.notify_one();
}

void CatalogLogSink::WriteBatch(std::vector<gizmosql::LogRecord>& batch) {
  if (batch.empty()) {
    return;
  }
  auto& conn = writer_connection_->Get();

  // Fast path: the whole batch in one explicit transaction. The guard commits
  // on success and rolls back (via its destructor) if anything throws — we never
  // rely on auto-commit, mirroring the instrumentation writer.
  try {
    CatalogTxnGuard txn(conn);
    auto stmt = conn.Prepare(kInsertSQL);
    if (stmt->HasError()) {
      throw std::runtime_error("prepare failed: " + stmt->GetError());
    }
    for (auto& rec : batch) {
      auto params = BindRecord(rec);
      auto result = stmt->Execute(params, /*allow_stream_result=*/false);
      if (result->HasError()) {
        throw std::runtime_error(result->GetError());
      }
    }
    txn.Commit();
    return;
  } catch (const std::exception& ex) {
    GIZMOSQL_LOG(WARNING) << "Catalog log batch write failed (" << batch.size()
                          << " record(s)); retrying individually: " << ex.what();
  }

  // Fallback: one transaction per record so a single bad record doesn't drop the
  // whole batch. Records that still fail are counted and reported.
  size_t failed = 0;
  for (auto& rec : batch) {
    try {
      CatalogTxnGuard txn(conn);
      auto stmt = conn.Prepare(kInsertSQL);
      if (stmt->HasError()) {
        throw std::runtime_error("prepare failed: " + stmt->GetError());
      }
      auto params = BindRecord(rec);
      auto result = stmt->Execute(params, /*allow_stream_result=*/false);
      if (result->HasError()) {
        throw std::runtime_error(result->GetError());
      }
      txn.Commit();
    } catch (const std::exception&) {
      ++failed;
    }
  }
  if (failed > 0) {
    GIZMOSQL_LOG(WARNING) << "Catalog log sink dropped " << failed
                          << " record(s) that failed to insert individually";
  }
}

void CatalogLogSink::WriterThreadLoop() {
  // Mark this thread so any log it emits (e.g. the write-failure warnings above)
  // goes only to stdout/file and is never re-dispatched back into this sink.
  gizmosql::ScopedLogSinkGuard sink_guard;

  constexpr size_t kMaxBatch = 512;
  while (true) {
    std::vector<gizmosql::LogRecord> batch;
    uint64_t total_dropped = 0;
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(lock, [this] { return !queue_.empty() || shutdown_requested_; });
      if (shutdown_requested_ && queue_.empty()) {
        break;
      }
      while (!queue_.empty() && batch.size() < kMaxBatch) {
        batch.push_back(std::move(queue_.front()));
        queue_.pop_front();
      }
      total_dropped = dropped_count_;
    }

    if (!batch.empty()) {
      WriteBatch(batch);
    }

    // Report newly-dropped records (throttled to once per drained batch).
    if (total_dropped != dropped_reported_) {
      GIZMOSQL_LOG(WARNING) << "Catalog log sink dropped "
                            << (total_dropped - dropped_reported_)
                            << " log record(s) due to a full queue (depth limit "
                            << max_queue_depth_ << "; total dropped " << total_dropped
                            << ")";
      dropped_reported_ = total_dropped;
    }
  }
}

void CatalogLogSink::Shutdown() {
  if (shutdown_requested_.exchange(true)) {
    return;
  }
  queue_cv_.notify_all();
  if (writer_thread_.joinable()) {
    writer_thread_.join();
  }
  GIZMOSQL_LOG(INFO) << "Catalog log sink shutdown complete";
}

}  // namespace gizmosql::enterprise
