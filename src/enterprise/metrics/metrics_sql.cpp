// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.
#include "metrics_service.h"
#include "enterprise/enterprise_features.h"
#include "detail/session_context.h"
#include <duckdb/main/client_context_state.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/function/table_function.hpp>
namespace gizmosql::enterprise {
namespace {
using namespace duckdb;
void RequireMetrics() {
  if (!EnterpriseFeatures::Instance().IsMetricsAvailable())
    throw PermissionException(
        "gizmosql_metrics() requires the 'metrics' Enterprise license feature");
  if (!MetricsRegistry::Current())
    throw InvalidInputException(
        "Metrics is disabled; enable it with --enable-metrics or "
        "GIZMOSQL_ENABLE_METRICS");
}
struct MetricsScan : GlobalTableFunctionState {
  std::vector<MetricSample> rows;
  idx_t offset = 0;
};
unique_ptr<FunctionData> Bind(ClientContext&, TableFunctionBindInput&,
                              vector<LogicalType>& types, vector<string>& names) {
  RequireMetrics();
  names = {"name", "kind", "labels", "value", "help"};
  types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
           LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR),
           LogicalType::DOUBLE, LogicalType::VARCHAR};
  return nullptr;
}
unique_ptr<GlobalTableFunctionState> Init(ClientContext&, TableFunctionInitInput&) {
  RequireMetrics();
  auto state = make_uniq<MetricsScan>();
  if (auto registry = MetricsRegistry::Current()) state->rows = registry->Snapshot();
  return std::move(state);
}
void Scan(ClientContext&, TableFunctionInput& input, DataChunk& output) {
  RequireMetrics();
  auto& state = input.global_state->Cast<MetricsScan>();
  idx_t count = 0;
  while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
    const auto& row = state.rows[state.offset++];
    output.SetValue(0, count, Value(row.name));
    output.SetValue(1, count, Value(row.kind));
    vector<Value> keys, values;
    for (const auto& [key, value] : row.labels) {
      keys.emplace_back(key);
      values.emplace_back(value);
    }
    output.SetValue(2, count,
                    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, keys, values));
    output.SetValue(3, count, Value::DOUBLE(row.value));
    output.SetValue(4, count, Value(row.help));
    ++count;
  }
  output.SetCardinality(count);
}
}  // namespace
// DuckDB invokes this under its own context lock after transaction cleanup.
// Sampling transaction internals from the HTTP/background thread would race.
class SessionMetricsState : public duckdb::ClientContextState {
 public:
  explicit SessionMetricsState(std::weak_ptr<gizmosql::ClientSession> session)
      : session_(std::move(session)) {}
  void QueryEnd(duckdb::ClientContext& context) override {
    if (auto session = session_.lock()) {
      auto& tx = context.transaction;
      const int state =
          tx.IsAutoCommit() ? 0
          : tx.HasActiveTransaction() &&
                  tx.ActiveTransaction().transaction_validity.IsInvalidated()
              ? 2
              : 1;
      session->metrics_transaction_state.store(state, std::memory_order_relaxed);
    }
  }

 private:
  std::weak_ptr<gizmosql::ClientSession> session_;
};
void TrackMetricsSession(const std::shared_ptr<gizmosql::ClientSession>& session) {
  session->metrics = MetricsRegistry::Current();
  if (!session->metrics) return;
  session->connection->Get().context->registered_state->Insert(
      "gizmosql_metrics", duckdb::make_shared_ptr<SessionMetricsState>(session));
}

void RegisterMetricsFunction(duckdb::DuckDB& db) {
  duckdb::TableFunction function("gizmosql_metrics", {}, Scan, Bind, Init);
  duckdb::Connection connection(db);
  connection.context->RunFunctionInTransaction([&] {
    duckdb::CreateTableFunctionInfo info(function);
    auto& catalog = duckdb::Catalog::GetSystemCatalog(*connection.context);
    catalog.CreateTableFunction(*connection.context, info);
  });
}
}  // namespace gizmosql::enterprise
