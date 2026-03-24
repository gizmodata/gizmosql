// =============================================================================
// Test: Internal query log level filtering
// Verifies that internal queries (e.g., DoGetTables) are logged at DEBUG
// severity and are correctly suppressed when the server's query_log_level
// threshold is INFO (the default), and emitted when the threshold is DEBUG.
// =============================================================================

#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/table.h>
#include <arrow/util/logger.h>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "test_server_fixture.h"
#include "test_util.h"

using arrow::util::ArrowLogLevel;
using arrow::util::LogDetails;
using arrow::util::Logger;
using arrow::util::LoggerRegistry;
using arrow::flight::sql::FlightSqlClient;

namespace {

// A logger that captures every message for later assertion.
class CapturingLogger final : public Logger {
 public:
  struct Entry {
    ArrowLogLevel severity;
    std::string message;
  };

  explicit CapturingLogger(ArrowLogLevel threshold) : threshold_(threshold) {}

  bool is_enabled() const override { return true; }
  ArrowLogLevel severity_threshold() const override { return threshold_; }

  void Log(const LogDetails& d) override {
    std::lock_guard<std::mutex> lk(mu_);
    entries_.push_back({d.severity, std::string(d.message)});
  }

  std::vector<Entry> TakeEntries() {
    std::lock_guard<std::mutex> lk(mu_);
    auto result = std::move(entries_);
    entries_.clear();
    return result;
  }

 private:
  ArrowLogLevel threshold_;
  mutable std::mutex mu_;
  std::vector<Entry> entries_;
};

}  // namespace

// =============================================================================
// Fixture: Server with query_log_level=INFO (default), print_queries=true
// Internal queries (DoGetTables) should be suppressed at this threshold.
// =============================================================================
class InternalQueryLogInfoFixture
    : public gizmosql::testing::ServerTestFixture<InternalQueryLogInfoFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "internal_query_log_info_test.db",
        .port = 31400,
        .health_port = 31401,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        .init_sql_commands = "CREATE TABLE log_test_table (id INTEGER, name VARCHAR);",
        .print_queries = true,
        .query_log_level = ArrowLogLevel::ARROW_INFO,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<InternalQueryLogInfoFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<InternalQueryLogInfoFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<InternalQueryLogInfoFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<InternalQueryLogInfoFixture>::config_{};

// =============================================================================
// Fixture: Server with query_log_level=DEBUG, print_queries=true
// Internal queries (DoGetTables) should be emitted at this threshold.
// =============================================================================
class InternalQueryLogDebugFixture
    : public gizmosql::testing::ServerTestFixture<InternalQueryLogDebugFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "internal_query_log_debug_test.db",
        .port = 31402,
        .health_port = 31403,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        .init_sql_commands = "CREATE TABLE log_test_table (id INTEGER, name VARCHAR);",
        .print_queries = true,
        .query_log_level = ArrowLogLevel::ARROW_DEBUG,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<InternalQueryLogDebugFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<InternalQueryLogDebugFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<InternalQueryLogDebugFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<InternalQueryLogDebugFixture>::config_{};

// =============================================================================
// Helper: Connect, authenticate, and call GetTables
// =============================================================================
static arrow::Result<std::shared_ptr<arrow::Table>> CallGetTables(
    int port, const std::string& username, const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                         arrow::flight::Location::ForGrpcTcp("localhost", port));
  arrow::flight::FlightClientOptions client_options;
  ARROW_ASSIGN_OR_RAISE(auto client,
                         arrow::flight::FlightClient::Connect(location, client_options));

  arrow::flight::FlightCallOptions call_options;
  ARROW_ASSIGN_OR_RAISE(auto bearer,
                         client->AuthenticateBasicToken({}, username, password));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  ARROW_ASSIGN_OR_RAISE(
      auto info,
      sql_client.GetTables(call_options,
                           /*catalog=*/nullptr,
                           /*db_schema_filter_pattern=*/nullptr,
                           /*table_filter_pattern=*/nullptr,
                           /*include_schema=*/false,
                           /*table_types=*/nullptr));

  // Read all results
  std::shared_ptr<arrow::Table> combined;
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                           sql_client.DoGet(call_options, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(combined, reader->ToTable());
  }
  return combined;
}

// =============================================================================
// Test: INFO threshold suppresses internal DoGetTables query logs
// =============================================================================
TEST_F(InternalQueryLogInfoFixture, DoGetTablesNotLoggedAtInfoThreshold) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Save original logger and install capturing logger
  auto original_logger = LoggerRegistry::GetDefaultLogger();
  auto capturing_logger = std::make_shared<CapturingLogger>(ArrowLogLevel::ARROW_DEBUG);
  LoggerRegistry::SetDefaultLogger(capturing_logger);

  // Call DoGetTables — this triggers internal queries in the server
  ASSERT_ARROW_OK_AND_ASSIGN(auto table,
                              CallGetTables(GetPort(), GetUsername(), GetPassword()));

  // Restore the original logger
  LoggerRegistry::SetDefaultLogger(original_logger);

  // Verify the endpoint actually worked — our init_sql created log_test_table
  ASSERT_GT(table->num_rows(), 0) << "GetTables should return at least one table";

  // Check that log_test_table is present in the results
  auto table_name_col = table->GetColumnByName("table_name");
  ASSERT_NE(table_name_col, nullptr) << "Expected 'table_name' column in GetTables result";
  bool found_table = false;
  for (int chunk = 0; chunk < table_name_col->num_chunks(); ++chunk) {
    auto array = std::static_pointer_cast<arrow::StringArray>(table_name_col->chunk(chunk));
    for (int64_t i = 0; i < array->length(); ++i) {
      if (!array->IsNull(i) && array->GetString(i) == "log_test_table") {
        found_table = true;
      }
    }
  }
  ASSERT_TRUE(found_table) << "GetTables should include 'log_test_table'";

  // Verify that NO internal query logs were emitted (they have DEBUG severity,
  // which should be suppressed by the INFO query_log_level threshold)
  auto entries = capturing_logger->TakeEntries();
  for (const auto& entry : entries) {
    if (entry.message.find("\"is_internal\":\"true\"") != std::string::npos) {
      FAIL() << "Internal query log should NOT be emitted at INFO threshold, but found: "
             << entry.message;
    }
  }
}

// =============================================================================
// Test: DEBUG threshold allows internal DoGetTables query logs through
// =============================================================================
TEST_F(InternalQueryLogDebugFixture, DoGetTablesLoggedAtDebugThreshold) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Save original logger and install capturing logger
  auto original_logger = LoggerRegistry::GetDefaultLogger();
  auto capturing_logger = std::make_shared<CapturingLogger>(ArrowLogLevel::ARROW_DEBUG);
  LoggerRegistry::SetDefaultLogger(capturing_logger);

  // Call DoGetTables — this triggers internal queries in the server
  ASSERT_ARROW_OK_AND_ASSIGN(auto table,
                              CallGetTables(GetPort(), GetUsername(), GetPassword()));

  // Restore the original logger
  LoggerRegistry::SetDefaultLogger(original_logger);

  // Verify the endpoint actually worked — our init_sql created log_test_table
  ASSERT_GT(table->num_rows(), 0) << "GetTables should return at least one table";

  // Check that log_test_table is present in the results
  auto table_name_col = table->GetColumnByName("table_name");
  ASSERT_NE(table_name_col, nullptr) << "Expected 'table_name' column in GetTables result";
  bool found_table = false;
  for (int chunk = 0; chunk < table_name_col->num_chunks(); ++chunk) {
    auto array = std::static_pointer_cast<arrow::StringArray>(table_name_col->chunk(chunk));
    for (int64_t i = 0; i < array->length(); ++i) {
      if (!array->IsNull(i) && array->GetString(i) == "log_test_table") {
        found_table = true;
      }
    }
  }
  ASSERT_TRUE(found_table) << "GetTables should include 'log_test_table'";

  // Verify that internal query logs WERE emitted (they have DEBUG severity,
  // which should pass the DEBUG query_log_level threshold)
  auto entries = capturing_logger->TakeEntries();
  bool found_internal_log = false;
  for (const auto& entry : entries) {
    if (entry.message.find("\"is_internal\":\"true\"") != std::string::npos) {
      found_internal_log = true;
      EXPECT_EQ(entry.severity, ArrowLogLevel::ARROW_DEBUG)
          << "Internal query logs should be emitted at DEBUG severity";
      break;
    }
  }
  ASSERT_TRUE(found_internal_log)
      << "Expected at least one internal query log entry with is_internal=true "
         "when query_log_level is DEBUG";
}
