// =============================================================================
// Test: SET gizmosql.query_log_level — session-level dynamic log level changes
// Verifies that SET gizmosql.query_log_level dynamically changes the query log
// threshold within a session. Normal queries (SELECT) log at INFO and should
// appear at both INFO and DEBUG thresholds. Internal queries (DoGetTables) log
// at DEBUG and should only appear when the threshold is lowered to DEBUG.
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

// Helper: find entries containing a substring
bool HasEntryContaining(const std::vector<CapturingLogger::Entry>& entries,
                        const std::string& substr) {
  for (const auto& e : entries) {
    if (e.message.find(substr) != std::string::npos) return true;
  }
  return false;
}

}  // namespace

// =============================================================================
// Fixture: Server with query_log_level=INFO (default), print_queries=true
// The test will use SET to dynamically change query_log_level within a session.
// =============================================================================
class SetQueryLogLevelFixture
    : public gizmosql::testing::ServerTestFixture<SetQueryLogLevelFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "set_query_log_level_test.db",
        .port = 31410,
        .health_port = 31411,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        .init_sql_commands = "CREATE TABLE set_log_test (id INTEGER, name VARCHAR);",
        .print_queries = true,
        .query_log_level = ArrowLogLevel::ARROW_INFO,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<SetQueryLogLevelFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<SetQueryLogLevelFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<SetQueryLogLevelFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<SetQueryLogLevelFixture>::config_{};

// =============================================================================
// Helper: Create an authenticated FlightSqlClient
// =============================================================================
struct AuthenticatedClient {
  std::unique_ptr<arrow::flight::FlightClient> flight_client;
  std::unique_ptr<FlightSqlClient> sql_client;
  arrow::flight::FlightCallOptions call_options;
};

static arrow::Result<AuthenticatedClient> MakeClient(
    int port, const std::string& username, const std::string& password) {
  AuthenticatedClient ac;
  ARROW_ASSIGN_OR_RAISE(auto location,
                         arrow::flight::Location::ForGrpcTcp("localhost", port));
  arrow::flight::FlightClientOptions client_options;
  ARROW_ASSIGN_OR_RAISE(ac.flight_client,
                         arrow::flight::FlightClient::Connect(location, client_options));

  ARROW_ASSIGN_OR_RAISE(auto bearer,
                         ac.flight_client->AuthenticateBasicToken({}, username, password));
  ac.call_options.headers.push_back(bearer);

  ac.sql_client = std::make_unique<FlightSqlClient>(std::move(ac.flight_client));
  return ac;
}

// Helper: Execute a SELECT query and read all results into a Table
static arrow::Result<std::shared_ptr<arrow::Table>> ExecuteSelect(
    FlightSqlClient& client, arrow::flight::FlightCallOptions& opts,
    const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, client.Execute(opts, sql));
  std::shared_ptr<arrow::Table> combined;
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, client.DoGet(opts, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(combined, reader->ToTable());
  }
  return combined;
}

// Helper: Execute an update/SET command
static arrow::Result<int64_t> ExecuteUpdate(
    FlightSqlClient& client, arrow::flight::FlightCallOptions& opts,
    const std::string& sql) {
  return client.ExecuteUpdate(opts, sql);
}

// Helper: Call GetTables and read results
static arrow::Result<std::shared_ptr<arrow::Table>> CallGetTables(
    FlightSqlClient& client, arrow::flight::FlightCallOptions& opts) {
  ARROW_ASSIGN_OR_RAISE(
      auto info,
      client.GetTables(opts,
                       /*catalog=*/nullptr,
                       /*db_schema_filter_pattern=*/nullptr,
                       /*table_filter_pattern=*/nullptr,
                       /*include_schema=*/false,
                       /*table_types=*/nullptr));

  std::shared_ptr<arrow::Table> combined;
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, client.DoGet(opts, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(combined, reader->ToTable());
  }
  return combined;
}

// =============================================================================
// Test: At default INFO threshold, normal SELECT is logged but DoGetTables is not.
// After SET gizmosql.query_log_level = DEBUG, both are logged.
// =============================================================================
TEST_F(SetQueryLogLevelFixture, SessionSetChangesLogThreshold) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect and authenticate
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto ac, MakeClient(GetPort(), GetUsername(), GetPassword()));

  auto original_logger = LoggerRegistry::GetDefaultLogger();
  auto capturing_logger = std::make_shared<CapturingLogger>(ArrowLogLevel::ARROW_DEBUG);

  // ---- Phase 1: Default INFO threshold ----
  // Normal SELECT should be logged, DoGetTables should NOT be logged

  LoggerRegistry::SetDefaultLogger(capturing_logger);

  // Execute a normal SELECT (logs at INFO severity)
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto select_result,
      ExecuteSelect(*ac.sql_client, ac.call_options,
                    "SELECT 1 AS test_value"));

  auto phase1_entries = capturing_logger->TakeEntries();

  // Normal query logs should be present (is_internal=false, logged at INFO)
  EXPECT_TRUE(HasEntryContaining(phase1_entries, "\"is_internal\":\"false\""))
      << "Normal SELECT query should be logged at INFO threshold";

  // Now call DoGetTables (logs at DEBUG severity)
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto tables1, CallGetTables(*ac.sql_client, ac.call_options));
  ASSERT_GT(tables1->num_rows(), 0) << "GetTables should return results";

  auto phase1_get_tables_entries = capturing_logger->TakeEntries();

  // Internal query logs should NOT be present (DEBUG < INFO threshold)
  EXPECT_FALSE(HasEntryContaining(phase1_get_tables_entries, "\"is_internal\":\"true\""))
      << "Internal DoGetTables query should NOT be logged at default INFO threshold";

  LoggerRegistry::SetDefaultLogger(original_logger);

  // ---- Phase 2: SET gizmosql.query_log_level = DEBUG ----
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto set_result,
      ExecuteSelect(*ac.sql_client, ac.call_options,
                    "SET gizmosql.query_log_level = DEBUG"));

  // Drain any log entries from the SET command itself
  LoggerRegistry::SetDefaultLogger(capturing_logger);
  capturing_logger->TakeEntries();

  // Execute a normal SELECT again (should still be logged)
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto select_result2,
      ExecuteSelect(*ac.sql_client, ac.call_options,
                    "SELECT 2 AS test_value"));

  auto phase2_select_entries = capturing_logger->TakeEntries();

  EXPECT_TRUE(HasEntryContaining(phase2_select_entries, "\"is_internal\":\"false\""))
      << "Normal SELECT query should still be logged at DEBUG threshold";

  // Now call DoGetTables again — should be logged this time
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto tables2, CallGetTables(*ac.sql_client, ac.call_options));
  ASSERT_GT(tables2->num_rows(), 0) << "GetTables should return results";

  auto phase2_get_tables_entries = capturing_logger->TakeEntries();

  // Internal query logs SHOULD be present now (DEBUG >= DEBUG threshold)
  bool found_internal = HasEntryContaining(phase2_get_tables_entries,
                                           "\"is_internal\":\"true\"");
  EXPECT_TRUE(found_internal)
      << "Internal DoGetTables query SHOULD be logged after "
         "SET gizmosql.query_log_level = DEBUG";

  // Verify internal logs are at DEBUG severity
  if (found_internal) {
    for (const auto& entry : phase2_get_tables_entries) {
      if (entry.message.find("\"is_internal\":\"true\"") != std::string::npos) {
        EXPECT_EQ(entry.severity, ArrowLogLevel::ARROW_DEBUG)
            << "Internal query logs should be emitted at DEBUG severity";
      }
    }
  }

  LoggerRegistry::SetDefaultLogger(original_logger);
}

// =============================================================================
// Test: SET GLOBAL propagates immediately to existing sessions
//
// 1. Client A connects (session created with default INFO threshold)
// 2. Client B connects and runs SET GLOBAL gizmosql.query_log_level = DEBUG
// 3. Client A (without reconnecting) calls DoGetTables
// 4. Internal query logs should appear — proving the global change propagated
// 5. Client B resets it back: SET GLOBAL gizmosql.query_log_level = INFO
// 6. Client A calls DoGetTables again — internal logs should be suppressed
// =============================================================================
TEST_F(SetQueryLogLevelFixture, SetGlobalPropagatesImmediatelyToExistingSessions) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Client A — connects first (session gets nullopt query_log_level, falls through
  // to server global which is INFO)
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto clientA, MakeClient(GetPort(), GetUsername(), GetPassword()));

  // Client B — will issue SET GLOBAL
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto clientB, MakeClient(GetPort(), GetUsername(), GetPassword()));

  auto original_logger = LoggerRegistry::GetDefaultLogger();
  auto capturing_logger = std::make_shared<CapturingLogger>(ArrowLogLevel::ARROW_DEBUG);

  // ---- Phase 1: Before SET GLOBAL, client A's DoGetTables should NOT log ----
  LoggerRegistry::SetDefaultLogger(capturing_logger);

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto tables1, CallGetTables(*clientA.sql_client, clientA.call_options));
  ASSERT_GT(tables1->num_rows(), 0);

  auto phase1_entries = capturing_logger->TakeEntries();
  EXPECT_FALSE(HasEntryContaining(phase1_entries, "\"is_internal\":\"true\""))
      << "Before SET GLOBAL, internal queries should be suppressed at INFO threshold";

  LoggerRegistry::SetDefaultLogger(original_logger);

  // ---- Phase 2: Client B runs SET GLOBAL gizmosql.query_log_level = DEBUG ----
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto set_result,
      ExecuteSelect(*clientB.sql_client, clientB.call_options,
                    "SET GLOBAL gizmosql.query_log_level = DEBUG"));

  // ---- Phase 3: Client A (no reconnect!) calls DoGetTables — should now log ----
  LoggerRegistry::SetDefaultLogger(capturing_logger);
  capturing_logger->TakeEntries();  // drain

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto tables2, CallGetTables(*clientA.sql_client, clientA.call_options));
  ASSERT_GT(tables2->num_rows(), 0);

  auto phase2_entries = capturing_logger->TakeEntries();
  EXPECT_TRUE(HasEntryContaining(phase2_entries, "\"is_internal\":\"true\""))
      << "After SET GLOBAL DEBUG, existing session should immediately see "
         "internal query logs without reconnecting";

  LoggerRegistry::SetDefaultLogger(original_logger);

  // ---- Phase 4: Client B resets global back to INFO ----
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto reset_result,
      ExecuteSelect(*clientB.sql_client, clientB.call_options,
                    "SET GLOBAL gizmosql.query_log_level = INFO"));

  // ---- Phase 5: Client A calls DoGetTables again — should be suppressed ----
  LoggerRegistry::SetDefaultLogger(capturing_logger);
  capturing_logger->TakeEntries();  // drain

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto tables3, CallGetTables(*clientA.sql_client, clientA.call_options));
  ASSERT_GT(tables3->num_rows(), 0);

  auto phase3_entries = capturing_logger->TakeEntries();
  EXPECT_FALSE(HasEntryContaining(phase3_entries, "\"is_internal\":\"true\""))
      << "After SET GLOBAL INFO, internal queries should be suppressed again";

  LoggerRegistry::SetDefaultLogger(original_logger);
}
