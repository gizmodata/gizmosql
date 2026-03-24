// =============================================================================
// Test: Auth log level filtering for bearer token validation
// Verifies that repeat bearer token validations (which display at DEBUG) are
// correctly suppressed when auth_log_level is INFO (the default), and that
// first-seen token validations (which display at INFO) are always emitted.
// =============================================================================

#include <gtest/gtest.h>

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

// Count entries matching a severity and containing a substring
int CountEntries(const std::vector<CapturingLogger::Entry>& entries,
                 ArrowLogLevel severity, const std::string& substr) {
  int count = 0;
  for (const auto& e : entries) {
    if (e.severity == severity &&
        e.message.find(substr) != std::string::npos) {
      ++count;
    }
  }
  return count;
}

bool HasEntryContaining(const std::vector<CapturingLogger::Entry>& entries,
                        const std::string& substr) {
  for (const auto& e : entries) {
    if (e.message.find(substr) != std::string::npos) return true;
  }
  return false;
}

}  // namespace

// =============================================================================
// Fixture: Server with default auth_log_level=INFO
// =============================================================================
class AuthLogLevelFixture
    : public gizmosql::testing::ServerTestFixture<AuthLogLevelFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "auth_log_level_test.db",
        .port = 31420,
        .health_port = 31421,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<AuthLogLevelFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<AuthLogLevelFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<AuthLogLevelFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<AuthLogLevelFixture>::config_{};

// =============================================================================
// Test: Repeat bearer token validations are suppressed at INFO auth_log_level
//
// The server uses auth_log_level=INFO (default). First-seen tokens are logged
// at INFO severity, while repeat tokens are logged at DEBUG severity. With the
// auth_log_level threshold set to INFO, repeat token logs (DEBUG) should be
// suppressed.
// =============================================================================
TEST_F(AuthLogLevelFixture, RepeatBearerTokenLogsSuppressedAtInfoThreshold) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect and get a bearer token via Basic auth
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto location,
      arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  arrow::flight::FlightClientOptions client_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto client,
      arrow::flight::FlightClient::Connect(location, client_options));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);

  // First bearer call — this triggers first-seen token validation (INFO display)
  // We don't capture this one; we just need the token to be "seen" by the server.
  FlightSqlClient sql_client(std::move(client));
  ASSERT_ARROW_OK_AND_ASSIGN(auto info1, sql_client.Execute(call_options, "SELECT 1"));
  for (const auto& endpoint : info1->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader, sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  }

  // Now install capturing logger and make a second call with the same bearer token.
  // This triggers a repeat token validation which displays at DEBUG severity.
  auto original_logger = LoggerRegistry::GetDefaultLogger();
  auto capturing_logger = std::make_shared<CapturingLogger>(ArrowLogLevel::ARROW_DEBUG);
  LoggerRegistry::SetDefaultLogger(capturing_logger);

  ASSERT_ARROW_OK_AND_ASSIGN(auto info2, sql_client.Execute(call_options, "SELECT 2"));
  for (const auto& endpoint : info2->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader, sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  }

  LoggerRegistry::SetDefaultLogger(original_logger);

  auto entries = capturing_logger->TakeEntries();

  // Repeat bearer token validation should NOT produce DEBUG-level auth logs,
  // because the auth_log_level threshold is INFO and DEBUG < INFO.
  int debug_bearer_count = CountEntries(entries, ArrowLogLevel::ARROW_DEBUG,
                                        "Bearer Token was validated successfully");
  EXPECT_EQ(debug_bearer_count, 0)
      << "Repeat bearer token validation (DEBUG display) should be suppressed "
         "when auth_log_level threshold is INFO";
}

// =============================================================================
// Test: First-seen bearer token validation IS logged at INFO threshold
// =============================================================================
TEST_F(AuthLogLevelFixture, FirstSeenBearerTokenLoggedAtInfoThreshold) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect with a fresh client to get a new bearer token
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto location,
      arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  arrow::flight::FlightClientOptions client_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto client,
      arrow::flight::FlightClient::Connect(location, client_options));

  // Install capturing logger BEFORE authenticating so we capture the first-seen log
  auto original_logger = LoggerRegistry::GetDefaultLogger();
  auto capturing_logger = std::make_shared<CapturingLogger>(ArrowLogLevel::ARROW_DEBUG);
  LoggerRegistry::SetDefaultLogger(capturing_logger);

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);

  // Make a call — this is the first use of this bearer token
  FlightSqlClient sql_client(std::move(client));
  ASSERT_ARROW_OK_AND_ASSIGN(auto info, sql_client.Execute(call_options, "SELECT 1"));
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader, sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  }

  LoggerRegistry::SetDefaultLogger(original_logger);

  auto entries = capturing_logger->TakeEntries();

  // First-seen bearer token validation should produce an INFO-level log
  EXPECT_TRUE(HasEntryContaining(entries, "Bearer Token was validated successfully"))
      << "First-seen bearer token validation should be logged at INFO threshold";

  // Verify it was logged at INFO severity (not DEBUG)
  int info_bearer_count = CountEntries(entries, ArrowLogLevel::ARROW_INFO,
                                       "Bearer Token was validated successfully");
  EXPECT_GT(info_bearer_count, 0)
      << "First-seen bearer token should be logged at INFO severity";
}
