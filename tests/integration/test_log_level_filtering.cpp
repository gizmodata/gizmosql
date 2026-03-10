// =============================================================================
// Test: Log level filtering for query_log_level and auth_log_level
// Verifies that GIZMOSQL_LOGKV_DYNAMIC_AT and GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT
// correctly suppress messages whose natural severity is below the component
// threshold, rather than promoting them to the threshold severity.
// Regression test for https://github.com/gizmodata/gizmosql/issues/136
// =============================================================================

#include <gtest/gtest.h>

#include <arrow/util/logger.h>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "detail/gizmosql_logging.h"

using arrow::util::ArrowLogLevel;
using arrow::util::LogDetails;
using arrow::util::Logger;
using arrow::util::LoggerRegistry;

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

// Minimal session-like struct that satisfies SESSION_KV_FIELDS(s) macro.
// The macro reads: s->session_id, s->username, s->role, s->peer
struct MockSession {
  std::string session_id;
  std::string username;
  std::string role;
  std::string peer;
};

}  // namespace

class LogLevelFilteringTest : public ::testing::Test {
 protected:
  void SetUp() override {
    original_logger_ = LoggerRegistry::GetDefaultLogger();
  }

  void TearDown() override {
    if (original_logger_) {
      LoggerRegistry::SetDefaultLogger(original_logger_);
    }
  }

  std::shared_ptr<CapturingLogger> InstallCapturingLogger(
      ArrowLogLevel overall_threshold = ArrowLogLevel::ARROW_DEBUG) {
    auto logger = std::make_shared<CapturingLogger>(overall_threshold);
    LoggerRegistry::SetDefaultLogger(logger);
    return logger;
  }

  std::shared_ptr<Logger> original_logger_;
};

// =============================================================================
// GIZMOSQL_LOGKV_DYNAMIC_AT tests (used by auth logging)
// =============================================================================

TEST_F(LogLevelFilteringTest, DynamicAt_InfoMessageSuppressedWhenThresholdIsError) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  // Simulate: auth_log_level=ERROR, message natural level=INFO
  GIZMOSQL_LOGKV_DYNAMIC_AT(
      ArrowLogLevel::ARROW_ERROR, ArrowLogLevel::ARROW_INFO,
      "Successful authentication", {"kind", "authentication"});

  auto entries = logger->TakeEntries();
  EXPECT_TRUE(entries.empty())
      << "INFO message should be suppressed when component threshold is ERROR";
}

TEST_F(LogLevelFilteringTest, DynamicAt_InfoMessageShownWhenThresholdIsInfo) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  GIZMOSQL_LOGKV_DYNAMIC_AT(
      ArrowLogLevel::ARROW_INFO, ArrowLogLevel::ARROW_INFO,
      "Successful authentication", {"kind", "authentication"});

  auto entries = logger->TakeEntries();
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].severity, ArrowLogLevel::ARROW_INFO);
}

TEST_F(LogLevelFilteringTest, DynamicAt_InfoMessageShownWhenThresholdIsDebug) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  GIZMOSQL_LOGKV_DYNAMIC_AT(
      ArrowLogLevel::ARROW_DEBUG, ArrowLogLevel::ARROW_INFO,
      "Successful authentication", {"kind", "authentication"});

  auto entries = logger->TakeEntries();
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].severity, ArrowLogLevel::ARROW_INFO)
      << "Display severity should be INFO regardless of threshold";
}

TEST_F(LogLevelFilteringTest, DynamicAt_ErrorMessageShownWhenThresholdIsError) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  GIZMOSQL_LOGKV_DYNAMIC_AT(
      ArrowLogLevel::ARROW_ERROR, ArrowLogLevel::ARROW_ERROR,
      "Authentication failed", {"kind", "authentication"});

  auto entries = logger->TakeEntries();
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].severity, ArrowLogLevel::ARROW_ERROR);
}

TEST_F(LogLevelFilteringTest, DynamicAt_InfoSuppressedByOverallLoggerThreshold) {
  // Overall logger threshold is WARNING, component threshold is DEBUG
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_WARNING);

  GIZMOSQL_LOGKV_DYNAMIC_AT(
      ArrowLogLevel::ARROW_DEBUG, ArrowLogLevel::ARROW_INFO,
      "Should be suppressed by overall threshold", {"kind", "test"});

  auto entries = logger->TakeEntries();
  EXPECT_TRUE(entries.empty())
      << "INFO message should be suppressed when overall logger threshold is WARNING";
}

// =============================================================================
// GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT tests (used by query logging)
// =============================================================================

class SessionLogLevelFilteringTest : public LogLevelFilteringTest {
 protected:
  void SetUp() override {
    LogLevelFilteringTest::SetUp();
    session_ = std::make_shared<MockSession>();
    session_->session_id = "test-session-id";
    session_->username = "testuser";
    session_->role = "admin";
    session_->peer = "ipv4:127.0.0.1:12345";
  }

  std::shared_ptr<MockSession> session_;
};

TEST_F(SessionLogLevelFilteringTest,
       SessionDynamicAt_InfoMessageSuppressedWhenQueryLogLevelIsError) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  // Simulate: query_log_level=ERROR, message natural level=INFO
  GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
      ArrowLogLevel::ARROW_ERROR, ArrowLogLevel::ARROW_INFO,
      session_, "Client SQL command execution succeeded",
      {"kind", "sql"}, {"status", "success"});

  auto entries = logger->TakeEntries();
  EXPECT_TRUE(entries.empty())
      << "INFO SQL log should be suppressed when query_log_level is ERROR";
}

TEST_F(SessionLogLevelFilteringTest,
       SessionDynamicAt_InfoMessageShownWhenQueryLogLevelIsInfo) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
      ArrowLogLevel::ARROW_INFO, ArrowLogLevel::ARROW_INFO,
      session_, "Client SQL command execution succeeded",
      {"kind", "sql"}, {"status", "success"});

  auto entries = logger->TakeEntries();
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].severity, ArrowLogLevel::ARROW_INFO);
}

TEST_F(SessionLogLevelFilteringTest,
       SessionDynamicAt_DisplaySeverityIsAlwaysNaturalLevel) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  // Even with threshold=DEBUG, display should be INFO (the natural level)
  GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
      ArrowLogLevel::ARROW_DEBUG, ArrowLogLevel::ARROW_INFO,
      session_, "Client is attempting to run a SQL command",
      {"kind", "sql"}, {"status", "attempt"});

  auto entries = logger->TakeEntries();
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].severity, ArrowLogLevel::ARROW_INFO)
      << "Display severity must be INFO, not the component threshold";
}

TEST_F(SessionLogLevelFilteringTest,
       SessionDynamicAt_WarningSuppressedWhenThresholdIsError) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
      ArrowLogLevel::ARROW_ERROR, ArrowLogLevel::ARROW_WARNING,
      session_, "Query took a long time",
      {"kind", "sql"}, {"status", "slow"});

  auto entries = logger->TakeEntries();
  EXPECT_TRUE(entries.empty())
      << "WARNING message should be suppressed when threshold is ERROR";
}

TEST_F(SessionLogLevelFilteringTest,
       SessionDynamicAt_ErrorShownWhenThresholdIsError) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
      ArrowLogLevel::ARROW_ERROR, ArrowLogLevel::ARROW_ERROR,
      session_, "SQL execution failed",
      {"kind", "sql"}, {"status", "failure"});

  auto entries = logger->TakeEntries();
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].severity, ArrowLogLevel::ARROW_ERROR);
}

// =============================================================================
// GIZMOSQL_LOGKV_SESSION_DYNAMIC convenience wrapper tests
// =============================================================================

TEST_F(SessionLogLevelFilteringTest,
       SessionDynamic_ConvenienceWrapperUsesThresholdAsDisplayLevel) {
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_DEBUG);

  GIZMOSQL_LOGKV_SESSION_DYNAMIC(
      ArrowLogLevel::ARROW_INFO, session_, "Test message",
      {"kind", "test"}, {"status", "ok"});

  auto entries = logger->TakeEntries();
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].severity, ArrowLogLevel::ARROW_INFO);
}

TEST_F(SessionLogLevelFilteringTest,
       SessionDynamic_ConvenienceWrapperSuppressesWhenOverallThresholdHigher) {
  // Overall logger threshold is ERROR
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_ERROR);

  GIZMOSQL_LOGKV_SESSION_DYNAMIC(
      ArrowLogLevel::ARROW_INFO, session_, "Should be suppressed",
      {"kind", "test"}, {"status", "ok"});

  auto entries = logger->TakeEntries();
  EXPECT_TRUE(entries.empty())
      << "INFO message should be suppressed when overall threshold is ERROR";
}

// =============================================================================
// Interaction between component threshold and overall logger threshold
// =============================================================================

TEST_F(LogLevelFilteringTest, BothThresholdsMustBeSatisfied) {
  // Overall=WARNING, component=DEBUG → INFO should be suppressed by overall
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_WARNING);

  GIZMOSQL_LOGKV_DYNAMIC_AT(
      ArrowLogLevel::ARROW_DEBUG, ArrowLogLevel::ARROW_INFO,
      "Should not appear", {"kind", "test"});

  auto entries = logger->TakeEntries();
  EXPECT_TRUE(entries.empty())
      << "INFO must be suppressed when overall threshold is WARNING, "
         "even if component threshold is DEBUG";
}

TEST_F(LogLevelFilteringTest, WarningShownWhenBothThresholdsMet) {
  // Overall=WARNING, component=INFO → WARNING should pass both
  auto logger = InstallCapturingLogger(ArrowLogLevel::ARROW_WARNING);

  GIZMOSQL_LOGKV_DYNAMIC_AT(
      ArrowLogLevel::ARROW_INFO, ArrowLogLevel::ARROW_WARNING,
      "Should appear at WARNING", {"kind", "test"});

  auto entries = logger->TakeEntries();
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0].severity, ArrowLogLevel::ARROW_WARNING);
}
