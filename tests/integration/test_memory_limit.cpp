// =============================================================================
// Tests: --memory-limit / GIZMOSQL_MEMORY_LIMIT
// An operator sets a DuckDB memory budget at startup so one shared instance
// cannot use the whole machine. These tests verify the flag reaches DuckDB
// (visible via current_setting('memory_limit')) and that unsafe values are
// rejected before the server begins serving — the value is concatenated into
// SET memory_limit, so it must stay a safe config token.
// =============================================================================

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <memory>
#include <string>

#include <arrow/array.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/table.h>

#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

std::string ToLower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

// DuckDB may render the same budget as "512.0MB" or "512.0 MiB" across versions.
// Assert the startup flag landed as ~512 megabytes, not the exact pretty-print.
bool LooksLike512Megabytes(const std::string& setting) {
  const std::string lower = ToLower(setting);
  const bool has_512 = lower.find("512") != std::string::npos;
  const bool has_mb =
      lower.find("mb") != std::string::npos || lower.find("mib") != std::string::npos;
  return has_512 && has_mb;
}

arrow::Result<std::string> QueryMemoryLimitSetting(int port, const std::string& username,
                                                   const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto flight_client,
                        arrow::flight::FlightClient::Connect(
                            location, arrow::flight::FlightClientOptions()));

  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        flight_client->AuthenticateBasicToken({}, username, password));
  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(flight_client));
  ARROW_ASSIGN_OR_RAISE(
      auto info, sql_client.Execute(call_options,
                                    "SELECT current_setting('memory_limit')::VARCHAR "
                                    "AS setting_value"));

  std::shared_ptr<arrow::Table> table;
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, sql_client.DoGet(call_options, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(table, reader->ToTable());
  }
  if (!table || table->num_rows() != 1) {
    return arrow::Status::Invalid("Expected exactly one row from current_setting()");
  }
  auto column = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
  return column->GetString(0);
}

}  // namespace

// =============================================================================
// Fixture: server started with --memory-limit=512MB
// =============================================================================
class MemoryLimitFixture
    : public gizmosql::testing::ServerTestFixture<MemoryLimitFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "memory_limit_test.db",
        .port = 31614,
        .health_port = 31615,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        .memory_limit = "512MB",
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<MemoryLimitFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<MemoryLimitFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<MemoryLimitFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<MemoryLimitFixture>::config_{};

TEST_F(MemoryLimitFixture, CurrentSettingReflectsStartupLimit) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  auto setting = QueryMemoryLimitSetting(GetPort(), GetUsername(), GetPassword());
  ASSERT_TRUE(setting.ok()) << setting.status().ToString();
  EXPECT_TRUE(LooksLike512Megabytes(*setting))
      << "Expected --memory-limit=512MB to reach DuckDB; current_setting('memory_limit') "
         "was: "
      << *setting;
}

// =============================================================================
// Reject values that cannot be a safe SET memory_limit token (quotes / SQL).
// =============================================================================
TEST(MemoryLimitValidation, RejectsDisallowedCharacters) {
  namespace fs = std::filesystem;

  const std::string kDb = "memory_limit_invalid_test.db";
  std::error_code ec;
  fs::remove(kDb, ec);
  fs::remove(kDb + ".wal", ec);

  fs::path db_path(kDb);
  // memory_limit is concatenated into SET; quote/semicolon must not open a server.
  auto result = gizmosql::CreateFlightSQLServer(
      BackendType::duckdb, db_path, "localhost", /*port=*/31616,
      /*username=*/"testuser", /*password=*/"testpassword",
      /*secret_key=*/"test_secret_key_for_testing",
      /*tls_cert_path=*/fs::path(),
      /*tls_key_path=*/fs::path(),
      /*mtls_ca_cert_path=*/fs::path(),
      /*init_sql_commands=*/"",
      /*init_sql_commands_file=*/fs::path(),
      /*print_queries=*/false,
      /*read_only=*/false,
      /*token_allowed_issuer=*/"",
      /*token_allowed_audience=*/"",
      /*token_signature_verify_cert_path=*/fs::path(),
      /*token_jwks_uri=*/"",
      /*token_default_role=*/"",
      /*token_authorized_emails=*/"",
      /*access_logging_enabled=*/false,
      /*query_timeout=*/0,
      /*query_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*auth_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*session_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*health_port=*/0,
      /*health_check_query=*/"",
      /*enable_instrumentation=*/false,
      /*instrumentation_db_path=*/"",
      /*instrumentation_catalog=*/"",
      /*instrumentation_schema=*/"",
      /*instance_tag=*/"",
      /*allow_cross_instance_tokens=*/false,
      /*oauth_client_id=*/"",
      /*oauth_client_secret=*/"",
      /*oauth_scopes=*/"",
      /*oauth_port=*/0,
      /*oauth_base_url=*/"",
      /*oauth_redirect_uri=*/"",
      /*oauth_instance_id=*/"",
      /*oauth_disable_tls=*/false,
      /*telemetry_enabled=*/false,
      /*max_metadata_size=*/0,
      /*storage_version=*/"",
      /*max_concurrent_statements=*/0,
      /*max_queued_statements=*/-1,
      /*max_queue_wait_seconds=*/-1,
      /*admin_bypass_queue_default=*/true,
      /*memory_limit=*/"512MB'; DROP TABLE t; --");

  ASSERT_FALSE(result.ok()) << "CreateFlightSQLServer must reject unsafe memory_limit";
  EXPECT_NE(result.status().ToString().find("Invalid memory_limit"), std::string::npos)
      << result.status().ToString();

  fs::remove(kDb, ec);
  fs::remove(kDb + ".wal", ec);
}
