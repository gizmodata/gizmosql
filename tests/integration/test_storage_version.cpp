// Verifies that the DuckDB `storage_version` config option (exposed via the
// --storage-version CLI flag / GIZMOSQL_STORAGE_VERSION env var / library
// `storage_version` parameter) is actually applied to the underlying DuckDB
// instance.
//
// Uses `FROM duckdb_databases()` and checks the `tags` map: when
// storage_version is set to "latest" against a fresh database file, DuckDB
// stamps the on-disk tag with the newest format the running engine supports
// (e.g. "v1.5.0+"), instead of the default "v1.0.0+" tag that a
// brand-new DB would otherwise carry.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

// Pull `tags['storage_version']` for the user database row out of
// duckdb_databases(). On DuckDB 1.5.x map element access on a
// MAP<VARCHAR, VARCHAR> already returns a scalar VARCHAR.
constexpr const char* kQuery =
    "SELECT tags['storage_version']::VARCHAR AS sv "
    "FROM duckdb_databases() "
    "WHERE database_name = 'storage_version_test'";

std::string GetStorageVersionTag(FlightSqlClient& sql_client,
                                 const arrow::flight::FlightCallOptions& opts) {
  auto info_res = sql_client.Execute(opts, kQuery);
  EXPECT_TRUE(info_res.ok()) << info_res.status().ToString();
  if (!info_res.ok()) return "";
  auto& info = *info_res;

  for (const auto& endpoint : info->endpoints()) {
    auto reader_res = sql_client.DoGet(opts, endpoint.ticket);
    EXPECT_TRUE(reader_res.ok()) << reader_res.status().ToString();
    if (!reader_res.ok()) return "";
    auto table_res = (*reader_res)->ToTable();
    EXPECT_TRUE(table_res.ok()) << table_res.status().ToString();
    if (!table_res.ok()) return "";
    auto table = *table_res;
    EXPECT_EQ(table->num_rows(), 1) << "duckdb_databases() returned no row "
                                       "for the user database";
    if (table->num_rows() != 1) return "";
    auto col = table->column(0);
    auto chunk = std::dynamic_pointer_cast<arrow::StringArray>(col->chunk(0));
    EXPECT_NE(chunk, nullptr) << "Expected VARCHAR column for tag value";
    if (!chunk) return "";
    EXPECT_FALSE(chunk->IsNull(0)) << "tags['storage_version'] was unexpectedly NULL";
    return chunk->GetString(0);
  }
  return "";
}

}  // namespace

// ----------------------------------------------------------------------------
// Fixture: server started with --storage-version=latest
// ----------------------------------------------------------------------------
class StorageVersionLatestFixture
    : public gizmosql::testing::ServerTestFixture<StorageVersionLatestFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "storage_version_test.db",
        .port = 31390,
        .health_port = 31391,
        .username = "sv_user",
        .password = "sv_password123",
        .storage_version = "latest",
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<StorageVersionLatestFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<StorageVersionLatestFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<StorageVersionLatestFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<StorageVersionLatestFixture>::config_{};

TEST_F(StorageVersionLatestFixture, TagReflectsLatestStorageVersion) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  const std::string tag = GetStorageVersionTag(sql_client, call_options);
  ASSERT_FALSE(tag.empty()) << "Did not retrieve a storage_version tag";

  // With --storage-version=latest against a fresh DB, DuckDB stamps the file
  // with the newest format the engine supports (currently v1.5.0+ on
  // DuckDB 1.5.x). The exact tag floats with DuckDB upgrades, so we just
  // assert it is NOT the v1.0.0+ baseline that a default-config DB would
  // carry. That is the regression we care about: that --storage-version
  // actually reaches DBConfig.options.serialization_compatibility.
  EXPECT_NE(tag, "v1.0.0+")
      << "Expected --storage-version=latest to bump the on-disk storage_version "
         "tag past the v1.0.0+ default; got: " << tag;
}

// ----------------------------------------------------------------------------
// Fixture: server started without --storage-version (default behavior)
// ----------------------------------------------------------------------------
class StorageVersionDefaultFixture
    : public gizmosql::testing::ServerTestFixture<StorageVersionDefaultFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "storage_version_test.db",
        .port = 31392,
        .health_port = 31393,
        .username = "sv_user",
        .password = "sv_password123",
        // storage_version intentionally left empty
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<StorageVersionDefaultFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<StorageVersionDefaultFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<StorageVersionDefaultFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<StorageVersionDefaultFixture>::config_{};

TEST_F(StorageVersionDefaultFixture, TagIsBaselineWhenStorageVersionUnset) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  const std::string tag = GetStorageVersionTag(sql_client, call_options);
  ASSERT_FALSE(tag.empty()) << "Did not retrieve a storage_version tag";

  // Pinned: control case. A fresh DB with default serialization_compatibility
  // is tagged v1.0.0+, which is what proves the "latest" assertion above is
  // actually testing something.
  EXPECT_EQ(tag, "v1.0.0+")
      << "Expected default storage_version tag to be v1.0.0+; got: " << tag;
}
