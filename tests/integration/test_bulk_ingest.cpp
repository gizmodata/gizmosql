#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <future>
#include <iostream>
#include <sstream>
#include <filesystem>
#include <cstdio>

#ifndef _WIN32
#include <unistd.h>  // pipe, dup2
#endif

#include "common/include/gizmosql_library.h"
#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using arrow::flight::sql::FlightSqlClient;
using arrow::flight::sql::TableDefinitionOptions;
using arrow::flight::sql::TableDefinitionOptionsTableExistsOption;
using arrow::flight::sql::TableDefinitionOptionsTableNotExistOption;

//--------------------------------------------------
// Helpers
//--------------------------------------------------

std::shared_ptr<arrow::RecordBatchReader> MakeTestBatches() {
  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder;
  ARROW_EXPECT_OK(id_builder.AppendValues({1, 2, 3}));
  ARROW_EXPECT_OK(name_builder.AppendValues({"alice", "bob", "carol"}));

  std::shared_ptr<arrow::Array> ids, names;
  ARROW_EXPECT_OK(id_builder.Finish(&ids));
  ARROW_EXPECT_OK(name_builder.Finish(&names));

  auto schema = arrow::schema(
      {arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});

  auto batch = arrow::RecordBatch::Make(schema, ids->length(), {ids, names});

  auto maybe_reader = arrow::RecordBatchReader::Make({batch});
  ARROW_EXPECT_OK(maybe_reader.status());
  return *maybe_reader;
}

bool WaitForPortOpen(int port, int timeout_seconds = 10) {
  using namespace std::chrono_literals;
  auto start = std::chrono::steady_clock::now();

  while (std::chrono::steady_clock::now() - start <
         std::chrono::seconds(timeout_seconds)) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(port);
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);  // 127.0.0.1

      int result = connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
      close(sock);
      if (result == 0) {
        return true;  // port is accepting connections
      }
    }
    std::this_thread::sleep_for(100ms);
  }
  return false;
}

//--------------------------------------------------
// Server fixture — spawns GizmoSQL server and waits
//--------------------------------------------------
class GizmoSQLServerFixture : public ::testing::Test {
 protected:
  std::thread server_thread_;
  std::atomic<bool> server_ready_{false};

  void SetUp() override {
    // Redirect stdout to a pipe so we can capture logs.
    FILE* pipe_read = nullptr;
    FILE* pipe_write = nullptr;
#if defined(_WIN32)
    _pipe((int*)&pipe_read, 4096, O_BINARY);
#else
    int fds[2];
    ASSERT_EQ(pipe(fds), 0) << "pipe() failed";

    pipe_read = fdopen(fds[0], "r");
    pipe_write = fdopen(fds[1], "w");
    ASSERT_NE(pipe_read, nullptr);
    ASSERT_NE(pipe_write, nullptr);

    // Duplicate stdout so server logs go to pipe
    fflush(stdout);
    dup2(fds[1], fileno(stdout));
#endif

    // Start server thread
    server_thread_ = std::thread([]() {
      RunFlightSQLServer(
          BackendType::duckdb, std::filesystem::path("tester.db"), "0.0.0.0",
          DEFAULT_FLIGHT_PORT, "tester", "tester", "", std::filesystem::path(),
          std::filesystem::path(), std::filesystem::path(), "", std::filesystem::path(),
          true, false, "", "", std::filesystem::path(), "debug", "text", "off", "", 0,
          "", "", DEFAULT_HEALTH_PORT,
          "off", "", "", "", "");  // OpenTelemetry params (disabled for tests)
    });

    ASSERT_TRUE(WaitForPortOpen(DEFAULT_FLIGHT_PORT, 10))
        << "Timed out waiting for GizmoSQL server startup";
    server_ready_ = true;

    ASSERT_TRUE(server_ready_) << "Timed out waiting for GizmoSQL server startup";
  }

  void TearDown() override {
    // No shutdown API yet — just detach
    if (server_thread_.joinable()) {
      server_thread_.detach();
    }
  }
};

//--------------------------------------------------
// Integration test
//--------------------------------------------------
TEST_F(GizmoSQLServerFixture, ExecuteIngestEndToEnd) {
  ASSERT_TRUE(server_ready_) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location, arrow::flight::Location::ForGrpcTcp(
                                                "localhost", DEFAULT_FLIGHT_PORT));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;

  // Authenticate and attach bearer header
  ASSERT_ARROW_OK_AND_ASSIGN(auto bearer,
                             client->AuthenticateBasicToken({}, "tester", "tester"));
  call_options.headers.push_back(bearer);

  arrow::flight::sql::FlightSqlClient sql_client(std::move(client));

  auto record_batch_reader = MakeTestBatches();

  TableDefinitionOptions table_opts;
  table_opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kCreate;
  table_opts.if_exists = TableDefinitionOptionsTableExistsOption::kAppend;

  std::unordered_map<std::string, std::string> ingest_options = {{"key1", "val1"},
                                                                 {"key2", "val2"}};

  auto maybe_rows = sql_client.ExecuteIngest(
      call_options, record_batch_reader, table_opts, "test_table_with_default",
      std::nullopt, std::nullopt, false /* temporary */,
      arrow::flight::sql::no_transaction(), ingest_options);

  if (!maybe_rows.ok()) {
    std::cerr << "\n❌ ExecuteIngest failed:\n"
              << maybe_rows.status().ToString() << std::endl;
    FAIL() << "ExecuteIngest failed";
  }

  auto updated_rows = *maybe_rows;
  std::cerr << "✅ ExecuteIngest succeeded: " << updated_rows << " rows" << std::endl;

  ASSERT_EQ(updated_rows, 3) << "Expected 3 ingested rows";
}
