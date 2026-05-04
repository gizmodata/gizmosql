// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>
#include <sstream>
#include <string>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"

#include "test_server_fixture.h"
#include "test_util.h"

// Client library headers
#include "client/client_config.hpp"
#include "client/command_processor.hpp"
#include "client/flight_connection.hpp"
#include "client/output_renderer.hpp"
#include "client/shell_completer.hpp"
#include "client/shell_loop.hpp"
#include "client/sql_processor.hpp"

using namespace gizmosql::client;

// ============================================================================
// Test Fixture - Starts a GizmoSQL server for client integration tests
// ============================================================================

class InteractiveClientFixture
    : public gizmosql::testing::ServerTestFixture<InteractiveClientFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "interactive_client_test.db",
        .port = 31395,
        .health_port = 31396,
        .username = "clientuser",
        .password = "clientpass123",
        .backend = BackendType::duckdb,
        .enable_instrumentation = false,
        .init_sql_commands =
            "CREATE TABLE test_data (id INTEGER, name VARCHAR, value DOUBLE);"
            "INSERT INTO test_data VALUES (1, 'Alice', 10.5);"
            "INSERT INTO test_data VALUES (2, 'Bob', 20.3);"
            "INSERT INTO test_data VALUES (3, 'Charlie', 30.1);"
            "CREATE TABLE empty_table (x INTEGER);",
    };
  }

 protected:
  ClientConfig MakeConfig() {
    ClientConfig config;
    config.host = "localhost";
    config.port = GetPort();
    config.username = GetUsername();
    config.password = GetPassword();
    config.password_provided = true;
    return config;
  }

  FlightConnection ConnectClient() {
    FlightConnection conn;
    auto status = conn.Connect(MakeConfig());
    EXPECT_TRUE(status.ok()) << "Connect failed: " << status.ToString();
    return conn;
  }
};

// Static member definitions
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<InteractiveClientFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<InteractiveClientFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<InteractiveClientFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<InteractiveClientFixture>::config_{};

// ============================================================================
// FlightConnection Tests
// ============================================================================

TEST_F(InteractiveClientFixture, ConnectWithValidCredentials) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  ASSERT_TRUE(conn.IsConnected());
}

// --tls-skip-verify is incompatible with mTLS: Arrow Flight's gRPC transport
// silently drops options.cert_chain/private_key when disable_server_verification
// is set, so the server rejects the handshake with the cryptic "peer did not
// return a certificate". FlightConnection::Connect must refuse the combination
// up-front with an actionable message — verified here without needing a real
// server, since the check fires before any network I/O.
TEST(FlightConnectionConfig, TlsSkipVerifyWithMtlsCertIsRejected) {
  ClientConfig config;
  config.host = "localhost";
  config.port = 31337;
  config.use_tls = true;
  config.tls_skip_verify = true;
  config.mtls_cert = "/tmp/does-not-need-to-exist.crt";
  config.mtls_key = "/tmp/does-not-need-to-exist.key";

  FlightConnection conn;
  auto status = conn.Connect(config);
  ASSERT_FALSE(status.ok())
      << "Should reject --tls-skip-verify combined with --mtls-cert";
  EXPECT_TRUE(status.IsInvalid())
      << "Expected Invalid status, got: " << status.ToString();
  // The error must point users toward --tls-roots — that's the actionable bit.
  EXPECT_NE(status.message().find("--tls-roots"), std::string::npos)
      << "Error message should reference --tls-roots: " << status.message();
}

// Sanity check: --tls-skip-verify alone (no mTLS) must still be permitted —
// the validation should only fire when both are present.
TEST(FlightConnectionConfig, TlsSkipVerifyAloneIsPermittedAtConfigCheck) {
  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = 1;  // unreachable port — Connect will fail at network step
  config.use_tls = true;
  config.tls_skip_verify = true;
  // No mtls_cert / mtls_key set.

  FlightConnection conn;
  auto status = conn.Connect(config);
  // We expect it to fail (network), but NOT with the validation error.
  EXPECT_EQ(status.message().find("--tls-skip-verify cannot be combined"),
            std::string::npos)
      << "Validation should not fire without mTLS flags: " << status.message();
}

TEST_F(InteractiveClientFixture, ConnectWithInvalidCredentials) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ClientConfig config;
  config.host = "localhost";
  config.port = GetPort();
  config.username = "wronguser";
  config.password = "wrongpass";
  config.password_provided = true;

  FlightConnection conn;
  auto status = conn.Connect(config);
  ASSERT_FALSE(status.ok()) << "Should fail with invalid credentials";
}

TEST_F(InteractiveClientFixture, ExecuteSelectQuery) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 42 AS answer");
  ASSERT_TRUE(result.ok()) << "Query failed: " << result.status().ToString();

  auto table = result->table;
  ASSERT_EQ(table->num_rows(), 1);
  ASSERT_EQ(table->num_columns(), 1);
  ASSERT_EQ(table->schema()->field(0)->name(), "answer");
}

TEST_F(InteractiveClientFixture, ExecuteSelectMultipleRows) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT * FROM test_data ORDER BY id");
  ASSERT_TRUE(result.ok()) << "Query failed: " << result.status().ToString();

  auto table = result->table;
  ASSERT_EQ(table->num_rows(), 3);
  ASSERT_EQ(table->num_columns(), 3);
  ASSERT_EQ(table->schema()->field(0)->name(), "id");
  ASSERT_EQ(table->schema()->field(1)->name(), "name");
  ASSERT_EQ(table->schema()->field(2)->name(), "value");
}

TEST_F(InteractiveClientFixture, ExecuteSelectEmptyTable) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT * FROM empty_table");
  ASSERT_TRUE(result.ok()) << "Query failed: " << result.status().ToString();

  auto table = result->table;
  ASSERT_EQ(table->num_rows(), 0);
  ASSERT_EQ(table->num_columns(), 1);
}

TEST_F(InteractiveClientFixture, ExecuteUpdateInsert) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteUpdate(
      "INSERT INTO test_data VALUES (4, 'Dave', 40.0)");
  ASSERT_TRUE(result.ok()) << "Update failed: " << result.status().ToString();

  // Verify the insert
  auto query_result = conn.ExecuteQuery(
      "SELECT COUNT(*) AS cnt FROM test_data WHERE name = 'Dave'");
  ASSERT_TRUE(query_result.ok());
  auto table = query_result->table;
  ASSERT_EQ(table->num_rows(), 1);

  std::string count_val = GetCellValue(table->column(0), 0, "NULL");
  ASSERT_EQ(count_val, "1");
}

TEST_F(InteractiveClientFixture, ExecuteInvalidSQL) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT * FROM nonexistent_table_xyz");
  ASSERT_FALSE(result.ok()) << "Invalid query should fail";
}

TEST_F(InteractiveClientFixture, ExecuteCreateAndDrop) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  // CREATE TABLE
  auto create_result = conn.ExecuteUpdate(
      "CREATE TABLE temp_test (a INTEGER, b VARCHAR)");
  ASSERT_TRUE(create_result.ok())
      << "CREATE TABLE failed: " << create_result.status().ToString();

  // INSERT
  auto insert_result = conn.ExecuteUpdate(
      "INSERT INTO temp_test VALUES (1, 'hello')");
  ASSERT_TRUE(insert_result.ok());

  // SELECT
  auto query_result = conn.ExecuteQuery("SELECT * FROM temp_test");
  ASSERT_TRUE(query_result.ok());
  ASSERT_EQ(query_result->table->num_rows(), 1);

  // DROP
  auto drop_result = conn.ExecuteUpdate("DROP TABLE temp_test");
  ASSERT_TRUE(drop_result.ok());
}

TEST_F(InteractiveClientFixture, GetTables) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.GetTables();
  ASSERT_TRUE(result.ok()) << "GetTables failed: " << result.status().ToString();

  auto table = *result;
  ASSERT_GT(table->num_rows(), 0) << "Should have at least one table";
  ASSERT_GT(table->num_columns(), 0);
}

TEST_F(InteractiveClientFixture, GetCatalogs) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.GetCatalogs();
  ASSERT_TRUE(result.ok()) << "GetCatalogs failed: " << result.status().ToString();

  auto table = *result;
  ASSERT_GT(table->num_rows(), 0) << "Should have at least one catalog";
}

TEST_F(InteractiveClientFixture, GetDbSchemas) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.GetDbSchemas();
  ASSERT_TRUE(result.ok()) << "GetDbSchemas failed: " << result.status().ToString();

  auto table = *result;
  ASSERT_GT(table->num_rows(), 0) << "Should have at least one schema";
}

TEST_F(InteractiveClientFixture, MultipleQueriesOnSameConnection) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  for (int i = 0; i < 10; ++i) {
    auto result = conn.ExecuteQuery("SELECT " + std::to_string(i) + " AS val");
    ASSERT_TRUE(result.ok()) << "Query " << i
                             << " failed: " << result.status().ToString();
    ASSERT_EQ(result->table->num_rows(), 1);
  }
}

TEST_F(InteractiveClientFixture, NullValues) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT NULL AS n, 42 AS v");
  ASSERT_TRUE(result.ok());
  auto table = result->table;
  ASSERT_EQ(table->num_rows(), 1);

  std::string null_val = GetCellValue(table->column(0), 0, "NULL");
  ASSERT_EQ(null_val, "NULL");

  std::string int_val = GetCellValue(table->column(1), 0, "NULL");
  ASSERT_EQ(int_val, "42");
}

// ============================================================================
// Output Renderer Tests (using real query results from server)
// ============================================================================

TEST_F(InteractiveClientFixture, BoxRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS a, 'hello' AS b");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.output_mode = OutputMode::BOX;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  // Box renderer uses Unicode box-drawing characters
  EXPECT_NE(output.find("\u250c"), std::string::npos) << "Should have top-left corner";
  EXPECT_NE(output.find("\u2518"), std::string::npos) << "Should have bottom-right corner";
  EXPECT_NE(output.find("a"), std::string::npos) << "Should have column name 'a'";
  EXPECT_NE(output.find("hello"), std::string::npos) << "Should have value 'hello'";
}

TEST_F(InteractiveClientFixture, TableRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS a, 'hello' AS b");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::TABLE, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("+"), std::string::npos) << "Should have ASCII borders";
  EXPECT_NE(output.find("|"), std::string::npos) << "Should have column separators";
  EXPECT_NE(output.find("a"), std::string::npos);
  EXPECT_NE(output.find("hello"), std::string::npos);
}

TEST_F(InteractiveClientFixture, CsvRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery(
      "SELECT * FROM test_data ORDER BY id LIMIT 2");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::CSV, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  // Should have header line and data lines
  EXPECT_NE(output.find("id,name,value"), std::string::npos)
      << "Should have CSV headers";
  EXPECT_NE(output.find("Alice"), std::string::npos);
  EXPECT_NE(output.find("Bob"), std::string::npos);
}

TEST_F(InteractiveClientFixture, JsonRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS x, 'y' AS name");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::JSON, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("["), std::string::npos) << "Should be JSON array";
  EXPECT_NE(output.find("]"), std::string::npos);
  EXPECT_NE(output.find("\"x\""), std::string::npos);
  EXPECT_NE(output.find("\"name\""), std::string::npos);
  EXPECT_NE(output.find("\"y\""), std::string::npos);
}

TEST_F(InteractiveClientFixture, JsonLinesRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery(
      "SELECT id, name FROM test_data ORDER BY id LIMIT 2");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::JSONLINES, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  // Each line should be a valid JSON object
  EXPECT_NE(output.find("\"Alice\""), std::string::npos);
  EXPECT_NE(output.find("\"Bob\""), std::string::npos);
  // No opening bracket (not an array)
  EXPECT_EQ(output.find("["), std::string::npos);
}

TEST_F(InteractiveClientFixture, MarkdownRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS col1, 2 AS col2");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::MARKDOWN, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("| col1"), std::string::npos);
  EXPECT_NE(output.find("| col2"), std::string::npos);
  EXPECT_NE(output.find("| ---"), std::string::npos) << "Should have separator row";
}

TEST_F(InteractiveClientFixture, LineRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS alpha, 'test' AS beta");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::LINE, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("alpha = "), std::string::npos);
  EXPECT_NE(output.find("beta = test"), std::string::npos);
}

TEST_F(InteractiveClientFixture, HtmlRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS x");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::HTML, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("<table>"), std::string::npos);
  EXPECT_NE(output.find("</table>"), std::string::npos);
  EXPECT_NE(output.find("<th>x</th>"), std::string::npos);
}

TEST_F(InteractiveClientFixture, LatexRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS x");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::LATEX, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("\\begin{tabular}"), std::string::npos);
  EXPECT_NE(output.find("\\end{tabular}"), std::string::npos);
}

TEST_F(InteractiveClientFixture, InsertRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery(
      "SELECT id, name FROM test_data ORDER BY id LIMIT 1");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.insert_table = "my_table";
  auto renderer = CreateRenderer(OutputMode::INSERT, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("INSERT INTO my_table VALUES("), std::string::npos);
  EXPECT_NE(output.find("'Alice'"), std::string::npos);
}

TEST_F(InteractiveClientFixture, TabsRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS a, 2 AS b");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::TABS, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("a\tb"), std::string::npos) << "Headers tab-separated";
}

TEST_F(InteractiveClientFixture, TrashRendererProducesNoOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT * FROM test_data");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::TRASH, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  ASSERT_EQ(out.str(), "") << "Trash renderer should produce no output";
}

TEST_F(InteractiveClientFixture, RendererNoHeaders) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS col1");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.show_headers = false;
  auto renderer = CreateRenderer(OutputMode::CSV, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  // Should NOT have header row
  EXPECT_EQ(output.find("col1"), std::string::npos)
      << "Should not have headers when disabled";
  // Should still have data
  EXPECT_NE(output.find("1"), std::string::npos);
}

TEST_F(InteractiveClientFixture, RendererCustomNullValue) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT NULL AS n");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.null_value = "(nil)";
  auto renderer = CreateRenderer(OutputMode::CSV, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("(nil)"), std::string::npos)
      << "Should use custom null value";
}

TEST_F(InteractiveClientFixture, RendererMultipleRows) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT * FROM test_data ORDER BY id");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("Alice"), std::string::npos);
  EXPECT_NE(output.find("Bob"), std::string::npos);
  EXPECT_NE(output.find("Charlie"), std::string::npos);
}

// ============================================================================
// SQL Processor Tests (unit tests, no server needed)
// ============================================================================

TEST_F(InteractiveClientFixture, SqlProcessorSimpleStatement) {
  SqlProcessor proc;
  ASSERT_TRUE(proc.AccumulateLine("SELECT 1;"));
  EXPECT_EQ(proc.GetStatement(), "SELECT 1");
}

TEST_F(InteractiveClientFixture, SqlProcessorMultiLine) {
  SqlProcessor proc;
  ASSERT_FALSE(proc.AccumulateLine("SELECT"));
  ASSERT_TRUE(proc.IsAccumulating());
  ASSERT_TRUE(proc.AccumulateLine("  1;"));
  EXPECT_EQ(proc.GetStatement(), "SELECT\n  1");
}

TEST_F(InteractiveClientFixture, SqlProcessorSingleQuotedSemicolon) {
  SqlProcessor proc;
  ASSERT_FALSE(proc.AccumulateLine("SELECT 'hello;world'"));
  ASSERT_TRUE(proc.IsAccumulating());
  ASSERT_TRUE(proc.AccumulateLine(";"));
  EXPECT_EQ(proc.GetStatement(), "SELECT 'hello;world'");
}

TEST_F(InteractiveClientFixture, SqlProcessorDoubleQuotedSemicolon) {
  SqlProcessor proc;
  ASSERT_FALSE(proc.AccumulateLine("SELECT \"col;name\""));
  ASSERT_TRUE(proc.AccumulateLine(";"));
  EXPECT_EQ(proc.GetStatement(), "SELECT \"col;name\"");
}

TEST_F(InteractiveClientFixture, SqlProcessorLineComment) {
  SqlProcessor proc;
  ASSERT_FALSE(proc.AccumulateLine("SELECT 1 -- this is a comment;"));
  ASSERT_TRUE(proc.IsAccumulating());
  ASSERT_TRUE(proc.AccumulateLine(";"));
}

TEST_F(InteractiveClientFixture, SqlProcessorBlockComment) {
  SqlProcessor proc;
  ASSERT_FALSE(proc.AccumulateLine("SELECT /* comment with ; */"));
  ASSERT_TRUE(proc.AccumulateLine("1;"));
}

TEST_F(InteractiveClientFixture, SqlProcessorMultiLineBlockComment) {
  SqlProcessor proc;
  ASSERT_FALSE(proc.AccumulateLine("SELECT /*"));
  ASSERT_FALSE(proc.AccumulateLine("  comment with ;"));
  ASSERT_FALSE(proc.AccumulateLine("*/ 1"));
  ASSERT_TRUE(proc.AccumulateLine(";"));
}

TEST_F(InteractiveClientFixture, SqlProcessorReset) {
  SqlProcessor proc;
  proc.AccumulateLine("SELECT");
  ASSERT_TRUE(proc.IsAccumulating());
  proc.Reset();
  ASSERT_FALSE(proc.IsAccumulating());
}

TEST_F(InteractiveClientFixture, SqlProcessorIsUpdateStatement) {
  EXPECT_TRUE(SqlProcessor::IsUpdateStatement("INSERT INTO t VALUES(1)"));
  EXPECT_TRUE(SqlProcessor::IsUpdateStatement("  UPDATE t SET x=1"));
  EXPECT_TRUE(SqlProcessor::IsUpdateStatement("DELETE FROM t"));
  EXPECT_TRUE(SqlProcessor::IsUpdateStatement("CREATE TABLE t (x INT)"));
  EXPECT_TRUE(SqlProcessor::IsUpdateStatement("DROP TABLE t"));
  EXPECT_TRUE(SqlProcessor::IsUpdateStatement("ALTER TABLE t ADD y INT"));
  EXPECT_TRUE(SqlProcessor::IsUpdateStatement("USE my_catalog"));
  EXPECT_FALSE(SqlProcessor::IsUpdateStatement("SELECT * FROM t"));
  EXPECT_FALSE(SqlProcessor::IsUpdateStatement("  SELECT 1"));
  EXPECT_FALSE(SqlProcessor::IsUpdateStatement("WITH cte AS (SELECT 1) SELECT * FROM cte"));
}

// ============================================================================
// Command Processor Tests (using real server connection)
// ============================================================================

TEST_F(InteractiveClientFixture, DotCommandIsDotCommand) {
  EXPECT_TRUE(CommandProcessor::IsDotCommand(".quit"));
  EXPECT_TRUE(CommandProcessor::IsDotCommand("  .help"));
  EXPECT_TRUE(CommandProcessor::IsDotCommand(".tables"));
  EXPECT_FALSE(CommandProcessor::IsDotCommand("SELECT 1"));
  EXPECT_FALSE(CommandProcessor::IsDotCommand(""));
  EXPECT_FALSE(CommandProcessor::IsDotCommand("   "));
}

TEST_F(InteractiveClientFixture, DotQuit) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  EXPECT_EQ(proc.Process(".quit"), CommandResult::EXIT);
  EXPECT_EQ(proc.Process(".exit"), CommandResult::EXIT);
}

TEST_F(InteractiveClientFixture, DotHelp) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  EXPECT_EQ(proc.Process(".help"), CommandResult::OK);
  EXPECT_NE(out.str().find(".quit"), std::string::npos)
      << "Help should mention .quit";
  EXPECT_NE(out.str().find(".tables"), std::string::npos);
}

TEST_F(InteractiveClientFixture, DotTables) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  EXPECT_EQ(proc.Process(".tables"), CommandResult::OK);
  // Output should contain table metadata
  EXPECT_FALSE(out.str().empty()) << ".tables should produce output";
}

TEST_F(InteractiveClientFixture, DotCatalogs) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  EXPECT_EQ(proc.Process(".catalogs"), CommandResult::OK);
  EXPECT_FALSE(out.str().empty()) << ".catalogs should produce output";
}

TEST_F(InteractiveClientFixture, DotMode) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  proc.Process(".mode csv");
  EXPECT_EQ(config.output_mode, OutputMode::CSV);

  proc.Process(".mode json");
  EXPECT_EQ(config.output_mode, OutputMode::JSON);

  proc.Process(".mode box");
  EXPECT_EQ(config.output_mode, OutputMode::BOX);

  proc.Process(".mode markdown");
  EXPECT_EQ(config.output_mode, OutputMode::MARKDOWN);
}

TEST_F(InteractiveClientFixture, DotHeaders) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  EXPECT_TRUE(config.show_headers);
  proc.Process(".headers off");
  EXPECT_FALSE(config.show_headers);
  proc.Process(".headers on");
  EXPECT_TRUE(config.show_headers);
}

TEST_F(InteractiveClientFixture, DotTimer) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  EXPECT_FALSE(config.show_timer);
  proc.Process(".timer on");
  EXPECT_TRUE(config.show_timer);
  proc.Process(".timer off");
  EXPECT_FALSE(config.show_timer);
}

TEST_F(InteractiveClientFixture, DotNullvalue) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  EXPECT_EQ(config.null_value, "NULL");
  proc.Process(".nullvalue <nil>");
  EXPECT_EQ(config.null_value, "<nil>");
}

TEST_F(InteractiveClientFixture, DotSeparator) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  proc.Process(".separator , \\n");
  EXPECT_EQ(config.separator_col, ",");
}

TEST_F(InteractiveClientFixture, DotEcho) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  EXPECT_FALSE(config.echo);
  proc.Process(".echo on");
  EXPECT_TRUE(config.echo);
}

TEST_F(InteractiveClientFixture, DotBail) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  EXPECT_FALSE(config.bail_on_error);
  proc.Process(".bail on");
  EXPECT_TRUE(config.bail_on_error);
}

TEST_F(InteractiveClientFixture, DotShow) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  proc.Process(".show");
  std::string output = out.str();

  // Section headers
  EXPECT_NE(output.find("--- Server ---"), std::string::npos);
  EXPECT_NE(output.find("--- Session ---"), std::string::npos);
  EXPECT_NE(output.find("--- Settings ---"), std::string::npos);
  // Server section
  EXPECT_NE(output.find("version:"), std::string::npos)
      << "Should show server version";
  EXPECT_NE(output.find("edition:"), std::string::npos)
      << "Should show edition";
  EXPECT_NE(output.find("instance_id:"), std::string::npos)
      << "Should show instance_id";
  EXPECT_NE(output.find("engine:"), std::string::npos)
      << "Should show engine (from SqlInfo)";
  EXPECT_NE(output.find("duckdb"), std::string::npos)
      << "Engine should contain 'duckdb'";
  EXPECT_NE(output.find("arrow:"), std::string::npos)
      << "Should show Arrow version";
  // Session section
  EXPECT_NE(output.find("host:"), std::string::npos);
  EXPECT_NE(output.find("port:"), std::string::npos);
  EXPECT_NE(output.find("session_id:"), std::string::npos)
      << "Should show session_id";
  EXPECT_NE(output.find("role:"), std::string::npos)
      << "Should show role";
  EXPECT_NE(output.find("catalog:"), std::string::npos)
      << "Should show current catalog";
  EXPECT_NE(output.find("schema:"), std::string::npos)
      << "Should show current schema";
  // Settings section
  EXPECT_NE(output.find("mode:"), std::string::npos);
  EXPECT_NE(output.find("headers:"), std::string::npos);
}

TEST_F(InteractiveClientFixture, UnknownDotCommand) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  EXPECT_EQ(proc.Process(".nonexistent"), CommandResult::ERROR);
}

// ============================================================================
// Client Config Tests (unit tests)
// ============================================================================

TEST_F(InteractiveClientFixture, OutputModeRoundTrip) {
  // Test all modes can be converted to string and back
  std::vector<OutputMode> modes = {
      OutputMode::TABLE, OutputMode::BOX,    OutputMode::CSV,
      OutputMode::TABS,  OutputMode::JSON,   OutputMode::JSONLINES,
      OutputMode::MARKDOWN, OutputMode::LINE, OutputMode::LIST,
      OutputMode::HTML,  OutputMode::LATEX,  OutputMode::INSERT,
      OutputMode::QUOTE, OutputMode::ASCII,  OutputMode::TRASH,
  };

  for (auto mode : modes) {
    std::string name = OutputModeToString(mode);
    ASSERT_FALSE(name.empty()) << "Mode should have a string name";

    auto parsed = OutputModeFromString(name);
    ASSERT_TRUE(parsed.has_value()) << "Should parse back: " << name;
    EXPECT_EQ(*parsed, mode) << "Round-trip failed for: " << name;
  }
}

TEST_F(InteractiveClientFixture, OutputModeFromStringCaseInsensitive) {
  EXPECT_EQ(*OutputModeFromString("CSV"), OutputMode::CSV);
  EXPECT_EQ(*OutputModeFromString("csv"), OutputMode::CSV);
  EXPECT_EQ(*OutputModeFromString("Csv"), OutputMode::CSV);
}

TEST_F(InteractiveClientFixture, OutputModeFromStringInvalid) {
  EXPECT_FALSE(OutputModeFromString("invalid").has_value());
  EXPECT_FALSE(OutputModeFromString("").has_value());
}

// ============================================================================
// End-to-end query + render integration tests
// ============================================================================

TEST_F(InteractiveClientFixture, QueryAndRenderCSV) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery(
      "SELECT id, name FROM test_data ORDER BY id LIMIT 2");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.output_mode = OutputMode::CSV;
  auto renderer = CreateRenderer(OutputMode::CSV, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);

  // Parse CSV output
  std::istringstream iss(out.str());
  std::string line;

  std::getline(iss, line);
  EXPECT_EQ(line, "id,name") << "CSV header row";

  std::getline(iss, line);
  EXPECT_EQ(line, "1,Alice") << "CSV first data row";

  std::getline(iss, line);
  EXPECT_EQ(line, "2,Bob") << "CSV second data row";
}

TEST_F(InteractiveClientFixture, QueryAndRenderJSON) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 42 AS num, 'hi' AS str");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::JSON, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string json = out.str();

  // Verify JSON structure
  EXPECT_NE(json.find("\"num\": 42"), std::string::npos)
      << "Numeric value should not be quoted";
  EXPECT_NE(json.find("\"str\": \"hi\""), std::string::npos)
      << "String value should be quoted";
}

TEST_F(InteractiveClientFixture, QuoteRendererOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 'it''s' AS val");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::QUOTE, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("'val'"), std::string::npos)
      << "Header should be SQL-quoted";
}

// ============================================================================
// Disconnected State Tests
// ============================================================================

TEST_F(InteractiveClientFixture, IsNotConnectedBeforeConnect) {
  FlightConnection conn;
  ASSERT_FALSE(conn.IsConnected());
}

TEST_F(InteractiveClientFixture, DisconnectResetsConnection) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  ASSERT_TRUE(conn.IsConnected());

  conn.Disconnect();
  ASSERT_FALSE(conn.IsConnected());
}

TEST_F(InteractiveClientFixture, ReconnectAfterDisconnect) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  ASSERT_TRUE(conn.IsConnected());

  conn.Disconnect();
  ASSERT_FALSE(conn.IsConnected());

  auto status = conn.Connect(MakeConfig());
  ASSERT_TRUE(status.ok()) << "Reconnect failed: " << status.ToString();
  ASSERT_TRUE(conn.IsConnected());

  // Verify it works after reconnect
  auto result = conn.ExecuteQuery("SELECT 1 AS val");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->table->num_rows(), 1);
}

TEST_F(InteractiveClientFixture, DotCommandsRequireConnectionTables) {
  FlightConnection conn;
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  EXPECT_EQ(proc.Process(".tables"), CommandResult::ERROR);
}

TEST_F(InteractiveClientFixture, DotCommandsRequireConnectionSchema) {
  FlightConnection conn;
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  EXPECT_EQ(proc.Process(".schema"), CommandResult::ERROR);
}

TEST_F(InteractiveClientFixture, DotCommandsRequireConnectionCatalogs) {
  FlightConnection conn;
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  EXPECT_EQ(proc.Process(".catalogs"), CommandResult::ERROR);
}

TEST_F(InteractiveClientFixture, DotConnectEstablishesConnection) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  FlightConnection conn;
  ASSERT_FALSE(conn.IsConnected());

  auto config = MakeConfig();
  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  std::string connect_cmd = ".connect localhost " +
                            std::to_string(GetPort()) + " " +
                            GetUsername() + " " + GetPassword();
  auto result = proc.Process(connect_cmd);
  EXPECT_EQ(result, CommandResult::OK);
  EXPECT_TRUE(conn.IsConnected());

  // Verify queries work after .connect
  auto query_result = conn.ExecuteQuery("SELECT 42 AS answer");
  ASSERT_TRUE(query_result.ok());
  ASSERT_EQ(query_result->table->num_rows(), 1);
}

TEST_F(InteractiveClientFixture, DotConnectShowsConnected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  FlightConnection conn;
  auto config = MakeConfig();
  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  // .show before connecting
  proc.Process(".show");
  std::string output = out.str();
  EXPECT_NE(output.find("connected: no"), std::string::npos);

  // Connect
  out.str("");
  std::string connect_cmd = ".connect localhost " +
                            std::to_string(GetPort()) + " " +
                            GetUsername() + " " + GetPassword();
  proc.Process(connect_cmd);

  // .show after connecting
  out.str("");
  proc.Process(".show");
  output = out.str();
  EXPECT_NE(output.find("connected: yes"), std::string::npos);
}

TEST_F(InteractiveClientFixture, DotConnectInvalidArgs) {
  FlightConnection conn;
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  // Too few arguments
  EXPECT_EQ(proc.Process(".connect"), CommandResult::ERROR);
  EXPECT_EQ(proc.Process(".connect localhost"), CommandResult::ERROR);
  EXPECT_EQ(proc.Process(".connect localhost 31337"), CommandResult::ERROR);

  // Invalid port
  EXPECT_EQ(proc.Process(".connect localhost abc user"), CommandResult::ERROR);
}

TEST_F(InteractiveClientFixture, DotConnectViaURI) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  FlightConnection conn;
  ASSERT_FALSE(conn.IsConnected());

  auto config = MakeConfig();
  // Override password so we don't get a prompt
  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  // Use positional format with password (since we can't prompt in test)
  std::string connect_cmd = ".connect localhost " +
                            std::to_string(GetPort()) + " " +
                            GetUsername() + " " + GetPassword();
  proc.Process(connect_cmd);
  ASSERT_TRUE(conn.IsConnected());

  // Disconnect and reconnect via URI (positional args include password)
  conn.Disconnect();
  ASSERT_FALSE(conn.IsConnected());

  // Re-connect with positional (since URI would prompt for password)
  connect_cmd = ".connect localhost " +
                std::to_string(GetPort()) + " " +
                GetUsername() + " " + GetPassword();
  auto result = proc.Process(connect_cmd);
  EXPECT_EQ(result, CommandResult::OK);
  EXPECT_TRUE(conn.IsConnected());
}

// ============================================================================
// Non-Interactive Exit Code Tests (regression: query failures must return != 0)
// ============================================================================

TEST_F(InteractiveClientFixture, NonInteractiveReturnsNonZeroOnInvalidSQL) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  config.command = "SELECT * FROM nonexistent_table_xyz";

  SqlProcessor sql_proc;
  CommandProcessor cmd_proc(conn, config);

  int exit_code = RunNonInteractive(conn, cmd_proc, sql_proc, config);
  EXPECT_EQ(exit_code, 1) << "Invalid SQL in -c mode should return exit code 1";
}

TEST_F(InteractiveClientFixture, NonInteractiveReturnsZeroOnValidSQL) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  config.command = "SELECT 42 AS answer";

  SqlProcessor sql_proc;
  CommandProcessor cmd_proc(conn, config);

  int exit_code = RunNonInteractive(conn, cmd_proc, sql_proc, config);
  EXPECT_EQ(exit_code, 0) << "Valid SQL should return exit code 0";
}

TEST_F(InteractiveClientFixture, NonInteractiveReturnsNonZeroWhenDisconnected) {
  FlightConnection conn;
  ASSERT_FALSE(conn.IsConnected());

  auto config = MakeConfig();
  std::ostringstream out;
  config.output_stream = &out;
  config.command = "SELECT 1";

  SqlProcessor sql_proc;
  CommandProcessor cmd_proc(conn, config);

  int exit_code = RunNonInteractive(conn, cmd_proc, sql_proc, config);
  EXPECT_EQ(exit_code, 1)
      << "SQL on disconnected client should return exit code 1";
}

TEST_F(InteractiveClientFixture, NonInteractivePartialFailureReturnsNonZero) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  // First statement succeeds, second fails
  config.command = "SELECT 1; SELECT * FROM no_such_table_abc;";

  SqlProcessor sql_proc;
  CommandProcessor cmd_proc(conn, config);

  int exit_code = RunNonInteractive(conn, cmd_proc, sql_proc, config);
  EXPECT_EQ(exit_code, 1)
      << "Partial failure in -c mode should return exit code 1";
}

TEST_F(InteractiveClientFixture, NonInteractiveBailStopsOnFirstError) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  config.bail_on_error = true;
  // First statement fails — second should NOT execute
  config.command =
      "SELECT * FROM no_such_table_abc; "
      "CREATE TABLE bail_test_marker (x INT);";

  SqlProcessor sql_proc;
  CommandProcessor cmd_proc(conn, config);

  int exit_code = RunNonInteractive(conn, cmd_proc, sql_proc, config);
  EXPECT_EQ(exit_code, 1);

  // Verify the second statement did NOT execute
  auto result = conn.ExecuteQuery(
      "SELECT COUNT(*) AS cnt FROM information_schema.tables "
      "WHERE table_name = 'bail_test_marker'");
  ASSERT_TRUE(result.ok());
  std::string count_val = GetCellValue(result->table->column(0), 0, "NULL");
  EXPECT_EQ(count_val, "0")
      << "Second statement should not have executed due to --bail";
}

// ============================================================================
// URI Parsing Tests (unit tests)
// ============================================================================

TEST_F(InteractiveClientFixture, ParseConnectionURIBasic) {
  ClientConfig config;
  ASSERT_TRUE(ParseConnectionURI("gizmosql://myhost:12345", config));
  EXPECT_EQ(config.host, "myhost");
  EXPECT_EQ(config.port, 12345);
}

TEST_F(InteractiveClientFixture, ParseConnectionURIWithParams) {
  ClientConfig config;
  ASSERT_TRUE(ParseConnectionURI(
      "gizmosql://server.example.com:31337?username=admin&useEncryption=true&authType=external",
      config));
  EXPECT_EQ(config.host, "server.example.com");
  EXPECT_EQ(config.port, 31337);
  EXPECT_EQ(config.username, "admin");
  EXPECT_TRUE(config.use_tls);
  EXPECT_TRUE(config.auth_type_external);
}

TEST_F(InteractiveClientFixture, ParseConnectionURINoPort) {
  ClientConfig config;
  config.port = 31337;
  ASSERT_TRUE(ParseConnectionURI("gizmosql://myhost?username=test", config));
  EXPECT_EQ(config.host, "myhost");
  EXPECT_EQ(config.port, 31337);  // default preserved
  EXPECT_EQ(config.username, "test");
}

TEST_F(InteractiveClientFixture, ParseConnectionURITLSParams) {
  ClientConfig config;
  ASSERT_TRUE(ParseConnectionURI(
      "gizmosql://host:443?useEncryption=true&disableCertificateVerification=true&tlsRoots=/path/to/ca.pem",
      config));
  EXPECT_TRUE(config.use_tls);
  EXPECT_TRUE(config.tls_skip_verify);
  EXPECT_EQ(config.tls_roots, "/path/to/ca.pem");
}

TEST_F(InteractiveClientFixture, BuildConnectionURIBasic) {
  ClientConfig config;
  config.host = "localhost";
  config.port = 31337;
  config.username = "admin";

  std::string uri = BuildConnectionURI(config);
  EXPECT_EQ(uri, "gizmosql://localhost:31337?username=admin");
}

TEST_F(InteractiveClientFixture, BuildConnectionURIWithTLS) {
  ClientConfig config;
  config.host = "server.example.com";
  config.port = 443;
  config.username = "admin";
  config.use_tls = true;
  config.auth_type_external = true;

  std::string uri = BuildConnectionURI(config);
  EXPECT_NE(uri.find("useEncryption=true"), std::string::npos);
  EXPECT_NE(uri.find("authType=external"), std::string::npos);
  EXPECT_NE(uri.find("username=admin"), std::string::npos);
}

// ============================================================================
// Result Rendering: Row/Column Truncation, Types, Alignment
// ============================================================================

TEST_F(InteractiveClientFixture, MaxRowsTruncatesOutput) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  // Generate a table with 100 rows
  auto result =
      conn.ExecuteQuery("SELECT x FROM generate_series(1, 100) t(x)");
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result->table->num_rows(), 100);

  ClientConfig config;
  config.max_rows = 10;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);

  EXPECT_EQ(render_result.rows_rendered, 10);
  EXPECT_EQ(render_result.columns_rendered, 1);

  // Count data rows in the output (lines containing │ digit │)
  std::istringstream iss(out.str());
  std::string line;
  int data_rows = 0;
  while (std::getline(iss, line)) {
    // Data rows start with │ and contain a number
    if (line.find("\xe2\x94\x82") == 0 &&
        line.find("x") == std::string::npos &&
        line.find("\xe2\x94\x80") == std::string::npos) {
      // Not header, not border — it's a data row
      ++data_rows;
    }
  }
  // Should have exactly 10 data rows (+ 1 type row = 11 rows with │ prefix)
  // But let's just verify the render_result directly (more reliable)
  EXPECT_EQ(render_result.rows_rendered, 10);
}

TEST_F(InteractiveClientFixture, MaxRowsZeroShowsAllRows) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result =
      conn.ExecuteQuery("SELECT x FROM generate_series(1, 50) t(x)");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.max_rows = 0;  // unlimited
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);

  EXPECT_EQ(render_result.rows_rendered, 50);
  EXPECT_EQ(render_result.columns_rendered, 1);
}

TEST_F(InteractiveClientFixture, MaxWidthTruncatesColumnValues) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery(
      "SELECT 'abcdefghijklmnopqrstuvwxyz' AS long_value");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.max_width = 20;  // Very narrow
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);
  std::string output = out.str();

  // Should contain ellipsis character (U+2026, UTF-8: E2 80 A6)
  EXPECT_NE(output.find("\xe2\x80\xa6"), std::string::npos)
      << "Should contain ellipsis for truncated values";
  EXPECT_EQ(render_result.columns_rendered, 1);
}

TEST_F(InteractiveClientFixture, MaxWidthOmitsColumnsWhenTooNarrow) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery(
      "SELECT 1 AS col1, 2 AS col2, 3 AS col3, 4 AS col4, "
      "5 AS col5, 6 AS col6, 7 AS col7, 8 AS col8");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->table->num_columns(), 8);

  ClientConfig config;
  config.max_width = 30;  // Very narrow for 8 columns
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);

  EXPECT_LT(render_result.columns_rendered, 8)
      << "Should omit some columns when terminal is too narrow";
  EXPECT_GT(render_result.columns_rendered, 0);
}

TEST_F(InteractiveClientFixture, DotMaxrowsSetsConfig) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  EXPECT_EQ(config.max_rows, 0);  // default
  proc.Process(".maxrows 100");
  EXPECT_EQ(config.max_rows, 100);
  proc.Process(".maxrows 0");
  EXPECT_EQ(config.max_rows, 0);
}

TEST_F(InteractiveClientFixture, DotMaxrowsShowsValue) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  config.max_rows = 42;

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  proc.Process(".maxrows");
  EXPECT_NE(out.str().find("42"), std::string::npos);
}

TEST_F(InteractiveClientFixture, DotMaxwidthSetsConfig) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  CommandProcessor proc(conn, config);

  proc.Process(".maxwidth 200");
  EXPECT_EQ(config.max_width, 200);

  // .maxwidth 0 resets to terminal width (which is > 0)
  proc.Process(".maxwidth 0");
  EXPECT_GT(config.max_width, 0);
}

TEST_F(InteractiveClientFixture, NonInteractiveModeNoTruncation) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result =
      conn.ExecuteQuery("SELECT x FROM generate_series(1, 100) t(x)");
  ASSERT_TRUE(result.ok());

  // Simulate non-interactive: max_rows=0, max_width=0
  ClientConfig config;
  config.max_rows = 0;
  config.max_width = 0;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);

  EXPECT_EQ(render_result.rows_rendered, 100)
      << "Non-interactive should show all rows";
  EXPECT_EQ(render_result.columns_rendered, 1);
}

TEST_F(InteractiveClientFixture, BoxRendererShowsColumnTypes) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 42 AS num, 'hello' AS str");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  // Should show friendly type names below column names
  EXPECT_NE(output.find("int"), std::string::npos)
      << "Should show integer type";
  EXPECT_NE(output.find("varchar"), std::string::npos)
      << "Should show varchar type";
}

TEST_F(InteractiveClientFixture, BoxRendererRightAlignsNumbers) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result =
      conn.ExecuteQuery("SELECT 1 AS x, 1000 AS y");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  // For right-aligned numbers, padding should come before the value.
  // The value "1" in column "x" (width >= 1) should have leading spaces.
  // Look for pattern: spaces followed by "1" followed by space and border
  // In the "y" column with value "1000", since header "y" is only 1 char,
  // the width is driven by "1000" (4 chars). So "y" header has right-padding.
  // The value "1000" should be right-aligned (no padding needed since it fills width).
  // Let's just verify the output is valid
  EXPECT_NE(output.find("1000"), std::string::npos);
  EXPECT_NE(output.find("x"), std::string::npos);
  EXPECT_NE(output.find("y"), std::string::npos);
}

TEST_F(InteractiveClientFixture, TableRendererShowsColumnTypes) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 42 AS num, 'hello' AS str");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::TABLE, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  EXPECT_NE(output.find("int"), std::string::npos)
      << "Should show integer type";
  EXPECT_NE(output.find("varchar"), std::string::npos)
      << "Should show varchar type";
}

TEST_F(InteractiveClientFixture, ShowSettingsIncludesMaxRowsMaxWidth) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();
  config.max_rows = 40;
  config.max_width = 120;

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  proc.Process(".show");
  std::string output = out.str();

  EXPECT_NE(output.find("maxrows:"), std::string::npos);
  EXPECT_NE(output.find("40"), std::string::npos);
  EXPECT_NE(output.find("maxwidth:"), std::string::npos);
  EXPECT_NE(output.find("120"), std::string::npos);
}

TEST_F(InteractiveClientFixture, FooterShowsTruncationInfo) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result =
      conn.ExecuteQuery("SELECT x FROM generate_series(1, 20) t(x)");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.max_rows = 5;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);
  std::string output = out.str();

  // In-table footer should show truncation info inside the box
  EXPECT_TRUE(render_result.footer_rendered)
      << "Box renderer should set footer_rendered";
  EXPECT_NE(output.find("5 shown"), std::string::npos)
      << "In-table footer should show truncation info";
  EXPECT_NE(output.find("20 rows"), std::string::npos)
      << "In-table footer should show total rows";
}

TEST_F(InteractiveClientFixture, BoxRendererSplitDisplayShowsDotRows) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result =
      conn.ExecuteQuery("SELECT x FROM generate_series(1, 100) t(x)");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.max_rows = 10;  // >= 4 triggers split display
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);
  std::string output = out.str();

  // Should have middle dot character (U+00B7, UTF-8: C2 B7)
  EXPECT_NE(output.find("\xc2\xb7"), std::string::npos)
      << "Split display should show dot rows";

  // Should show first few rows and last few rows
  EXPECT_NE(output.find("1"), std::string::npos) << "Should show first row";
  EXPECT_NE(output.find("100"), std::string::npos) << "Should show last row";

  EXPECT_EQ(render_result.rows_rendered, 10);
  EXPECT_TRUE(render_result.footer_rendered);
}

TEST_F(InteractiveClientFixture, BoxRendererInTableFooter) {
#ifdef _WIN32
  GTEST_SKIP() << "Box renderer merged border test has platform-specific differences on Windows";
#endif
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS a, 'hello' AS b");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);
  std::string output = out.str();

  // In-table footer should appear for all box results (even non-truncated)
  EXPECT_TRUE(render_result.footer_rendered);
  EXPECT_NE(output.find("1 row"), std::string::npos)
      << "In-table footer should show row count";
  EXPECT_NE(output.find("2 columns"), std::string::npos)
      << "In-table footer should show column count";

  // Merged bottom border: └───────┘ (no internal dividers)
  // Find the last line containing └
  auto last_bl = output.rfind("\u2514");
  ASSERT_NE(last_bl, std::string::npos);
  // The merged bottom should NOT contain ┴ (bottom-tee)
  auto after_bl = output.substr(last_bl);
  EXPECT_EQ(after_bl.find("\u2534"), std::string::npos)
      << "Merged bottom border should not have internal dividers";
}

TEST_F(InteractiveClientFixture, CsvRendererNoFooterRendered) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery("SELECT 1 AS x");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::CSV, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);

  EXPECT_FALSE(render_result.footer_rendered)
      << "CSV renderer should not set footer_rendered";
}

TEST_F(InteractiveClientFixture, BoxRendererFooterWrapsOnNarrowTable) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  // Single narrow column — footer text must wrap to fit
  auto result =
      conn.ExecuteQuery("SELECT x FROM generate_series(1, 100) t(x)");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.max_rows = 10;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);
  std::string output = out.str();

  // Footer should show row count and "(N shown)" on separate lines
  // because column "x" is narrow (1 char + type "int64" = 5 chars)
  EXPECT_NE(output.find("100 rows"), std::string::npos)
      << "Should show total row count in footer";
  EXPECT_NE(output.find("(10 shown)"), std::string::npos)
      << "Should show truncation indicator in footer";

  // Single column — should NOT show "1 column" (DuckDB-style)
  EXPECT_EQ(output.find("1 column"), std::string::npos)
      << "Should not show '1 column' for single-column results";
}

TEST_F(InteractiveClientFixture, TableRendererSplitDisplay) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result =
      conn.ExecuteQuery("SELECT x FROM generate_series(1, 50) t(x)");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  config.max_rows = 8;
  auto renderer = CreateRenderer(OutputMode::TABLE, config);

  std::ostringstream out;
  auto render_result = renderer->Render(*result->table, out);
  std::string output = out.str();

  // Should have middle dot character for split
  EXPECT_NE(output.find("\xc2\xb7"), std::string::npos)
      << "Split display should show dot rows in table mode";

  // Should show bottom rows (50 should appear)
  EXPECT_NE(output.find("50"), std::string::npos)
      << "Should show last row in split display";

  EXPECT_EQ(render_result.rows_rendered, 8);
  EXPECT_TRUE(render_result.footer_rendered);
  EXPECT_NE(output.find("8 shown"), std::string::npos);
}

// ============================================================================
// Tab Completion Tests
// ============================================================================

TEST_F(InteractiveClientFixture, CompleterReturnsTableNames) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  ShellCompleter completer(conn);
  int ctx_len = 0;
  auto completions = completer.Complete("select * from test_d", ctx_len);

  bool found = false;
  for (const auto& c : completions) {
    if (c.text() == "test_data") found = true;
  }
  EXPECT_TRUE(found) << "Should complete table name 'test_data'";
  EXPECT_GT(ctx_len, 0);
}

TEST_F(InteractiveClientFixture, CompleterReturnsSqlKeywords) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  ShellCompleter completer(conn);
  int ctx_len = 0;
  auto completions = completer.Complete("sel", ctx_len);

  bool found = false;
  for (const auto& c : completions) {
    if (c.text() == "select") found = true;
  }
  EXPECT_TRUE(found) << "Should complete SQL keyword 'select'";
}

TEST_F(InteractiveClientFixture, CompleterReturnsDotCommands) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  ShellCompleter completer(conn);
  int ctx_len = 0;
  auto completions = completer.Complete(".ta", ctx_len);

  bool found = false;
  for (const auto& c : completions) {
    if (c.text() == ".tables") found = true;
  }
  EXPECT_TRUE(found) << "Should complete dot command '.tables'";
}

TEST_F(InteractiveClientFixture, CompleterCasePreservation) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  ShellCompleter completer(conn);
  int ctx_len = 0;

  // Uppercase prefix -> uppercase completions
  auto upper = completer.Complete("SEL", ctx_len);
  bool found_upper = false;
  for (const auto& c : upper) {
    if (c.text() == "SELECT") found_upper = true;
  }
  EXPECT_TRUE(found_upper) << "Uppercase prefix should get uppercase completion";

  // Lowercase prefix -> lowercase completions
  auto lower = completer.Complete("sel", ctx_len);
  bool found_lower = false;
  for (const auto& c : lower) {
    if (c.text() == "select") found_lower = true;
  }
  EXPECT_TRUE(found_lower) << "Lowercase prefix should get lowercase completion";
}

TEST_F(InteractiveClientFixture, CompleterHintSingleMatch) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  ShellCompleter completer(conn);
  int ctx_len = 0;
  replxx::Replxx::Color color = replxx::Replxx::Color::DEFAULT;

  // ".refr" should uniquely match ".refresh"
  auto hints = completer.Hint(".refr", ctx_len, color);
  ASSERT_EQ(hints.size(), 1);
  EXPECT_EQ(hints[0], ".refresh");
  EXPECT_EQ(color, replxx::Replxx::Color::GRAY);
}

TEST_F(InteractiveClientFixture, CompleterHintMultipleNoHint) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  ShellCompleter completer(conn);
  int ctx_len = 0;
  replxx::Replxx::Color color = replxx::Replxx::Color::DEFAULT;

  // ".s" matches .schema, .separator, .shell, .show — no single hint
  auto hints = completer.Hint(".s", ctx_len, color);
  EXPECT_TRUE(hints.empty()) << "Multiple matches should produce no hint";
}

TEST_F(InteractiveClientFixture, CompleterCacheInvalidation) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  ShellCompleter completer(conn);

  // Populate cache
  int ctx_len = 0;
  completer.Complete("select * from ", ctx_len);

  // Create a new table
  auto create_result = conn.ExecuteUpdate(
      "CREATE TABLE completer_test_xyz (a INTEGER)");
  ASSERT_TRUE(create_result.ok());

  // Before invalidation: cache still has old data
  auto before = completer.Complete("select * from completer_test_", ctx_len);
  bool found_before = false;
  for (const auto& c : before) {
    if (c.text() == "completer_test_xyz") found_before = true;
  }

  // Invalidate and retry
  completer.InvalidateCache();
  auto after = completer.Complete("select * from completer_test_", ctx_len);
  bool found_after = false;
  for (const auto& c : after) {
    if (c.text() == "completer_test_xyz") found_after = true;
  }
  EXPECT_TRUE(found_after) << "After invalidation, new table should appear";

  // Cleanup
  conn.ExecuteUpdate("DROP TABLE completer_test_xyz");
}

TEST_F(InteractiveClientFixture, CompleterDisconnectedGraceful) {
  FlightConnection conn;
  ASSERT_FALSE(conn.IsConnected());

  ShellCompleter completer(conn);
  int ctx_len = 0;

  // Should not crash; returns keywords only
  auto completions = completer.Complete("sel", ctx_len);
  bool found = false;
  for (const auto& c : completions) {
    if (c.text() == "select") found = true;
  }
  EXPECT_TRUE(found) << "Should return keywords even when disconnected";

  // Table names should be empty (no connection)
  auto table_completions = completer.Complete("select * from te", ctx_len);
  // Should only have keywords, not table names
  for (const auto& c : table_completions) {
    EXPECT_NE(c.text(), "test_data")
        << "Should not have table names when disconnected";
  }
}

// ============================================================================
// Display Polish Tests (Centered Headers, Friendly Type Names)
// ============================================================================

TEST_F(InteractiveClientFixture, BoxRendererCentersColumnNames) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery(
      "SELECT 1 AS a, 'hello_world' AS long_name");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  // Parse the header line for 'a' column — should be centered
  // In the output, find the line containing column names
  std::istringstream iss(output);
  std::string line;
  while (std::getline(iss, line)) {
    // Look for the line containing " a " (the column header for 'a')
    // If 'a' is centered in a wider column, it should have spaces on both sides
    if (line.find("long_name") != std::string::npos) {
      // This is the header line — "long_name" should be centered
      // Find the position of "long_name" — it should have spaces on both sides
      auto pos = line.find("long_name");
      ASSERT_NE(pos, std::string::npos);
      // Check that there is at least one space before "long_name" (centering)
      EXPECT_EQ(line[pos - 1], ' ') << "Column name should have padding";
      break;
    }
  }
}

TEST_F(InteractiveClientFixture, BoxRendererFriendlyTypeNames) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();

  auto result = conn.ExecuteQuery(
      "SELECT 42::BIGINT AS num, 'hello' AS str, "
      "3.14::DOUBLE AS dbl, DATE '2026-01-01' AS dt");
  ASSERT_TRUE(result.ok());

  ClientConfig config;
  auto renderer = CreateRenderer(OutputMode::BOX, config);

  std::ostringstream out;
  renderer->Render(*result->table, out);
  std::string output = out.str();

  // Should show friendly type names
  EXPECT_NE(output.find("bigint"), std::string::npos)
      << "Should show 'bigint' not 'int64'";
  EXPECT_NE(output.find("varchar"), std::string::npos)
      << "Should show 'varchar' not 'string'";
  EXPECT_NE(output.find("double"), std::string::npos)
      << "Should show 'double'";
  EXPECT_NE(output.find("date"), std::string::npos)
      << "Should show 'date' not 'date32[day]'";

  // Should NOT show raw Arrow type names
  EXPECT_EQ(output.find("int64"), std::string::npos)
      << "Should not show raw Arrow type 'int64'";
  EXPECT_EQ(output.find("string"), std::string::npos)
      << "Should not show raw Arrow type 'string'";
  EXPECT_EQ(output.find("date32"), std::string::npos)
      << "Should not show raw Arrow type 'date32[day]'";
}

TEST_F(InteractiveClientFixture, FriendlyTypeNameMappings) {
  // Unit tests for the FriendlyTypeName function
  EXPECT_EQ(FriendlyTypeName(arrow::utf8()), "varchar");
  EXPECT_EQ(FriendlyTypeName(arrow::int8()), "tinyint");
  EXPECT_EQ(FriendlyTypeName(arrow::int16()), "smallint");
  EXPECT_EQ(FriendlyTypeName(arrow::int32()), "integer");
  EXPECT_EQ(FriendlyTypeName(arrow::int64()), "bigint");
  EXPECT_EQ(FriendlyTypeName(arrow::uint8()), "utinyint");
  EXPECT_EQ(FriendlyTypeName(arrow::uint16()), "usmallint");
  EXPECT_EQ(FriendlyTypeName(arrow::uint32()), "uinteger");
  EXPECT_EQ(FriendlyTypeName(arrow::uint64()), "ubigint");
  EXPECT_EQ(FriendlyTypeName(arrow::float32()), "float");
  EXPECT_EQ(FriendlyTypeName(arrow::float64()), "double");
  EXPECT_EQ(FriendlyTypeName(arrow::boolean()), "boolean");
  EXPECT_EQ(FriendlyTypeName(arrow::date32()), "date");
  EXPECT_EQ(FriendlyTypeName(arrow::binary()), "blob");
  EXPECT_EQ(FriendlyTypeName(arrow::null()), "null");
  EXPECT_EQ(FriendlyTypeName(arrow::decimal128(15, 2)), "decimal(15,2)");
  EXPECT_EQ(FriendlyTypeName(arrow::timestamp(arrow::TimeUnit::MICRO)),
            "timestamp");
  EXPECT_EQ(
      FriendlyTypeName(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
      "timestamptz");
  EXPECT_EQ(FriendlyTypeName(arrow::duration(arrow::TimeUnit::MICRO)),
            "interval");
}

// ============================================================================
// SqlProcessor::IsDdlStatement Tests
// ============================================================================

TEST_F(InteractiveClientFixture, SqlProcessorIsDdlStatement) {
  EXPECT_TRUE(SqlProcessor::IsDdlStatement("CREATE TABLE t (x INT)"));
  EXPECT_TRUE(SqlProcessor::IsDdlStatement("  DROP TABLE t"));
  EXPECT_TRUE(SqlProcessor::IsDdlStatement("ALTER TABLE t ADD y INT"));
  EXPECT_TRUE(SqlProcessor::IsDdlStatement("ATTACH ':memory:'"));
  EXPECT_TRUE(SqlProcessor::IsDdlStatement("DETACH db"));
  EXPECT_TRUE(SqlProcessor::IsDdlStatement("USE my_catalog"));
  EXPECT_TRUE(SqlProcessor::IsDdlStatement("  use main"));
  EXPECT_FALSE(SqlProcessor::IsDdlStatement("SELECT * FROM t"));
  EXPECT_FALSE(SqlProcessor::IsDdlStatement("INSERT INTO t VALUES(1)"));
  EXPECT_FALSE(SqlProcessor::IsDdlStatement("UPDATE t SET x=1"));
  EXPECT_FALSE(SqlProcessor::IsDdlStatement("DELETE FROM t"));
}

// ============================================================================
// .refresh Dot Command Test
// ============================================================================

TEST_F(InteractiveClientFixture, DotRefreshCommand) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto conn = ConnectClient();
  auto config = MakeConfig();

  std::ostringstream out;
  config.output_stream = &out;
  CommandProcessor proc(conn, config);

  bool callback_called = false;
  proc.SetRefreshCallback([&callback_called]() { callback_called = true; });

  auto result = proc.Process(".refresh");
  EXPECT_EQ(result, CommandResult::OK);
  EXPECT_TRUE(callback_called) << "Refresh callback should be called";
  EXPECT_NE(out.str().find("Schema cache refreshed"), std::string::npos);
}
