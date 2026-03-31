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

// Integration tests that run gizmosql_client as a subprocess to verify
// the CLI behavior matches the documentation examples.

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include "test_server_fixture.h"

// ============================================================================
// Helper: run gizmosql_client as a subprocess and capture stdout + stderr
// ============================================================================

namespace {

struct CliResult {
  std::string stdout_output;
  std::string stderr_output;
  int exit_code;
};

// Find the gizmosql_client binary. The path is injected at compile time
// via GIZMOSQL_CLIENT_BINARY from CMake's $<TARGET_FILE:gizmosql_client>.
std::string FindClientBinary() {
#ifdef GIZMOSQL_CLIENT_BINARY
  return GIZMOSQL_CLIENT_BINARY;
#else
  // Fallback for manual builds
  const char* candidates[] = {
      "./gizmosql_client",
      "../gizmosql_client",
      "gizmosql_client",
  };
  for (const auto* path : candidates) {
    if (std::ifstream(path).good()) {
      return path;
    }
  }
  return "gizmosql_client";
#endif
}

CliResult RunClient(const std::string& args, const std::string& env_prefix = "") {
  std::string client = FindClientBinary();

  // Redirect stderr to a temp file so we can capture both streams
  std::string stderr_file = "/tmp/gizmosql_test_stderr_" +
                             std::to_string(::getpid()) + ".txt";

  std::string cmd = env_prefix + client + " " + args +
                    " 2>" + stderr_file;

  CliResult result;
  FILE* pipe = popen(cmd.c_str(), "r");
  if (!pipe) {
    result.exit_code = -1;
    result.stderr_output = "popen() failed";
    return result;
  }

  std::array<char, 4096> buf;
  while (fgets(buf.data(), buf.size(), pipe) != nullptr) {
    result.stdout_output += buf.data();
  }

  result.exit_code = pclose(pipe);
#ifndef _WIN32
  result.exit_code = WEXITSTATUS(result.exit_code);
#endif

  // Read stderr
  std::ifstream stderr_stream(stderr_file);
  if (stderr_stream.is_open()) {
    std::ostringstream ss;
    ss << stderr_stream.rdbuf();
    result.stderr_output = ss.str();
  }
  std::remove(stderr_file.c_str());

  return result;
}

// Strip ANSI escape codes from a string for assertion matching
std::string StripAnsi(const std::string& s) {
  std::string result;
  bool in_escape = false;
  for (char c : s) {
    if (c == '\033') {
      in_escape = true;
      continue;
    }
    if (in_escape) {
      if (c == 'm') {
        in_escape = false;
      }
      continue;
    }
    result += c;
  }
  return result;
}

}  // namespace

// ============================================================================
// Test Fixture
// ============================================================================

class ClientCliFixture
    : public gizmosql::testing::ServerTestFixture<ClientCliFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "client_cli_test.db",
        .port = 31430,
        .health_port = 31431,
        .username = "testuser",
        .password = "testpass",
        .backend = BackendType::duckdb,
        .enable_instrumentation = false,
        .init_sql_commands =
            "CREATE TABLE employees (id INTEGER, name VARCHAR, dept VARCHAR, "
            "salary DOUBLE);"
            "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 120000);"
            "INSERT INTO employees VALUES (2, 'Bob', 'Marketing', 85000);"
            "INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 110000);"
            "INSERT INTO employees VALUES (4, 'Dave', 'Sales', 95000);"
            "INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 130000);"
            "CREATE TABLE region (r_regionkey INTEGER, r_name VARCHAR, "
            "r_comment VARCHAR);"
            "INSERT INTO region VALUES (0, 'AFRICA', 'comment0');"
            "INSERT INTO region VALUES (1, 'AMERICA', 'comment1');"
            "INSERT INTO region VALUES (2, 'ASIA', 'comment2');"
            "INSERT INTO region VALUES (3, 'EUROPE', 'comment3');"
            "INSERT INTO region VALUES (4, 'MIDDLE EAST', 'comment4');",
    };
  }

 protected:
  // Build common connection args for the CLI
  std::string ConnArgs() const {
    return "--host localhost --port " + std::to_string(GetPort()) +
           " --username " + GetUsername();
  }

  // Build connection args with env var for password
  std::string EnvPrefix() const {
    return "GIZMOSQL_PASSWORD=" + GetPassword() + " ";
  }

  // Run a client command and return the result
  CliResult Run(const std::string& extra_args) {
    return RunClient(ConnArgs() + " " + extra_args, EnvPrefix());
  }
};

// Static member definitions
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<ClientCliFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<ClientCliFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<ClientCliFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<ClientCliFixture>::config_{};

// ============================================================================
// Doc Example: Quick Start — run a single query
// ============================================================================

TEST_F(ClientCliFixture, DocQuickStartSingleQuery) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Matches doc: gizmosql_client --host localhost --username admin
  //              --command "SELECT * FROM employees"
  auto result = Run("--command \"SELECT * FROM employees ORDER BY id\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  auto output = StripAnsi(result.stdout_output);
  EXPECT_NE(output.find("Alice"), std::string::npos) << "Should contain Alice";
  EXPECT_NE(output.find("Bob"), std::string::npos) << "Should contain Bob";
  EXPECT_NE(output.find("5 rows"), std::string::npos)
      << "Should show 5 rows. Output:\n" << output;
}

// ============================================================================
// Doc Example: Output Modes
// ============================================================================

TEST_F(ClientCliFixture, DocOutputModeCsv) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Matches doc: --csv --command "SELECT ..."
  auto result = Run("--csv --command \"SELECT id, name FROM employees ORDER BY id LIMIT 2\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  // CSV should have header + data rows
  EXPECT_NE(result.stdout_output.find("id,name"), std::string::npos)
      << "CSV should have header row. Output:\n" << result.stdout_output;
  EXPECT_NE(result.stdout_output.find("1,Alice"), std::string::npos)
      << "CSV should have data. Output:\n" << result.stdout_output;
}

TEST_F(ClientCliFixture, DocOutputModeCsvNoHeader) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Matches doc: --csv --no-header --command "SELECT name FROM employees"
  auto result = Run("--csv --no-header --command \"SELECT name FROM employees ORDER BY id LIMIT 1\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  EXPECT_EQ(result.stdout_output.find("name"), std::string::npos)
      << "No-header CSV should not have header. Output:\n" << result.stdout_output;
  EXPECT_NE(result.stdout_output.find("Alice"), std::string::npos);
}

TEST_F(ClientCliFixture, DocOutputModeJson) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Matches doc: --json --command "SELECT name, salary FROM employees"
  auto result = Run("--json --command \"SELECT name, salary FROM employees ORDER BY id LIMIT 2\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  EXPECT_NE(result.stdout_output.find("\"name\""), std::string::npos)
      << "JSON should have name field. Output:\n" << result.stdout_output;
  EXPECT_NE(result.stdout_output.find("Alice"), std::string::npos);
  // Should be valid JSON array
  EXPECT_NE(result.stdout_output.find("["), std::string::npos);
  EXPECT_NE(result.stdout_output.find("]"), std::string::npos);
}

TEST_F(ClientCliFixture, DocOutputModeMarkdown) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Matches doc: --markdown --command "SELECT name, dept FROM employees LIMIT 3"
  auto result = Run("--markdown --command \"SELECT name, dept FROM employees ORDER BY id LIMIT 3\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  // Markdown tables have | separators and --- dividers
  EXPECT_NE(result.stdout_output.find("|"), std::string::npos);
  EXPECT_NE(result.stdout_output.find("---"), std::string::npos);
  EXPECT_NE(result.stdout_output.find("Alice"), std::string::npos);
  EXPECT_NE(result.stdout_output.find("Engineering"), std::string::npos);
}

TEST_F(ClientCliFixture, DocOutputModeBox) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Default box mode
  auto result = Run("--command \"SELECT 42 AS answer\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  auto output = StripAnsi(result.stdout_output);
  // Box mode uses Unicode box-drawing characters
  EXPECT_NE(output.find("\u250C"), std::string::npos)
      << "Box mode should have Unicode borders. Output:\n" << output;
  EXPECT_NE(output.find("42"), std::string::npos);
  EXPECT_NE(output.find("answer"), std::string::npos);
}

// ============================================================================
// Doc Example: Dot commands via --command (semicolons separate)
// ============================================================================

TEST_F(ClientCliFixture, DocDotCommandTables) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // .tables should list our test tables
  auto result = Run("--command \".tables --flat\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  auto output = StripAnsi(result.stdout_output);
  EXPECT_NE(output.find("employees"), std::string::npos)
      << "Should list employees table. Output:\n" << output;
  EXPECT_NE(output.find("region"), std::string::npos)
      << "Should list region table. Output:\n" << output;
}

TEST_F(ClientCliFixture, DocDotCommandDescribe) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  auto result = Run("--command \".describe employees\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  auto output = StripAnsi(result.stdout_output);
  EXPECT_NE(output.find("id"), std::string::npos)
      << "Should show column 'id'. Output:\n" << output;
  EXPECT_NE(output.find("name"), std::string::npos)
      << "Should show column 'name'. Output:\n" << output;
  EXPECT_NE(output.find("salary"), std::string::npos)
      << "Should show column 'salary'. Output:\n" << output;
}

// ============================================================================
// Doc Example: Pipe SQL from stdin
// ============================================================================

TEST_F(ClientCliFixture, DocPipeFromStdin) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Matches doc: echo "SELECT 42 AS answer;" | gizmosql_client ... --quiet
  std::string client = FindClientBinary();
  std::string cmd = "echo 'SELECT 42 AS answer;' | " +
                    EnvPrefix() + client + " " + ConnArgs() + " --quiet";
  auto result = RunClient("", "");
  // Use the full command directly
  FILE* pipe = popen(cmd.c_str(), "r");
  ASSERT_NE(pipe, nullptr);
  std::string output;
  std::array<char, 4096> buf;
  while (fgets(buf.data(), buf.size(), pipe) != nullptr) {
    output += buf.data();
  }
  int exit_code = pclose(pipe);
#ifndef _WIN32
  exit_code = WEXITSTATUS(exit_code);
#endif

  ASSERT_EQ(exit_code, 0) << "Pipe mode failed. Output:\n" << output;
  output = StripAnsi(output);
  EXPECT_NE(output.find("42"), std::string::npos)
      << "Should contain answer 42. Output:\n" << output;
}

// ============================================================================
// Doc Example: File mode (--file)
// ============================================================================

TEST_F(ClientCliFixture, DocFileMode) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a temp SQL file
  std::string sql_file = "/tmp/gizmosql_test_" +
                          std::to_string(::getpid()) + ".sql";
  {
    std::ofstream f(sql_file);
    f << "SELECT COUNT(*) AS total FROM employees;\n";
  }

  auto result = Run("--quiet --file " + sql_file);

  std::remove(sql_file.c_str());

  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;
  auto output = StripAnsi(result.stdout_output);
  EXPECT_NE(output.find("5"), std::string::npos)
      << "Should show count of 5. Output:\n" << output;
}

// ============================================================================
// Doc Example: Output to file (--output)
// ============================================================================

TEST_F(ClientCliFixture, DocOutputToFile) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string out_file = "/tmp/gizmosql_test_output_" +
                          std::to_string(::getpid()) + ".csv";

  auto result = Run("--csv --no-header --output " + out_file +
                    " --command \"SELECT id, name FROM employees ORDER BY id\"");

  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  // Read the output file
  std::ifstream f(out_file);
  ASSERT_TRUE(f.is_open()) << "Output file should exist";
  std::ostringstream ss;
  ss << f.rdbuf();
  std::string file_content = ss.str();

  std::remove(out_file.c_str());

  EXPECT_NE(file_content.find("1,Alice"), std::string::npos)
      << "File should contain CSV data. Content:\n" << file_content;
}

// ============================================================================
// Doc Example: Multiple statements in --command
// ============================================================================

TEST_F(ClientCliFixture, DocMultipleStatements) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Matches doc: "CREATE TABLE t (x INT); INSERT INTO t VALUES (1); SELECT * FROM t;"
  auto result = Run("--command \"CREATE TABLE doc_test (x INTEGER); "
                    "INSERT INTO doc_test VALUES (42); "
                    "SELECT * FROM doc_test;\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  auto output = StripAnsi(result.stdout_output);
  EXPECT_NE(output.find("42"), std::string::npos)
      << "Should contain 42. Output:\n" << output;

  // Clean up
  Run("--command \"DROP TABLE IF EXISTS doc_test\"");
}

// ============================================================================
// Doc Example: Quiet mode (--quiet) suppresses banner
// ============================================================================

TEST_F(ClientCliFixture, DocQuietMode) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  auto result = Run("--quiet --command \"SELECT 1 AS x\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  auto output = StripAnsi(result.stdout_output);
  // Quiet mode should NOT show the welcome banner
  EXPECT_EQ(output.find("GizmoSQL Client"), std::string::npos)
      << "Quiet mode should suppress banner. Output:\n" << output;
}

// ============================================================================
// Doc Example: Error handling (invalid SQL returns non-zero exit)
// ============================================================================

TEST_F(ClientCliFixture, DocErrorHandling) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  auto result = Run("--bail --command \"SELECT * FROM nonexistent_table_xyz\"");
  EXPECT_NE(result.exit_code, 0) << "Invalid SQL should return non-zero exit code";

  auto stderr_clean = StripAnsi(result.stderr_output);
  EXPECT_NE(stderr_clean.find("Error"), std::string::npos)
      << "Should contain error message. stderr:\n" << stderr_clean;
}

// ============================================================================
// Feature: Last result reference (_)
// ============================================================================

TEST_F(ClientCliFixture, FeatureLastResultUnderscore) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Run two statements: first caches result, second references it via _
  auto result = Run("--csv --no-header --command \""
                    "SELECT * FROM region WHERE r_regionkey < 2; "
                    "SELECT count(*) FROM _\"");
  ASSERT_EQ(result.exit_code, 0) << "stderr: " << result.stderr_output;

  auto output = StripAnsi(result.stdout_output);
  // The count of the cached result (2 rows where r_regionkey < 2) should be 2
  EXPECT_NE(output.find("2"), std::string::npos)
      << "Last result count should be 2. Output:\n" << output;
}

// ============================================================================
// Feature: Heredoc-style multi-statement input
// ============================================================================

TEST_F(ClientCliFixture, DocHeredocStyle) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string client = FindClientBinary();
  std::string cmd = EnvPrefix() + client + " " + ConnArgs() +
                    " --quiet --csv --no-header <<'SQLEOF'\n"
                    "SELECT r_name FROM region WHERE r_regionkey = 2;\n"
                    "SQLEOF";

  FILE* pipe = popen(cmd.c_str(), "r");
  ASSERT_NE(pipe, nullptr);
  std::string output;
  std::array<char, 4096> buf;
  while (fgets(buf.data(), buf.size(), pipe) != nullptr) {
    output += buf.data();
  }
  int exit_code = pclose(pipe);
#ifndef _WIN32
  exit_code = WEXITSTATUS(exit_code);
#endif

  ASSERT_EQ(exit_code, 0) << "Heredoc mode failed";
  EXPECT_NE(StripAnsi(output).find("ASIA"), std::string::npos)
      << "Should find ASIA. Output:\n" << output;
}
