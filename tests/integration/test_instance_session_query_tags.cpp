// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <array>
#include <cstdio>
#include <fstream>
#include <filesystem>
#include <sstream>

#ifdef _WIN32
#define popen _popen
#define pclose _pclose
#endif

#include <duckdb.hpp>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// Test fixture with instance_tag set
// ============================================================================

class TagServerFixture
    : public gizmosql::testing::ServerTestFixture<TagServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "tag_test.db",
        .port = 31440,
        .health_port = 31441,
        .username = "tester",
        .password = "tester",
        .instance_tag = R"({"env":"test","region":"us-east-1"})",
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<TagServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<TagServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<TagServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<TagServerFixture>::config_{};

// Helper to execute a query and return results as a table
static arrow::Result<std::shared_ptr<arrow::Table>> ExecuteAndFetch(
    FlightSqlClient& sql_client, arrow::flight::FlightCallOptions& call_options,
    const std::string& query) {
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(call_options, query));
  if (info->endpoints().empty()) {
    return arrow::Status::Invalid("No endpoints returned");
  }
  ARROW_ASSIGN_OR_RAISE(auto reader,
                         sql_client.DoGet(call_options, info->endpoints()[0].ticket));
  return reader->ToTable();
}

// ============================================================================
// Instance Tag Tests
// ============================================================================

TEST_F(TagServerFixture, InstanceTagRecordedInInstrumentationTable) {
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

  // Allow async write queue to flush
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Query the instances table for the instance_tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT instance_tag FROM _gizmosql_instr.instances "
                      "WHERE status = 'running' LIMIT 1"));
  ASSERT_GT(table->num_rows(), 0) << "No running instance found";

  auto tag_col = table->column(0);
  ASSERT_EQ(tag_col->type()->id(), arrow::Type::STRING);
  auto tag_val = std::static_pointer_cast<arrow::StringArray>(tag_col->chunk(0))->GetString(0);
  ASSERT_FALSE(tag_val.empty()) << "instance_tag should not be empty";
  // Verify it contains expected JSON content
  ASSERT_NE(tag_val.find("us-east-1"), std::string::npos)
      << "instance_tag should contain 'us-east-1', got: " << tag_val;
}

// ============================================================================
// Session Tag Tests
// ============================================================================

TEST_F(TagServerFixture, SetSessionTagUpdatesSessionsTable) {
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

  // Set session tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto set_info,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.session_tag = '{"team":"data-eng","cost_center":"CC-123"}')"));
  // Drain the result
  for (const auto& endpoint : set_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result_table, reader->ToTable());
  }

  // Allow async write queue to flush
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Query the sessions table for the session_tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT session_tag FROM _gizmosql_instr.sessions "
                      "WHERE status = 'active' ORDER BY start_time DESC LIMIT 1"));
  ASSERT_GT(table->num_rows(), 0) << "No active session found";

  auto tag_col = table->column(0);
  auto tag_val = std::static_pointer_cast<arrow::StringArray>(tag_col->chunk(0))->GetString(0);
  ASSERT_NE(tag_val.find("data-eng"), std::string::npos)
      << "session_tag should contain 'data-eng', got: " << tag_val;
}

TEST_F(TagServerFixture, SetSessionTagRejectsInvalidJSON) {
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

  // Attempt to set invalid JSON as session_tag
  // Execute returns FlightInfo (always OK for admin commands),
  // the error surfaces during DoGet when the SET is actually processed
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client.Execute(call_options,
                                    "SET gizmosql.session_tag = 'not-valid-json'"));
  bool got_error = false;
  std::string error_msg;
  for (const auto& endpoint : info->endpoints()) {
    auto reader_result = sql_client.DoGet(call_options, endpoint.ticket);
    if (!reader_result.ok()) {
      got_error = true;
      error_msg = reader_result.status().message();
      break;
    }
    auto table_result = reader_result.ValueOrDie()->ToTable();
    if (!table_result.ok()) {
      got_error = true;
      error_msg = table_result.status().message();
      break;
    }
  }
  ASSERT_TRUE(got_error) << "Expected error for invalid JSON session_tag";
  ASSERT_NE(error_msg.find("Invalid JSON"), std::string::npos)
      << "Error should mention 'Invalid JSON', got: " << error_msg;
}

// ============================================================================
// Query Tag Tests
// ============================================================================

TEST_F(TagServerFixture, SetQueryTagRecordedInStatementsTable) {
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

  // Set query tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto set_info,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.query_tag = '{"request_id":"abc-123","source":"etl"}')"));
  for (const auto& endpoint : set_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result_table, reader->ToTable());
  }

  // Execute a query that should be tagged
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client.Execute(call_options, "SELECT 42 AS tagged_query"));
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result_table, reader->ToTable());
    ASSERT_EQ(result_table->num_rows(), 1);
  }

  // Allow async write queue to flush
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Query the sql_statements table for the query_tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT query_tag FROM _gizmosql_instr.sql_statements "
                      "WHERE sql_text LIKE '%tagged_query%' AND sql_text NOT LIKE '%sql_statements%' "
                      "ORDER BY created_time DESC LIMIT 1"));
  ASSERT_GT(table->num_rows(), 0) << "No tagged statement found";

  auto tag_col = table->column(0);
  auto tag_val = std::static_pointer_cast<arrow::StringArray>(tag_col->chunk(0))->GetString(0);
  ASSERT_NE(tag_val.find("abc-123"), std::string::npos)
      << "query_tag should contain 'abc-123', got: " << tag_val;
}

TEST_F(TagServerFixture, QueryTagPersistsAcrossQueries) {
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

  // Set query tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto set_info,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.query_tag = '{"batch":"batch-42"}')"));
  for (const auto& endpoint : set_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result_table, reader->ToTable());
  }

  // Execute two queries — both should inherit the tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info1, sql_client.Execute(call_options, "SELECT 1 AS persist_query_one"));
  for (const auto& endpoint : info1->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result_table, reader->ToTable());
  }

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info2, sql_client.Execute(call_options, "SELECT 2 AS persist_query_two"));
  for (const auto& endpoint : info2->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result_table, reader->ToTable());
  }

  // Allow async write queue to flush
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Both queries should have the same query_tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT query_tag FROM _gizmosql_instr.sql_statements "
                      "WHERE sql_text LIKE '%persist_query_%' AND sql_text NOT LIKE '%sql_statements%' "
                      "ORDER BY created_time DESC LIMIT 2"));
  ASSERT_EQ(table->num_rows(), 2) << "Expected 2 tagged statements";

  auto tag_col = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
  for (int i = 0; i < 2; ++i) {
    ASSERT_NE(tag_col->GetString(i).find("batch-42"), std::string::npos)
        << "Statement " << i << " should have query_tag containing 'batch-42'";
  }
}

TEST_F(TagServerFixture, ClearQueryTagWithEmptyString) {
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

  // Set a query tag, then clear it
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto set_info,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.query_tag = '{"temp":"tag"}')"));
  for (const auto& endpoint : set_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result_table, reader->ToTable());
  }

  // Clear the tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto clear_info,
      sql_client.Execute(call_options, "SET gizmosql.query_tag = ''"));
  for (const auto& endpoint : clear_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result_table, reader->ToTable());
  }

  // Execute a query after clearing — should have NULL query_tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client.Execute(call_options, "SELECT 1 AS cleared_tag_query"));
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result_table, reader->ToTable());
  }

  // Allow async write queue to flush
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // The query after clearing should have NULL query_tag
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT query_tag FROM _gizmosql_instr.sql_statements "
                      "WHERE sql_text LIKE '%cleared_tag_query%' AND sql_text NOT LIKE '%sql_statements%' "
                      "ORDER BY created_time DESC LIMIT 1"));
  ASSERT_GT(table->num_rows(), 0) << "No statement found for cleared_tag_query";

  auto tag_col = table->column(0);
  auto tag_array = std::static_pointer_cast<arrow::StringArray>(tag_col->chunk(0));
  ASSERT_TRUE(tag_array->IsNull(0)) << "query_tag should be NULL after clearing";
}

TEST_F(TagServerFixture, SetQueryTagRejectsInvalidJSON) {
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

  // Attempt to set invalid JSON as query_tag
  // Execute returns FlightInfo (always OK for admin commands),
  // the error surfaces during DoGet when the SET is actually processed
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client.Execute(call_options,
                                    "SET gizmosql.query_tag = 'not-valid-json'"));
  bool got_error = false;
  std::string error_msg;
  for (const auto& endpoint : info->endpoints()) {
    auto reader_result = sql_client.DoGet(call_options, endpoint.ticket);
    if (!reader_result.ok()) {
      got_error = true;
      error_msg = reader_result.status().message();
      break;
    }
    auto table_result = reader_result.ValueOrDie()->ToTable();
    if (!table_result.ok()) {
      got_error = true;
      error_msg = table_result.status().message();
      break;
    }
  }
  ASSERT_TRUE(got_error) << "Expected error for invalid JSON query_tag";
  ASSERT_NE(error_msg.find("Invalid JSON"), std::string::npos)
      << "Error should mention 'Invalid JSON', got: " << error_msg;
}

// ============================================================================
// Doc Example Tests — verify JSON query patterns from documentation work
// ============================================================================

TEST_F(TagServerFixture, DocExampleFilterSessionsByTag) {
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

  // Set session tag (doc example)
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto set_info,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.session_tag = '{"team":"data-eng","cost_center":"CC-456"}')"));
  for (const auto& endpoint : set_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto t, reader->ToTable());
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Doc example: "Find all sessions for a specific team"
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT session_id, session_tag->>'team' AS team "
                      "FROM _gizmosql_instr.sessions "
                      "WHERE session_tag->>'team' = 'data-eng' LIMIT 5"));
  ASSERT_GT(table->num_rows(), 0) << "Should find sessions with team='data-eng'";

  // Verify the team column value
  auto team_col = std::static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));
  ASSERT_EQ(team_col->GetString(0), "data-eng");
}

TEST_F(TagServerFixture, DocExampleFilterQueriesByRequestId) {
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

  // Set query tag (doc example)
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto set_info,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.query_tag = '{"request_id":"req-doc-test","pipeline":"daily-etl"}')"));
  for (const auto& endpoint : set_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto t, reader->ToTable());
  }

  // Execute a tagged query
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client.Execute(call_options, "SELECT 99 AS doc_example_query"));
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto t, reader->ToTable());
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Doc example: "Find queries by request ID"
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT sql_text, query_tag->>'request_id' AS request_id, created_time "
                      "FROM _gizmosql_instr.sql_statements "
                      "WHERE query_tag->>'request_id' = 'req-doc-test' "
                      "AND sql_text NOT LIKE '%sql_statements%' LIMIT 5"));
  ASSERT_GT(table->num_rows(), 0) << "Should find queries with request_id='req-doc-test'";

  auto request_id_col = std::static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));
  ASSERT_EQ(request_id_col->GetString(0), "req-doc-test");
}

TEST_F(TagServerFixture, DocExampleAggregateByInstanceTag) {
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

  // Execute a query to generate some execution data
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client.Execute(call_options, "SELECT 1 AS agg_test_query"));
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto t, reader->ToTable());
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Doc example: "Aggregate execution stats by instance environment"
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT i.instance_tag->>'env' AS environment, "
                      "       COUNT(*) AS total_executions "
                      "FROM _gizmosql_instr.execution_details e "
                      "JOIN _gizmosql_instr.instances i ON e.instance_id = i.instance_id "
                      "GROUP BY 1"));
  ASSERT_GT(table->num_rows(), 0) << "Should have execution stats grouped by env";

  // The instance_tag has env="test" so we should find that
  auto env_col = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
  ASSERT_EQ(env_col->GetString(0), "test");
}

TEST_F(TagServerFixture, TagsVisibleInInstrumentationViews) {
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

  // Set both session and query tags
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto s1,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.session_tag = '{"view_test":"session"}')"));
  for (const auto& ep : s1->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto r, sql_client.DoGet(call_options, ep.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto t, r->ToTable());
  }

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto s2,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.query_tag = '{"view_test":"query"}')"));
  for (const auto& ep : s2->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto r, sql_client.DoGet(call_options, ep.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto t, r->ToTable());
  }

  // Execute a query
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client.Execute(call_options, "SELECT 1 AS view_tag_test"));
  for (const auto& ep : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto r, sql_client.DoGet(call_options, ep.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto t, r->ToTable());
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Verify instance_tag in active_sessions view
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto active_table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT instance_tag, session_tag FROM _gizmosql_instr.active_sessions LIMIT 1"));
  ASSERT_GT(active_table->num_rows(), 0);

  // Verify tags in session_activity view
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto activity_table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT instance_tag, session_tag, query_tag "
                      "FROM _gizmosql_instr.session_activity "
                      "WHERE query_tag IS NOT NULL LIMIT 1"));
  ASSERT_GT(activity_table->num_rows(), 0);

  // Verify tags in execution_details view
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto exec_table,
      ExecuteAndFetch(sql_client, call_options,
                      "SELECT query_tag, session_tag "
                      "FROM _gizmosql_instr.execution_details "
                      "WHERE query_tag IS NOT NULL LIMIT 1"));
  ASSERT_GT(exec_table->num_rows(), 0);
}

// ============================================================================
// Client CLI Tag Tests — verify --session-tag, --query-tag, and env vars
// ============================================================================

namespace {

struct CliResult {
  std::string stdout_output;
  std::string stderr_output;
  int exit_code;
};

std::string FindClientBinary() {
#ifdef GIZMOSQL_CLIENT_BINARY
  return GIZMOSQL_CLIENT_BINARY;
#else
  const char* candidates[] = {"./gizmosql_client", "../gizmosql_client", "gizmosql_client"};
  for (const auto* path : candidates) {
    if (std::ifstream(path).good()) return path;
  }
  return "gizmosql_client";
#endif
}

CliResult RunClientCmd(const std::string& args, const std::string& env_prefix = "") {
  std::string client = FindClientBinary();
  std::string stderr_file = "/tmp/gizmosql_tag_test_stderr_" +
                             std::to_string(::getpid()) + ".txt";
  std::string cmd = env_prefix + client + " " + args + " 2>" + stderr_file;

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

  std::ifstream stderr_stream(stderr_file);
  if (stderr_stream.is_open()) {
    std::ostringstream ss;
    ss << stderr_stream.rdbuf();
    result.stderr_output = ss.str();
  }
  std::remove(stderr_file.c_str());
  return result;
}

}  // namespace

TEST_F(TagServerFixture, ClientSessionTagFlag) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string conn_args = "--host localhost --port " + std::to_string(GetPort()) +
                          " --username " + GetUsername();
  std::string env = "GIZMOSQL_PASSWORD=" + GetPassword() + " ";

  // Use --session-tag flag, then query to verify the tag was applied
  auto result = RunClientCmd(
      conn_args +
      R"( --session-tag '{"client_flag":"test"}' )"
      R"( -c "SELECT session_tag FROM _gizmosql_instr.sessions WHERE status = 'active' AND session_tag IS NOT NULL ORDER BY start_time DESC LIMIT 1")",
      env);

  ASSERT_EQ(result.exit_code, 0)
      << "Client exited with error. stderr: " << result.stderr_output;
  ASSERT_NE(result.stdout_output.find("client_flag"), std::string::npos)
      << "Output should contain session_tag with 'client_flag', got: " << result.stdout_output;
}

TEST_F(TagServerFixture, ClientQueryTagFlag) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string conn_args = "--host localhost --port " + std::to_string(GetPort()) +
                          " --username " + GetUsername();
  std::string env = "GIZMOSQL_PASSWORD=" + GetPassword() + " ";

  // Use --query-tag flag, run a query, then check sql_statements for the tag.
  // The -c flag runs the command and exits, so we need to run two queries:
  // first a tagged query, then a lookup. We can use a semicolon-separated batch.
  // However, -c only runs a single statement. So we'll run two separate invocations.

  // First: run a tagged query
  auto run1 = RunClientCmd(
      conn_args +
      R"( --query-tag '{"pipeline":"client-test"}' )"
      R"( -c "SELECT 1 AS client_tag_probe")",
      env);
  ASSERT_EQ(run1.exit_code, 0)
      << "First client run failed. stderr: " << run1.stderr_output;

  // Allow async instrumentation queue to flush
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Second: query the instrumentation table (no tag needed for the lookup)
  auto run2 = RunClientCmd(
      conn_args +
      R"( -c "SELECT query_tag FROM _gizmosql_instr.sql_statements WHERE sql_text LIKE '%client_tag_probe%' AND sql_text NOT LIKE '%sql_statements%' ORDER BY created_time DESC LIMIT 1")",
      env);
  ASSERT_EQ(run2.exit_code, 0)
      << "Second client run failed. stderr: " << run2.stderr_output;
  ASSERT_NE(run2.stdout_output.find("client-test"), std::string::npos)
      << "query_tag should contain 'client-test', got: " << run2.stdout_output;
}

TEST_F(TagServerFixture, ClientSessionTagEnvVar) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string conn_args = "--host localhost --port " + std::to_string(GetPort()) +
                          " --username " + GetUsername();
  std::string env = "GIZMOSQL_PASSWORD=" + GetPassword() +
                    R"( GIZMOSQL_SESSION_TAG='{"env_var":"session_test"}' )";

  auto result = RunClientCmd(
      conn_args +
      R"( -c "SELECT session_tag FROM _gizmosql_instr.sessions WHERE status = 'active' AND session_tag IS NOT NULL ORDER BY start_time DESC LIMIT 1")",
      env);

  ASSERT_EQ(result.exit_code, 0)
      << "Client exited with error. stderr: " << result.stderr_output;
  ASSERT_NE(result.stdout_output.find("env_var"), std::string::npos)
      << "Output should contain session_tag from env var, got: " << result.stdout_output;
}

TEST_F(TagServerFixture, ClientQueryTagEnvVar) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string conn_args = "--host localhost --port " + std::to_string(GetPort()) +
                          " --username " + GetUsername();
  std::string env = "GIZMOSQL_PASSWORD=" + GetPassword() +
                    R"( GIZMOSQL_QUERY_TAG='{"env_var":"query_test"}' )";

  // Run a tagged query
  auto run1 = RunClientCmd(
      conn_args + R"( -c "SELECT 1 AS env_query_tag_probe")", env);
  ASSERT_EQ(run1.exit_code, 0)
      << "First client run failed. stderr: " << run1.stderr_output;

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Check the instrumentation table
  std::string lookup_env = "GIZMOSQL_PASSWORD=" + GetPassword() + " ";
  auto run2 = RunClientCmd(
      conn_args +
      R"( -c "SELECT query_tag FROM _gizmosql_instr.sql_statements WHERE sql_text LIKE '%env_query_tag_probe%' AND sql_text NOT LIKE '%sql_statements%' ORDER BY created_time DESC LIMIT 1")",
      lookup_env);
  ASSERT_EQ(run2.exit_code, 0)
      << "Second client run failed. stderr: " << run2.stderr_output;
  ASSERT_NE(run2.stdout_output.find("query_test"), std::string::npos)
      << "query_tag should contain 'query_test', got: " << run2.stdout_output;
}
