// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Access control must not cost concurrency. The same TPC-H workload runs
// against two servers that differ only in access control: one without it, one
// where every client is a store user whose statements are checked and rewritten
// for a row policy and a mask. For each,
// one client and then many clients run every TPC-H query; the test fails if
// access control makes the concurrent run much slower, or scales worse with
// more clients, than the baseline.
//
// GIZMOSQL_TEST_CONCURRENCY_SF sets the scale factor (default 0.1; small
// queries make any per-statement or shared-lock cost easier to see).
// Needs a license that includes RBAC.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "enterprise/enterprise_features.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr int kClients = 10;
constexpr int kRounds = 3;     // passes over the 22 queries per client
constexpr int kTrials = 3;     // medians over this many measurements per mode

std::string ScaleFactor() {
  const char* sf = std::getenv("GIZMOSQL_TEST_CONCURRENCY_SF");
  return sf && *sf ? sf : "0.1";
}

std::string Token(const std::string& user, const std::string& role, const std::string& session,
                  const std::string& instance) {
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id("concurrency-" + session)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(user))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(instance))
      .set_payload_claim("session_id", jwt::claim(session))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

std::string SessionId(int server, int n) {
  char sid[64];
  std::snprintf(sid, sizeof(sid), "00000000-0000-4000-6%03d-%012d", server, n);
  return sid;
}

class Client {
 public:
  Client(int port, const std::string& user, const std::string& role, const std::string& session) {
    header_ = {"authorization", "Bearer " + Token(user, role, session, "concurrency")};
    auto loc = arrow::flight::Location::ForGrpcTcp("127.0.0.1", port).ValueOrDie();
    client_ = std::make_unique<FlightSqlClient>(
        arrow::flight::FlightClient::Connect(loc, {}).ValueOrDie());
  }
  // Rows of the whole result, fetched completely; -1 on error.
  int64_t Rows(const std::string& sql, std::string* error = nullptr) {
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(header_);
    auto info = client_->Execute(co, sql);
    if (!info.ok()) {
      if (error) *error = info.status().ToString();
      return -1;
    }
    int64_t rows = 0;
    for (const auto& ep : (*info)->endpoints()) {
      auto reader = client_->DoGet(co, ep.ticket);
      if (!reader.ok()) {
        if (error) *error = reader.status().ToString();
        return -1;
      }
      auto table = (*reader)->ToTable();
      if (!table.ok()) {
        if (error) *error = table.status().ToString();
        return -1;
      }
      rows += (*table)->num_rows();
    }
    return rows;
  }
  std::vector<std::string> Strings(const std::string& sql) {
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(header_);
    std::vector<std::string> out;
    auto info = client_->Execute(co, sql).ValueOrDie();
    for (const auto& ep : info->endpoints()) {
      auto table = client_->DoGet(co, ep.ticket).ValueOrDie()->ToTable().ValueOrDie();
      for (int64_t r = 0; r < table->num_rows(); ++r) {
        out.push_back(table->column(0)->GetScalar(r).ValueOrDie()->ToString());
      }
    }
    return out;
  }

 private:
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

// Two servers in one test: the fixture's start/stop and state, made reachable.
template <typename Derived>
class BenchServer : public gizmosql::testing::ServerTestFixture<Derived> {
  using Base = gizmosql::testing::ServerTestFixture<Derived>;

 public:
  static void Start() { Base::SetUpTestSuite(); }
  static void Stop() { Base::TearDownTestSuite(); }
  static bool Ready() { return Base::server_ready_; }
  static int Port() { return Base::config_.port; }
};

class BaselineServer : public BenchServer<BaselineServer> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "concurrency_baseline.db",
        .port = 31750,
        .health_port = 31751,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
    };
  }
  void TestBody() override {}
};

class RbacServer : public BenchServer<RbacServer> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "concurrency_rbac.db",
        .port = 31752,
        .health_port = 31753,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
        .rbac_store_uri = "memory",
    };
  }
  void TestBody() override {}
};

struct Measurement {
  double single_s = 0;  // one client
  double multi_s = 0;   // kClients clients at once
  double Scaling() const {
    const double queries = 22.0 * kRounds;
    return (queries * kClients / multi_s) / (queries / single_s);
  }
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<BaselineServer>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<BaselineServer>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<BaselineServer>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig gizmosql::testing::ServerTestFixture<BaselineServer>::config_{};
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacServer>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacServer>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacServer>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig gizmosql::testing::ServerTestFixture<RbacServer>::config_{};

class RbacConcurrency : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    BaselineServer::Start();
    RbacServer::Start();
  }
  static void TearDownTestSuite() {
    RbacServer::Stop();
    BaselineServer::Stop();
  }

  // Generate the data on a server and return the 22 queries.
  static std::vector<std::string> Prepare(int port, bool rbac) {
    Client admin(port, "admin_user", "admin", SessionId(port, 0));
    std::string error;
    EXPECT_GE(admin.Rows("CALL dbgen(sf=" + ScaleFactor() + ")", &error), 0) << error;
    if (rbac) {
      // A row policy on the largest table (it keeps every row, so results
      // match the baseline) and a mask: the policy rewrite runs too.
      for (const char* sql : {"CREATE ROLE bench", "GRANT SELECT ON SCHEMA main TO bench",
                              "CREATE ROW POLICY bench_rows ON lineitem TO bench USING (l_orderkey > 0)",
                              "CREATE MASKING POLICY bench_mask AS (val VARCHAR) RETURNS VARCHAR -> 'x'",
                              "ALTER TABLE nation MODIFY COLUMN n_comment SET MASKING POLICY bench_mask"}) {
        EXPECT_GE(admin.Rows(sql, &error), 0) << sql << ": " << error;
      }
      for (int i = 0; i <= kClients; ++i) {
        const std::string user = "bench" + std::to_string(i);
        EXPECT_GE(admin.Rows("CREATE USER " + user, &error), 0) << error;
        EXPECT_GE(admin.Rows("GRANT bench TO " + user, &error), 0) << error;
      }
    }
    return admin.Strings("SELECT query FROM tpch_queries() ORDER BY query_nr");
  }

  // `clients` clients at once, each with its own session, run every query
  // kRounds times. Returns seconds; checks every result's row count.
  static double Run(int port, bool rbac, int clients, const std::vector<std::string>& queries,
                    const std::vector<int64_t>& expected, int session_base) {
    std::vector<std::unique_ptr<Client>> sessions;
    for (int c = 0; c < clients; ++c) {
      // Baseline clients are token users too (role "user"), so both servers
      // take the same path except for access control itself.
      sessions.push_back(std::make_unique<Client>(port, rbac ? "bench" + std::to_string(c) : "bench",
                                                  "user", SessionId(port, session_base + c)));
      std::string error;
      EXPECT_EQ(sessions.back()->Rows("SELECT 1", &error), 1) << error;  // session up before timing
    }
    std::atomic<int> wrong{0};
    std::mutex log_mutex;
    const auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (int c = 0; c < clients; ++c) {
      threads.emplace_back([&, c] {
        for (int round = 0; round < kRounds; ++round) {
          for (size_t q = 0; q < queries.size(); ++q) {
            std::string error;
            const int64_t rows = sessions[c]->Rows(queries[q], &error);
            if (rows != expected[q]) {
              ++wrong;
              std::lock_guard<std::mutex> lock(log_mutex);
              std::cerr << (rbac ? "rbac" : "baseline") << " client " << c << " q" << q + 1 << ": "
                        << rows << " rows, expected " << expected[q] << " " << error << "\n";
            }
          }
        }
      });
    }
    for (auto& t : threads) t.join();
    const double seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    EXPECT_EQ(wrong.load(), 0) << (rbac ? "access control" : "baseline") << " returned wrong results";
    return seconds;
  }

  static double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
  }
};

TEST_F(RbacConcurrency, AccessControlDoesNotCostConcurrency) {
  if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsRbacAvailable()) {
    GTEST_SKIP() << "license does not include RBAC";
  }
  ASSERT_TRUE(BaselineServer::Ready() && RbacServer::Ready());
  const int base_port = BaselineServer::Port(), rbac_port = RbacServer::Port();
  const auto queries = Prepare(base_port, false);
  ASSERT_EQ(queries.size(), 22u);
  ASSERT_EQ(Prepare(rbac_port, true), queries);

  // Reference row counts, and a warm-up pass on each server.
  std::vector<int64_t> expected;
  {
    Client reference(base_port, "bench", "user", SessionId(base_port, 900));
    for (const auto& q : queries) {
      std::string error;
      expected.push_back(reference.Rows(q, &error));
      ASSERT_GE(expected.back(), 0) << error;
    }
  }
  Run(base_port, false, 1, queries, expected, 800);
  Run(rbac_port, true, 1, queries, expected, 800);

  // Alternate the servers so drift on the machine hits both alike.
  std::vector<double> base_single, base_multi, rbac_single, rbac_multi;
  for (int trial = 0; trial < kTrials; ++trial) {
    const int sessions = 100 + trial * 20;
    base_single.push_back(Run(base_port, false, 1, queries, expected, sessions));
    rbac_single.push_back(Run(rbac_port, true, 1, queries, expected, sessions));
    base_multi.push_back(Run(base_port, false, kClients, queries, expected, sessions + 1));
    rbac_multi.push_back(Run(rbac_port, true, kClients, queries, expected, sessions + 1));
  }
  const Measurement base{Median(base_single), Median(base_multi)};
  const Measurement rbac{Median(rbac_single), Median(rbac_multi)};

  std::cerr << std::fixed << std::setprecision(2) << "\n=== Concurrency, TPC-H SF " << ScaleFactor()
            << ", " << kClients << " clients x " << kRounds << " passes, median of " << kTrials
            << " ===\n"
            << "               1 client (s)   " << kClients << " clients (s)   scaling (x)\n"
            << "baseline       " << std::setw(10) << base.single_s << std::setw(16) << base.multi_s
            << std::setw(14) << base.Scaling() << "\n"
            << "access control " << std::setw(10) << rbac.single_s << std::setw(16) << rbac.multi_s
            << std::setw(14) << rbac.Scaling() << "\n"
            << "overhead       " << std::setw(9) << (rbac.single_s / base.single_s - 1) * 100 << "%"
            << std::setw(15) << (rbac.multi_s / base.multi_s - 1) * 100 << "%\n";

  // A shared lock or anything else that serializes sessions shows up as the
  // concurrent run slowing down far more than the single-client one, and as
  // worse scaling. Allow for timing noise on a busy machine.
  EXPECT_LE(rbac.multi_s, base.multi_s * 1.25 + 0.5)
      << "access control slowed down concurrent clients";
  EXPECT_GE(rbac.Scaling(), base.Scaling() * 0.8)
      << "access control scales worse with concurrent clients than the baseline";
}
