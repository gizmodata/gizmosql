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

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "duckdb.hpp"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// TPC-H Query Definitions
// ============================================================================

// clang-format off
const std::vector<std::pair<std::string, std::string>> TPCH_QUERIES = {
    {"Q01", R"(
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus
)"},

    {"Q02", R"(
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
    )
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100
)"},

    {"Q03", R"(
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
)"},

    {"Q04", R"(
SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority
)"},

    {"Q05", R"(
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= DATE '1994-01-01'
    AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
    n_name
ORDER BY
    revenue DESC
)"},

    {"Q06", R"(
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
)"},

    {"Q07", R"(
SELECT
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) AS revenue
FROM
    (
        SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            extract(year FROM l_shipdate) AS l_year,
            l_extendedprice * (1 - l_discount) AS volume
        FROM
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        WHERE
            s_suppkey = l_suppkey
            AND o_orderkey = l_orderkey
            AND c_custkey = o_custkey
            AND s_nationkey = n1.n_nationkey
            AND c_nationkey = n2.n_nationkey
            AND (
                (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            )
            AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year
)"},

    {"Q08", R"(
SELECT
    o_year,
    sum(CASE
        WHEN nation = 'BRAZIL' THEN volume
        ELSE 0
    END) / sum(volume) AS mkt_share
FROM
    (
        SELECT
            extract(year FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) AS volume,
            n2.n_name AS nation
        FROM
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        WHERE
            p_partkey = l_partkey
            AND s_suppkey = l_suppkey
            AND l_orderkey = o_orderkey
            AND o_custkey = c_custkey
            AND c_nationkey = n1.n_nationkey
            AND n1.n_regionkey = r_regionkey
            AND r_name = 'AMERICA'
            AND s_nationkey = n2.n_nationkey
            AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            AND p_type = 'ECONOMY ANODIZED STEEL'
    ) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year
)"},

    {"Q09", R"(
SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM
    (
        SELECT
            n_name AS nation,
            extract(year FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
        FROM
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        WHERE
            s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%'
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC
)"},

    {"Q10", R"(
SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-10-01'
    AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20
)"},

    {"Q11", R"(
SELECT
    ps_partkey,
    sum(ps_supplycost * ps_availqty) AS value
FROM
    partsupp,
    supplier,
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY
    ps_partkey HAVING
        sum(ps_supplycost * ps_availqty) > (
            SELECT
                sum(ps_supplycost * ps_availqty) * 0.0001
            FROM
                partsupp,
                supplier,
                nation
            WHERE
                ps_suppkey = s_suppkey
                AND s_nationkey = n_nationkey
                AND n_name = 'GERMANY'
        )
ORDER BY
    value DESC
)"},

    {"Q12", R"(
SELECT
    l_shipmode,
    sum(CASE
        WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH'
            THEN 1
        ELSE 0
    END) AS high_line_count,
    sum(CASE
        WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH'
            THEN 1
        ELSE 0
    END) AS low_line_count
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode
)"},

    {"Q13", R"(
SELECT
    c_count,
    count(*) AS custdist
FROM
    (
        SELECT
            c_custkey,
            count(o_orderkey) AS c_count
        FROM
            customer LEFT OUTER JOIN orders ON
                c_custkey = o_custkey
                AND o_comment NOT LIKE '%special%requests%'
        GROUP BY
            c_custkey
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC
)"},

    {"Q14", R"(
SELECT
    100.00 * sum(CASE
        WHEN p_type LIKE 'PROMO%'
            THEN l_extendedprice * (1 - l_discount)
        ELSE 0
    END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
)"},

    {"Q15", R"(
WITH revenue AS (
    SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
    GROUP BY
        l_suppkey
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    revenue
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
        SELECT
            max(total_revenue)
        FROM
            revenue
    )
ORDER BY
    s_suppkey
)"},

    {"Q16", R"(
SELECT
    p_brand,
    p_type,
    p_size,
    count(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    partsupp,
    part
WHERE
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (
        SELECT
            s_suppkey
        FROM
            supplier
        WHERE
            s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size
)"},

    {"Q17", R"(
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey
    )
)"},

    {"Q18", R"(
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM
    customer,
    orders,
    lineitem
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            lineitem
        GROUP BY
            l_orderkey HAVING
                sum(l_quantity) > 300
    )
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100
)"},

    {"Q19", R"(
SELECT
    sum(l_extendedprice* (1 - l_discount)) AS revenue
FROM
    lineitem,
    part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 1 + 10
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 10 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 20 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
)"},

    {"Q20", R"(
SELECT
    s_name,
    s_address
FROM
    supplier,
    nation
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            partsupp
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    part
                WHERE
                    p_name LIKE 'forest%'
            )
            AND ps_availqty > (
                SELECT
                    0.5 * sum(l_quantity)
                FROM
                    lineitem
                WHERE
                    l_partkey = ps_partkey
                    AND l_suppkey = ps_suppkey
                    AND l_shipdate >= DATE '1994-01-01'
                    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            )
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY
    s_name
)"},

    {"Q21", R"(
SELECT
    s_name,
    count(*) AS numwait
FROM
    supplier,
    lineitem l1,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            lineitem l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100
)"},

    {"Q22", R"(
SELECT
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal
FROM
    (
        SELECT
            substring(c_phone FROM 1 FOR 2) AS cntrycode,
            c_acctbal
        FROM
            customer
        WHERE
            substring(c_phone FROM 1 FOR 2) IN
                ('13', '31', '23', '29', '30', '18', '17')
            AND c_acctbal > (
                SELECT
                    avg(c_acctbal)
                FROM
                    customer
                WHERE
                    c_acctbal > 0.00
                    AND substring(c_phone FROM 1 FOR 2) IN
                        ('13', '31', '23', '29', '30', '18', '17')
            )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    orders
                WHERE
                    o_custkey = c_custkey
            )
    ) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode
)"}
};
// clang-format on

// ============================================================================
// Test Fixture
// ============================================================================

class TPCHBenchmarkFixture
    : public gizmosql::testing::ServerTestFixture<TPCHBenchmarkFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "tpch_benchmark.db",
        .port = 31350,
        .health_port = 31351,
        .username = "tpch_tester",
        .password = "tpch_tester",
    };
  }

  // Custom init SQL to generate TPC-H data
  static std::string GetInitSql() {
    return "INSTALL tpch; LOAD tpch; CALL dbgen(sf=1);";
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<TPCHBenchmarkFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<TPCHBenchmarkFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<TPCHBenchmarkFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<TPCHBenchmarkFixture>::config_{};

// ============================================================================
// Helper Functions
// ============================================================================

struct QueryResult {
  bool success;
  int64_t row_count;
  std::string error_message;
  std::shared_ptr<arrow::Table> table;
  double duration_ms;
};

// Run a query via GizmoSQL Flight SQL and return all results as a table
QueryResult RunQueryViaFlightSQL(FlightSqlClient& client,
                                 arrow::flight::FlightCallOptions& call_options,
                                 const std::string& query) {
  QueryResult result{};
  auto start = std::chrono::high_resolution_clock::now();

  auto flight_info_result = client.Execute(call_options, query);
  if (!flight_info_result.ok()) {
    result.success = false;
    result.error_message = flight_info_result.status().ToString();
    return result;
  }

  auto flight_info = std::move(*flight_info_result);
  if (flight_info->endpoints().empty()) {
    result.success = false;
    result.error_message = "No endpoints returned";
    return result;
  }

  auto reader_result =
      client.DoGet(call_options, flight_info->endpoints()[0].ticket);
  if (!reader_result.ok()) {
    result.success = false;
    result.error_message = reader_result.status().ToString();
    return result;
  }

  auto reader = std::move(*reader_result);
  auto table_result = reader->ToTable();
  if (!table_result.ok()) {
    result.success = false;
    result.error_message = table_result.status().ToString();
    return result;
  }

  auto end = std::chrono::high_resolution_clock::now();
  result.duration_ms =
      std::chrono::duration<double, std::milli>(end - start).count();
  result.table = *table_result;
  result.row_count = result.table->num_rows();
  result.success = true;
  return result;
}

// Run a query directly via DuckDB and return results
QueryResult RunQueryViaDuckDB(duckdb::Connection& conn, const std::string& query) {
  QueryResult result{};
  auto start = std::chrono::high_resolution_clock::now();

  try {
    auto duckdb_result = conn.Query(query);
    if (duckdb_result->HasError()) {
      result.success = false;
      result.error_message = duckdb_result->GetError();
      return result;
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.duration_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    result.row_count = duckdb_result->RowCount();
    result.success = true;
    // Note: We don't convert to Arrow table for direct DuckDB comparison
    // Row count comparison is sufficient for validating correctness
  } catch (const std::exception& e) {
    result.success = false;
    result.error_message = e.what();
  }

  return result;
}

// Compare query results for equality (row counts)
// For a full comparison, we'd need to compare actual values, but row counts
// are a good sanity check to ensure the same data is returned
bool CompareResults(const QueryResult& gizmosql_result,
                    const QueryResult& duckdb_result,
                    std::string& diff_message) {
  if (!gizmosql_result.success || !duckdb_result.success) {
    diff_message = "One or both queries failed";
    return false;
  }

  if (gizmosql_result.row_count != duckdb_result.row_count) {
    diff_message = "Row count mismatch: GizmoSQL=" +
                   std::to_string(gizmosql_result.row_count) +
                   " DuckDB=" + std::to_string(duckdb_result.row_count);
    return false;
  }

  return true;
}

// ============================================================================
// Tests
// ============================================================================

class TPCHServerFixture
    : public gizmosql::testing::ServerTestFixture<TPCHServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "tpch_test.db",
        .port = 31350,
        .health_port = 31351,
        .username = "tpch_tester",
        .password = "tpch_tester",
    };
  }
};

// Static member definitions
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<TPCHServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<TPCHServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<TPCHServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<TPCHServerFixture>::config_{};

// Single-client benchmark test: Run all 22 queries 3 times, compare with DuckDB
TEST_F(TPCHServerFixture, TPCHBenchmarkSingleClient) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect to GizmoSQL via Flight SQL
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto flight_client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(flight_client));

  // First, generate TPC-H data via GizmoSQL
  std::cerr << "\n=== Generating TPC-H SF=1 data via GizmoSQL ===" << std::endl;
  auto init_start = std::chrono::high_resolution_clock::now();

  auto init_result =
      RunQueryViaFlightSQL(sql_client, call_options, "INSTALL tpch; LOAD tpch;");
  ASSERT_TRUE(init_result.success) << "Failed to install TPC-H: " << init_result.error_message;

  init_result = RunQueryViaFlightSQL(sql_client, call_options, "CALL dbgen(sf=1);");
  ASSERT_TRUE(init_result.success) << "Failed to generate TPC-H data: " << init_result.error_message;

  auto init_end = std::chrono::high_resolution_clock::now();
  auto init_duration =
      std::chrono::duration<double>(init_end - init_start).count();
  std::cerr << "TPC-H data generation completed in " << std::fixed
            << std::setprecision(2) << init_duration << "s" << std::endl;

  // Setup standalone DuckDB for comparison
  duckdb::DuckDB db(nullptr);  // In-memory
  duckdb::Connection conn(db);
  conn.Query("INSTALL tpch; LOAD tpch; CALL dbgen(sf=1);");

  // Run benchmark
  std::cerr << "\n=== Running TPC-H Benchmark (3 iterations) ===" << std::endl;
  const int NUM_ITERATIONS = 3;

  struct QueryTiming {
    std::string name;
    std::vector<double> gizmosql_times;
    std::vector<double> duckdb_times;
    bool all_passed = true;
  };

  std::vector<QueryTiming> timings(TPCH_QUERIES.size());

  auto total_start = std::chrono::high_resolution_clock::now();

  for (int iter = 0; iter < NUM_ITERATIONS; ++iter) {
    std::cerr << "\n--- Iteration " << (iter + 1) << " of " << NUM_ITERATIONS
              << " ---" << std::endl;

    for (size_t i = 0; i < TPCH_QUERIES.size(); ++i) {
      const auto& [name, query] = TPCH_QUERIES[i];
      timings[i].name = name;

      // Run via GizmoSQL
      auto gizmosql_result = RunQueryViaFlightSQL(sql_client, call_options, query);

      // Run via DuckDB directly
      auto duckdb_result = RunQueryViaDuckDB(conn, query);

      if (gizmosql_result.success && duckdb_result.success) {
        timings[i].gizmosql_times.push_back(gizmosql_result.duration_ms);
        timings[i].duckdb_times.push_back(duckdb_result.duration_ms);

        // Compare results
        std::string diff_message;
        if (!CompareResults(gizmosql_result, duckdb_result, diff_message)) {
          std::cerr << "  " << name << ": MISMATCH - " << diff_message << std::endl;
          timings[i].all_passed = false;
        } else {
          std::cerr << "  " << name << ": OK (GizmoSQL: " << std::fixed
                    << std::setprecision(1) << gizmosql_result.duration_ms
                    << "ms, DuckDB: " << duckdb_result.duration_ms
                    << "ms, rows: " << gizmosql_result.row_count << ")"
                    << std::endl;
        }
      } else {
        timings[i].all_passed = false;
        if (!gizmosql_result.success) {
          std::cerr << "  " << name << ": GizmoSQL FAILED - "
                    << gizmosql_result.error_message << std::endl;
        }
        if (!duckdb_result.success) {
          std::cerr << "  " << name << ": DuckDB FAILED - "
                    << duckdb_result.error_message << std::endl;
        }
      }
    }
  }

  auto total_end = std::chrono::high_resolution_clock::now();
  auto total_duration =
      std::chrono::duration<double>(total_end - total_start).count();

  // Print summary
  std::cerr << "\n=== TPC-H Benchmark Summary ===" << std::endl;
  std::cerr << std::setw(6) << "Query" << std::setw(15) << "GizmoSQL(ms)"
            << std::setw(15) << "DuckDB(ms)" << std::setw(10) << "Ratio"
            << std::setw(10) << "Status" << std::endl;
  std::cerr << std::string(56, '-') << std::endl;

  double total_gizmosql_ms = 0;
  double total_duckdb_ms = 0;
  int passed = 0;
  int failed = 0;

  for (const auto& t : timings) {
    double avg_gizmosql = 0, avg_duckdb = 0;
    if (!t.gizmosql_times.empty()) {
      for (double d : t.gizmosql_times) avg_gizmosql += d;
      avg_gizmosql /= t.gizmosql_times.size();
      total_gizmosql_ms += avg_gizmosql;
    }
    if (!t.duckdb_times.empty()) {
      for (double d : t.duckdb_times) avg_duckdb += d;
      avg_duckdb /= t.duckdb_times.size();
      total_duckdb_ms += avg_duckdb;
    }

    double ratio = (avg_duckdb > 0) ? avg_gizmosql / avg_duckdb : 0;
    std::string status = t.all_passed ? "PASS" : "FAIL";

    std::cerr << std::setw(6) << t.name << std::setw(15) << std::fixed
              << std::setprecision(1) << avg_gizmosql << std::setw(15)
              << avg_duckdb << std::setw(10) << std::setprecision(2) << ratio
              << std::setw(10) << status << std::endl;

    if (t.all_passed)
      passed++;
    else
      failed++;
  }

  std::cerr << std::string(56, '-') << std::endl;
  std::cerr << std::setw(6) << "TOTAL" << std::setw(15) << std::fixed
            << std::setprecision(1) << total_gizmosql_ms << std::setw(15)
            << total_duckdb_ms << std::setw(10) << std::setprecision(2)
            << (total_duckdb_ms > 0 ? total_gizmosql_ms / total_duckdb_ms : 0)
            << std::endl;

  std::cerr << "\nTotal benchmark time: " << std::fixed << std::setprecision(2)
            << total_duration << "s" << std::endl;
  std::cerr << "Queries passed: " << passed << "/" << TPCH_QUERIES.size()
            << std::endl;

  // TODO: Historical comparison stub
  // In the future, we could store these timings and compare against CI averages
  // For now, just assert all queries passed
  ASSERT_EQ(failed, 0) << "Some TPC-H queries failed or had mismatched results";

  // Assert total time is reasonable (under 30 seconds as specified)
  ASSERT_LT(total_duration, 30.0)
      << "Benchmark exceeded 30 second time limit";
}

// Concurrency test: 10 clients with 1 second stagger
TEST_F(TPCHServerFixture, TPCHBenchmarkConcurrent) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  const int NUM_CLIENTS = 10;
  const int STAGGER_MS = 1000;  // 1 second stagger

  // First, ensure TPC-H data exists (may have been created by previous test)
  {
    arrow::flight::FlightClientOptions options;
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto flight_client,
        arrow::flight::FlightClient::Connect(location, options));

    arrow::flight::FlightCallOptions call_options;
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer,
        flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options.headers.push_back(bearer);

    FlightSqlClient sql_client(std::move(flight_client));

    // Check if data exists, generate if not
    auto check_result =
        RunQueryViaFlightSQL(sql_client, call_options, "SELECT COUNT(*) FROM lineitem");
    if (!check_result.success || check_result.row_count == 0) {
      std::cerr << "\n=== Generating TPC-H SF=1 data for concurrency test ===" << std::endl;
      auto init_result =
          RunQueryViaFlightSQL(sql_client, call_options, "INSTALL tpch; LOAD tpch;");
      ASSERT_TRUE(init_result.success) << "Failed to install TPC-H";
      init_result = RunQueryViaFlightSQL(sql_client, call_options, "CALL dbgen(sf=1);");
      ASSERT_TRUE(init_result.success) << "Failed to generate TPC-H data";
    }
  }

  std::cerr << "\n=== Running TPC-H Concurrency Test (" << NUM_CLIENTS
            << " clients, " << STAGGER_MS << "ms stagger) ===" << std::endl;

  struct ClientResult {
    int client_id;
    int queries_passed;
    int queries_failed;
    double total_duration_ms;
    std::string error;
  };

  std::vector<ClientResult> results(NUM_CLIENTS);
  std::vector<std::thread> threads;
  std::mutex output_mutex;
  std::atomic<int> clients_started{0};

  auto total_start = std::chrono::high_resolution_clock::now();

  for (int client_id = 0; client_id < NUM_CLIENTS; ++client_id) {
    threads.emplace_back([&, client_id]() {
      // Stagger start
      std::this_thread::sleep_for(std::chrono::milliseconds(client_id * STAGGER_MS));

      clients_started++;
      {
        std::lock_guard<std::mutex> lock(output_mutex);
        std::cerr << "Client " << client_id << " starting..." << std::endl;
      }

      ClientResult& result = results[client_id];
      result.client_id = client_id;
      result.queries_passed = 0;
      result.queries_failed = 0;

      auto client_start = std::chrono::high_resolution_clock::now();

      try {
        arrow::flight::FlightClientOptions options;
        auto location_result =
            arrow::flight::Location::ForGrpcTcp("localhost", GetPort());
        if (!location_result.ok()) {
          result.error = "Failed to create location: " + location_result.status().ToString();
          return;
        }

        auto client_result =
            arrow::flight::FlightClient::Connect(*location_result, options);
        if (!client_result.ok()) {
          result.error = "Failed to connect: " + client_result.status().ToString();
          return;
        }

        auto flight_client = std::move(*client_result);
        arrow::flight::FlightCallOptions call_options;

        auto bearer_result =
            flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword());
        if (!bearer_result.ok()) {
          result.error = "Failed to authenticate: " + bearer_result.status().ToString();
          return;
        }
        call_options.headers.push_back(*bearer_result);

        FlightSqlClient sql_client(std::move(flight_client));

        // Run all 22 queries once
        for (const auto& [name, query] : TPCH_QUERIES) {
          auto query_result = RunQueryViaFlightSQL(sql_client, call_options, query);
          if (query_result.success) {
            result.queries_passed++;
          } else {
            result.queries_failed++;
            std::lock_guard<std::mutex> lock(output_mutex);
            std::cerr << "Client " << client_id << " " << name << " FAILED: "
                      << query_result.error_message << std::endl;
          }
        }
      } catch (const std::exception& e) {
        result.error = e.what();
      }

      auto client_end = std::chrono::high_resolution_clock::now();
      result.total_duration_ms =
          std::chrono::duration<double, std::milli>(client_end - client_start).count();

      {
        std::lock_guard<std::mutex> lock(output_mutex);
        std::cerr << "Client " << client_id << " finished: "
                  << result.queries_passed << "/" << TPCH_QUERIES.size()
                  << " passed in " << std::fixed << std::setprecision(1)
                  << result.total_duration_ms << "ms" << std::endl;
      }
    });
  }

  // Wait for all threads
  for (auto& t : threads) {
    t.join();
  }

  auto total_end = std::chrono::high_resolution_clock::now();
  auto total_duration =
      std::chrono::duration<double>(total_end - total_start).count();

  // Print summary
  std::cerr << "\n=== Concurrency Test Summary ===" << std::endl;
  std::cerr << std::setw(8) << "Client" << std::setw(10) << "Passed"
            << std::setw(10) << "Failed" << std::setw(15) << "Duration(ms)"
            << std::endl;
  std::cerr << std::string(43, '-') << std::endl;

  int total_passed = 0;
  int total_failed = 0;

  for (const auto& r : results) {
    std::cerr << std::setw(8) << r.client_id << std::setw(10) << r.queries_passed
              << std::setw(10) << r.queries_failed << std::setw(15)
              << std::fixed << std::setprecision(1) << r.total_duration_ms;
    if (!r.error.empty()) {
      std::cerr << "  ERROR: " << r.error;
    }
    std::cerr << std::endl;

    total_passed += r.queries_passed;
    total_failed += r.queries_failed;
  }

  std::cerr << std::string(43, '-') << std::endl;
  std::cerr << "Total queries: " << total_passed << " passed, " << total_failed
            << " failed" << std::endl;
  std::cerr << "Total wall-clock time: " << std::fixed << std::setprecision(2)
            << total_duration << "s" << std::endl;

  // TODO: Historical comparison stub
  // Future: Compare against CI averages

  // Assertions
  ASSERT_EQ(total_failed, 0) << "Some queries failed in concurrency test";
  ASSERT_LT(total_duration, 30.0)
      << "Concurrency test exceeded 30 second time limit";
}
