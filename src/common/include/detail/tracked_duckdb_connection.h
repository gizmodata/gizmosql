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

#pragma once

#include <cstdint>

#include <duckdb.hpp>

namespace gizmosql {
namespace metrics {
void RecordOpenDuckDBConnections(int64_t count);
}  // namespace metrics

// RAII wrapper around duckdb::Connection that tracks the open DuckDB
// connection count via the gizmosql.duckdb.connections.open metric.
// Increments the counter on construction, decrements on destruction.
class TrackedDuckDBConnection {
 public:
  explicit TrackedDuckDBConnection(duckdb::DuckDB& db)
      : connection_(db) {
    ::gizmosql::metrics::RecordOpenDuckDBConnections(1);
  }

  ~TrackedDuckDBConnection() {
    ::gizmosql::metrics::RecordOpenDuckDBConnections(-1);
  }

  // Non-copyable
  TrackedDuckDBConnection(const TrackedDuckDBConnection&) = delete;
  TrackedDuckDBConnection& operator=(const TrackedDuckDBConnection&) = delete;

  // Non-movable (prevent counter mismatches from moved-from state)
  TrackedDuckDBConnection(TrackedDuckDBConnection&&) = delete;
  TrackedDuckDBConnection& operator=(TrackedDuckDBConnection&&) = delete;

  duckdb::Connection& Get() { return connection_; }
  const duckdb::Connection& Get() const { return connection_; }

 private:
  duckdb::Connection connection_;
};

}  // namespace gizmosql
