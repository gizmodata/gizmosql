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

#include <arrow/flight/sql/types.h>
#include "flight_sql_fwd.h"

namespace gizmosql::ddb {
// Forward declaration
class DuckDBFlightSqlServer;

/// \brief Gets the mapping from SQL info ids to SqlInfoResult instances.
/// Uses dynamic queries to DuckDB like the Java driver.
/// \param server The DuckDB server instance to use for dynamic queries
/// \return the cache.
flight::sql::SqlInfoResultMap GetSqlInfoResultMap(const DuckDBFlightSqlServer* server,
                                                  const int32_t& query_timeout_seconds);
}  // namespace gizmosql::ddb