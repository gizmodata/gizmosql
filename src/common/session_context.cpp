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

#include "session_context.h"

#include "gizmosql_logging.h"
#include "gizmosql_telemetry.h"

#ifdef GIZMOSQL_ENTERPRISE
#include "instrumentation/instrumentation_records.h"
#endif

namespace gizmosql {

ClientSession::~ClientSession() {
  // 1. Interrupt any in-flight query on the DuckDB connection
  if (connection && active_sql_handle.has_value() && !active_sql_handle->empty()) {
    try {
      connection->Get().Interrupt();
    } catch (...) {
      // Swallow exceptions in destructor
    }
  }

  // 2. Clear prepared statements before destroying the connection.
  //    This ensures DuckDB prepared statement handles are released
  //    while the connection is still alive.
  {
    std::unique_lock lock(statements_mutex);
    if (!prepared_statements.empty()) {
      GIZMOSQL_LOG(INFO) << "Released " << prepared_statements.size()
                         << " prepared statement(s) for session " << session_id;
      prepared_statements.clear();
    }
  }

  // 3. Decrement active session counter
  ::gizmosql::metrics::RecordActiveConnections(-1);

  // 4. TrackedDuckDBConnection destructor fires after this,
  //    decrementing the open DuckDB connection counter
}

}  // namespace gizmosql
