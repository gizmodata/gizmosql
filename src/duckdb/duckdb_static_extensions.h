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

#include <arrow/status.h>

namespace gizmosql::ddb {

// Registers the DuckDB extensions linked into this binary (core_functions,
// parquet, icu, tpch; plus ducklake/httpfs/postgres_scanner on iOS) so the
// next duckdb::DuckDB opened sees them.
//
// DuckDB 2.0 no longer registers static extensions on its own: a program
// that links extension archives must register each one through its
// duckdb_extension_<name>_describe function, or it contributes nothing and
// e.g. icu's timezone functions silently go missing. DuckDB 1.x (the LTS
// channel) still auto-loads them, so this is a no-op there.
//
// Idempotent and thread-safe; call it before creating any duckdb::DuckDB.
arrow::Status RegisterStaticExtensions();

}  // namespace gizmosql::ddb
