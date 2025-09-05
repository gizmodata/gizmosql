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

#include <gflags/gflags.h>
#define BOOST_NO_CXX98_FUNCTION_BASE  // ARROW-17805
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <fstream>

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/flight/api.h"
#include "arrow/flight/sql/api.h"
#include "arrow/io/memory.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/table.h"

#include "common/include/flight_sql_fwd.h"

using arrow::Status;

namespace gizmosql {

DEFINE_string(host, "localhost", "Host to connect to");
DEFINE_int32(port, 31337, "Port to connect to");
DEFINE_string(username, "", "Username");
DEFINE_string(password, "", "Password");
DEFINE_bool(use_tls, false, "Use TLS for connection");
DEFINE_string(tls_roots, "", "Path to Root certificates for TLS (in PEM format)");
DEFINE_bool(tls_skip_verify, false, "Skip TLS server certificate verification");
DEFINE_string(mtls_cert_chain, "",
              "Path to Certificate chain (in PEM format) used for mTLS authentication - "
              "if server requires it, must be accompanied by mtls_private_key");
DEFINE_string(mtls_private_key, "",
              "Path to Private key (in PEM format) used for mTLS authentication - if "
              "server requires it");

DEFINE_string(command, "", "Method to run");
DEFINE_string(query, "", "Query");
DEFINE_string(catalog, "", "Catalog");
DEFINE_string(schema, "", "Schema");
DEFINE_string(table, "", "Table");

Status PrintResultsForEndpoint(flight::sql::FlightSqlClient &client,
                               const flight::FlightCallOptions &call_options,
                               const flight::FlightEndpoint &endpoint) {
  ARROW_ASSIGN_OR_RAISE(auto stream, client.DoGet(call_options, endpoint.ticket));

  const arrow::Result<std::shared_ptr<arrow::Schema>> &schema = stream->GetSchema();
  ARROW_RETURN_NOT_OK(schema);

  GIZMOSQL_LOG(INFO) << "Schema:" << std::endl;
  GIZMOSQL_LOG(INFO) << schema->get()->ToString() << std::endl << std::endl;

  GIZMOSQL_LOG(INFO) << "Results:" << std::endl;

  int64_t num_rows = 0;

  while (true) {
    ARROW_ASSIGN_OR_RAISE(flight::FlightStreamChunk chunk, stream->Next());
    if (chunk.data == nullptr) {
      break;
    }
    GIZMOSQL_LOG(INFO) << chunk.data->ToString() << std::endl;
    num_rows += chunk.data->num_rows();
  }

  GIZMOSQL_LOG(INFO) << "Total: " << num_rows << std::endl;

  return Status::OK();
}

Status PrintResults(flight::sql::FlightSqlClient &client,
                    const flight::FlightCallOptions &call_options,
                    const std::unique_ptr<flight::FlightInfo> &info) {
  const std::vector<flight::FlightEndpoint> &endpoints = info->endpoints();

  for (size_t i = 0; i < endpoints.size(); i++) {
    GIZMOSQL_LOG(INFO) << "Results from endpoint " << i + 1 << " of " << endpoints.size()
                       << std::endl;
    ARROW_RETURN_NOT_OK(PrintResultsForEndpoint(client, call_options, endpoints[i]));
  }

  return Status::OK();
}

Status getPEMCertFileContents(const std::string &cert_file_path,
                              std::string &cert_contents) {
  std::ifstream cert_file(cert_file_path);
  if (!cert_file.is_open()) {
    return Status::IOError("Could not open file: " + cert_file_path);
  }

  std::stringstream cert_stream;
  cert_stream << cert_file.rdbuf();
  cert_contents = cert_stream.str();

  return Status::OK();
}

Status RunMain() {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        (FLAGS_use_tls)
                            ? flight::Location::ForGrpcTls(FLAGS_host, FLAGS_port)
                            : flight::Location::ForGrpcTcp(FLAGS_host, FLAGS_port));

  // Setup our options
  flight::FlightClientOptions options;

  if (!FLAGS_tls_roots.empty()) {
    ARROW_RETURN_NOT_OK(getPEMCertFileContents(FLAGS_tls_roots, options.tls_root_certs));
  }

  options.disable_server_verification = FLAGS_tls_skip_verify;

  if (!FLAGS_mtls_cert_chain.empty()) {
    ARROW_RETURN_NOT_OK(
        getPEMCertFileContents(FLAGS_mtls_cert_chain, options.cert_chain));

    if (!FLAGS_mtls_private_key.empty()) {
      ARROW_RETURN_NOT_OK(
          getPEMCertFileContents(FLAGS_mtls_private_key, options.private_key));
    } else {
      return Status::Invalid(
          "mTLS private key file must be provided if mTLS certificate chain is provided");
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto client, flight::FlightClient::Connect(location, options));

  flight::FlightCallOptions call_options;

  if (!FLAGS_username.empty() || !FLAGS_password.empty()) {
    arrow::Result<std::pair<std::string, std::string>> bearer_result =
        client->AuthenticateBasicToken({}, FLAGS_username, FLAGS_password);
    ARROW_RETURN_NOT_OK(bearer_result);

    call_options.headers.push_back(bearer_result.ValueOrDie());
  }

  flight::sql::FlightSqlClient sql_client(std::move(client));

  if (FLAGS_command == "ExecuteUpdate") {
    ARROW_ASSIGN_OR_RAISE(auto rows, sql_client.ExecuteUpdate(call_options, FLAGS_query));

    GIZMOSQL_LOG(INFO) << "Result: " << rows << std::endl;

    return Status::OK();
  }

  std::unique_ptr<flight::FlightInfo> info;

  std::shared_ptr<arrow::flight::sql::PreparedStatement> prepared_statement;

  if (FLAGS_command == "Execute") {
    ARROW_ASSIGN_OR_RAISE(info, sql_client.Execute(call_options, FLAGS_query));
  } else if (FLAGS_command == "GetCatalogs") {
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetCatalogs(call_options));
  } else if (FLAGS_command == "PreparedStatementExecute") {
    ARROW_ASSIGN_OR_RAISE(prepared_statement,
                          sql_client.Prepare(call_options, FLAGS_query));
    ARROW_ASSIGN_OR_RAISE(info, prepared_statement->Execute(call_options));
  } else if (FLAGS_command == "PreparedStatementExecuteParameterBinding") {
    ARROW_ASSIGN_OR_RAISE(prepared_statement, sql_client.Prepare({}, FLAGS_query));
    auto parameter_schema = prepared_statement->parameter_schema();
    auto result_set_schema = prepared_statement->dataset_schema();

    GIZMOSQL_LOG(INFO) << result_set_schema->ToString(false) << std::endl;
    arrow::Int64Builder int_builder;
    ARROW_RETURN_NOT_OK(int_builder.Append(1));
    std::shared_ptr<arrow::Array> int_array;
    ARROW_RETURN_NOT_OK(int_builder.Finish(&int_array));
    std::shared_ptr<arrow::RecordBatch> result;
    result = arrow::RecordBatch::Make(parameter_schema, 1, {int_array});

    ARROW_RETURN_NOT_OK(prepared_statement->SetParameters(result));
    ARROW_ASSIGN_OR_RAISE(info, prepared_statement->Execute(call_options));
  } else if (FLAGS_command == "GetDbSchemas") {
    ARROW_ASSIGN_OR_RAISE(
        info, sql_client.GetDbSchemas(call_options, &FLAGS_catalog, &FLAGS_schema));
  } else if (FLAGS_command == "GetTableTypes") {
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetTableTypes(call_options));
  } else if (FLAGS_command == "GetTables") {
    ARROW_ASSIGN_OR_RAISE(
        info, sql_client.GetTables(call_options, &FLAGS_catalog, &FLAGS_schema,
                                   &FLAGS_table, false, nullptr));
  } else if (FLAGS_command == "GetExportedKeys") {
    flight::sql::TableRef table_ref = {std::make_optional(FLAGS_catalog),
                                       std::make_optional(FLAGS_schema), FLAGS_table};
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetExportedKeys(call_options, table_ref));
  } else if (FLAGS_command == "GetImportedKeys") {
    flight::sql::TableRef table_ref = {std::make_optional(FLAGS_catalog),
                                       std::make_optional(FLAGS_schema), FLAGS_table};
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetImportedKeys(call_options, table_ref));
  } else if (FLAGS_command == "GetPrimaryKeys") {
    flight::sql::TableRef table_ref = {std::make_optional(FLAGS_catalog),
                                       std::make_optional(FLAGS_schema), FLAGS_table};
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetPrimaryKeys(call_options, table_ref));
  } else if (FLAGS_command == "GetSqlInfo") {
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetSqlInfo(call_options, {}));
  }

  arrow::Status print_status;
  if (info != NULLPTR) {
    print_status = PrintResults(sql_client, call_options, info);

    if (prepared_statement != NULLPTR) {
      ARROW_RETURN_NOT_OK(prepared_statement->Close(call_options));
    }
  }

  return print_status;
}

}  // namespace gizmosql

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  Status st = gizmosql::RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
