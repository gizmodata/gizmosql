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

#include "client_config.hpp"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <sstream>

namespace gizmosql::client {

namespace {
bool ParseBoolValue(const std::string& val) {
  std::string lower = val;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
  return lower == "true" || lower == "1";
}
}  // namespace

std::string OutputModeToString(OutputMode mode) {
  switch (mode) {
    case OutputMode::TABLE:     return "table";
    case OutputMode::BOX:       return "box";
    case OutputMode::CSV:       return "csv";
    case OutputMode::TABS:      return "tabs";
    case OutputMode::JSON:      return "json";
    case OutputMode::JSONLINES: return "jsonlines";
    case OutputMode::MARKDOWN:  return "markdown";
    case OutputMode::LINE:      return "line";
    case OutputMode::LIST:      return "list";
    case OutputMode::HTML:      return "html";
    case OutputMode::LATEX:     return "latex";
    case OutputMode::INSERT:    return "insert";
    case OutputMode::QUOTE:     return "quote";
    case OutputMode::ASCII:     return "ascii";
    case OutputMode::TRASH:     return "trash";
  }
  return "unknown";
}

std::optional<OutputMode> OutputModeFromString(const std::string& name) {
  std::string lower = name;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

  if (lower == "table")     return OutputMode::TABLE;
  if (lower == "box")       return OutputMode::BOX;
  if (lower == "csv")       return OutputMode::CSV;
  if (lower == "tabs")      return OutputMode::TABS;
  if (lower == "json")      return OutputMode::JSON;
  if (lower == "jsonlines") return OutputMode::JSONLINES;
  if (lower == "markdown")  return OutputMode::MARKDOWN;
  if (lower == "line")      return OutputMode::LINE;
  if (lower == "list")      return OutputMode::LIST;
  if (lower == "html")      return OutputMode::HTML;
  if (lower == "latex")     return OutputMode::LATEX;
  if (lower == "insert")    return OutputMode::INSERT;
  if (lower == "quote")     return OutputMode::QUOTE;
  if (lower == "ascii")     return OutputMode::ASCII;
  if (lower == "trash")     return OutputMode::TRASH;
  return std::nullopt;
}

void ResolveEnvironmentVariables(ClientConfig& config) {
  auto get_env = [](const char* name) -> const char* {
    const char* val = std::getenv(name);
    return (val != nullptr && val[0] != '\0') ? val : nullptr;
  };

  if (config.host == "localhost") {
    if (const char* val = get_env("GIZMOSQL_HOST")) {
      config.host = val;
    }
  }

  if (config.port == 31337) {
    if (const char* val = get_env("GIZMOSQL_PORT")) {
      try {
        config.port = std::stoi(val);
      } catch (...) {
        // ignore invalid port
      }
    }
  }

  if (config.username.empty()) {
    if (const char* val = get_env("GIZMOSQL_USER")) {
      config.username = val;
    }
  }

  if (!config.password_provided) {
    if (const char* val = get_env("GIZMOSQL_PASSWORD")) {
      config.password = val;
      config.password_provided = true;
    }
  }

  if (!config.use_tls) {
    if (const char* val = get_env("GIZMOSQL_TLS")) {
      std::string v(val);
      config.use_tls = (v == "1" || v == "true" || v == "TRUE" || v == "True");
    }
  }

  if (config.tls_roots.empty()) {
    if (const char* val = get_env("GIZMOSQL_TLS_ROOTS")) {
      config.tls_roots = val;
    }
  }

  if (config.oauth_port == 31339) {
    if (const char* val = get_env("GIZMOSQL_OAUTH_PORT")) {
      try {
        config.oauth_port = std::stoi(val);
      } catch (...) {
        // ignore invalid port
      }
    }
  }
}

namespace {

std::map<std::string, std::string> ParseQueryString(const std::string& qs) {
  std::map<std::string, std::string> params;
  std::istringstream stream(qs);
  std::string pair;
  while (std::getline(stream, pair, '&')) {
    auto eq = pair.find('=');
    if (eq != std::string::npos) {
      params[pair.substr(0, eq)] = pair.substr(eq + 1);
    }
  }
  return params;
}

}  // namespace

bool ParseConnectionURI(const std::string& uri, ClientConfig& config) {
  std::string remainder = uri;

  // Strip scheme
  auto scheme_end = remainder.find("://");
  if (scheme_end != std::string::npos) {
    remainder = remainder.substr(scheme_end + 3);
  }

  // Split host:port from query string
  std::string host_port;
  std::string query_string;
  auto qmark = remainder.find('?');
  if (qmark != std::string::npos) {
    host_port = remainder.substr(0, qmark);
    query_string = remainder.substr(qmark + 1);
  } else {
    host_port = remainder;
  }

  // Parse host:port
  auto colon = host_port.rfind(':');
  if (colon != std::string::npos) {
    config.host = host_port.substr(0, colon);
    try {
      config.port = std::stoi(host_port.substr(colon + 1));
    } catch (...) {
      std::cerr << "Error: invalid port in URI" << std::endl;
      return false;
    }
  } else if (!host_port.empty()) {
    config.host = host_port;
  }

  // Parse query parameters
  if (!query_string.empty()) {
    auto params = ParseQueryString(query_string);

    if (params.count("username")) {
      config.username = params["username"];
    }
    if (params.count("useEncryption")) {
      config.use_tls = ParseBoolValue(params["useEncryption"]);
    }
    if (params.count("disableCertificateVerification")) {
      config.tls_skip_verify = ParseBoolValue(params["disableCertificateVerification"]);
    }
    if (params.count("tlsRoots")) {
      config.tls_roots = params["tlsRoots"];
    }
    if (params.count("authType")) {
      config.auth_type_external = (params["authType"] == "external");
    }
  }

  return true;
}

std::string BuildConnectionURI(const ClientConfig& config) {
  std::ostringstream uri;
  uri << "gizmosql://" << config.host << ":" << config.port;

  std::string sep = "?";
  auto addParam = [&](const std::string& key, const std::string& value) {
    uri << sep << key << "=" << value;
    sep = "&";
  };

  if (!config.username.empty()) {
    addParam("username", config.username);
  }
  if (config.use_tls) {
    addParam("useEncryption", "true");
  }
  if (config.tls_skip_verify) {
    addParam("disableCertificateVerification", "true");
  }
  if (!config.tls_roots.empty()) {
    addParam("tlsRoots", config.tls_roots);
  }
  if (config.auth_type_external) {
    addParam("authType", "external");
  }

  return uri.str();
}

}  // namespace gizmosql::client
