// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Tests for the gizmosql_server CLI's early-exit informational flags
// (--version, --print-duckdb-version): run the actual binary and check output.

#include <gtest/gtest.h>

#include <array>
#include <cctype>
#include <cstdio>
#include <string>

#ifdef _WIN32
#define popen _popen
#define pclose _pclose
#else
#include <sys/wait.h>
#endif

#include "gizmosql_library.h"  // GetDuckDBVersion()

namespace {

std::string ServerBinary() {
#ifdef GIZMOSQL_SERVER_BINARY
  return GIZMOSQL_SERVER_BINARY;
#else
  return "gizmosql_server";
#endif
}

struct CliResult {
  std::string stdout_output;
  int exit_code;
};

CliResult RunServer(const std::string& args) {
  // Discard stderr via the platform null device (cmd.exe uses NUL, not /dev/null).
#ifdef _WIN32
  const char* kNullDevice = "NUL";
#else
  const char* kNullDevice = "/dev/null";
#endif
  const std::string cmd = ServerBinary() + " " + args + " 2>" + kNullDevice;
  CliResult result;
  FILE* pipe = popen(cmd.c_str(), "r");
  if (!pipe) {
    result.exit_code = -1;
    return result;
  }
  std::array<char, 4096> buf{};
  while (fgets(buf.data(), buf.size(), pipe) != nullptr) {
    result.stdout_output += buf.data();
  }
  int rc = pclose(pipe);
#ifdef _WIN32
  result.exit_code = rc;
#else
  result.exit_code = (rc >= 0 && WIFEXITED(rc)) ? WEXITSTATUS(rc) : -1;
#endif
  return result;
}

std::string Trim(std::string s) {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.pop_back();
  size_t i = 0;
  while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
  return s.substr(i);
}

}  // namespace

TEST(ServerCli, PrintDuckDBVersionMatchesLinkedLibrary) {
  auto res = RunServer("--print-duckdb-version");
  EXPECT_EQ(res.exit_code, 0);
  const std::string printed = Trim(res.stdout_output);
  EXPECT_FALSE(printed.empty()) << "--print-duckdb-version printed nothing";
  // Must exactly match the version the library reports (the flag's source).
  EXPECT_EQ(printed, std::string(GetDuckDBVersion()));
  // Sanity: looks like a version (contains a digit).
  EXPECT_NE(printed.find_first_of("0123456789"), std::string::npos)
      << "not a version string: " << printed;
}

TEST(ServerCli, VersionFlagPrintsServerVersion) {
  auto res = RunServer("--version");
  EXPECT_EQ(res.exit_code, 0);
  EXPECT_NE(res.stdout_output.find("GizmoSQL Server CLI"), std::string::npos)
      << "got: " << res.stdout_output;
}
