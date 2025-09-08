// src/common/include/session_context.h
#pragma once
#include <memory>
#include <string>
#include <duckdb.hpp>

struct ClientSession {
  std::shared_ptr<duckdb::Connection> connection;
  std::string session_id; // from session middleware
  std::string username; // from bearer auth middleware (JWT sub/email/etc.)
  std::string role; // from JWT claims (e.g. "role") or header
  std::string peer; // client ip:port (ctx.peer())
};
