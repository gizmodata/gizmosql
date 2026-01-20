// src/common/include/request_ctx.h
#pragma once
#include <string>
#include <optional>
#include <vector>

// Access levels for catalog permissions (ordered by privilege)
enum class CatalogAccessLevel {
    kNone = 0,   // No access
    kRead = 1,   // Read-only access
    kWrite = 2   // Read and write access (write implies read)
};

// A single catalog access rule from token claims
struct CatalogAccessRule {
    std::string catalog;  // Catalog name or "*" for wildcard
    CatalogAccessLevel access;
};

struct RequestCtx
{
    std::optional<std::string> username;
    std::optional<std::string> role;
    std::optional<std::string> peer;
    std::optional<std::string> peer_identity;
    std::optional<std::string> session_id;
    std::optional<std::string> auth_method;
    std::optional<std::string> user_agent;
    std::optional<std::string> connection_protocol;
    std::optional<std::vector<CatalogAccessRule>> catalog_access;
};

// One scratchpad per RPC thread
inline thread_local RequestCtx tl_request_ctx;