// src/common/include/request_ctx.h
#pragma once
#include <string>
#include <optional>

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
};

// One scratchpad per RPC thread
inline thread_local RequestCtx tl_request_ctx;