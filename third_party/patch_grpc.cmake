# Source fix for the gRPC that Arrow builds via FetchContent (run as its
# PATCH_COMMAND, working directory = the gRPC source tree; wired in by
# patch_arrow.cmake).
#
# MSVC 14.5x (Visual Studio 2026) fails gRPC's filter templates in C++20 mode,
# which Arrow 25 forces on its bundled dependencies:
#   call_filters.h / promise_based_filter.h: error C3539: a template-argument
#   cannot be a type that contains 'auto'
# It is a compiler bug, open upstream as grpc/grpc#41436 (still present in gRPC
# 1.82). Forming the filter's member pointers as constexpr locals before the
# decltype() template arguments use them avoids it. Plain C++, so it is applied
# on every platform. Verified on windows-11-arm with MSVC 14.51.36231 against
# gRPC 1.76.0; clang accepts it too.
#
# A pattern that no longer matches (a gRPC upgrade) only warns: if the bug is
# still there, the MSVC build fails loudly with C3539 anyway.

function(gizmosql_patch_grpc_file relative_path old new)
  # A Windows checkout can give this script CRLF line endings; the gRPC
  # tarball is LF. Compare and write LF.
  string(REPLACE "\r" "" old "${old}")
  string(REPLACE "\r" "" new "${new}")
  set(path "${CMAKE_CURRENT_SOURCE_DIR}/${relative_path}")
  file(READ "${path}" content)
  string(FIND "${content}" "GizmoSQL patch" already_patched)
  if(NOT already_patched EQUAL -1)
    return()
  endif()
  string(FIND "${content}" "${old}" found)
  if(found EQUAL -1)
    message(WARNING "patch_grpc.cmake: pattern not found in ${relative_path}; "
                    "MSVC C3539 workaround (grpc/grpc#41436) not applied")
    return()
  endif()
  string(REPLACE "${old}" "${new}" content "${content}")
  file(WRITE "${path}" "${content}")
  message(STATUS "patch_grpc.cmake: patched ${relative_path}")
endfunction()

gizmosql_patch_grpc_file("src/core/call/call_filters.h"
"inline constexpr bool CallHasChannelAccess() {
  return AnyMethodHasChannelAccess<"
"inline constexpr bool CallHasChannelAccess() {
  // GizmoSQL patch: MSVC 14.5x (VS 2026) in C++20 mode fails the decltype()s
  // below with C3539 unless the member pointers are formed first
  // (grpc/grpc#41436).
  [[maybe_unused]] constexpr auto on_client_initial_metadata =
      &Derived::Call::OnClientInitialMetadata;
  [[maybe_unused]] constexpr auto on_client_to_server_message =
      &Derived::Call::OnClientToServerMessage;
  [[maybe_unused]] constexpr auto on_server_initial_metadata =
      &Derived::Call::OnServerInitialMetadata;
  [[maybe_unused]] constexpr auto on_server_to_client_message =
      &Derived::Call::OnServerToClientMessage;
  [[maybe_unused]] constexpr auto on_server_trailing_metadata =
      &Derived::Call::OnServerTrailingMetadata;
  [[maybe_unused]] constexpr auto on_finalize = &Derived::Call::OnFinalize;
  return AnyMethodHasChannelAccess<")

gizmosql_patch_grpc_file("src/core/lib/channel/promise_based_filter.h"
"      CallArgs call_args, NextPromiseFactory next_promise_factory) final {
    auto* call = promise_filter_detail::MakeFilterCall<Derived>("
"      CallArgs call_args, NextPromiseFactory next_promise_factory) final {
    // GizmoSQL patch: see CallHasChannelAccess() in call_filters.h
    // (MSVC 14.5x C3539, grpc/grpc#41436).
    [[maybe_unused]] constexpr auto on_client_initial_metadata =
        &Derived::Call::OnClientInitialMetadata;
    [[maybe_unused]] constexpr auto on_client_to_server_message =
        &Derived::Call::OnClientToServerMessage;
    [[maybe_unused]] constexpr auto on_client_to_server_half_close =
        &Derived::Call::OnClientToServerHalfClose;
    [[maybe_unused]] constexpr auto on_server_initial_metadata =
        &Derived::Call::OnServerInitialMetadata;
    [[maybe_unused]] constexpr auto on_server_to_client_message =
        &Derived::Call::OnServerToClientMessage;
    [[maybe_unused]] constexpr auto on_server_trailing_metadata =
        &Derived::Call::OnServerTrailingMetadata;
    [[maybe_unused]] constexpr auto on_finalize = &Derived::Call::OnFinalize;
    auto* call = promise_filter_detail::MakeFilterCall<Derived>(")
