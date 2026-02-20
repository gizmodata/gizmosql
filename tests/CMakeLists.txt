# =========================================================
# GizmoSQL Tests
# =========================================================

cmake_minimum_required(VERSION 3.25)

# Only need GTest, since Arrow & friends already defined in root CMakeLists
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()

# ---------------------------------------------------------
# Integration tests
# ---------------------------------------------------------
# Core integration tests (always built)
set(GIZMOSQL_CORE_TEST_SRCS
        integration/test_authentication.cpp
        integration/test_bulk_ingest.cpp
        integration/test_catalog_access.cpp
        integration/test_catalog_permissions_enterprise.cpp
        integration/test_cross_instance_tokens.cpp
        integration/test_health_check.cpp
        integration/test_interactive_client.cpp
        integration/test_sqlite_backend.cpp
        integration/test_token_authorized_emails.cpp
        integration/test_tpch_benchmark.cpp
        integration/test_ducklake.cpp
        integration/test_enterprise_gating.cpp
        integration/test_geoarrow.cpp
)

# Client library sources needed by interactive client tests
set(GIZMOSQL_CLIENT_LIB_SRCS
        ${CMAKE_SOURCE_DIR}/src/client/client_config.cpp
        ${CMAKE_SOURCE_DIR}/src/client/flight_connection.cpp
        ${CMAKE_SOURCE_DIR}/src/client/oauth_flow.cpp
        ${CMAKE_SOURCE_DIR}/src/client/sql_processor.cpp
        ${CMAKE_SOURCE_DIR}/src/client/command_processor.cpp
        ${CMAKE_SOURCE_DIR}/src/client/output_renderer.cpp
        ${CMAKE_SOURCE_DIR}/src/client/password_prompt.cpp
        ${CMAKE_SOURCE_DIR}/src/client/shell_loop.cpp
        ${CMAKE_SOURCE_DIR}/src/client/shell_completer.cpp
)

# Enterprise integration tests (only built when GIZMOSQL_ENTERPRISE is ON)
if(GIZMOSQL_ENTERPRISE)
    set(GIZMOSQL_ENTERPRISE_TEST_SRCS
            integration/test_instrumentation.cpp
            integration/test_instrumentation_ducklake.cpp
            integration/test_kill_session.cpp
            integration/test_oauth_server.cpp
    )
endif()

add_executable(gizmosql_integration_tests
        ${GIZMOSQL_CORE_TEST_SRCS}
        ${GIZMOSQL_ENTERPRISE_TEST_SRCS}
        ${GIZMOSQL_CLIENT_LIB_SRCS}
)

# ---------------------------------------------------------
# Include directories
# ---------------------------------------------------------
target_include_directories(gizmosql_integration_tests PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/src/common/include
        ${CMAKE_SOURCE_DIR}/src/client
        ${DUCKDB_INCLUDE_DIR}
        ${SQLITE_INCLUDE_DIR}
        ${CMAKE_BINARY_DIR}/third_party/jwt-cpp/include
        ${HTTPLIB_INCLUDE_DIR}
        ${HEALTH_PROTO_GEN_DIR}
        ${PROTOBUF_INCLUDE_DIR}
        ${GRPC_INCLUDE_DIR}
        ${ABSL_INCLUDE_DIR}
)

# Add enterprise-specific include directories when building enterprise edition
if(GIZMOSQL_ENTERPRISE)
    target_include_directories(gizmosql_integration_tests PRIVATE
            ${CMAKE_SOURCE_DIR}/src/enterprise
            ${CMAKE_SOURCE_DIR}/src/enterprise/license_mgr
            ${CMAKE_SOURCE_DIR}/src/enterprise/instrumentation
            ${CMAKE_SOURCE_DIR}/src/enterprise/kill_session
            ${CMAKE_SOURCE_DIR}/src/enterprise/catalog_permissions
            ${CMAKE_SOURCE_DIR}/src/enterprise/oauth
            ${HTTPLIB_INCLUDE_DIR}
    )
    target_link_libraries(gizmosql_integration_tests PRIVATE
            nlohmann_json::nlohmann_json
    )
endif()

# ---------------------------------------------------------
# Link dependencies
# ---------------------------------------------------------
target_link_libraries(gizmosql_integration_tests PRIVATE
        gizmosqlserver          # your main library target
        GTest::gtest_main
        Arrow::arrow_static
        ArrowFlightSql::arrow_flight_sql_static
        arrow_absl              # Arrow 23 abseil libraries
        nlohmann_json::nlohmann_json  # for client OAuth flow tests
        replxx                        # for client shell_loop
        Threads::Threads
)

# Platform-specific link libraries
if(WIN32)
    target_link_libraries(gizmosql_integration_tests PRIVATE ws2_32 iphlpapi bcrypt userenv crypt32 shell32 rstrtmgr ntdll)
elseif(APPLE)
    target_link_libraries(gizmosql_integration_tests PRIVATE "-framework CoreFoundation")
elseif(UNIX AND NOT APPLE)
    target_link_libraries(gizmosql_integration_tests PRIVATE "-lssl -lcrypto -lresolv")
endif()

# ---------------------------------------------------------
# Discover and register tests
# ---------------------------------------------------------
include(GoogleTest)
gtest_discover_tests(gizmosql_integration_tests)

set_target_properties(gizmosql_integration_tests PROPERTIES
        FOLDER "Tests"
)
