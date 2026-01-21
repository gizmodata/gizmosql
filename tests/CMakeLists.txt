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
add_executable(gizmosql_integration_tests
        integration/test_authentication.cpp
        integration/test_bulk_ingest.cpp
        integration/test_catalog_access.cpp
        integration/test_health_check.cpp
        integration/test_instrumentation.cpp
        integration/test_kill_session.cpp
        integration/test_sqlite_backend.cpp
        integration/test_tpch_benchmark.cpp
        integration/test_ducklake.cpp
)

# ---------------------------------------------------------
# Include directories
# ---------------------------------------------------------
target_include_directories(gizmosql_integration_tests PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/src/common/include
        ${DUCKDB_INCLUDE_DIR}
        ${SQLITE_INCLUDE_DIR}
        ${CMAKE_BINARY_DIR}/third_party/jwt-cpp/include
        ${HEALTH_PROTO_GEN_DIR}
        ${PROTOBUF_INCLUDE_DIR}
        ${GRPC_INCLUDE_DIR}
        ${ABSL_INCLUDE_DIR}
)

# ---------------------------------------------------------
# Link dependencies
# ---------------------------------------------------------
target_link_libraries(gizmosql_integration_tests PRIVATE
        gizmosqlserver          # your main library target
        GTest::gtest_main
        Arrow::arrow_static
        ArrowFlightSql::arrow_flight_sql_static
        arrow_absl              # Arrow 23 abseil libraries
        Threads::Threads
)

# macOS / Linux specifics
if (APPLE)
    target_link_libraries(gizmosql_integration_tests PRIVATE "-framework CoreFoundation")
elseif (UNIX AND NOT APPLE)
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
