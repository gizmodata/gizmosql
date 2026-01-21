# =========================================================
# GizmoSQL Enterprise Features
# GizmoData Commercial License
# Copyright (c) 2026 GizmoData LLC. All rights reserved.
# =========================================================

# Enterprise source files
set(GIZMOSQL_ENTERPRISE_SRCS
    enterprise_features.cpp
    license_mgr/license_manager.cpp
    instrumentation/instrumentation_manager.cpp
    instrumentation/instrumentation_records.cpp
    kill_session/kill_session_handler.cpp
)

# Create the enterprise library
add_library(gizmosql_enterprise STATIC ${GIZMOSQL_ENTERPRISE_SRCS})

# Include directories for enterprise module
target_include_directories(gizmosql_enterprise PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/license_mgr
    ${CMAKE_CURRENT_SOURCE_DIR}/instrumentation
    ${CMAKE_CURRENT_SOURCE_DIR}/kill_session
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/common/include
    ${CMAKE_SOURCE_DIR}/src/duckdb
    ${DUCKDB_INCLUDE_DIR}
    ${CMAKE_BINARY_DIR}/third_party/jwt-cpp/include
)

# Link dependencies
target_link_libraries(gizmosql_enterprise PUBLIC
    Arrow::arrow_static
    duckdb_static
)

# Define GIZMOSQL_ENTERPRISE for code compiled with this library
target_compile_definitions(gizmosql_enterprise PUBLIC GIZMOSQL_ENTERPRISE)
