cmake_minimum_required(VERSION 3.25)
project(gizmosql)

set(CMAKE_CXX_STANDARD 17)

# Optional OpenTelemetry support (requires libcurl)
option(WITH_OPENTELEMETRY "Build with OpenTelemetry tracing/metrics support" OFF)

# Function to retrieve the latest Git tag
function(get_latest_git_tag OUTPUT_VARIABLE)
    execute_process(
            COMMAND git describe --tags --abbrev=0
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE RETURN_VALUE
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    set(${OUTPUT_VARIABLE} ${RETURN_VALUE} PARENT_SCOPE)
endfunction()

# Set the version
get_latest_git_tag(LATEST_TAG)
set(PROJECT_VERSION ${LATEST_TAG})

# Display variable values using message
message(STATUS "PROJECT_VERSION: ${PROJECT_VERSION}")

# Configure a header file with the version
configure_file(
        ${CMAKE_SOURCE_DIR}/src/common/include/version.h.in
        ${CMAKE_SOURCE_DIR}/src/common/include/version.h
)

# --------------------- Arrow ---------------------
configure_file(third_party/Arrow_CMakeLists.txt.in
        arrow/CMakeLists.txt
)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" . -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/arrow"
        COMMAND_ERROR_IS_FATAL ANY
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/arrow"
        COMMAND_ERROR_IS_FATAL ANY
)
set(ARROW_INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/arrow")

find_package(Arrow REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)
find_package(ArrowFlight REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)
find_package(ArrowFlightSql REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)

# --------------------- SQLite ---------------------
configure_file(third_party/SQLite_CMakeLists.txt.in sqlite/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" . -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/sqlite"
        COMMAND_ERROR_IS_FATAL ANY
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/sqlite"
        COMMAND_ERROR_IS_FATAL ANY
)
execute_process(COMMAND "${CMAKE_COMMAND}" --install .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/sqlite"
        COMMAND_ERROR_IS_FATAL ANY
)

set(SQLITE_INCLUDE_DIR "${CMAKE_BINARY_DIR}/third_party/sqlite/sqlite_project-install/include")
set(SQLITE_LIBRARY_PATH "${CMAKE_BINARY_DIR}/third_party/sqlite/sqlite_project-install/lib/libsqlite3.a")

add_library(sqlite STATIC IMPORTED)
set_property(TARGET sqlite PROPERTY IMPORTED_LOCATION ${SQLITE_LIBRARY_PATH})
set_property(TARGET sqlite PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${SQLITE_INCLUDE_DIR})

# --------------------- DuckDB ---------------------

configure_file(third_party/DuckDB_CMakeLists.txt.in duckdb/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" . -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/duckdb"
        COMMAND_ERROR_IS_FATAL ANY
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/duckdb"
        COMMAND_ERROR_IS_FATAL ANY
)

set(DUCKDB_INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/duckdb")

set(DUCKDB_LIBRARY_PATH "${DUCKDB_INSTALL_DIR}/lib/libduckdb_static.a")
set(DUCKDB_INCLUDE_DIR "${DUCKDB_INSTALL_DIR}/include")

add_library(duckdb STATIC IMPORTED)
set_property(TARGET duckdb PROPERTY IMPORTED_LOCATION ${DUCKDB_LIBRARY_PATH})
set_property(TARGET duckdb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${DUCKDB_INCLUDE_DIR})

target_link_libraries(duckdb
        INTERFACE
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_fastpforlib.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_fmt.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_fsst.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_hyperloglog.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_mbedtls.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_miniz.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_pg_query.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_re2.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_utf8proc.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_yyjson.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_zstd.a"
        "${DUCKDB_INSTALL_DIR}/lib/libparquet_extension.a"
        "${DUCKDB_INSTALL_DIR}/lib/libcore_functions_extension.a"
        "${DUCKDB_INSTALL_DIR}/lib/libduckdb_skiplistlib.a"
)

# Only link jemalloc_extension on Linux
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(duckdb
            INTERFACE
            "${DUCKDB_INSTALL_DIR}/lib/libjemalloc_extension.a"
    )
endif ()

# --------------------- OpenTelemetry (Optional) ---------------------
if(WITH_OPENTELEMETRY)
    message(STATUS "Building with OpenTelemetry support")
    # Build OpenTelemetry C++ SDK with OTLP exporters
    # This must come after Arrow (uses Arrow's bundled protobuf)

    set(ARROW_PROTOBUF_DIR "${CMAKE_BINARY_DIR}/third_party/src/arrow_project-build/protobuf_ep-install")
    configure_file(third_party/OpenTelemetry_CMakeLists.txt.in opentelemetry/CMakeLists.txt @ONLY)
    execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" . -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/opentelemetry"
            RESULT_VARIABLE OTEL_CONFIG_RESULT
            OUTPUT_VARIABLE OTEL_CONFIG_OUTPUT
            ERROR_VARIABLE OTEL_CONFIG_ERROR
    )
    if(NOT OTEL_CONFIG_RESULT EQUAL 0)
        message(FATAL_ERROR "OpenTelemetry configure failed:\nOutput: ${OTEL_CONFIG_OUTPUT}\nError: ${OTEL_CONFIG_ERROR}")
    endif()

    execute_process(COMMAND "${CMAKE_COMMAND}" --build . -- -j4
            WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/opentelemetry"
            RESULT_VARIABLE OTEL_BUILD_RESULT
            OUTPUT_VARIABLE OTEL_BUILD_OUTPUT
            ERROR_VARIABLE OTEL_BUILD_ERROR
    )
    if(NOT OTEL_BUILD_RESULT EQUAL 0)
        message(FATAL_ERROR "OpenTelemetry build failed:\nOutput: ${OTEL_BUILD_OUTPUT}\nError: ${OTEL_BUILD_ERROR}")
    endif()

    set(OTEL_INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/opentelemetry")
    set(OTEL_INCLUDE_DIR "${OTEL_INSTALL_DIR}/include")
    set(OTEL_LIB_DIR "${OTEL_INSTALL_DIR}/lib")
    add_compile_definitions(GIZMOSQL_WITH_OPENTELEMETRY)
else()
    message(STATUS "Building without OpenTelemetry support (use -DWITH_OPENTELEMETRY=ON to enable)")
endif()

# --------------------- JWT-CPP ---------------------

configure_file(third_party/JWTCPP_CMakeLists.txt.in jwt-cpp/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" . -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/jwt-cpp"
        COMMAND_ERROR_IS_FATAL ANY
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/jwt-cpp"
        COMMAND_ERROR_IS_FATAL ANY
)

set(JWT_CPP_INCLUDE_DIR "${CMAKE_BINARY_DIR}/third_party/jwt-cpp/include")

# --------------------- JSON ---------------------

include(FetchContent)

FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz)
FetchContent_MakeAvailable(json)

# --------------------------------------------------

find_package(Threads REQUIRED)

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost COMPONENTS program_options REQUIRED)

# --------------------- gRPC Health Proto Generation ---------------------
# Use Arrow's bundled protobuf and gRPC for proto generation
set(ARROW_BUILD_DIR "${CMAKE_BINARY_DIR}/third_party/src/arrow_project-build")
set(PROTOBUF_INSTALL_DIR "${ARROW_BUILD_DIR}/protobuf_ep-install")
set(GRPC_INSTALL_DIR "${ARROW_BUILD_DIR}/grpc_ep-install")

set(PROTOC_EXECUTABLE "${PROTOBUF_INSTALL_DIR}/bin/protoc")
set(GRPC_CPP_PLUGIN "${GRPC_INSTALL_DIR}/bin/grpc_cpp_plugin")
set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_INSTALL_DIR}/include")
set(GRPC_INCLUDE_DIR "${GRPC_INSTALL_DIR}/include")
set(GRPC_REFLECTION_LIBRARY "${GRPC_INSTALL_DIR}/lib/libgrpc++_reflection.a")
set(ABSL_INCLUDE_DIR "${ARROW_BUILD_DIR}/absl_ep-install/include")

# Define the proto source and output paths
set(HEALTH_PROTO_DIR "${CMAKE_SOURCE_DIR}/src/protos")
set(HEALTH_PROTO_FILE "${HEALTH_PROTO_DIR}/grpc/health/v1/health.proto")
set(HEALTH_PROTO_GEN_DIR "${CMAKE_BINARY_DIR}/generated")

# Create output directory
file(MAKE_DIRECTORY ${HEALTH_PROTO_GEN_DIR})

# Generated file paths
set(HEALTH_PROTO_SRCS "${HEALTH_PROTO_GEN_DIR}/grpc/health/v1/health.pb.cc")
set(HEALTH_PROTO_HDRS "${HEALTH_PROTO_GEN_DIR}/grpc/health/v1/health.pb.h")
set(HEALTH_GRPC_SRCS "${HEALTH_PROTO_GEN_DIR}/grpc/health/v1/health.grpc.pb.cc")
set(HEALTH_GRPC_HDRS "${HEALTH_PROTO_GEN_DIR}/grpc/health/v1/health.grpc.pb.h")

# Custom command to generate protobuf and gRPC files
add_custom_command(
    OUTPUT ${HEALTH_PROTO_SRCS} ${HEALTH_PROTO_HDRS} ${HEALTH_GRPC_SRCS} ${HEALTH_GRPC_HDRS}
    COMMAND ${CMAKE_COMMAND} -E make_directory "${HEALTH_PROTO_GEN_DIR}/grpc/health/v1"
    COMMAND ${PROTOC_EXECUTABLE}
        --proto_path=${HEALTH_PROTO_DIR}
        --cpp_out=${HEALTH_PROTO_GEN_DIR}
        --grpc_out=${HEALTH_PROTO_GEN_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        ${HEALTH_PROTO_FILE}
    DEPENDS ${HEALTH_PROTO_FILE} arrow
    COMMENT "Generating gRPC health proto files"
)

# Custom target to ensure proto generation happens
add_custom_target(health_proto_gen DEPENDS
    ${HEALTH_PROTO_SRCS}
    ${HEALTH_PROTO_HDRS}
    ${HEALTH_GRPC_SRCS}
    ${HEALTH_GRPC_HDRS}
)

# --------------------------------------------------

set(GIZMOSQL_SQLITE_SERVER_SRCS
        src/sqlite/sqlite_sql_info.cc
        src/sqlite/sqlite_type_info.cc
        src/sqlite/sqlite_statement.cc
        src/sqlite/sqlite_statement_batch_reader.cc
        src/sqlite/sqlite_server.cc
        src/sqlite/sqlite_tables_schema_batch_reader.cc)


set(GIZMOSQL_DUCKDB_SERVER_SRCS
        src/duckdb/duckdb_sql_info.cpp
        src/duckdb/duckdb_type_info.cpp
        src/duckdb/duckdb_statement.cpp
        src/duckdb/duckdb_statement_batch_reader.cpp
        src/duckdb/duckdb_server.cpp
        src/duckdb/duckdb_tables_schema_batch_reader.cpp)

# Add a library target
add_library(gizmosqlserver STATIC
        src/common/gizmosql_logging.cpp
        src/common/gizmosql_library.cpp
        src/common/gizmosql_security.cpp
        src/common/gizmosql_telemetry.cpp
        src/common/access_log_middleware.cpp
        src/common/telemetry_middleware.cpp
        src/common/health_service.cpp
        ${HEALTH_PROTO_SRCS}
        ${HEALTH_GRPC_SRCS}
        ${GIZMOSQL_SQLITE_SERVER_SRCS}
        ${GIZMOSQL_DUCKDB_SERVER_SRCS}
)

# Ensure proto generation happens before compiling gizmosqlserver
add_dependencies(gizmosqlserver health_proto_gen)

set_target_properties(gizmosqlserver PROPERTIES
        PUBLIC_HEADER src/common/include/gizmosql_library.h
)

target_include_directories(gizmosqlserver
        PUBLIC
        src/common/include
        PRIVATE
        src/sqlite
        src/duckdb
        src/common/include/detail
        ${SQLITE_INCLUDE_DIR}
        ${DUCKDB_INCLUDE_DIR}
        ${JWT_CPP_INCLUDE_DIR}
        ${HEALTH_PROTO_GEN_DIR}
        ${PROTOBUF_INCLUDE_DIR}
        ${GRPC_INCLUDE_DIR}
        ${ABSL_INCLUDE_DIR}
)

# Add OpenTelemetry include directory if enabled
if(WITH_OPENTELEMETRY)
    target_include_directories(gizmosqlserver PRIVATE ${OTEL_INCLUDE_DIR})
endif()

target_link_libraries(gizmosqlserver
        PRIVATE
        Threads::Threads
        ArrowFlightSql::arrow_flight_sql_static
        sqlite
        duckdb
        ${Boost_LIBRARIES}
        nlohmann_json::nlohmann_json
        ${GRPC_REFLECTION_LIBRARY}
        "-lresolv"
)

# Link OpenTelemetry libraries if enabled
if(WITH_OPENTELEMETRY)
    target_link_libraries(gizmosqlserver
            PRIVATE
            # OpenTelemetry libraries (HTTP exporter only, no gRPC)
            "${OTEL_LIB_DIR}/libopentelemetry_trace.a"
            "${OTEL_LIB_DIR}/libopentelemetry_metrics.a"
            "${OTEL_LIB_DIR}/libopentelemetry_common.a"
            "${OTEL_LIB_DIR}/libopentelemetry_resources.a"
            "${OTEL_LIB_DIR}/libopentelemetry_otlp_recordable.a"
            "${OTEL_LIB_DIR}/libopentelemetry_http_client_curl.a"
            "${OTEL_LIB_DIR}/libopentelemetry_exporter_otlp_http.a"
            "${OTEL_LIB_DIR}/libopentelemetry_exporter_otlp_http_client.a"
            "${OTEL_LIB_DIR}/libopentelemetry_exporter_otlp_http_metric.a"
            "-lcurl"
    )
endif()

if (APPLE)
    # macOS-specific libraries and options
    target_link_libraries(gizmosqlserver PRIVATE "-framework CoreFoundation")
elseif (UNIX AND NOT APPLE)
    target_link_libraries(gizmosqlserver PRIVATE "-lssl -lcrypto")
endif ()

# Specify the installation directory for the library
install(TARGETS gizmosqlserver
        LIBRARY DESTINATION lib
        PUBLIC_HEADER DESTINATION include
)

# ------------ Server Executable section ------------
add_executable(gizmosql_server
        src/gizmosql_server.cpp
)

target_link_libraries(gizmosql_server PRIVATE
        gizmosqlserver
        ${Boost_LIBRARIES}
        Arrow::arrow_static
        ${GRPC_REFLECTION_LIBRARY}
)

target_compile_options(gizmosql_server PRIVATE "-static")

install(TARGETS gizmosql_server
        DESTINATION bin
)

# ------------ Client Executable section ------------

# --------------------- GFlags ---------------------
configure_file(third_party/gflags_CMakeLists.txt.in
        gflags/CMakeLists.txt
)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" . -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/gflags"
        COMMAND_ERROR_IS_FATAL ANY
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/gflags"
        COMMAND_ERROR_IS_FATAL ANY
)

set(GFLAGS_INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/gflags")
set(GFLAGS_LIBRARY_PATH "${GFLAGS_INSTALL_DIR}/lib/libgflags.a")
set(GFLAGS_INCLUDE_DIR "${GFLAGS_INSTALL_DIR}/include")

add_library(gflags STATIC IMPORTED)
set_property(TARGET gflags PROPERTY IMPORTED_LOCATION ${GFLAGS_LIBRARY_PATH})
set_property(TARGET gflags APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${GFLAGS_INCLUDE_DIR})

# ------------------------------------------

add_executable(gizmosql_client
        src/gizmosql_client.cpp
)

target_include_directories(gizmosql_client
        PRIVATE
        ${CMAKE_SOURCE_DIR}/src/common/include
)

target_link_libraries(gizmosql_client PRIVATE
        Threads::Threads
        Arrow::arrow_static
        ArrowFlightSql::arrow_flight_sql_static
        gflags
        ${Boost_LIBRARIES}
        "-lresolv"
)

if (APPLE)
    # macOS-specific libraries and options
    target_link_libraries(gizmosql_client PRIVATE "-framework CoreFoundation")
elseif (UNIX AND NOT APPLE)
    target_link_libraries(gizmosql_client PRIVATE "-lssl -lcrypto")
endif ()

target_compile_options(gizmosql_client PRIVATE "-static")

install(TARGETS gizmosql_client
        DESTINATION bin
)

# =========================================================
# GizmoSQL Tests
# =========================================================
enable_testing()
add_subdirectory(tests)
