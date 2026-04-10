cmake_minimum_required(VERSION 3.25)
project(gizmosql)

# Use Boost's own CMake config instead of the removed FindBoost module
cmake_policy(SET CMP0167 NEW)

set(CMAKE_CXX_STANDARD 20)

# --------------------- Platform Settings ---------------------
if(WIN32)
    set(LIB_PREFIX "")
    set(LIB_SUFFIX ".lib")
else()
    set(LIB_PREFIX "lib")
    set(LIB_SUFFIX ".a")
endif()

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
    add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN)
    add_compile_options(/utf-8)
endif()

set(EXE_SUFFIX "")
if(WIN32)
    set(EXE_SUFFIX ".exe")
endif()

# --------------------- Enterprise Edition Option ---------------------
option(GIZMOSQL_ENTERPRISE "Build with enterprise features (instrumentation, kill session)" ON)

# --------------------- iOS: force Core edition, no OTel ---------------------
if(CMAKE_SYSTEM_NAME STREQUAL "iOS")
    set(GIZMOSQL_ENTERPRISE OFF CACHE BOOL "Enterprise disabled on iOS" FORCE)
    set(WITH_OPENTELEMETRY OFF CACHE BOOL "OTel disabled on iOS" FORCE)
    message(STATUS "Building GizmoSQL iOS Edition (Core only)")
endif()

if(GIZMOSQL_ENTERPRISE)
    message(STATUS "Building GizmoSQL Enterprise Edition")
else()
    message(STATUS "Building GizmoSQL Core Edition")
endif()

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

# Parse version components (strip leading 'v' if present)
string(REGEX REPLACE "^v" "" VERSION_STRIPPED "${PROJECT_VERSION}")
string(REPLACE "." ";" VERSION_LIST "${VERSION_STRIPPED}")
list(LENGTH VERSION_LIST VERSION_LIST_LEN)
if(VERSION_LIST_LEN GREATER_EQUAL 1)
    list(GET VERSION_LIST 0 VERSION_MAJOR)
else()
    set(VERSION_MAJOR 0)
endif()
if(VERSION_LIST_LEN GREATER_EQUAL 2)
    list(GET VERSION_LIST 1 VERSION_MINOR)
else()
    set(VERSION_MINOR 0)
endif()
if(VERSION_LIST_LEN GREATER_EQUAL 3)
    list(GET VERSION_LIST 2 VERSION_PATCH)
else()
    set(VERSION_PATCH 0)
endif()

# Configure a header file with the version
configure_file(
        ${CMAKE_SOURCE_DIR}/src/common/include/version.h.in
        ${CMAKE_SOURCE_DIR}/src/common/include/version.h
)

# Configure Windows version resource files
if(WIN32)
    set(RC_ICON_PATH "${CMAKE_SOURCE_DIR}/installer/gizmosql.ico")
    set(RC_FILE_DESCRIPTION "GizmoSQL Server")
    set(RC_INTERNAL_NAME "gizmosql_server")
    set(RC_ORIGINAL_FILENAME "gizmosql_server.exe")
    configure_file(
            ${CMAKE_SOURCE_DIR}/src/common/version.rc.in
            ${CMAKE_BINARY_DIR}/gizmosql_server_version.rc
    )
    set(RC_ICON_PATH "${CMAKE_SOURCE_DIR}/installer/gizmosql_client.ico")
    set(RC_FILE_DESCRIPTION "GizmoSQL Client")
    set(RC_INTERNAL_NAME "gizmosql_client")
    set(RC_ORIGINAL_FILENAME "gizmosql_client.exe")
    configure_file(
            ${CMAKE_SOURCE_DIR}/src/common/version.rc.in
            ${CMAKE_BINARY_DIR}/gizmosql_client_version.rc
    )
endif()

# --------------------- Arrow ---------------------
# Arrow's SIMD auto-detection defines __SSE4_2__ etc., which causes Abseil
# (bundled by Arrow) to include x86intrin.h — a GCC/Clang header that MSVC
# doesn't have. Disable SIMD on Windows to avoid this incompatibility.
set(ARROW_SIMD_LEVEL "DEFAULT")
set(ARROW_RUNTIME_SIMD_LEVEL "MAX")
if(WIN32)
    set(ARROW_SIMD_LEVEL "NONE")
    set(ARROW_RUNTIME_SIMD_LEVEL "NONE")
endif()

# On Windows, Arrow's bundled gRPC needs zlib at compile time. Rather than
# building zlib from source (which has timing issues with gRPC), tell Arrow
# to use the system/vcpkg-installed zlib via -DZLIB_SOURCE=SYSTEM and pass
# CMAKE_PREFIX_PATH so Arrow (and gRPC via FetchContent) can find it.
# This mirrors the approach used by the gizmosql-odbc-driver.
set(VCPKG_PREFIX_PATH "")
if(WIN32 AND VCPKG_TARGET_TRIPLET)
    if(VCPKG_INSTALLED_DIR)
        set(VCPKG_PREFIX_PATH "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}")
    elseif(CMAKE_TOOLCHAIN_FILE)
        # Derive vcpkg root from toolchain: .../scripts/buildsystems/vcpkg.cmake
        get_filename_component(_VCPKG_ROOT "${CMAKE_TOOLCHAIN_FILE}" DIRECTORY)
        get_filename_component(_VCPKG_ROOT "${_VCPKG_ROOT}" DIRECTORY)
        get_filename_component(_VCPKG_ROOT "${_VCPKG_ROOT}" DIRECTORY)
        set(VCPKG_PREFIX_PATH "${_VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}")
    endif()
    if(VCPKG_PREFIX_PATH)
        # Use AUTO instead of BUNDLED so Arrow finds vcpkg zlib via the
        # toolchain, then sets gRPC_ZLIB_PROVIDER=package for gRPC.
        # With BUNDLED, Arrow overrides ZLIB_SOURCE and sets gRPC to "module".
        set(ARROW_DEPENDENCY_SOURCE_ARG "-DARROW_DEPENDENCY_SOURCE=AUTO")
        set(ARROW_ZLIB_SOURCE_ARG "-DZLIB_SOURCE=SYSTEM")
        set(ARROW_PREFIX_PATH_ARG "-DCMAKE_PREFIX_PATH=${VCPKG_PREFIX_PATH}")
        set(ARROW_ZLIB_ROOT_ARG "-DZLIB_ROOT=${VCPKG_PREFIX_PATH}")
        # Pass the vcpkg toolchain file to Arrow so nested builds (gRPC via
        # FetchContent) also find vcpkg packages. Use VCPKG_MANIFEST_MODE=OFF
        # to prevent Arrow from auto-installing packages via vcpkg manifest.
        # Force gRPC to use find_package(ZLIB) instead of its local module.
        # Arrow's ThirdpartyToolchain unconditionally sets gRPC_ZLIB_PROVIDER=module
        # when building gRPC from source. Since gRPC is built via FetchContent
        # (add_subdirectory), this cache variable propagates to gRPC's cmake.
        set(ARROW_GRPC_ZLIB_ARG "-DgRPC_ZLIB_PROVIDER=package")
        # Force Arrow to download its own Boost headers. With AUTO, Arrow finds
        # vcpkg Boost (which only has our app's components) and then fails when
        # Arrow-internal headers like boost/multiprecision/cpp_int.hpp are missing.
        set(ARROW_BOOST_SOURCE_ARG "-DBoost_SOURCE=BUNDLED")
        if(CMAKE_TOOLCHAIN_FILE)
            set(ARROW_TOOLCHAIN_ARG "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
            set(ARROW_VCPKG_TRIPLET_ARG "-DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}")
            set(ARROW_VCPKG_MANIFEST_ARG "-DVCPKG_MANIFEST_MODE=OFF")
        endif()
        message(STATUS "Windows: vcpkg prefix path for Arrow zlib: ${VCPKG_PREFIX_PATH}")
    endif()
endif()

# Forward iOS cross-compilation settings to Arrow ExternalProject
if(CMAKE_SYSTEM_NAME STREQUAL "iOS")
    set(ARROW_SIMD_LEVEL "NONE")
    set(ARROW_RUNTIME_SIMD_LEVEL "NONE")
    # Arrow/gRPC/DuckDB ExternalProjects are built for the host (macOS arm64),
    # not cross-compiled for iOS. The resulting static libraries are binary-
    # compatible (same arm64 instructions) — we retag the platform metadata
    # from macOS to iOS using vtool after the build. This avoids the deep
    # cross-compilation issues with gRPC's proto tool generation.
    set(ARROW_IOS_ARGS "")
endif()

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
# Arrow 23 doesn't build grpc++_reflection by default, but we need it for gRPC server reflection
execute_process(COMMAND "${CMAKE_COMMAND}" --build . --target grpc++_reflection
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/third_party/src/arrow_project-build"
        COMMAND_ERROR_IS_FATAL ANY
)
set(ARROW_INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/arrow")

find_package(Arrow REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)
find_package(ArrowFlight REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)
find_package(ArrowFlightSql REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)

# Arrow 23 uses FetchContent for bundled deps (absl, gRPC, c-ares, protobuf, re2).
# On some platforms / rebuild scenarios, arrow_bundled_dependencies may not contain
# all symbols.  Collect the individual static libraries as a fallback.
set(ARROW_BUILD_DIR "${CMAKE_BINARY_DIR}/third_party/src/arrow_project-build")
file(GLOB_RECURSE ARROW_FETCHCONTENT_LIBS
        # FetchContent layout (Arrow 23+)
        "${ARROW_BUILD_DIR}/_deps/absl-build/absl/*${CMAKE_STATIC_LIBRARY_SUFFIX}"
        "${ARROW_BUILD_DIR}/_deps/cares-build/*${CMAKE_STATIC_LIBRARY_SUFFIX}"
        "${ARROW_BUILD_DIR}/_deps/re2-build/*${CMAKE_STATIC_LIBRARY_SUFFIX}"
        # ExternalProject layout (older Arrow)
        "${ARROW_BUILD_DIR}/absl_ep-install/lib/*${CMAKE_STATIC_LIBRARY_SUFFIX}")
# Filter out CMakeFiles/ build artifacts that match the glob
list(FILTER ARROW_FETCHCONTENT_LIBS EXCLUDE REGEX "CMakeFiles")
add_library(arrow_absl INTERFACE)
if(ARROW_FETCHCONTENT_LIBS)
    if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
        target_link_libraries(arrow_absl INTERFACE
                "$<LINK_GROUP:RESCAN,${ARROW_FETCHCONTENT_LIBS}>")
    else()
        target_link_libraries(arrow_absl INTERFACE ${ARROW_FETCHCONTENT_LIBS})
    endif()
endif()

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
set(SQLITE_LIBRARY_PATH "${CMAKE_BINARY_DIR}/third_party/sqlite/sqlite_project-install/lib/${LIB_PREFIX}sqlite3${LIB_SUFFIX}")

add_library(sqlite STATIC IMPORTED)
set_property(TARGET sqlite PROPERTY IMPORTED_LOCATION ${SQLITE_LIBRARY_PATH})
set_property(TARGET sqlite PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${SQLITE_INCLUDE_DIR})

# --------------------- DuckDB ---------------------
# Extensions to statically link into DuckDB (core_functions and parquet are always included)
set(DUCKDB_EXTENSION_CONFIGS "${CMAKE_SOURCE_DIR}/third_party/duckdb_extensions.cmake")

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

set(DUCKDB_LIBRARY_PATH "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_static${LIB_SUFFIX}")
set(DUCKDB_INCLUDE_DIR "${DUCKDB_INSTALL_DIR}/include")

add_library(duckdb STATIC IMPORTED)
set_property(TARGET duckdb PROPERTY IMPORTED_LOCATION ${DUCKDB_LIBRARY_PATH})
set_property(TARGET duckdb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${DUCKDB_INCLUDE_DIR})

target_link_libraries(duckdb
        INTERFACE
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_fastpforlib${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_fmt${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_fsst${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_hyperloglog${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_mbedtls${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_miniz${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_pg_query${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_re2${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_utf8proc${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_yyjson${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_zstd${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_generated_extension_loader${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}parquet_extension${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}core_functions_extension${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}icu_extension${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}tpch_extension${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}ducklake_extension${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}httpfs_extension${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_skiplistlib${LIB_SUFFIX}"
)

# Only link postgres_scanner_extension on iOS. The static target is
# produced by the patch in ios/scripts/build-ios-libs.sh; other
# platforms use the loadable .duckdb_extension via INSTALL/LOAD.
if (CMAKE_SYSTEM_NAME STREQUAL "iOS")
    target_link_libraries(duckdb
            INTERFACE
            "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}postgres_scanner_extension${LIB_SUFFIX}"
    )
endif()

# Only link jemalloc_extension on Linux
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(duckdb
            INTERFACE
            "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}jemalloc_extension${LIB_SUFFIX}"
    )
endif ()

# --------------------- OpenTelemetry (Optional) ---------------------
if(WITH_OPENTELEMETRY)
    message(STATUS "Building with OpenTelemetry support")
    # Build OpenTelemetry C++ SDK with OTLP exporters
    # This must come after Arrow (uses Arrow's bundled protobuf)

    # Arrow 23 builds protobuf via FetchContent — headers and libs live in the
    # build tree, NOT the install prefix.  Point OTel at the FetchContent paths.
    set(ARROW_BUILD_DIR_OTEL "${CMAKE_BINARY_DIR}/third_party/src/arrow_project-build")
    set(ARROW_PROTOBUF_INCLUDE_DIR "${ARROW_BUILD_DIR_OTEL}/_deps/protobuf-src/src")
    set(ARROW_PROTOBUF_LIB "${ARROW_BUILD_DIR_OTEL}/_deps/protobuf-build/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(ARROW_PROTOBUF_LITE_LIB "${ARROW_BUILD_DIR_OTEL}/_deps/protobuf-build/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf-lite${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(ARROW_PROTOC_EXECUTABLE "${ARROW_BUILD_DIR_OTEL}/_deps/protobuf-build/protoc${CMAKE_EXECUTABLE_SUFFIX}")
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

# --------------------- cpp-httplib (header-only HTTP client) ---------------------

configure_file(third_party/httplib_CMakeLists.txt.in cpp-httplib/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" . -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/cpp-httplib"
        COMMAND_ERROR_IS_FATAL ANY
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/cpp-httplib"
        COMMAND_ERROR_IS_FATAL ANY
)

set(HTTPLIB_INCLUDE_DIR "${CMAKE_BINARY_DIR}/third_party/cpp-httplib/include")

# --------------------- JSON ---------------------

include(FetchContent)

FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz)
FetchContent_MakeAvailable(json)

# --------------------------------------------------

find_package(Threads REQUIRED)

set(Boost_USE_STATIC_LIBS ON)
if(NOT CMAKE_SYSTEM_NAME STREQUAL "iOS")
    find_package(Boost COMPONENTS program_options REQUIRED)
else()
    # iOS only needs Boost headers (uuid, algorithm/string) — no compiled libraries
    find_package(Boost REQUIRED)
endif()

# --------------------- gRPC Health Proto Generation ---------------------
# Use Arrow's bundled protobuf and gRPC for proto generation
# Arrow 23 uses FetchContent instead of ExternalProject, so paths changed
set(ARROW_BUILD_DIR "${CMAKE_BINARY_DIR}/third_party/src/arrow_project-build")

# Arrow 23 FetchContent paths
set(PROTOC_EXECUTABLE "${ARROW_BUILD_DIR}/_deps/protobuf-build/protoc${EXE_SUFFIX}")
set(GRPC_CPP_PLUGIN "${ARROW_BUILD_DIR}/_deps/grpc-build/grpc_cpp_plugin${EXE_SUFFIX}")
set(PROTOBUF_INCLUDE_DIR "${ARROW_BUILD_DIR}/_deps/protobuf-src/src")
set(GRPC_INCLUDE_DIR "${ARROW_BUILD_DIR}/_deps/grpc-src/include")
set(GRPC_REFLECTION_LIBRARY "${ARROW_BUILD_DIR}/_deps/grpc-build/${LIB_PREFIX}grpc++_reflection${LIB_SUFFIX}")
set(ABSL_INCLUDE_DIR "${ARROW_BUILD_DIR}/_deps/absl-src")

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

# Enterprise vs Core: instrumentation source files
if(GIZMOSQL_ENTERPRISE)
    # Enterprise edition: instrumentation from enterprise directory
    set(GIZMOSQL_ENTERPRISE_SRCS
            src/enterprise/enterprise_features.cpp
            src/enterprise/license_mgr/license_manager.cpp
            src/enterprise/instrumentation/instrumentation_manager.cpp
            src/enterprise/instrumentation/instrumentation_records.cpp
            src/enterprise/kill_session/kill_session_handler.cpp
            src/enterprise/catalog_permissions/catalog_permissions_handler.cpp
            src/enterprise/jwks/jwks_manager.cpp
            src/enterprise/oauth/oauth_http_server.cpp)
endif()

# Add a library target
add_library(gizmosqlserver STATIC
        src/common/gizmosql_logging.cpp
        src/common/gizmosql_library.cpp
        src/common/session_context.cpp
        src/common/gizmosql_security.cpp
        src/common/gizmosql_telemetry.cpp
        src/common/access_log_middleware.cpp
        src/common/telemetry_middleware.cpp
        src/common/health_service.cpp
        ${HEALTH_PROTO_SRCS}
        ${HEALTH_GRPC_SRCS}
        ${GIZMOSQL_SQLITE_SERVER_SRCS}
        ${GIZMOSQL_DUCKDB_SERVER_SRCS}
        ${GIZMOSQL_ENTERPRISE_SRCS}
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
        ${Boost_INCLUDE_DIRS}
        ${SQLITE_INCLUDE_DIR}
        ${DUCKDB_INCLUDE_DIR}
        ${JWT_CPP_INCLUDE_DIR}
        ${HEALTH_PROTO_GEN_DIR}
        ${PROTOBUF_INCLUDE_DIR}
        ${GRPC_INCLUDE_DIR}
        ${ABSL_INCLUDE_DIR}
)

# Add enterprise-specific include directories and compile definition
if(GIZMOSQL_ENTERPRISE)
    target_include_directories(gizmosqlserver PRIVATE
            src
            src/enterprise
            src/enterprise/license_mgr
            src/enterprise/instrumentation
            src/enterprise/kill_session
            src/enterprise/catalog_permissions
            src/enterprise/jwks
            src/enterprise/oauth
            ${HTTPLIB_INCLUDE_DIR}
    )
    target_compile_definitions(gizmosqlserver PUBLIC GIZMOSQL_ENTERPRISE)
endif()

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
        nlohmann_json::nlohmann_json
        ${GRPC_REFLECTION_LIBRARY}
)

# Link OpenTelemetry libraries if enabled
if(WITH_OPENTELEMETRY)
    find_package(CURL REQUIRED)

    # OpenTelemetry installs additional static deps (notably Abseil) into the same prefix.
    # Collect all static archives so the final static link can resolve transitive symbols.
    file(GLOB OTEL_STATIC_LIBS "${OTEL_LIB_DIR}/*.a")
    if(NOT OTEL_STATIC_LIBS)
        message(FATAL_ERROR "No OpenTelemetry static libraries found in ${OTEL_LIB_DIR}")
    endif()
    list(SORT OTEL_STATIC_LIBS)

    if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
        target_link_libraries(gizmosqlserver PRIVATE
                "$<LINK_GROUP:RESCAN,${OTEL_STATIC_LIBS};${ARROW_PROTOBUF_LIB}>"
                CURL::libcurl
        )
    else()
        target_link_libraries(gizmosqlserver PRIVATE
                ${OTEL_STATIC_LIBS}
                ${ARROW_PROTOBUF_LIB}
                CURL::libcurl
        )
    endif()
endif()

if(WIN32)
    target_link_libraries(gizmosqlserver PRIVATE ws2_32 iphlpapi bcrypt userenv crypt32 rstrtmgr ntdll)
    target_link_options(gizmosqlserver PRIVATE /NODEFAULTLIB:msvcrt.lib /NODEFAULTLIB:msvcprt.lib)
elseif(APPLE)
    target_link_libraries(gizmosqlserver PRIVATE "-framework CoreFoundation" "-framework Security" "-lresolv")
elseif(UNIX)
    target_link_libraries(gizmosqlserver PRIVATE "-lssl" "-lcrypto" "-lresolv")
endif()

# Specify the installation directory for the library
install(TARGETS gizmosqlserver
        LIBRARY DESTINATION lib
        PUBLIC_HEADER DESTINATION include
)

# ------------ Executables and tests (skip for iOS) ------------
if(NOT CMAKE_SYSTEM_NAME STREQUAL "iOS")

# ------------ Server Executable section ------------
add_executable(gizmosql_server
        src/gizmosql_server.cpp
        $<$<BOOL:${WIN32}>:${CMAKE_BINARY_DIR}/gizmosql_server_version.rc>
)

target_link_libraries(gizmosql_server PRIVATE
        gizmosqlserver
        ${Boost_LIBRARIES}
        Arrow::arrow_static
        arrow_absl
        ${GRPC_REFLECTION_LIBRARY}
)

if(NOT MSVC)
    target_compile_options(gizmosql_server PRIVATE "-static")
endif()

install(TARGETS gizmosql_server
        DESTINATION bin
)

# ------------ Client Executable section ------------

# --------------------- replxx ---------------------
configure_file(third_party/replxx_CMakeLists.txt.in
        replxx/CMakeLists.txt
)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" . -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/replxx"
        COMMAND_ERROR_IS_FATAL ANY
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/replxx"
        COMMAND_ERROR_IS_FATAL ANY
)

set(REPLXX_INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/replxx")
set(REPLXX_INCLUDE_DIR "${REPLXX_INSTALL_DIR}/include")
# replxx appends -d suffix in Debug builds; on Windows the target is "replxx-static"
if(EXISTS "${REPLXX_INSTALL_DIR}/lib/${LIB_PREFIX}replxx-d${LIB_SUFFIX}")
    set(REPLXX_LIBRARY_PATH "${REPLXX_INSTALL_DIR}/lib/${LIB_PREFIX}replxx-d${LIB_SUFFIX}")
elseif(EXISTS "${REPLXX_INSTALL_DIR}/lib/${LIB_PREFIX}replxx${LIB_SUFFIX}")
    set(REPLXX_LIBRARY_PATH "${REPLXX_INSTALL_DIR}/lib/${LIB_PREFIX}replxx${LIB_SUFFIX}")
elseif(EXISTS "${REPLXX_INSTALL_DIR}/lib/replxx-static.lib")
    set(REPLXX_LIBRARY_PATH "${REPLXX_INSTALL_DIR}/lib/replxx-static.lib")
else()
    message(FATAL_ERROR "replxx library not found in ${REPLXX_INSTALL_DIR}/lib/")
endif()

add_library(replxx STATIC IMPORTED)
set_property(TARGET replxx PROPERTY IMPORTED_LOCATION ${REPLXX_LIBRARY_PATH})
set_property(TARGET replxx APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${REPLXX_INCLUDE_DIR})
# On Windows, replxx.hxx defaults to __declspec(dllimport) unless REPLXX_BUILDING_DLL
# is defined. Define it so REPLXX_IMPEXP becomes __declspec(dllexport) instead, which
# is harmless for static linking and avoids __imp_ symbol mismatch.
if(WIN32)
    set_property(TARGET replxx APPEND PROPERTY INTERFACE_COMPILE_DEFINITIONS REPLXX_BUILDING_DLL)
endif()

# ------------------------------------------

set(GIZMOSQL_CLIENT_SRCS
        src/client/main.cpp
        src/client/client_config.cpp
        src/client/flight_connection.cpp
        src/client/oauth_flow.cpp
        src/client/shell_loop.cpp
        src/client/shell_completer.cpp
        src/client/sql_processor.cpp
        src/client/command_processor.cpp
        src/client/output_renderer.cpp
        src/client/password_prompt.cpp
        src/client/syntax_highlighter.cpp
        src/client/pager.cpp
)

if(WIN32)
    list(APPEND GIZMOSQL_CLIENT_SRCS ${CMAKE_BINARY_DIR}/gizmosql_client_version.rc)
endif()

add_executable(gizmosql_client ${GIZMOSQL_CLIENT_SRCS})

target_include_directories(gizmosql_client
        PRIVATE
        ${CMAKE_SOURCE_DIR}/src/common/include
        ${CMAKE_SOURCE_DIR}/src/client
        ${HTTPLIB_INCLUDE_DIR}
)

target_link_libraries(gizmosql_client PRIVATE
        Threads::Threads
        Arrow::arrow_static
        ArrowFlightSql::arrow_flight_sql_static
        arrow_absl
        replxx
        ${Boost_LIBRARIES}
        nlohmann_json::nlohmann_json
)

if(WIN32)
    target_link_libraries(gizmosql_client PRIVATE ws2_32 iphlpapi bcrypt userenv crypt32 shell32 rstrtmgr ntdll)
    target_link_options(gizmosql_client PRIVATE /NODEFAULTLIB:msvcrt.lib /NODEFAULTLIB:msvcprt.lib)
elseif(APPLE)
    target_link_libraries(gizmosql_client PRIVATE "-framework CoreFoundation" "-framework Security" "-lresolv")
elseif(UNIX)
    target_link_libraries(gizmosql_client PRIVATE "-lssl" "-lcrypto" "-lresolv")
endif()

if(NOT MSVC)
    target_compile_options(gizmosql_client PRIVATE "-static")
endif()

install(TARGETS gizmosql_client
        DESTINATION bin
)

# =========================================================
# GizmoSQL Tests
# =========================================================
enable_testing()
add_subdirectory(tests)

endif() # NOT iOS
