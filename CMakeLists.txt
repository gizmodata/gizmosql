cmake_minimum_required(VERSION 3.25)
project(gizmosql)

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
endif()

set(EXE_SUFFIX "")
if(WIN32)
    set(EXE_SUFFIX ".exe")
endif()

# --------------------- Enterprise Edition Option ---------------------
option(GIZMOSQL_ENTERPRISE "Build with enterprise features (instrumentation, kill session)" ON)

if(GIZMOSQL_ENTERPRISE)
    message(STATUS "Building GizmoSQL Enterprise Edition")
else()
    message(STATUS "Building GizmoSQL Core Edition")
endif()

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

# Arrow 23's bundled_dependencies archive doesn't include all abseil symbols.
# We need to link the abseil libraries that Arrow built separately.
# On Linux, abseil libraries have circular dependencies, so we use link groups.
set(ABSL_BUILD_DIR "${CMAKE_BINARY_DIR}/third_party/src/arrow_project-build/_deps/absl-build")
file(GLOB_RECURSE ABSL_LIBRARIES "${ABSL_BUILD_DIR}/absl/*${LIB_SUFFIX}")
add_library(arrow_absl INTERFACE)
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Use linker groups to handle circular dependencies in abseil
    target_link_libraries(arrow_absl INTERFACE
        -Wl,--start-group
        ${ABSL_LIBRARIES}
        -Wl,--end-group
    )
else()
    target_link_libraries(arrow_absl INTERFACE ${ABSL_LIBRARIES})
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
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}parquet_extension${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}core_functions_extension${LIB_SUFFIX}"
        "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}duckdb_skiplistlib${LIB_SUFFIX}"
)

# Only link jemalloc_extension on Linux
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(duckdb
            INTERFACE
            "${DUCKDB_INSTALL_DIR}/lib/${LIB_PREFIX}jemalloc_extension${LIB_SUFFIX}"
    )
endif ()

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
find_package(Boost COMPONENTS program_options REQUIRED)

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
        src/common/gizmosql_security.cpp
        src/common/access_log_middleware.cpp
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

target_link_libraries(gizmosqlserver
        PRIVATE
        Threads::Threads
        ArrowFlightSql::arrow_flight_sql_static
        sqlite
        duckdb
        ${Boost_LIBRARIES}
        nlohmann_json::nlohmann_json
        ${GRPC_REFLECTION_LIBRARY}
)

if(WIN32)
    target_link_libraries(gizmosqlserver PRIVATE ws2_32 iphlpapi bcrypt userenv crypt32 rstrtmgr ntdll)
    target_link_options(gizmosqlserver PRIVATE /NODEFAULTLIB:msvcrt.lib /NODEFAULTLIB:msvcprt.lib)
elseif(APPLE)
    target_link_libraries(gizmosqlserver PRIVATE "-framework CoreFoundation" "-lresolv")
elseif(UNIX)
    target_link_libraries(gizmosqlserver PRIVATE "-lssl" "-lcrypto" "-lresolv")
endif()

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
)

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
    target_link_libraries(gizmosql_client PRIVATE "-framework CoreFoundation" "-lresolv")
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
