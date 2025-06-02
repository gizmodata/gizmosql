cmake_minimum_required(VERSION 3.25)
project(gizmosql)

set(CMAKE_CXX_STANDARD 17)

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
        ${CMAKE_SOURCE_DIR}/src/library/include/version.h.in
        ${CMAKE_SOURCE_DIR}/src/library/include/version.h
)

# --------------------- Arrow ---------------------
configure_file(third_party/Arrow_CMakeLists.txt.in arrow/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/arrow"
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/arrow"
)

set(ARROW_INSTALL_DIR "${CMAKE_BINARY_DIR}/third_party/arrow")

find_package(Arrow REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)
find_package(ArrowFlight REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)
find_package(ArrowFlightSql REQUIRED PATHS ${ARROW_INSTALL_DIR} NO_DEFAULT_PATH)

# --------------------- SQLite ---------------------

configure_file(third_party/SQLite_CMakeLists.txt.in sqlite/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/sqlite"
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/sqlite"
)

set(SQLITE_INCLUDE_DIR "${CMAKE_BINARY_DIR}/third_party/sqlite/src/sqlite_project")
set(SQLITE_LIBRARY_PATH "${CMAKE_BINARY_DIR}/third_party/sqlite/src/sqlite_project-build/sqlite3-sqlite3.o")

add_library(sqlite STATIC IMPORTED)
set_property(TARGET sqlite PROPERTY IMPORTED_LOCATION ${SQLITE_LIBRARY_PATH})

# --------------------- DuckDB ---------------------

configure_file(third_party/DuckDB_CMakeLists.txt.in duckdb/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/duckdb"
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/duckdb"
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
endif()

# --------------------- JWT-CPP ---------------------

configure_file(third_party/JWTCPP_CMakeLists.txt.in jwt-cpp/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/jwt-cpp"
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/jwt-cpp"
)

set(JWT_CPP_INCLUDE_DIR "${CMAKE_BINARY_DIR}/third_party/jwt-cpp/include")

# --------------------------------------------------

find_package(Threads REQUIRED)

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost COMPONENTS program_options REQUIRED)

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

# Add header file
set(HEADER_FILES src/library/include/gizmosql_library.h)

# Add a library target
add_library(gizmosqlserver STATIC
        src/library/gizmosql_library.cpp
        src/library/gizmosql_security.cpp
        ${GIZMOSQL_SQLITE_SERVER_SRCS}
        ${GIZMOSQL_DUCKDB_SERVER_SRCS}
)

set_target_properties(gizmosqlserver PROPERTIES PUBLIC_HEADER ${HEADER_FILES})

target_include_directories(gizmosqlserver PRIVATE
        src/sqlite
        src/duckdb
        src/library/include
        ${SQLITE_INCLUDE_DIR}
        ${DUCKDB_INCLUDE_DIR}
        ${JWT_CPP_INCLUDE_DIR}
)

target_link_libraries(gizmosqlserver
        PRIVATE
        Threads::Threads
        ArrowFlightSql::arrow_flight_sql_static
        sqlite
        duckdb
        ${Boost_LIBRARIES}
        "-lresolv"
)

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
)

target_compile_options(gizmosql_server PRIVATE "-static")

install(TARGETS gizmosql_server
        DESTINATION bin
)

# ------------ Client Executable section ------------
find_package(gflags REQUIRED)

add_executable(gizmosql_client
        src/gizmosql_client.cpp
)

target_link_libraries(gizmosql_client PRIVATE
        Threads::Threads
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
