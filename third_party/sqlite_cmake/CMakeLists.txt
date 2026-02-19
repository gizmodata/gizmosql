cmake_minimum_required(VERSION 3.25)
project(sqlite3 C)

# Build SQLite amalgamation as a static library (for Windows).
# SQLITE_SOURCE_DIR is passed by the parent ExternalProject_Add and
# points to the extracted tarball containing sqlite3.c / sqlite3.h.

if(NOT SQLITE_SOURCE_DIR)
    message(FATAL_ERROR "SQLITE_SOURCE_DIR must be set to the SQLite source directory")
endif()

set(SQLITE_SOURCES "${SQLITE_SOURCE_DIR}/sqlite3.c")
set(SQLITE_HEADERS "${SQLITE_SOURCE_DIR}/sqlite3.h"
                    "${SQLITE_SOURCE_DIR}/sqlite3ext.h")

if(NOT EXISTS "${SQLITE_SOURCES}")
    message(FATAL_ERROR "SQLite source file not found: ${SQLITE_SOURCES}")
endif()

add_library(sqlite3 STATIC ${SQLITE_SOURCES})
target_compile_definitions(sqlite3 PRIVATE SQLITE_ENABLE_COLUMN_METADATA=1)
target_include_directories(sqlite3 PUBLIC
    $<BUILD_INTERFACE:${SQLITE_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)

install(TARGETS sqlite3 ARCHIVE DESTINATION lib)
install(FILES ${SQLITE_HEADERS} DESTINATION include)
