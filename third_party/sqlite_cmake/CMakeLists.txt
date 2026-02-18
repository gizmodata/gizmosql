cmake_minimum_required(VERSION 3.25)
project(sqlite3 C)

# Build SQLite amalgamation as a static library (for Windows)
# The source files are downloaded by the parent ExternalProject_Add and
# placed in the source directory. We reference them via the SQLite source dir.

# Find the SQLite source files from the ExternalProject download
file(GLOB SQLITE_SOURCES "${CMAKE_BINARY_DIR}/../sqlite_project-src/sqlite3.c")
file(GLOB SQLITE_HEADERS "${CMAKE_BINARY_DIR}/../sqlite_project-src/sqlite3.h"
                          "${CMAKE_BINARY_DIR}/../sqlite_project-src/sqlite3ext.h")

if(NOT SQLITE_SOURCES)
    # Fallback: sources may be in a different relative location
    file(GLOB SQLITE_SOURCES "${CMAKE_BINARY_DIR}/../../sqlite_project-src/sqlite3.c")
    file(GLOB SQLITE_HEADERS "${CMAKE_BINARY_DIR}/../../sqlite_project-src/sqlite3.h"
                              "${CMAKE_BINARY_DIR}/../../sqlite_project-src/sqlite3ext.h")
endif()

if(NOT SQLITE_SOURCES)
    message(FATAL_ERROR "SQLite source file sqlite3.c not found")
endif()

add_library(sqlite3 STATIC ${SQLITE_SOURCES})
target_compile_definitions(sqlite3 PRIVATE SQLITE_ENABLE_COLUMN_METADATA=1)
target_include_directories(sqlite3 PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)

# Get the directory containing sqlite3.h for install
get_filename_component(SQLITE_HEADER_DIR "${SQLITE_HEADERS}" DIRECTORY)
target_include_directories(sqlite3 PUBLIC $<BUILD_INTERFACE:${SQLITE_HEADER_DIR}>)

install(TARGETS sqlite3 ARCHIVE DESTINATION lib)
install(FILES ${SQLITE_HEADERS} DESTINATION include)
