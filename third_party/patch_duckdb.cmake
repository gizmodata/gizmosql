# Applies the GizmoSQL source patches to the DuckDB checkout (PATCH_COMMAND of
# third_party/DuckDB_CMakeLists.txt.in). Idempotent: a patch that is already
# applied is skipped, so re-running the patch step on a patched tree is safe.
#
# duckdb_temp_size_on_disk.patch — temp-directory spill accounting
#   (https://github.com/duckdb/duckdb/issues/20561): the `size_on_disk` counter
#   could be decremented by more than was reserved and wrap to ~2^64, after
#   which every query that spills fails with
#   "failed to offload data block ... (16383.9 PiB/... used)" until restart.
#   Remove this patch once the fix ships in the DuckDB version we pin.
#
# Required: -DDUCKDB_SOURCE_DIR=<path to the DuckDB checkout>

if(NOT DUCKDB_SOURCE_DIR)
  message(FATAL_ERROR "patch_duckdb.cmake: DUCKDB_SOURCE_DIR is required")
endif()

find_program(GIT_EXECUTABLE git REQUIRED)

set(PATCH_DIR "${CMAKE_CURRENT_LIST_DIR}/patches")
file(GLOB PATCHES "${PATCH_DIR}/duckdb_*.patch")
list(SORT PATCHES)

foreach(PATCH IN LISTS PATCHES)
  get_filename_component(PATCH_NAME "${PATCH}" NAME)

  # Already applied? (the reverse patch applies cleanly)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" apply --check --reverse --ignore-whitespace "${PATCH}"
    WORKING_DIRECTORY "${DUCKDB_SOURCE_DIR}"
    RESULT_VARIABLE ALREADY_APPLIED
    OUTPUT_QUIET ERROR_QUIET)
  if(ALREADY_APPLIED EQUAL 0)
    message(STATUS "DuckDB patch ${PATCH_NAME}: already applied")
    continue()
  endif()

  execute_process(
    COMMAND "${GIT_EXECUTABLE}" apply --ignore-whitespace "${PATCH}"
    WORKING_DIRECTORY "${DUCKDB_SOURCE_DIR}"
    RESULT_VARIABLE APPLY_RESULT
    ERROR_VARIABLE APPLY_ERROR)
  if(NOT APPLY_RESULT EQUAL 0)
    message(FATAL_ERROR
      "DuckDB patch ${PATCH_NAME} does not apply to this DuckDB version "
      "(it targets a specific release; drop or rebase it when bumping DuckDB):\n${APPLY_ERROR}")
  endif()
  message(STATUS "DuckDB patch ${PATCH_NAME}: applied")
endforeach()
