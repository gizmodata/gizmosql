# Cross-platform patch for Arrow 23's ThirdpartyToolchain.cmake
# Removes problematic set_target_properties on ALIAS target (c-ares::cares)
# and LIBRESOLV_LIBRARY references that break the build.
#
# This replaces the POSIX sed-based patch command for Windows compatibility.

set(TOOLCHAIN_FILE "${ARROW_SOURCE_DIR}/cpp/cmake_modules/ThirdpartyToolchain.cmake")

file(READ "${TOOLCHAIN_FILE}" CONTENT)

# Remove lines containing set_target_properties on c-ares::cares ALIAS target
string(REGEX REPLACE "[^\n]*set_target_properties[^\n]*c-ares::cares[^\n]*PROPERTIES[^\n]*\n" "" CONTENT "${CONTENT}")

# Remove lines referencing LIBRESOLV_LIBRARY
string(REGEX REPLACE "[^\n]*LIBRESOLV_LIBRARY[^\n]*\n" "" CONTENT "${CONTENT}")

file(WRITE "${TOOLCHAIN_FILE}" "${CONTENT}")

# Fix: Arrow's SetupCxxFlags.cmake unconditionally adds -D__SSE2__ -D__SSE4_1__
# -D__SSE4_2__ in the MSVC block, even when ARROW_SIMD_LEVEL=NONE. These defines
# cause bundled Abseil to include x86intrin.h, which is a GCC/Clang header that
# MSVC doesn't have. Wrap the add_definitions in a SIMD level check.
set(CXX_FLAGS_FILE "${ARROW_SOURCE_DIR}/cpp/cmake_modules/SetupCxxFlags.cmake")

file(READ "${CXX_FLAGS_FILE}" CXX_FLAGS_CONTENT)

string(REPLACE
  "add_definitions(-D__SSE2__ -D__SSE4_1__ -D__SSE4_2__)"
  "if(NOT ARROW_SIMD_LEVEL STREQUAL \"NONE\")\n      add_definitions(-D__SSE2__ -D__SSE4_1__ -D__SSE4_2__)\n    endif()"
  CXX_FLAGS_CONTENT
  "${CXX_FLAGS_CONTENT}"
)

file(WRITE "${CXX_FLAGS_FILE}" "${CXX_FLAGS_CONTENT}")
