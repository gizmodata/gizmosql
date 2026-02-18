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
