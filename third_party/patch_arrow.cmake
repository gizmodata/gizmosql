# Cross-platform patch for Arrow's ThirdpartyToolchain.cmake
# Removes problematic set_target_properties on ALIAS target (c-ares::cares)
# and LIBRESOLV_LIBRARY references that break the build.
# (The c-ares fix was needed for Arrow 23; fixed upstream in Arrow 25 so the
# regex no longer matches there — each patch below is a no-op when its
# pattern is absent.)
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

# Fix: Arrow's BuildUtils.cmake libtool detection regex fails on newer macOS
# where libtool -V reports "cctools_ld-NNNN" instead of "cctools-NNNN".
# Broaden the regex to accept both formats.
set(BUILD_UTILS_FILE "${ARROW_SOURCE_DIR}/cpp/cmake_modules/BuildUtils.cmake")

if(EXISTS "${BUILD_UTILS_FILE}")
  file(READ "${BUILD_UTILS_FILE}" BUILD_UTILS_CONTENT)

  string(REPLACE
    "\".*cctools-([0-9.]+).*\""
    "\".*cctools[_a-z]*-([0-9.]+).*\""
    BUILD_UTILS_CONTENT
    "${BUILD_UTILS_CONTENT}"
  )

  file(WRITE "${BUILD_UTILS_FILE}" "${BUILD_UTILS_CONTENT}")
endif()

# Patch the gRPC that ThirdpartyToolchain.cmake fetches, via a PATCH_COMMAND on
# its fetchcontent_declare(): patch_grpc.cmake works around an MSVC 14.5x
# (Visual Studio 2026) C3539 compiler bug in gRPC's filter templates (see the
# comments there). The script path is absolute, because ThirdpartyToolchain.cmake
# runs from Arrow's build tree.
file(READ "${TOOLCHAIN_FILE}" CONTENT)
set(GRPC_DECLARE_ANCHOR "URL_HASH \"SHA256=\${ARROW_GRPC_BUILD_SHA256_CHECKSUM}\")")
string(FIND "${CONTENT}" "patch_grpc.cmake" GRPC_ALREADY_HOOKED)
string(FIND "${CONTENT}" "${GRPC_DECLARE_ANCHOR}" GRPC_DECLARE_POS)
if(NOT GRPC_ALREADY_HOOKED EQUAL -1)
  # Already hooked: the patch step ran on this source tree before.
elseif(GRPC_DECLARE_POS EQUAL -1)
  message(FATAL_ERROR "patch_arrow.cmake: gRPC fetchcontent_declare() not found in "
                      "${TOOLCHAIN_FILE}; update the patch_grpc.cmake hook for this Arrow version")
else()
  string(REPLACE
    "${GRPC_DECLARE_ANCHOR}"
    "URL_HASH \"SHA256=\${ARROW_GRPC_BUILD_SHA256_CHECKSUM}\"\n                       PATCH_COMMAND \"\${CMAKE_COMMAND}\" -P \"${CMAKE_CURRENT_LIST_DIR}/patch_grpc.cmake\")"
    CONTENT
    "${CONTENT}"
  )
  file(WRITE "${TOOLCHAIN_FILE}" "${CONTENT}")
endif()

# Fix: Arrow 26 bundles uriparser 1.0.2, which calls reallocarray() whenever
# check_function_exists finds the symbol, but only defines the feature macros
# that declare it for glibc >= 2.29. On the glibc 2.28 portable Linux build
# (manylinux_2_28) the symbol exists while the declaration needs _GNU_SOURCE,
# so UriMemory.c fails with "implicit declaration of function 'reallocarray'".
# Presetting HAVE_REALLOCARRAY=0 skips that check; uriparser then uses its own
# overflow-checked realloc() path, on every platform.
file(READ "${TOOLCHAIN_FILE}" CONTENT)
set(URIPARSER_ANCHOR "  set(URIPARSER_ENABLE_INSTALL OFF)\n")
string(FIND "${CONTENT}" "HAVE_REALLOCARRAY" URIPARSER_ALREADY_PATCHED)
string(FIND "${CONTENT}" "${URIPARSER_ANCHOR}" URIPARSER_ANCHOR_POS)
if(NOT URIPARSER_ALREADY_PATCHED EQUAL -1)
  # Already patched: the patch step ran on this source tree before.
elseif(URIPARSER_ANCHOR_POS EQUAL -1)
  message(WARNING "patch_arrow.cmake: uriparser block not found in ${TOOLCHAIN_FILE}; "
                  "reallocarray workaround not applied")
else()
  string(REPLACE "${URIPARSER_ANCHOR}"
    "${URIPARSER_ANCHOR}  # GizmoSQL patch: no reallocarray (glibc 2.28 hides its declaration).\n  set(HAVE_REALLOCARRAY 0)\n"
    CONTENT "${CONTENT}")
  file(WRITE "${TOOLCHAIN_FILE}" "${CONTENT}")
endif()
