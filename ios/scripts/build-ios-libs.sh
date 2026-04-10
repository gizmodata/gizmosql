#!/bin/bash
# Build GizmoSQL and all dependencies as static libraries for iOS ARM64.
#
# This is a two-phase build:
#   Phase 1: Build host tools (protoc, grpc_cpp_plugin) for macOS
#   Phase 2: Cross-compile all libraries for iOS ARM64
#
# Prerequisites:
#   - Xcode with iOS SDK installed
#   - CMake 3.25+, Ninja
#   - Boost headers (brew install boost)
#
# Usage:
#   cd <repo-root>
#   ./ios/scripts/build-ios-libs.sh
#
# Output:
#   ios/build/libs/lib/*.a    — Static libraries for iOS ARM64
#   ios/build/libs/include/*  — Headers

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
IOS_BUILD_DIR="${REPO_ROOT}/ios/build"
HOST_BUILD_DIR="${IOS_BUILD_DIR}/host-tools"
IOS_LIB_BUILD_DIR="${IOS_BUILD_DIR}/ios-arm64"
OUTPUT_DIR="${IOS_BUILD_DIR}/libs"
TOOLCHAIN="${REPO_ROOT}/ios/cmake/ios-toolchain.cmake"

echo "=== GizmoSQL iOS Build ==="
echo "Repo root:    ${REPO_ROOT}"
echo "Host build:   ${HOST_BUILD_DIR}"
echo "iOS build:    ${IOS_LIB_BUILD_DIR}"
echo "Output:       ${OUTPUT_DIR}"
echo ""

# -------------------------------------------------------
# Phase 1: Build host tools (protoc + grpc_cpp_plugin)
# -------------------------------------------------------
echo "=== Phase 1: Building host tools (protoc, grpc_cpp_plugin) ==="

mkdir -p "${HOST_BUILD_DIR}"
cd "${HOST_BUILD_DIR}"

if [ ! -f "${HOST_BUILD_DIR}/protoc" ] || [ ! -f "${HOST_BUILD_DIR}/grpc_cpp_plugin" ]; then
    cmake "${REPO_ROOT}" \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DGIZMOSQL_ENTERPRISE=OFF \
        -DWITH_OPENTELEMETRY=OFF

    # Build just the proto tools (they are host-architecture binaries)
    ninja health_proto_gen 2>/dev/null || true

    # Find and copy the host tools (protoc may be a symlink)
    PROTOC=$(find "${HOST_BUILD_DIR}" -name "protoc" \( -type f -o -type l \) ! -name "*.py" ! -name "*.cmake" ! -name "*.patch" | head -1)
    GRPC_PLUGIN=$(find "${HOST_BUILD_DIR}" -name "grpc_cpp_plugin" \( -type f -o -type l \) | head -1)

    if [ -n "${PROTOC}" ]; then
        cp -L "${PROTOC}" "${HOST_BUILD_DIR}/protoc"
        echo "Found protoc: ${PROTOC}"
    else
        echo "ERROR: Could not find protoc binary"
        exit 1
    fi

    if [ -n "${GRPC_PLUGIN}" ]; then
        cp "${GRPC_PLUGIN}" "${HOST_BUILD_DIR}/grpc_cpp_plugin"
        echo "Found grpc_cpp_plugin: ${GRPC_PLUGIN}"
    else
        echo "ERROR: Could not find grpc_cpp_plugin binary"
        exit 1
    fi
else
    echo "Host tools already built (cached)"
fi

echo ""

# -------------------------------------------------------
# Phase 2: Cross-compile for iOS ARM64
# -------------------------------------------------------
echo "=== Phase 2: Cross-compiling for iOS ARM64 ==="

mkdir -p "${IOS_LIB_BUILD_DIR}"
cd "${IOS_LIB_BUILD_DIR}"

cmake "${REPO_ROOT}" \
    -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE="${TOOLCHAIN}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DGIZMOSQL_ENTERPRISE=OFF \
    -DWITH_OPENTELEMETRY=OFF \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_RUNTIME_SIMD_LEVEL=NONE

# -------------------------------------------------------
# Phase 2.5: Patch out-of-tree extensions for iOS
# -------------------------------------------------------
# Two patches needed:
#
# 1. postgres_scanner: only ships with build_loadable_extension() (no
#    static target). iOS can't dlopen, so we patch in a static target.
#
# 2. httpfs: uses libcurl by default for HTTP, but libcurl is not
#    available on iOS. httpfs has an httplib-based fallback (used in
#    EMSCRIPTEN builds). We patch the source list to exclude
#    httpfs_curl_client.cpp and patch httpfs_extension.cpp to use
#    HTTPFSUtil (httplib) instead of HTTPFSCurlUtil.
NEED_RECONFIGURE=0

PSCAN_SRC="${IOS_LIB_BUILD_DIR}/third_party/src/duckdb_project-build/_deps/postgres_scanner_extension_fc-src/CMakeLists.txt"
if [ -f "${PSCAN_SRC}" ] && ! grep -q "Patch added by GizmoSQL" "${PSCAN_SRC}"; then
    echo "Patching postgres_scanner CMakeLists to add static extension target..."
    cat >> "${PSCAN_SRC}" << 'PATCH_EOF'

# Patch added by GizmoSQL: also build static extension target
build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES} ${LIBPG_SOURCES_FULLPATH})
target_include_directories(
  ${TARGET_NAME}_extension
  PRIVATE include postgres/src/include postgres/src/backend
          postgres/src/interfaces/libpq ${OPENSSL_INCLUDE_DIR})
target_link_libraries(${TARGET_NAME}_extension ${OPENSSL_LIBRARIES})
set_property(TARGET ${TARGET_NAME}_extension PROPERTY C_STANDARD 99)
PATCH_EOF
    NEED_RECONFIGURE=1
fi

# Patch httpfs to drop the curl dependency
HTTPFS_DIR="${IOS_LIB_BUILD_DIR}/third_party/src/duckdb_project-build/_deps/httpfs_extension_fc-src"
HTTPFS_TOPLEVEL="${HTTPFS_DIR}/CMakeLists.txt"
HTTPFS_SRC_CMAKE="${HTTPFS_DIR}/src/CMakeLists.txt"
HTTPFS_EXT_CPP="${HTTPFS_DIR}/src/httpfs_extension.cpp"
if [ -f "${HTTPFS_SRC_CMAKE}" ] && ! grep -q "Patch added by GizmoSQL" "${HTTPFS_SRC_CMAKE}"; then
    echo "Patching httpfs to drop libcurl dependency on iOS..."
    # Remove httpfs_curl_client.cpp from the source list
    python3 - << PYEOF
import re
p = "${HTTPFS_SRC_CMAKE}"
with open(p) as f: s = f.read()
s = s.replace("httpfs_curl_client.cpp", "")
s = "# Patch added by GizmoSQL\n" + s
with open(p, "w") as f: f.write(s)
PYEOF
    # Drop the find_package(CURL) and target_link_libraries lines
    python3 - << PYEOF
p = "${HTTPFS_TOPLEVEL}"
with open(p) as f: s = f.read()
s = s.replace("find_package(CURL REQUIRED)", "# Patched out by GizmoSQL: find_package(CURL REQUIRED)")
s = s.replace("\${CURL_LIBRARIES}", "")
with open(p, "w") as f: f.write(s)
PYEOF
    # Patch httpfs_extension.cpp to use HTTPFSUtil instead of HTTPFSCurlUtil
    python3 - << PYEOF
p = "${HTTPFS_EXT_CPP}"
with open(p) as f: s = f.read()
s = s.replace(
    'config.SetHTTPUtil(make_shared_ptr<HTTPFSCurlUtil>());',
    'config.SetHTTPUtil(make_shared_ptr<HTTPFSUtil>());  // Patched by GizmoSQL: use httplib instead of curl on iOS')
# Also remove the #include of httpfs_curl_client.hpp
s = s.replace('#include "httpfs_curl_client.hpp"', '// #include "httpfs_curl_client.hpp"  // Patched by GizmoSQL')
with open(p, "w") as f: f.write(s)
PYEOF
    NEED_RECONFIGURE=1
fi

if [ "${NEED_RECONFIGURE}" = "1" ]; then
    # Re-run cmake so it picks up patched CMakeLists.
    rm -rf "${IOS_LIB_BUILD_DIR}/third_party/src/duckdb_project-build/CMakeCache.txt" \
           "${IOS_LIB_BUILD_DIR}/third_party/src/duckdb_project-build/CMakeFiles"
    cmake "${REPO_ROOT}" \
        -G Ninja \
        -DCMAKE_TOOLCHAIN_FILE="${TOOLCHAIN}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DGIZMOSQL_ENTERPRISE=OFF \
        -DWITH_OPENTELEMETRY=OFF \
        -DARROW_SIMD_LEVEL=NONE \
        -DARROW_RUNTIME_SIMD_LEVEL=NONE
fi

echo "Building gizmosqlserver static library for iOS..."
ninja gizmosqlserver

echo ""

# -------------------------------------------------------
# Collect output
# -------------------------------------------------------
echo "=== Collecting build artifacts ==="

mkdir -p "${OUTPUT_DIR}/lib" "${OUTPUT_DIR}/include"

# Copy the gizmosqlserver static library to both lib/ and the top-level
# libs/ directory (the latter is what liball.a is built from).
find "${IOS_LIB_BUILD_DIR}" -name "libgizmosqlserver.a" -exec cp {} "${OUTPUT_DIR}/lib/" \;
find "${IOS_LIB_BUILD_DIR}" -name "libgizmosqlserver.a" -exec cp {} "${OUTPUT_DIR}/" \;

# Copy ALL static libraries from the iOS build dir into the top-level
# libs/ directory. This includes Arrow, gRPC, DuckDB, OpenSSL, ICU,
# DuckDB extensions (ducklake, icu, tpch, parquet, etc.), and all
# transitive deps. We exclude host-tools and FetchContent subbuild dirs
# to avoid pulling in test/demo libs.
#
# IMPORTANT: We delete any pre-existing .a files in the output dir
# first, then copy fresh ones. Without this, stale files from previous
# builds (with different extension lists, etc.) will be linked into
# liball.a, causing subtle "extension not loaded" runtime bugs.
echo "Collecting all static libraries for liball.a..."
rm -f "${OUTPUT_DIR}"/*.a
find "${IOS_LIB_BUILD_DIR}" -name "*.a" \
    ! -path "*/host-tools/*" \
    ! -path "*-subbuild/*" \
    ! -name "*test*" \
    ! -name "*demo*" \
    -exec cp {} "${OUTPUT_DIR}/" \; 2>/dev/null || true

# Also copy iOS-cross-compiled OpenSSL (already iOS-tagged, no retag needed).
# These live outside ios-arm64/ in their own openssl-ios/ install dir.
if [ -d "${IOS_BUILD_DIR}/openssl-ios/lib" ]; then
    echo "Copying iOS OpenSSL libraries..."
    cp "${IOS_BUILD_DIR}/openssl-ios/lib/"*.a "${OUTPUT_DIR}/" 2>/dev/null || true
fi

# -------------------------------------------------------
# Retag macOS-built objects to iOS platform
# -------------------------------------------------------
# Arrow/gRPC/DuckDB/OpenSSL ExternalProjects build for the host
# (macOS arm64) because cross-compiling them to iOS is too complex
# (gRPC's proto tool generation, OpenSSL's perl scripts, etc).
# The resulting object files are binary-compatible with iOS arm64
# (same instruction set), but the Mach-O LC_BUILD_VERSION says
# platform 1 (macOS). The iOS linker rejects them with:
#   "Building for 'iOS', but linking in object file ... built for
#    'macOS'".
#
# We patch each .a archive IN PLACE: read the file, walk the BSD ar
# format, find each member's Mach-O header, patch the LC_BUILD_VERSION
# platform/minos/sdk fields, and write back. The file size and member
# offsets are unchanged. This avoids ar -x / ar -r which would lose
# duplicate-named members (libgrpc.a has e.g. 3 copies of
# `status.upb.c.o` from different source dirs).
#
# We skip libgizmosqlserver.a (built directly with iOS toolchain) and
# libssl/libcrypto (cross-compiled separately for iOS).
echo "Retagging macOS objects in static libraries to iOS..."
RETAG_PY="${REPO_ROOT}/ios/scripts/retag_objects_to_ios.py"
for archive in "${OUTPUT_DIR}"/*.a; do
    name=$(basename "${archive}")
    case "${name}" in
        libgizmosqlserver.a|libssl.a|libcrypto.a) continue ;;
    esac
    python3 "${RETAG_PY}" "${archive}" 2>/dev/null || true
done

# Combine everything into a single fat archive (liball.a) for the iOS app.
# The Xcode project links against this single archive.
echo "Building liball.a..."
cd "${OUTPUT_DIR}"
rm -f liball.a
# shellcheck disable=SC2046
libtool -static -o liball.a $(ls *.a 2>/dev/null | grep -v "^liball.a$") 2>&1 \
    | grep -v "has no symbols" || true
cd "${REPO_ROOT}"

# Copy public header
cp "${REPO_ROOT}/src/common/include/gizmosql_library.h" "${OUTPUT_DIR}/include/"
cp "${IOS_LIB_BUILD_DIR}/src/common/include/version.h" "${OUTPUT_DIR}/include/" 2>/dev/null || true


echo ""
echo "=== Build complete ==="
echo "Combined archive:"
ls -lh "${OUTPUT_DIR}/liball.a" 2>/dev/null || echo "  (liball.a not found)"
echo ""
echo "Individual libraries in lib/:"
ls -lh "${OUTPUT_DIR}/lib/"*.a 2>/dev/null || echo "  (none found)"
echo ""
echo "Headers:"
ls "${OUTPUT_DIR}/include/"
