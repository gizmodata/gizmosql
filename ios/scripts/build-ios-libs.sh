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
echo "Collecting all static libraries for liball.a..."
find "${IOS_LIB_BUILD_DIR}" -name "*.a" \
    ! -path "*/host-tools/*" \
    ! -path "*-subbuild/*" \
    ! -name "*test*" \
    ! -name "*demo*" \
    -exec cp -n {} "${OUTPUT_DIR}/" \; 2>/dev/null || true

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
