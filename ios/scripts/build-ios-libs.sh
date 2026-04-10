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

# Copy the gizmosqlserver static library
find "${IOS_LIB_BUILD_DIR}" -name "libgizmosqlserver.a" -exec cp {} "${OUTPUT_DIR}/lib/" \;

# Copy dependency libraries
for lib in arrow arrow_flight arrow_flight_sql grpc grpc++ protobuf absl duckdb sqlite; do
    find "${IOS_LIB_BUILD_DIR}" -name "lib${lib}*.a" -exec cp {} "${OUTPUT_DIR}/lib/" \; 2>/dev/null || true
done

# Copy public header
cp "${REPO_ROOT}/src/common/include/gizmosql_library.h" "${OUTPUT_DIR}/include/"
cp "${IOS_LIB_BUILD_DIR}/src/common/include/version.h" "${OUTPUT_DIR}/include/" 2>/dev/null || true

echo ""
echo "=== Build complete ==="
echo "Libraries:"
ls -lh "${OUTPUT_DIR}/lib/"*.a 2>/dev/null || echo "  (none found — check build output above)"
echo ""
echo "Headers:"
ls "${OUTPUT_DIR}/include/"
