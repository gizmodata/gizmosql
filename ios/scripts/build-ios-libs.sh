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
# Phase 2.5: Patch postgres_scanner extension to add static target
# -------------------------------------------------------
# DuckDB's postgres_scanner extension only ships with a
# build_loadable_extension() call (no static target). On iOS we cannot
# load .duckdb_extension files dynamically, so we patch the fetched
# source to also call build_static_extension(). This produces
# libpostgres_scanner_extension.a which we link into liball.a.
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
    # Re-run cmake so it picks up the patched CMakeLists. We delete the
    # cache so the duckdb ExternalProject re-configures with the patch.
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

# -------------------------------------------------------
# Retag macOS-built objects to iOS platform
# -------------------------------------------------------
# Arrow/gRPC/DuckDB/OpenSSL ExternalProjects build for the host
# (macOS arm64) because cross-compiling them to iOS is too complex
# (gRPC's proto tool generation, OpenSSL's perl scripts, etc).
# The resulting object files are binary-compatible with iOS arm64
# (same instruction set), but the Mach-O LC_BUILD_VERSION says
# platform 1 (macOS). When the iOS app linker tries to link them, it
# refuses with: "Building for 'iOS', but linking in object file ...
# built for 'macOS'".
#
# vtool can't retag .o files in place because they don't have padding
# to grow load commands. Instead we use a Python script that patches
# the LC_BUILD_VERSION command's platform field in-place (cmd size
# unchanged). This is fast and works on every .o we throw at it.
#
# We skip libgizmosqlserver.a since it's compiled directly with the
# iOS toolchain and is already iOS-tagged.
echo "Retagging macOS objects in static libraries to iOS..."
RETAG_PY="${REPO_ROOT}/ios/scripts/retag_objects_to_ios.py"
RETAG_TMP=$(mktemp -d)
for archive in "${OUTPUT_DIR}"/*.a; do
    name=$(basename "${archive}")
    case "${name}" in
        libgizmosqlserver.a) continue ;;
    esac
    archive_tmp="${RETAG_TMP}/$(basename "${archive}" .a)"
    mkdir -p "${archive_tmp}"
    (
        cd "${archive_tmp}"
        ar -x "${archive}" 2>/dev/null
        # Patch the LC_BUILD_VERSION platform field in every .o file
        # in this archive. Pass them all as args to one Python invocation.
        python3 "${RETAG_PY}" *.o 2>/dev/null || true
        # Repack the archive
        rm -f "${archive}"
        ar -rcs "${archive}" *.o 2>/dev/null || true
    )
    rm -rf "${archive_tmp}"
done
rm -rf "${RETAG_TMP}"

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
