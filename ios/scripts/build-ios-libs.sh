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
# Phase 1.5: Cross-compile CRoaring for iOS
# -------------------------------------------------------
# DuckLake v1.0+ uses CRoaring for deletion-vector bitmap storage.
# DuckDB's official build pulls it in via vcpkg — we don't use vcpkg, so
# we build it ourselves with the iOS toolchain and install it at a path
# that looks like any other third-party dep in the iOS build dir, so the
# lib-collection find at the end of the script picks up libroaring.a
# automatically and DuckLake's `find_package(roaring CONFIG REQUIRED)`
# can resolve it via CMAKE_PREFIX_PATH (wired through in
# third_party/DuckDB_CMakeLists.txt.in).
#
# When bumping DuckLake, verify this pin stays API-compatible with
# whatever CRoaring version DuckDB's vcpkg baseline resolves at that time.
CROARING_VERSION="v4.6.1"
CROARING_SRC="${IOS_BUILD_DIR}/croaring-src"
CROARING_BUILD="${IOS_BUILD_DIR}/croaring-build-ios"
CROARING_INSTALL="${IOS_LIB_BUILD_DIR}/third_party/croaring-ios"

if [ ! -f "${CROARING_INSTALL}/lib/libroaring.a" ]; then
    echo "=== Phase 1.5: Cross-compiling CRoaring ${CROARING_VERSION} for iOS ==="

    if [ ! -d "${CROARING_SRC}/.git" ]; then
        rm -rf "${CROARING_SRC}"
        git clone --depth 1 --branch "${CROARING_VERSION}" \
            https://github.com/RoaringBitmap/CRoaring "${CROARING_SRC}"
    fi

    mkdir -p "${CROARING_INSTALL}"
    rm -rf "${CROARING_BUILD}"
    cmake -S "${CROARING_SRC}" -B "${CROARING_BUILD}" \
        -G Ninja \
        -DCMAKE_TOOLCHAIN_FILE="${TOOLCHAIN}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${CROARING_INSTALL}" \
        -DENABLE_ROARING_TESTS=OFF \
        -DROARING_DISABLE_NATIVE=ON \
        -DROARING_BUILD_STATIC=ON \
        -DBUILD_SHARED_LIBS=OFF

    cmake --build "${CROARING_BUILD}"
    cmake --install "${CROARING_BUILD}"
    echo "CRoaring installed at ${CROARING_INSTALL}"
else
    echo "CRoaring already built at ${CROARING_INSTALL} (cached)"
fi

echo ""

# -------------------------------------------------------
# Phase 1.7: Cross-compile OpenSSL for iOS
# -------------------------------------------------------
# Arrow/gRPC link against OpenSSL for TLS. We can't reuse the host
# (macOS) OpenSSL — its objects are macOS-tagged and the `ios64-xcrun`
# target is the only OpenSSL Configure preset that produces an iOS-tagged
# arm64 build. We do this here (not via ExternalProject inside Arrow's
# build) because OpenSSL's Configure perl scripts and ios64-xcrun target
# are simpler to drive directly than to bend Arrow's bundled-deps machinery.
#
# Output: ${IOS_BUILD_DIR}/openssl-ios/lib/{libssl,libcrypto}.a, picked up
# by the artifact-collection step below.
OPENSSL_VERSION="3.5.0"
OPENSSL_SHA256="344d0a79f1a9b08029b0744e2cc401a43f9c90acd1044d09a530b4885a8e9fc0"
OPENSSL_SRC_DIR="${IOS_BUILD_DIR}/openssl-src"
OPENSSL_INSTALL="${IOS_BUILD_DIR}/openssl-ios"

if [ ! -f "${OPENSSL_INSTALL}/lib/libssl.a" ] || [ ! -f "${OPENSSL_INSTALL}/lib/libcrypto.a" ]; then
    echo "=== Phase 1.7: Cross-compiling OpenSSL ${OPENSSL_VERSION} for iOS ==="

    mkdir -p "${OPENSSL_SRC_DIR}"
    OPENSSL_TARBALL="${OPENSSL_SRC_DIR}/openssl-${OPENSSL_VERSION}.tar.gz"
    OPENSSL_BUILD_DIR="${OPENSSL_SRC_DIR}/openssl-${OPENSSL_VERSION}"

    if [ ! -f "${OPENSSL_TARBALL}" ]; then
        echo "Downloading OpenSSL ${OPENSSL_VERSION}..."
        curl -fSL --retry 3 \
            -o "${OPENSSL_TARBALL}" \
            "https://github.com/openssl/openssl/releases/download/openssl-${OPENSSL_VERSION}/openssl-${OPENSSL_VERSION}.tar.gz"
    fi

    actual_sha=$(shasum -a 256 "${OPENSSL_TARBALL}" | awk '{print $1}')
    if [ "${actual_sha}" != "${OPENSSL_SHA256}" ]; then
        echo "ERROR: OpenSSL tarball SHA256 mismatch"
        echo "  expected: ${OPENSSL_SHA256}"
        echo "  actual:   ${actual_sha}"
        exit 1
    fi

    rm -rf "${OPENSSL_BUILD_DIR}"
    tar -C "${OPENSSL_SRC_DIR}" -xf "${OPENSSL_TARBALL}"

    pushd "${OPENSSL_BUILD_DIR}" >/dev/null
    # ios64-xcrun target sets the iPhoneOS SDK and arm64 arch. We disable
    # shared libs (we only ever link statically), tests, asm, and dso
    # (loadable modules — App Store forbids dynamic code loading anyway).
    export IPHONEOS_DEPLOYMENT_TARGET=17.0
    ./Configure ios64-xcrun \
        no-shared no-tests no-asm no-dso \
        --prefix="${OPENSSL_INSTALL}"
    make -j"$(sysctl -n hw.ncpu)" build_libs
    make install_dev
    popd >/dev/null

    echo "OpenSSL installed at ${OPENSSL_INSTALL}"
else
    echo "OpenSSL already built at ${OPENSSL_INSTALL} (cached)"
fi

echo ""

# -------------------------------------------------------
# Phase 2: Cross-compile for iOS ARM64
# -------------------------------------------------------
echo "=== Phase 2: Cross-compiling for iOS ARM64 ==="

mkdir -p "${IOS_LIB_BUILD_DIR}"
cd "${IOS_LIB_BUILD_DIR}"

# The first configure may fail on a fresh DuckDB clone: our root
# CMakeLists.txt builds DuckDB inline via execute_process(cmake --build),
# and DuckDB's libduckdb.dylib link pulls in httpfs, which we need to
# patch (drop libcurl) before its static target compiles cleanly. The
# failure is tolerated — FetchContent still downloads the extension
# sources during DuckDB's configure step, which is all we need to let
# Phase 2.5 patch them below. On cached rebuilds the initial configure
# just no-ops and everything proceeds.
cmake "${REPO_ROOT}" \
    -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE="${TOOLCHAIN}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DGIZMOSQL_ENTERPRISE=OFF \
    -DWITH_OPENTELEMETRY=OFF \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_RUNTIME_SIMD_LEVEL=NONE \
    || echo "(initial configure failed — extensions will be patched in Phase 2.5)"

# -------------------------------------------------------
# Phase 2.5: Patch out-of-tree extensions for iOS
# -------------------------------------------------------
# Two patches needed:
#
# 1. ducklake: its top-level CMakeLists computes the roaring include
#    path as `${roaring_DIR}/../../include`, which assumes a 2-level
#    layout (vcpkg's `share/roaring/`). The standard CMake install
#    layout `lib/cmake/roaring/` needs three `..` levels — so the
#    upstream path resolves to `<prefix>/lib/include` and DuckLake's
#    OBJECT libraries (e.g. `ducklake_storage`) fail to find
#    `roaring/roaring.hh`. We replace it with the IMPORTED target's
#    INTERFACE_INCLUDE_DIRECTORIES, which is correct regardless of
#    layout.
#
# 2. httpfs uses libcurl by default for HTTP, but libcurl is not
#    available on iOS. httpfs has an httplib-based fallback (used in
#    EMSCRIPTEN builds). We patch the source list to exclude
#    httpfs_curl_client.cpp and patch httpfs_extension.cpp to use
#    HTTPFSUtil (httplib) instead of HTTPFSCurlUtil.
#
# Note: we used to also patch postgres_scanner here to add a
# build_static_extension() call, but as of the DuckDB 1.5.2-era pin
# (postgres_scanner c89234f0...) upstream now ships its own static
# target. Re-adding ours collides — see CLAUDE.md "Bumping DuckDB
# (iOS extension sync)".
NEED_RECONFIGURE=0

# Patch DuckDB's tools/CMakeLists.txt to skip plan_serializer on iOS.
# It's built unconditionally (no BUILD_TOOLS or similar gate), and like
# the other DuckDB executables it links as a host (macOS) binary that
# pulls in iOS-tagged transitive deps via link_extension_libraries().
# We don't ship plan_serializer.
DUCKDB_TOOLS_CMAKE="${IOS_LIB_BUILD_DIR}/third_party/src/duckdb_project/tools/CMakeLists.txt"
if [ -f "${DUCKDB_TOOLS_CMAKE}" ] && ! grep -q "Patch added by GizmoSQL" "${DUCKDB_TOOLS_CMAKE}"; then
    echo "Patching DuckDB tools/CMakeLists.txt to skip plan_serializer on iOS..."
    python3 - << PYEOF
p = "${DUCKDB_TOOLS_CMAKE}"
with open(p) as f: s = f.read()
prefix = ("# Patch added by GizmoSQL: skip the entire tools/ subdir on iOS\n"
          "# (plan_serializer links as a host binary and pulls in iOS-tagged\n"
          "# extension libs via link_extension_libraries — see CLAUDE.md).\n"
          "if(GIZMOSQL_IOS)\n"
          "  return()\n"
          "endif()\n\n")
s = prefix + s
with open(p, "w") as f: f.write(s)
PYEOF
    NEED_RECONFIGURE=1
fi

# Patch DuckDB's src/CMakeLists.txt to build the main `duckdb` library
# as STATIC on iOS instead of SHARED. The inner duckdb build always
# defines BOTH `duckdb_static` (STATIC) and `duckdb` (SHARED) — same
# objects, two outputs. The SHARED variant link uses bare /usr/bin/c++
# without iOS toolchain flags, so any iOS-tagged transitive dep
# (e.g. libroaring.a via DuckLake) fails with
#   ld: building for 'macOS', but linking in object file ... built for 'iOS'
# We only consume libduckdb_static.a downstream, so making `duckdb` a
# STATIC archive too produces a (harmless) duplicate but skips the
# broken dylib link.
DUCKDB_SRC_CMAKE="${IOS_LIB_BUILD_DIR}/third_party/src/duckdb_project/src/CMakeLists.txt"
if [ -f "${DUCKDB_SRC_CMAKE}" ] && ! grep -q "Patch added by GizmoSQL" "${DUCKDB_SRC_CMAKE}"; then
    echo "Patching DuckDB src/CMakeLists.txt to build duckdb as STATIC on iOS..."
    python3 - << PYEOF
p = "${DUCKDB_SRC_CMAKE}"
with open(p) as f: s = f.read()
old = "add_library(duckdb SHARED \${ALL_OBJECT_FILES} \${LINK_OBJECTS})"
new = ("# Patch added by GizmoSQL: build duckdb as STATIC on iOS — the SHARED\n"
       "# link uses bare /usr/bin/c++ without iOS flags and breaks on iOS-tagged\n"
       "# transitive deps. Downstream consumers use libduckdb_static.a anyway.\n"
       "if(GIZMOSQL_IOS)\n"
       "    add_library(duckdb STATIC \${ALL_OBJECT_FILES} \${LINK_OBJECTS})\n"
       "else()\n"
       "    add_library(duckdb SHARED \${ALL_OBJECT_FILES} \${LINK_OBJECTS})\n"
       "endif()")
assert old in s, "DuckDB src/CMakeLists.txt no longer contains the expected add_library(duckdb SHARED ...) line — patch needs updating"
s = s.replace(old, new)
with open(p, "w") as f: f.write(s)
PYEOF
    NEED_RECONFIGURE=1
fi

# Patch DuckDB's extension_build_tools to build "loadable" extensions
# as STATIC archives on iOS (mirroring the existing EMSCRIPTEN branch).
# iOS sets -DDISABLE_EXTENSION_LOAD=ON so loadables are never used at
# runtime, but DuckDB still tries to LINK them as host-arch dylibs
# during the build — and any extension that pulls in iOS-tagged libs
# (e.g. ducklake → libroaring.a) fails with
#   ld: building for 'macOS', but linking in object file ... built for 'iOS'
# because the dylib link uses bare /usr/bin/c++ -arch arm64 with no
# iOS toolchain flags. STATIC archives skip the link step entirely.
DUCKDB_EBT="${IOS_LIB_BUILD_DIR}/third_party/src/duckdb_project/extension/extension_build_tools.cmake"
if [ -f "${DUCKDB_EBT}" ] && ! grep -q "Patch added by GizmoSQL" "${DUCKDB_EBT}"; then
    echo "Patching DuckDB extension_build_tools to build loadables as STATIC on iOS..."
    python3 - << PYEOF
p = "${DUCKDB_EBT}"
with open(p) as f: s = f.read()
old = "    if(EMSCRIPTEN)\n        add_library(\${TARGET_NAME} STATIC \${FILES})\n    else()\n        add_library(\${TARGET_NAME} SHARED \${FILES})\n    endif()"
new = ("    # Patch added by GizmoSQL: build loadables as STATIC on iOS too,\n"
       "    # so the dylib link step is skipped (iOS doesn't dlopen anyway).\n"
       "    if(EMSCRIPTEN OR GIZMOSQL_IOS)\n"
       "        add_library(\${TARGET_NAME} STATIC \${FILES})\n"
       "    else()\n"
       "        add_library(\${TARGET_NAME} SHARED \${FILES})\n"
       "    endif()")
assert old in s, "DuckDB extension_build_tools.cmake no longer matches the expected EMSCRIPTEN branch — patch needs updating"
s = s.replace(old, new)
with open(p, "w") as f: f.write(s)
PYEOF
    NEED_RECONFIGURE=1
fi

# Patch ducklake to use the IMPORTED roaring target's include dirs
DUCKLAKE_TOPLEVEL="${IOS_LIB_BUILD_DIR}/third_party/src/duckdb_project-build/_deps/ducklake_extension_fc-src/CMakeLists.txt"
if [ -f "${DUCKLAKE_TOPLEVEL}" ] && ! grep -q "Patch added by GizmoSQL" "${DUCKLAKE_TOPLEVEL}"; then
    echo "Patching ducklake CMakeLists to use IMPORTED roaring include dirs..."
    python3 - << PYEOF
p = "${DUCKLAKE_TOPLEVEL}"
with open(p) as f: s = f.read()
old = "include_directories(\${roaring_DIR}/../../include)"
new = ("# Patch added by GizmoSQL: upstream's relative path math assumes a\n"
       "# 2-level vcpkg layout (share/roaring/) but the standard CMake install\n"
       "# layout uses lib/cmake/roaring/, which needs three '..' levels.\n"
       "# Use the IMPORTED target's INTERFACE_INCLUDE_DIRECTORIES instead.\n"
       "get_target_property(_GIZMOSQL_ROARING_INCS roaring::roaring-headers-cpp INTERFACE_INCLUDE_DIRECTORIES)\n"
       "include_directories(\${_GIZMOSQL_ROARING_INCS})")
assert old in s, "DuckLake CMakeLists.txt no longer contains the expected include_directories line — patch needs updating"
s = s.replace(old, new)
with open(p, "w") as f: f.write(s)
PYEOF
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
# Remove the dynamic_cast<HTTPFSCurlUtil*> block: it's the only thing
# referencing HTTPFSCurlUtil's typeinfo, and excluding httpfs_curl_client.cpp
# from the build means the vtable's key function is never defined → typeinfo
# missing → undefined symbol at iOS app link time.
old_dc = (
    "#ifndef EMSCRIPTEN\n"
    "\t\tauto *curl_util = dynamic_cast<HTTPFSCurlUtil *>(&http_util);\n"
    "\t\tif (curl_util) {\n"
    "\t\t\tcurl_util->connection_caching_enabled = BooleanValue::Get(parameter);\n"
    "\t\t}\n"
    "#endif")
new_dc = "// Patched by GizmoSQL: HTTPFSCurlUtil is unused on iOS"
assert old_dc in s, "httpfs_extension.cpp dynamic_cast block doesn't match expected shape — patch needs updating"
s = s.replace(old_dc, new_dc)
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

# Defensive: nuke cached gizmosqlserver .o files if the DuckDB headers
# have been reinstalled since they were last compiled. DuckDB is built
# via ExternalProject, so its header install (triggered by a version
# bump, a Phase 2.5 patch, or any rebuild of the inner duckdb_project)
# is NOT a ninja dep of our gizmosqlserver target. Stale .o files
# compiled against an older layout for e.g. `duckdb::DBConfig` produce
# an ABI mismatch at link time that surfaces as a __stack_chk_fail
# inside DuckDBFlightSqlServer::Create at runtime — silent until the
# first server start. This check catches the drift cheaply.
GIZMOSQL_DIR="${IOS_LIB_BUILD_DIR}/CMakeFiles/gizmosqlserver.dir"
DUCKDB_INC="${IOS_LIB_BUILD_DIR}/third_party/duckdb/include"
if [ -d "${GIZMOSQL_DIR}" ] && [ -d "${DUCKDB_INC}" ]; then
    NEWEST_O=$(find "${GIZMOSQL_DIR}" -name "*.o" -exec stat -f "%m %N" {} \; 2>/dev/null \
        | sort -rn | head -1 | cut -d' ' -f2-)
    if [ -n "${NEWEST_O}" ]; then
        STALE_HDR=$(find "${DUCKDB_INC}" -name "*.hpp" -newer "${NEWEST_O}" -print -quit 2>/dev/null)
        if [ -n "${STALE_HDR}" ]; then
            echo "WARNING: DuckDB headers are newer than cached gizmosqlserver .o files."
            echo "  Newest .o: ${NEWEST_O}"
            echo "  Newer header: ${STALE_HDR}"
            echo "  Nuking ${GIZMOSQL_DIR} to force full recompile against fresh headers."
            rm -rf "${GIZMOSQL_DIR}"
        fi
    fi
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
        # Built directly with iOS toolchain — already iOS-tagged, no retag.
        libgizmosqlserver.a|libssl.a|libcrypto.a|libroaring.a) continue ;;
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
