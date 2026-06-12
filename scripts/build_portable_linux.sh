#!/usr/bin/env bash
# Build portable Linux gizmosql binaries with a glibc 2.28 baseline.
#
# Runs INSIDE a quay.io/pypa/manylinux_2_28_{x86_64,aarch64} container
# (AlmaLinux 8, glibc 2.28, gcc-toolset-14 on PATH). Binaries produced here
# run out of the box on any distro with glibc >= 2.28: Raspberry Pi OS
# (bullseye/bookworm), Amazon Linux 2023, Ubuntu 20.04+, Debian 11+, etc.
# gcc-toolset statically links the newer-than-EL8 libstdc++/libgcc pieces,
# so the binary's dynamic GLIBCXX requirement stays at the EL8 baseline.
#
# Usage (from the repo root; mount the workspace at the SAME path as the
# host and pass it via REPO_ROOT, so absolute paths baked into artifacts —
# e.g. the client path compiled into the integration test binary — stay
# valid when tests run on the host afterwards):
#   docker run --rm -v "$PWD":"$PWD" -e REPO_ROOT="$PWD" \
#     quay.io/pypa/manylinux_2_28_$(uname -m) \
#     bash "$PWD/scripts/build_portable_linux.sh" <duckdb_channel> [build_dir]
#
#   duckdb_channel : stable | lts
#   build_dir      : CMake build dir relative to the repo root
#                    (default: build)
#
# Dependency prefixes (static OpenSSL 3 + static Boost program_options) are
# built into <build_dir>/portable-deps with stamp files, so caching the build
# dir across CI runs skips the ~8 min dependency build.
set -euxo pipefail

DUCKDB_CHANNEL="${1:-stable}"
BUILD_DIR="${2:-build}"
REPO_ROOT="${REPO_ROOT:-/work}"
DEPS_PREFIX="${REPO_ROOT}/${BUILD_DIR}/portable-deps"
NPROC=$(nproc)

OPENSSL_VERSION=3.5.1
BOOST_VERSION=1.89.0

cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# System packages (headers only / build tools — nothing here ends up as a
# runtime dependency except libcurl.so.4, which every target distro ships).
# ---------------------------------------------------------------------------
dnf install -y --setopt=install_weak_deps=False \
    ninja-build \
    flex \
    libcurl-devel \
    zlib-devel \
    perl-IPC-Cmd \
    perl-Pod-Html

# ---------------------------------------------------------------------------
# Static OpenSSL 3 (cpp-httplib v0.16+ and jwt-cpp need >= 3.0; EL8 ships
# 1.1.1). no-shared so the final binaries carry no libssl/libcrypto runtime
# dependency.
# ---------------------------------------------------------------------------
if [ ! -f "${DEPS_PREFIX}/.openssl-${OPENSSL_VERSION}.stamp" ]; then
    rm -rf /tmp/openssl-src
    mkdir -p /tmp/openssl-src
    curl -fsSL "https://github.com/openssl/openssl/releases/download/openssl-${OPENSSL_VERSION}/openssl-${OPENSSL_VERSION}.tar.gz" \
        | tar -xz -C /tmp/openssl-src --strip-components=1
    pushd /tmp/openssl-src
    ./config no-shared no-tests no-docs --prefix="${DEPS_PREFIX}" --libdir=lib
    make -j"${NPROC}" build_libs
    make install_dev
    popd
    rm -rf /tmp/openssl-src
    touch "${DEPS_PREFIX}/.openssl-${OPENSSL_VERSION}.stamp"
fi

# ---------------------------------------------------------------------------
# Boost: headers + static program_options (the only compiled Boost lib we
# link; uuid/algorithm are header-only). Built from source because EL8's
# boost 1.66 predates BoostConfig.cmake, which our CMake (>= 4, no FindBoost
# module) requires.
# ---------------------------------------------------------------------------
if [ ! -f "${DEPS_PREFIX}/.boost-${BOOST_VERSION}.stamp" ]; then
    BOOST_UNDER=${BOOST_VERSION//./_}
    rm -rf /tmp/boost-src
    mkdir -p /tmp/boost-src
    curl -fsSL "https://archives.boost.io/release/${BOOST_VERSION}/source/boost_${BOOST_UNDER}.tar.gz" \
        | tar -xz -C /tmp/boost-src --strip-components=1
    pushd /tmp/boost-src
    ./bootstrap.sh --with-libraries=program_options --prefix="${DEPS_PREFIX}"
    ./b2 -j"${NPROC}" link=static variant=release install
    popd
    rm -rf /tmp/boost-src
    touch "${DEPS_PREFIX}/.boost-${BOOST_VERSION}.stamp"
fi

# ---------------------------------------------------------------------------
# Configure + build. gcc-toolset-14 is already first on PATH in the
# manylinux image; CMAKE_PREFIX_PATH points the build at the static deps.
# ---------------------------------------------------------------------------
SCCACHE_ARGS=()
if command -v sccache >/dev/null 2>&1; then
    SCCACHE_ARGS=(
        -DCMAKE_C_COMPILER_LAUNCHER=sccache
        -DCMAKE_CXX_COMPILER_LAUNCHER=sccache
    )
fi

cmake -B "${BUILD_DIR}" -G Ninja \
    -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}" \
    -DCMAKE_PREFIX_PATH="${DEPS_PREFIX}" \
    -DOPENSSL_ROOT_DIR="${DEPS_PREFIX}" \
    -DOPENSSL_USE_STATIC_LIBS=TRUE \
    -DCMAKE_EXE_LINKER_FLAGS="-L${DEPS_PREFIX}/lib" \
    -DGIZMOSQL_ENTERPRISE=ON \
    -DWITH_OPENTELEMETRY=ON \
    -DGIZMOSQL_DUCKDB_CHANNEL="${DUCKDB_CHANNEL}" \
    "${SCCACHE_ARGS[@]}"

cmake --build "${BUILD_DIR}"

# ---------------------------------------------------------------------------
# Portability self-check: fail the build if the produced binaries require
# glibc newer than 2.28 or any GLIBCXX newer than EL8's system libstdc++
# (gcc 8 = GLIBCXX_3.4.25, CXXABI_1.3.11) — i.e. if the baseline ever
# silently regresses.
# ---------------------------------------------------------------------------
check_baseline() {
    local bin="$1"
    local bad
    bad=$( (objdump -T "$bin"; objdump -p "$bin") | grep -oE 'GLIBC_2\.[0-9]+(\.[0-9]+)?' | sort -Vu | awk -F'GLIBC_' '$2+0 > 0 {split($2,v,"."); if (v[1] > 2 || (v[1] == 2 && v[2] > 28)) print "GLIBC_" $2}' || true)
    local badxx
    badxx=$( (objdump -T "$bin"; objdump -p "$bin") | grep -oE 'GLIBCXX_3\.4\.[0-9]+' | sort -Vu | awk -F'GLIBCXX_3.4.' '$2+0 > 25 {print "GLIBCXX_3.4." $2}' || true)
    if [ -n "${bad}${badxx}" ]; then
        echo "ERROR: $bin requires symbols above the glibc 2.28 / EL8 baseline:" >&2
        echo "${bad}" "${badxx}" >&2
        exit 1
    fi
    echo "OK: $bin is glibc 2.28 baseline clean"
}

for bin in "${BUILD_DIR}"/gizmosql_server* "${BUILD_DIR}"/gizmosql_client*; do
    [ -f "$bin" ] && [ -x "$bin" ] && check_baseline "$bin"
done
