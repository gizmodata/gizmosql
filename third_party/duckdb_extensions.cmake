# DuckDB extensions to build as static (linked into the binary).
# These are available without INSTALL/LOAD at runtime.

# In-tree extensions (bundled with DuckDB)
duckdb_extension_load(icu)
duckdb_extension_load(tpch)

# Out-of-tree extensions — fetched from GitHub during the DuckDB build.
# Required for the "lakehouse in your pocket" iOS experience.
# Pinned commits match DuckDB's own extension config to ensure
# compatibility with the bundled DuckDB version.
#
# iOS-only: iOS is compiled with DISABLE_EXTENSION_LOAD (for App Store
# compliance with Guideline 2.5.2 — no runtime code loading), so these
# extensions MUST be statically linked to be usable at all. On other
# platforms we leave them as normal runtime-loadable extensions, which
# avoids pulling in new build-time dependencies (libcurl, libpq,
# Azure SDK, etc.) and keeps the CI green on Linux/macOS/Windows.
#
# Note: APPLY_PATCHES is intentionally omitted. DuckDB's official build
# uses APPLY_PATCHES which expects patches at
# <duckdb_repo>/.github/patches/extensions/<name>/, but we don't ship
# those patches. The pinned commits work correctly without them.
#
# postgres_scanner also has no upstream static target
# (build_loadable_extension only), so ios/scripts/build-ios-libs.sh
# patches its CMakeLists to add one.
if(GIZMOSQL_IOS)
    # Commits below must match DuckDB's own out-of-tree extension pins
    # for the bundled DuckDB version — see
    # <duckdb_repo>/.github/config/extensions/*.cmake. When bumping DuckDB
    # in third_party/DuckDB_CMakeLists.txt.in, sync these commits too.
    duckdb_extension_load(ducklake
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG 415a9ebdbd73db50a8c6ba703eb733ed16bcf33a
    )

    duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG 13e18b3c9f3810334f5972b76a3acc247b28e537
    )

    duckdb_extension_load(postgres_scanner
        GIT_URL https://github.com/duckdb/duckdb-postgres
        GIT_TAG c89234f0b1985f4ee0f52f16e742a1ab2d4ae4f0
        SUBMODULES database-connector
    )
endif()

# NOTE: azure extension is NOT included because it requires the Azure
# SDK for C++ (azure-identity-cpp, azure-storage-blobs-cpp,
# azure-storage-files-datalake-cpp). DuckDB normally fetches these via
# vcpkg, which we don't have set up. Cross-compiling the Azure SDK for
# iOS (and its transitive deps: libxml2, libcurl, etc.) is a multi-day
# effort that we're deferring to a future release.
