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
# Note: APPLY_PATCHES is intentionally omitted. DuckDB's official build
# uses APPLY_PATCHES which expects patches at
# <duckdb_repo>/.github/patches/extensions/<name>/, but we don't ship
# those patches. The pinned commits work correctly without them in
# practice.
duckdb_extension_load(postgres_scanner
    GIT_URL https://github.com/duckdb/duckdb-postgres
    GIT_TAG a42c490df0019406658073c003b7d89dd4338466
)

duckdb_extension_load(ducklake
    GIT_URL https://github.com/duckdb/ducklake
    GIT_TAG 7ea15644fd5f5ff42b86b8a703c14172acc7b8bd
)

duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 7e86e7a5e5a1f01f458361bebdfa9b0a9a73a619
)

# NOTE: azure extension is NOT included because it requires the Azure
# SDK for C++ (azure-identity-cpp, azure-storage-blobs-cpp,
# azure-storage-files-datalake-cpp). DuckDB normally fetches these via
# vcpkg, which we don't have set up. Cross-compiling the Azure SDK for
# iOS (and its transitive deps: libxml2, libcurl, etc.) is a multi-day
# effort that we're deferring to a future release.
