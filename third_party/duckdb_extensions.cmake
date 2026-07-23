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
        GIT_TAG d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4
    )

    duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG 827222fb45a043a7a852d1f7aae46901492a3cda
    )

    # postgres_scanner: intentionally pinned to the DuckDB v1.5.2-era
    # commit (c89234f0...), NOT DuckDB's official v1.5.5 pin
    # (41223e515...). The newer pin removed the vendored libpq source
    # tree from the extension repo and now relies on
    # `find_package(PostgreSQL REQUIRED)` — it expects an externally
    # cross-compiled libpq, which would be a multi-day effort to set
    # up for the iOS toolchain. The older pin vendors libpq and
    # `database-connector` submodules and builds them from source.
    # DuckDB 1.5.2 → 1.5.5 are patch releases with ABI compatibility,
    # so the older extension links cleanly against DuckDB v1.5.5.
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
