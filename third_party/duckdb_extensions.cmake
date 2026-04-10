# DuckDB extensions to build as static (linked into the binary).
# These are available without INSTALL/LOAD at runtime.

# In-tree extensions (bundled with DuckDB)
duckdb_extension_load(icu)
duckdb_extension_load(tpch)

# Out-of-tree extensions — fetched from GitHub during the DuckDB build.
# Required for the "lakehouse in your pocket" iOS experience.
# Pinned commits match DuckDB's own extension config to ensure
# compatibility with the bundled DuckDB version.
duckdb_extension_load(postgres_scanner
    GIT_URL https://github.com/duckdb/duckdb-postgres
    GIT_TAG a42c490df0019406658073c003b7d89dd4338466
    APPLY_PATCHES
)

duckdb_extension_load(ducklake
    GIT_URL https://github.com/duckdb/ducklake
    GIT_TAG 7ea15644fd5f5ff42b86b8a703c14172acc7b8bd
    APPLY_PATCHES
)
