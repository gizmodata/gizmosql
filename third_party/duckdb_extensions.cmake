# DuckDB extensions to build as static (linked into the binary).
# These are available without INSTALL/LOAD at runtime.
duckdb_extension_load(icu)
duckdb_extension_load(tpch)
