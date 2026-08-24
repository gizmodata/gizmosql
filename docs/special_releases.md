# Special (pinned-DuckDB) releases

Occasionally a customer needs a GizmoSQL release built against a specific
DuckDB version that differs from the current stable channel — for example to
stay on the DuckDB (and therefore `ducklake` / `postgres_scanner` / `httpfs`
extension) version their other tooling is pinned to.

A **special release** is a normal release build cut from a branch whose
`CMakeLists.txt` pins a different `DUCKDB_STABLE_VERSION`. It publishes its own
artifacts but **never displaces the regular release**.

## Naming

The tag name is the base version plus a `-duckdb<version>` suffix:

```
v1.36.1-duckdb1.5.4
```

CI treats any tag whose name contains `-duckdb` as special
(`contains(github.ref_name, '-duckdb')` in `.github/workflows/ci.yml`).

## What CI does differently for a special tag

| Regular tag (`v1.36.1`) | Special tag (`v1.36.1-duckdb1.5.4`) |
|---|---|
| Docker `gizmodata/gizmosql:v1.36.1` **and** `:latest` (+ `-slim`, `-adbc`) | Docker `gizmodata/gizmosql:v1.36.1-duckdb1.5.4` (+ `-slim`, `-adbc`) only — **`latest` untouched** |
| GitHub release marked *Latest* | GitHub release marked **pre-release, not latest** |
| Homebrew tap updated | **skipped** |
| `gizmosql-py` / PyPI release triggered | **skipped** |
| iOS TestFlight upload | **skipped** |
| CLI zips, MSI, signed binaries, SBOM/attestations | same |

## Cutting one

1. Branch from the release you want to pin: `git checkout -b release/v1.36.1-duckdb1.5.4 v1.36.1`
2. In `CMakeLists.txt`, set `DUCKDB_STABLE_VERSION` to the requested DuckDB tag.
3. In `.github/workflows/ci.yml`, set the stable `DUCKDB_VERSION` Docker build-arg
   (the bundled DuckDB CLI) to the same version.
4. If the DuckDB version differs from the one the iOS extension pins were synced
   to, re-sync the `ducklake` / `httpfs` pins in `third_party/duckdb_extensions.cmake`
   to DuckDB's own pins for that version.
5. Add a `## [1.36.1-duckdb1.5.4] - YYYY-MM-DD` section to `CHANGELOG.md` (the
   release notes are extracted from the section matching the tag).
6. Commit, tag, push: `git push origin release/v1.36.1-duckdb1.5.4 v1.36.1-duckdb1.5.4`.

`gizmosql_server --print-duckdb-version` confirms the linked DuckDB at runtime.
