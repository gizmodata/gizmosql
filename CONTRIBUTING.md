# Contributing to GizmoSQL

Use the same pinned quality tools as CI, add tests for changed behavior, and
update the documentation and changelog before opening a pull request.

## Contributor License Agreement

All contributors must sign a Contributor License Agreement before their first
contribution can be merged. Sign the
[Individual CLA](CLA.md) if you are contributing on your own behalf, or have
your employer sign the [Corporate CLA](CLA-CORPORATE.md) if you are
contributing as part of your job. Both are the Apache Software Foundation's
contributor agreements adapted for GizmoData LLC: you keep ownership of your
work and grant GizmoData and downstream recipients a copyright and patent
license to it.

The `CLA` check runs on every pull request and must pass before merge. On your
first pull request the bot comments with instructions: to sign the Individual
CLA, read it and reply on the pull request with exactly

> I have read the CLA Document and I hereby sign the CLA

and the check turns green for that and all later pull requests. For the
Corporate CLA, your employer emails the completed agreement to
info@gizmodata.com and a maintainer records the designated contributors. Do
not post private contact details or employer documents in public PR comments.

## Set up formatting and linting

Use Python 3.12 or newer. From the repository root:

```sh
python3 -m venv .venv-quality
. .venv-quality/bin/activate
python -m pip install -r scripts/quality-requirements.txt
```

On Windows, activate with `.venv-quality\Scripts\Activate.ps1` in PowerShell.
Use Git Bash to run the shell checks. Install ShellCheck separately:

```sh
# macOS
brew install shellcheck

# Debian / Ubuntu
sudo apt-get install shellcheck
```

The requirements file pins clang-format, clang-tidy, and Ruff. Keep these
versions in sync with CI; a different formatter version can produce a different
result even with the same configuration.

## Format and lint your changes

```sh
# Apply C/C++ formatting using the existing .clang-format style.
python scripts/check_quality.py format --fix

# Apply safe Python lint fixes and Python formatting.
# Shell errors are reported for you to fix manually.
python scripts/check_quality.py lint --fix

# Verify without modifying files.
python scripts/check_quality.py format
python scripts/check_quality.py lint
git diff --check
```

By default these commands compare your working tree with `HEAD`, including
untracked source files. **After committing, compare your whole branch with its
base** so the check includes your committed changes:

```sh
python scripts/check_quality.py format --base origin/main
python scripts/check_quality.py lint --base origin/main
```

C/C++ formatting is limited to changed lines and their surrounding syntactic
constructs. Python linting and formatting check complete changed files. Shell
scripts receive Bash syntax checks and ShellCheck checks at error severity.
Generated files and third-party sources are outside the check scope.

Use `--all` for a repository-wide audit. Existing files may have older formatting
debt; keep unrelated mass-formatting changes out of a feature or bug-fix PR.
Review the diff after any automatic fix.

## Build and run clang-tidy

Build prerequisites include CMake 3.30 or newer, Ninja, a C++20 compiler,
Boost.ProgramOptions, and OpenSSL 3.x. For example:

```sh
# macOS
brew install cmake ninja boost openssl@3

# Debian / Ubuntu (use a compiler and CMake meeting the requirements above)
sudo apt-get install build-essential cmake ninja-build libboost-program-options-dev libssl-dev
```

The first build downloads and compiles Arrow, DuckDB, gRPC, and other dependencies
and can take substantially longer than subsequent builds. Clang-tidy needs
the real compiler options, generated headers, and dependency headers; running
it on an isolated source file without that context is insufficient.

```sh
cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build

python scripts/check_quality.py tidy --jobs 3
# For committed branch changes:
python scripts/check_quality.py tidy --base origin/main --jobs 3
```

The `.clang-tidy` configuration focuses on correctness and selected concurrency
checks. Enabled findings fail the check. Fix the cause; use a narrowly scoped
`NOLINT(check-name)` with an explanation only for a verified false positive.
Do not disable an entire check simply to make a PR pass.

A changed header causes all project translation units to be analyzed, with
diagnostics limited to changed lines. `--all` removes that diagnostic filter.
Logs are written to `build/quality/`. See [Code quality checks](docs/code_quality.md)
for implementation details and the documented check exclusions.

## Test behavior

For manual testing, run `./build/gizmosql_server --password local_test_password`
from the repository root; add `--database-filename local_test.duckdb` for a
persistent database, or use `--help` to inspect startup options.

Add integration coverage under `tests/integration/`, follow the CRTP fixture
pattern in `test_server_fixture.h`, and register new test files in
`tests/CMakeLists.txt`. Use unique ports and clean up test-owned databases.

```sh
cmake --build build --target gizmosql_integration_tests
cd build
./tests/gizmosql_integration_tests --gtest_filter='*YourFeature*'
./tests/gizmosql_integration_tests
```

Some integration tests require PostgreSQL, MinIO, or an Enterprise test license.
Check test output for skips and state which dependencies and licensed cases you
actually exercised. Never commit license files, signing keys, credentials, or
local `.env` files. Changes to Flight SQL execution should also exercise the
released ADBC, JDBC, and ODBC clients, including prepared-statement reuse and
transaction behavior.

Python end-to-end tests also use released ADBC packages. Install their dependencies
in a separate environment, start a test server, and run the relevant scripts:

```sh
python -m pip install adbc-driver-gizmosql pyarrow geopandas shapely duckdb
python tests/test_geoarrow.py
python tests/test_bulk_ingest.py
```

Use the connection environment variables documented in each script. The
additional driver compatibility tests under `tests/drivers/` document their
artifact paths and configuration at the top of each file.

For concurrency changes, test cancellation and shutdown as well as successful
requests. Keep locks scoped to the affected session or object and keep network
I/O and expensive work outside shared locks wherever possible. Formatting and
static analysis supplement runtime testing; they do not prove race freedom.

## Construct SQL safely

**Bind data values instead of concatenating them into SQL.** For example, the
DuckDB C++ connection accepts explicitly typed parameter values:

```cpp
auto result = connection.Query(
    "SELECT * FROM postgres_query(?, 'SELECT 1')", duckdb::Value(catalog_name));
```

Use the relevant driver's prepared-statement/bind API for client SQL. Catalog
names used as table-function arguments are values and should also be bound.
Add regression tests containing quotes, semicolons, comment markers, and Unicode.

SQL identifiers such as table or schema names generally cannot be bound. Use
a fixed allowlist where practical; otherwise quote each identifier component
correctly, including embedded quote characters. Do not treat an identifier as
a string literal. If a statement's parser cannot accept parameters, document
that limitation and use a centralized, tested literal-quoting helper. Never
substitute an unescaped value into SQL.

## Documentation and pull requests

- Add every user-facing change to `CHANGELOG.md` under `Unreleased`.
- Update relevant documentation in `docs/` and CLI help for changed options.
- Resolve environment-variable fallbacks in `src/common/gizmosql_library.cpp`,
  so library callers and CLI users behave consistently. Keep both startup
  scripts' environment-variable tables in sync.
- Document public library API changes in `gizmosql_library.h`.
- Put Enterprise implementations under `src/enterprise/`, guard them with
  `GIZMOSQL_ENTERPRISE`, and test license rejection as well as licensed use.
- Describe the behavior change, tests run, skipped cases, and remaining risks
  in the PR. Preserve the independence of query, authentication, and global
  log-level controls.

## What CI runs

GitHub Actions checks formatting and script linting before the build jobs.
Source and test changes trigger builds. Both macOS DuckDB channels run
clang-tidy against their actual compilation databases and upload diagnostics.
The existing platform integration tests continue to run as well.

Run the local commands above before pushing. If CI reports a formatting or lint
failure, reproduce it using the same comparison commit shown in the job and the
pinned tool versions, then fix and re-run the checks.
