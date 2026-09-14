# Code quality checks

Install the pinned tools in a virtual environment and install ShellCheck with
your system package manager:

```sh
python3 -m venv .venv-quality
. .venv-quality/bin/activate
python -m pip install -r scripts/quality-requirements.txt
python scripts/check_quality.py format
python scripts/check_quality.py lint
```

Add `--fix` to format changed C/C++ lines using the existing `.clang-format`,
or to apply Ruff's safe Python fixes and formatting. Without `--fix`, checks
are read-only and return a nonzero status on findings. Python checks apply to
entire changed files; shell checks include Bash syntax and ShellCheck errors.

Clang-tidy needs the actual CMake compilation database and generated dependency
headers. Configure and build normally, with the database enabled:

```sh
cmake -S . -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build
python scripts/check_quality.py tidy --jobs 3
```

Checks focus on static-analysis defects, dangling references, moved objects,
RAII misuse, unsafe signal handlers, and concurrency mistakes. They do not
enforce a new naming convention or reorder structures for padding. Startup
environment reads are excluded from the blanket `concurrency-mt-unsafe` check;
configuration still must be resolved before workers start.

The default comparison is the working tree against `HEAD`, including untracked
source files. Use `--base origin/main` to check a branch or `--all` to audit the
whole project. Dependencies and generated code are excluded. Changed headers
cause every project translation unit to be analyzed; diagnostics are filtered
to changed lines for incremental checks. Full audits do not filter diagnostics.
Logs are retained under `build/quality/` (override with `--report`).
Clang-tidy's YAML and text reports retain dependency findings for inspection,
but only project findings fail the lint gate. Compiler errors in dependencies
still fail, because analysis cannot be trusted when a translation unit does not
compile. The diagnostic-scope tests exercise this distinction.

GitHub Actions runs formatting and script checks before build jobs. C++ source,
header, and test changes now trigger builds as well as dependency changes.
Both macOS DuckDB channels run clang-tidy after building and upload diagnostic
logs. These gates supplement integration and driver tests; static analysis
does not establish race freedom or replace sanitizer and runtime testing.

## ThreadSanitizer

Static analysis cannot prove the absence of data races, so a separate GitHub
Actions workflow (`.github/workflows/tsan.yml`) builds the entire server with
ThreadSanitizer and runs the integration suite under it. TSan instruments every
memory access; when two threads touch the same location without a lock or
atomic ordering them, it prints both stack traces the first time the accesses
merely happen close together, without needing the race to cause a failure.
Unsuppressed reports fail the job even when every test assertion passes.

The whole program must be instrumented (Arrow with its bundled gRPC and
protobuf, DuckDB and its extensions, gflags, replxx, SQLite, OpenTelemetry),
otherwise TSan cannot see locks taken inside library code and reports false
positives. The `GIZMOSQL_SANITIZER` CMake option forwards the flags into every
third-party superbuild and turns on Arrow's and DuckDB's own sanitizer
switches. Use a separate build directory; the superbuild input digest includes
the sanitizer, so a switch re-drives the third-party builds:

```sh
cmake -S . -B build-tsan -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DGIZMOSQL_SANITIZER=thread -DGIZMOSQL_ENTERPRISE=ON -DWITH_OPENTELEMETRY=OFF \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
cmake --build build-tsan --target gizmosql_integration_tests
TSAN_OPTIONS="suppressions=$PWD/tests/tsan.supp halt_on_error=0 abort_on_error=0 exitcode=66" \
  ./build-tsan/tests/gizmosql_integration_tests --gtest_filter='-TPCH*:*Benchmark*'
```

Instrumented code runs roughly five to fifteen times slower and uses several
times the memory, so the job is slow by nature, runs only on Linux amd64 with
the stable DuckDB channel, keeps its own caches keyed on the sanitizer, and
skips the TPC-H and benchmark suites, which assert wall-clock limits. On
recent Linux kernels TSan needs `vm.mmap_rnd_bits` at 28 or lower. `abort_on_error=0`
matters on macOS, where TSan otherwise aborts (exit 134) instead of using `exitcode`.
`GIZMOSQL_SANITIZER=address` builds with AddressSanitizer the same way.

`tests/tsan.supp` holds suppressions. Keep it short and justified: every entry
hides a report, so each must name a known-benign pattern in third-party code
with the reason, never a GizmoSQL symbol. A race in `src/` is a bug to fix.

