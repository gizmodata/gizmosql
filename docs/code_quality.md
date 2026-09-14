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
