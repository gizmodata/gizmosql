#!/usr/bin/env python3
"""Run the same quality checks locally and in CI, without editing dependencies.

By default check changes relative to HEAD, including untracked source files.
Use --base COMMIT for a branch/PR, --all for an audit, or --fix to format.
"""

import argparse
import concurrent.futures
import difflib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CPP = {".cpp", ".cc", ".c", ".h", ".hpp"}
SCOPES = ("src/", "tests/", "scripts/")


def actionable_diagnostics(diagnostics, root=ROOT):
    """Compiler failures always matter; third-party lint remains in raw reports."""
    result = []
    for diagnostic in diagnostics:
        name = diagnostic["DiagnosticName"]
        filename = diagnostic["DiagnosticMessage"].get("FilePath", "")
        path = Path(filename).resolve() if filename else None
        if (
            name == "clang-diagnostic-error"
            or path is None
            or (
                path.is_relative_to(root / "src")
                or path.is_relative_to(root / "tests/integration")
            )
        ):
            result.append(diagnostic)
    return result


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT).decode()


def changed_files(base, all_files):
    tracked = git("ls-files", "-z").split("\0")
    untracked = git("ls-files", "--others", "--exclude-standard", "-z").split("\0")
    names = (
        tracked
        if all_files
        else git("diff", "--name-only", "--diff-filter=ACMR", "-z", base, "--").split(
            "\0"
        )
    )
    return sorted(
        {
            name
            for name in names + untracked
            if name.startswith(SCOPES) and (ROOT / name).is_file()
        }
    )


def changed_lines(name, base, all_files):
    if all_files or not git("ls-files", "--", name).strip():
        return []  # Empty means the entire file.
    diff = git("diff", "--unified=0", base, "--", name)
    return [
        (int(start), int(start) + int(count or "1") - 1)
        for start, count in re.findall(r"^@@ .* \+(\d+)(?:,(\d+))? @@", diff, re.M)
        if int(count or "1") > 0
    ]


def format_cpp(files, args):
    failed = False
    for name in files:
        if Path(name).suffix not in CPP:
            continue
        cmd = ["clang-format", "--style=file"]
        for start, end in changed_lines(name, args.base, args.all):
            cmd += [f"--lines={start}:{end}"]
        before = (ROOT / name).read_bytes()
        after = subprocess.check_output([*cmd, name], cwd=ROOT)
        if before == after:
            continue
        if args.fix:
            (ROOT / name).write_bytes(after)
        else:
            failed = True
            print(
                "".join(
                    difflib.unified_diff(
                        before.decode().splitlines(True),
                        after.decode().splitlines(True),
                        fromfile=name,
                        tofile=name + " (formatted)",
                    )
                )
            )
    return int(failed)


def lint_scripts(files, args):
    python = [name for name in files if name.endswith(".py")]
    failed = False
    if python:
        failed |= (
            subprocess.call(
                ["ruff", "check", *(["--fix"] if args.fix else []), *python], cwd=ROOT
            )
            != 0
        )
        failed |= (
            subprocess.call(
                ["ruff", "format", *([] if args.fix else ["--check"]), *python], cwd=ROOT
            )
            != 0
        )
    for name in files:
        if name.endswith(".sh"):
            failed |= subprocess.call(["bash", "-n", name], cwd=ROOT) != 0
            failed |= (
                subprocess.call(["shellcheck", "--severity=error", name], cwd=ROOT) != 0
            )
    return int(failed)


def tidy_cpp(files, args):
    build = (ROOT / args.build).resolve()
    database = json.loads((build / "compile_commands.json").read_text())
    sources = sorted(
        {
            str(Path(entry["file"]).resolve())
            for entry in database
            if Path(entry["file"]).is_relative_to(ROOT / "src")
            or Path(entry["file"]).is_relative_to(ROOT / "tests/integration")
        }
    )
    cpp = [name for name in files if Path(name).suffix in CPP]
    if not cpp and not args.all:
        print("No C/C++ changes to analyze.")
        return 0
    # Header changes can affect any TU. Analyze them all and filter diagnostics
    # to changed lines, rather than silently omitting header-only changes.
    if not args.all and not any(Path(name).suffix in {".h", ".hpp"} for name in cpp):
        selected = {str(ROOT / name) for name in cpp}
        sources = [name for name in sources if name in selected]
    cmd = ["clang-tidy", "-p", str(build), "--quiet"]
    # Match the repository's absolute source roots, not a dependency path that
    # happens to contain '/src/' (e.g. build/third_party/src/protobuf).
    cmd += ["--header-filter=" + re.escape(str(ROOT)) + "/(src|tests/integration)/"]
    # Include all owned source lines during a full audit, but never include
    # dependency files just because an analyzer path traverses their headers.
    line_filter = []
    for name in cpp:
        item = {"name": str(ROOT / name)}
        lines = changed_lines(name, args.base, args.all)
        if lines:
            item["lines"] = lines
        line_filter.append(item)
    cmd += ["--line-filter=" + json.dumps(line_filter)]
    if sys.platform == "darwin":
        # Standalone LLVM does not discover Apple's SDK C++ headers reliably.
        sdk = subprocess.check_output(["xcrun", "--show-sdk-path"], text=True).strip()
        cmd += [
            "--extra-arg=-isysroot",
            "--extra-arg=" + sdk,
            "--extra-arg=-isystem",
            "--extra-arg=" + sdk + "/usr/include/c++/v1",
        ]
    report = (ROOT / args.report).resolve()
    report.mkdir(parents=True, exist_ok=True)

    def analyze(source):
        relative = Path(source).relative_to(ROOT)
        output = report / (str(relative).replace("/", "_") + ".log")
        fixes = output.with_suffix(".yaml")
        fixes.unlink(missing_ok=True)
        with output.open("w") as log:
            result = subprocess.run(
                [*cmd, "--export-fixes=" + str(fixes), source],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=300,
            )
        diagnostics = (
            (yaml.safe_load(fixes.read_text()) or {}).get("Diagnostics", [])
            if fixes.exists()
            else []
        )
        findings = actionable_diagnostics(diagnostics)
        # Do not turn a crash, malformed report, or failed compiler invocation
        # into a pass just because there are no source diagnostics to display.
        failed = (
            bool(findings)
            or result.returncode not in (0, 1)
            or (result.returncode != 0 and not diagnostics)
        )
        print(f"{'FAIL' if failed else 'PASS'} {relative}", flush=True)
        if failed:
            print(output.read_text(), flush=True)
        elif diagnostics:
            print(
                f"  {len(diagnostics)} dependency diagnostics retained in {output.name}",
                flush=True,
            )
        return failed

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        results = list(pool.map(analyze, sources))
    return int(any(results))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["format", "lint", "tidy"])
    parser.add_argument("--base", default=os.getenv("QUALITY_BASE", "HEAD"))
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--fix", action="store_true")
    parser.add_argument("--build", default="build")
    parser.add_argument("--report", default="build/quality")
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    files = changed_files(args.base, args.all)
    print(f"Checking {args.mode}: {len(files)} source/script files", flush=True)
    return {"format": format_cpp, "lint": lint_scripts, "tidy": tidy_cpp}[args.mode](
        files, args
    )


if __name__ == "__main__":
    sys.exit(main())
