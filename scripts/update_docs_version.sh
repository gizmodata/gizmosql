#!/usr/bin/env bash
#
# update_docs_version.sh — Sync the example GizmoSQL version strings in the
# Quick Start docs (docs/quickstart.md) to the latest released version.
#
# These are illustrative output snippets (the server startup banner and the
# GIZMOSQL_VERSION() result), so they should always reflect the current
# release. This script is run automatically by the `sync-docs-version` CI job
# on every tag push, and can also be run by hand:
#
#     scripts/update_docs_version.sh            # uses the latest git tag
#     scripts/update_docs_version.sh v1.28.0    # explicit version
#
# It only rewrites the version token in two well-known lines, so it is safe to
# re-run and idempotent.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
QUICKSTART="${REPO_ROOT}/docs/quickstart.md"

# Resolve the target version: explicit arg, else the latest git tag.
VERSION="${1:-$(git -C "${REPO_ROOT}" describe --tags --abbrev=0)}"
VERSION="v${VERSION#v}"  # normalize so both "1.28.0" and "v1.28.0" work

if [[ ! "${VERSION}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: '${VERSION}' is not a vMAJOR.MINOR.PATCH version" >&2
  exit 1
fi

# 1. Server startup banner: "GizmoSQL server version: vX.Y.Z - with engine..."
sed -i.bak -E \
  "s|(GizmoSQL server version: )v[0-9]+\.[0-9]+\.[0-9]+|\1${VERSION}|" \
  "${QUICKSTART}"

# 2. GIZMOSQL_VERSION() result cell. The column is 18 chars wide and
#    left-aligned (1 leading + 1 trailing space inside the 20-wide box cell),
#    so regenerate the whole line with printf to keep the box drawing aligned.
CELL="$(printf '%-18s' "${VERSION}")"
sed -i.bak -E \
  "s|^│ v[0-9]+\.[0-9]+\.[0-9]+ +│ Core               │$|│ ${CELL} │ Core               │|" \
  "${QUICKSTART}"

rm -f "${QUICKSTART}.bak"

echo "Updated ${QUICKSTART#${REPO_ROOT}/} to ${VERSION}"
