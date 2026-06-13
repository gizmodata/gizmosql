# Viewing Documentation Locally

The documentation site is built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/).
The configuration lives in `mkdocs.yml` at the repo root; the page sources are
the Markdown files in this directory.

## Quick Start

```bash
# Install MkDocs Material (one time, ideally in a virtualenv)
pip install 'mkdocs-material>=9,<10'

# Serve with live reload (run from the repo root)
mkdocs serve
```

Then open: http://localhost:8000

Changes to the Markdown files reload in the browser automatically.

## Building the static site

```bash
# From the repo root — outputs to site/ (gitignored)
mkdocs build --strict
```

`--strict` fails the build on broken internal links and anchors; CI runs the
same command, so build locally before pushing docs changes.

## Adding a new page

1. Add the Markdown file to `docs/`
2. Add it to the `nav:` section of `mkdocs.yml`
3. If it's a published page, add it to `docs/llms.txt` (the AI-crawler index)

## Deployment

Pushing docs changes to `main` triggers `.github/workflows/deploy-docs.yml`,
which builds the site and deploys it to GitHub Pages at
https://docs.gizmosql.com. The raw Markdown files are published alongside the
rendered HTML (e.g. `https://docs.gizmosql.com/quickstart.md`) for AI crawlers.
