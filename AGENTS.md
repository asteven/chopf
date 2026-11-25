# AGENTS.md

## Sandbox: set TMPDIR before running tools

This repo is developed inside a `nono` OS-level sandbox that denies writes to
the system `/tmp`. Any tool that creates temp files there fails with a
misleading `Permission denied (os error 13)` at a path like `/tmp/.tmpXXXXXX`.

Always prefix commands that may create temp files with:

```bash
TMPDIR=$PWD/tmp
```

This includes `uv run` (and therefore pytest, ruff, etc. invoked through it):

```bash
TMPDIR=$PWD/tmp uv run --group dev pytest tests -q
TMPDIR=$PWD/tmp uv run --group dev ruff check .
TMPDIR=$PWD/tmp uv run --group dev ruff format --check .
```

The `tmp/` directory at the repo root exists for this purpose; it is not
part of the source tree.

## Imports: order is load-bearing in some `__init__.py` files

`src/chopf/__init__.py`, `src/chopf/cache/__init__.py` and
`src/chopf/controller/__init__.py` have deliberately ordered imports to avoid
circular-import failures. They carry `# ruff: noqa: I001` — do not let import
sorting reorder them (this happened once and broke the package).
