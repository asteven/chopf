# AGENTS.md

## Sandbox: check for nono, then set TMPDIR

Some developers run agents inside a `nono` OS-level sandbox that denies writes
to the system `/tmp`. Detect this before running tools: if a command fails with
a misleading `Permission denied (os error 13)` at a path like `/tmp/.tmpXXXXXX`,
or `nono why --self --path /tmp --op write` reports a denial, you are inside
nono and must redirect temp files into the repo:

```bash
TMPDIR=$PWD/.tmp
```

Prefix every command that may create temp files — this includes `uv run` (and
therefore pytest, ruff, etc. invoked through it):

```bash
TMPDIR=$PWD/.tmp uv run --group dev pytest tests -q
TMPDIR=$PWD/.tmp uv run --group dev ruff check .
TMPDIR=$PWD/.tmp uv run --group dev ruff format --check .
```

The `.tmp/` directory at the repo root exists for this purpose; it is
gitignored and not part of the source tree. If nono is not present, no prefix is needed.

## Imports: order is load-bearing in some `__init__.py` files

`src/chopf/__init__.py`, `src/chopf/cache/__init__.py` and
`src/chopf/controller/__init__.py` have deliberately ordered imports to avoid
circular-import failures. They carry `# ruff: noqa: I001` — do not let import
sorting reorder them (this happened once and broke the package).
