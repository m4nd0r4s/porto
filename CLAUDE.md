# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:46cd31e7 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/core-concepts/sync-concepts.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   bd dolt push
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->


## What this is

**Porto** is an async runtime for Python written in Rust (and a usable Rust
runtime of its own). **Douro** is an HTTP framework built on Porto. Both are
working names. The complete specification is in `docs/spec/` (start with
`docs/spec/README.md`); it is normative (MUST/SHOULD), and code follows it.
When code and spec disagree, fix one of them in the same change.

Nothing is implemented yet. The roadmap is in beads: one epic per milestone
(M0…M9), plus `Housekeeping`, `Decisions` and `External: oxiform`. Every bead
names its spec section (`bd show <id>` → `Spec:`).

```bash
bd ready                     # what can start now (milestone order is enforced by deps)
bd list --label milestone:m1 # one milestone
bd show <id>                 # description, acceptance, spec section, deps
```

Order matters: M0 is a spike with a kill criterion (`spawn/switch >= 2x
asyncio+uvloop`, else redesign). Nothing in M1 starts until that gate closes.
Douro (M6) starts only after M3, and needs oxiform's OX-1…OX-5.

## Build & Test

Layout: `docs/spec/50-engineering/50-repository-layout.md`; checks:
`docs/spec/50-engineering/51-build-ci-release.md`. Rust is pinned by
`rust-toolchain.toml`; Python tooling comes from uv (the root `pyproject.toml`
is a dev-only workspace, the `porto` wheel is `packaging/porto/pyproject.toml`).

**The check list.** These are the checks CONTRIBUTING, the PR template and CI
run; a change is ready when all of them pass:

```bash
uv sync                                               # dev tools + an editable porto in .venv
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
uv run ruff check
uv run ruff format --check
uv run pyright                                        # strict, python/ + tests/typing/ (ADR-028)
uv run pytest -q                                      # tests/python/ + tests/typing/
```

Tool settings live in the root `pyproject.toml` (ruff, pyright, pytest) and
`.editorconfig`. `tests/typing/should_fail/` holds samples that MUST fail
pyright strict; `tests/typing/test_should_fail.py` proves they do, so keep
them out of the main pyright run.

While iterating:

```bash
# Rebuild the editable porto after Rust changes (faster than `uv sync`)
uv run --directory packaging/porto maturin develop
uv run python -c 'import porto; print(porto.__version__)'
uv run ruff format                                    # apply formatting

# A wheel for one interpreter (per-version, no abi3: ADR-024)
uv run --directory packaging/porto maturin build -i python3.11 --out ../../dist

# Another interpreter (3.11, 3.14t, ...) in its own venv
uv venv -p 3.14t .venv314t
(cd packaging/porto && VIRTUAL_ENV=$PWD/../../.venv314t ../../.venv/bin/maturin develop --uv)
```

maturin runs from `packaging/porto` (its `-m` flag takes a `Cargo.toml`, not a
`pyproject.toml`). `uv.lock` is not committed.

## Conventions

porto follows [kinemo](https://github.com/m4nd0r4s/kinemo) and
[hapto](https://github.com/m4nd0r4s/hapto):

- **Personal GitHub only.** Remote `https://github.com/m4nd0r4s/porto.git` (private for now);
  commits as `M4nd0r4s <25236463+m4nd0r4s@users.noreply.github.com>` (set in
  this repo's git config). Never the work identity.
- **English everywhere:** code, comments, docs, commit messages, beads.
- **Descriptive names**, **small modules**, **typed public API** (pyright
  strict, `.pyi` stubs).
- **Every error says what to do**, and every new mistake a user can make gets
  a test that triggers it.
- **Commits** are small and focused, with a short imperative message
  ("Add the timer wheel's level cascade").
- Dual license MIT OR Apache-2.0 (ADR-029, accepted).

## Working with several agents

- One agent per bead: `bd update <id> --claim` before starting, so two agents
  never take the same one.
- Each agent works in its own git worktree and branch; merge through a PR.
- New work found on the way becomes a bead
  (`bd create ... --deps discovered-from:<id>`), not a TODO in the code.
- A spec gap becomes a bead in the milestone it blocks, or a `decision` in the
  Decisions epic.
