# 51 — Build, CI, Release

## 1. Build
- `maturin` ≥ 1.x builds both wheels. `porto` contains `porto._core` + `python/porto`, and `douro` contains `douro._native` + `python/douro` + a console script `douro`.
- Rust profile `release`: `lto = "fat"`, `codegen-units = 1`, `panic = "unwind"`, `opt-level = 3`. Profile `dist` adds PGO (§3).
- Allocator: mimalloc as `#[global_allocator]` for Rust-side allocations (feature, default on).
- Wheels: CPython 3.11–3.14 and 3.14t (and later t builds once released) × manylinux_2_28 (x86_64, aarch64), musllinux_1_2 (x86_64, aarch64), macOS (arm64, x86_64), and Windows (x86_64, arm64). Built with `cibuildwheel` or `maturin-action`.
- The douro wheel depends on `porto>=X.Y,<X.(Y+1)` and checks ABI compatibility at import via the capsule's `abi_major`.

## 2. CI (GitHub Actions)
| Job | Content |
|---|---|
| lint | `cargo fmt --check`, `clippy -D warnings`, `ruff`, `pyright --strict` on python/ and tests/typing, `scripts/check_borrowed_refs.py` (§3 of 25), `scripts/check_lock_across_python.py` (P3 of 25) |
| rust-test | `cargo nextest` (Linux/macOS/Windows), `--exclude porto-py douro-py` + with them via `pyo3` test harness |
| loom | `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release` (no cargo feature; `52` item 2 lists the models), through `scripts/run_loom.py`, one process per model. Pull requests that touch `crates/porto-core` or `crates/porto-io` run it with `LOOM_MAX_PREEMPTIONS=2`; a nightly workflow runs every model at its full bound with `LOOM_CHECKPOINT_FILE`, resuming searches the run's time budget cut short. A failure uploads the model's log and a rerun with loom's trace (`LOOM_LOG=trace`) |
| sanitizers | TSAN/ASAN builds of porto-core + Python free-threaded TSAN build of CPython for the porto-py tests (nightly) |
| py-test | pytest against the installed wheel (built `--release`, one per OS and Python, no abi3; the tests never import the source tree) on Linux x86_64 (`ubuntu-latest`), Linux arm64 (`ubuntu-24.04-arm`), macOS arm64 (`macos-15`) and Windows (`windows-latest`). Pull requests run the oldest and newest Python (3.11, 3.14) on each; pushes to main and manual runs run 3.11–3.14. Still to add: 3.14t (`porto-j62.20`), Linux io_uring (privileged runner, `porto-99f.23`) |
| py-test-debug | pytest (Linux, 3.14) against a debug build of porto on a debug CPython (Py_DEBUG), so the `debug_assert`s of porto-core and porto-py and CPython's own object checks (the collector validates every object it starts tracking) run under the Python tests; the examples test fails on any stderr output, where a panic in a drop shows |
| deterministic | 1000 seeds of the deterministic suite |
| compat | asyncpg/redis/httpx/aiohttp test suites under porto (services in containers) |
| conformance | h2spec, Autobahn |
| perf | nightly on the reference machine (`53` §1, ADR-033: a local script on the maintainer's Mac for now); regressions > 5% fail; results committed to `bench/results` |
| soak | weekly 72 h |
| audit | `cargo deny check` (`deny.toml`: permissive licenses only, aws-lc excepted for its OpenSSL-style notice; no `openssl`/`openssl-sys`/`native-tls` (ADR-026); one major of `tokio` and `hyper`; crates.io only; RustSec advisories), `cargo audit --deny warnings` (`.cargo/audit.toml`) and `pip-audit` over the dev dependencies (`uv export`, since `uv.lock` is not committed). Runs on pull requests that touch a manifest or the policy files, on pushes to main with the same paths, and daily; not a required check. An ignored advisory needs an id and a reason in both files |

## 3. PGO (dist profile)
Train with `bench/http` JSON and plaintext scenarios + `bench/micro` task benchmarks, merge profiles, and rebuild. Applied to release wheels only.

## 4. Release
- SemVer. 0.x until M8. Changelog via towncrier fragments.
- PyPI Trusted Publishing and crates.io (`porto`, `porto-abi`, `porto-pyo3` crates are published; internals have `publish = false` until stable).
- ABI policy: §21 B.3.
