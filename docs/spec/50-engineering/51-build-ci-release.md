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
| lint | `cargo fmt --check`, `clippy -D warnings`, `ruff`, `pyright --strict` on python/ and tests/typing |
| rust-test | `cargo nextest` (Linux/macOS/Windows), `--exclude porto-py douro-py` + with them via `pyo3` test harness |
| loom | `RUSTFLAGS="--cfg loom" cargo test -p porto-core --features loom` (nightly-scheduled heavy run) |
| sanitizers | TSAN/ASAN builds of porto-core + Python free-threaded TSAN build of CPython for the porto-py tests (nightly) |
| py-test | pytest matrix: 3.11–3.14, 3.14t; Linux epoll, Linux io_uring (privileged runner), macOS, Windows |
| py-test-debug | pytest (Linux, 3.14) against a debug build of porto, so the `debug_assert`s of porto-core and porto-py run under the Python tests; the examples test fails on any stderr output, where a panic in a drop shows |
| deterministic | 1000 seeds of the deterministic suite |
| compat | asyncpg/redis/httpx/aiohttp test suites under porto (services in containers) |
| conformance | h2spec, Autobahn |
| perf | nightly on the reference machine (`53` §1, ADR-033: a local script on the maintainer's Mac for now); regressions > 5% fail; results committed to `bench/results` |
| soak | weekly 72 h |
| audit | `cargo deny`, `cargo audit`, `pip-audit` |

## 3. PGO (dist profile)
Train with `bench/http` JSON and plaintext scenarios + `bench/micro` task benchmarks, merge profiles, and rebuild. Applied to release wheels only.

## 4. Release
- SemVer. 0.x until M8. Changelog via towncrier fragments.
- PyPI Trusted Publishing and crates.io (`porto`, `porto-abi`, `porto-pyo3` crates are published; internals have `publish = false` until stable).
- ABI policy: §21 B.3.
