# 55 — Risks & Open Questions

## 1. Risks
| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Trap protocol isn't fast enough vs asyncio C Task | Low–Med | Critical | M0 spike with a kill criterion |
| asyncio ecosystem gravity: users won't leave asyncio | High | High | Compat facade on by default, ASGI mount, AnyIO backend, Douro as the "killer app" |
| AnyIO internals change, breaking backend injection | Med | Med | Version gate + facade fallback + upstream PR for an entry-point registry |
| CPython changes coroutine internals (`am_send`, `PyIter_Send` behavior) | Low | High | Only public C API used; CI against CPython main nightly |
| Free-threaded ecosystem immaturity (C extensions re-enabling the GIL) | High (2026) | Med | GIL-degraded mode, clear logs; GIL build is a first-class target |
| hyper/h2 not fitting runtime-agnostic usage perfectly | Low | Med | hyper 1.x rt traits; fallback: own h1 parser (httparse) |
| oxiform not ready (OX-1…OX-5) when Douro M6 starts | Med | High | OX items scheduled into oxiform phases 1–2; stopgap: link `oxiform-py` as an rlib with an exact version pin (duplicate plan cache, accepted temporarily) |
| Scope too large for a small team | High | High | Strict milestone order; Douro starts only after M3; each milestone usable alone |
| Name collisions (PyPI `porto`/`douro`, crates.io) | Med | Low | Check before M3; candidates in reserve |
| Security of the HTTP engine | Med | High | Fuzzing, conformance suites, external review before 1.0 |

## 2. Open questions (with current defaults)
1. **`select`-like API** (§16 §3): deferred. Default: none.
2. **Should `porto.run` default to `lanes="auto"` on free-threaded builds?** Default: no (1). Revisit when Phase III of free-threading is announced.
3. **Subinterpreters (PEP 734) as lanes on GIL builds**, for parallelism without free-threading: research item. Cross-interpreter object sharing makes channels copy-based. Possible "isolated lanes" mode in v2.
4. **Native DB drivers** (Postgres wire protocol in Rust on Porto, exposing Python models directly via the schema engine) would be the next 2× for Douro's DB-bound endpoints. Candidate separate project after 1.0.
5. **Exposing Douro over ASGI** for platform compatibility (serverless). Default: no.
6. **Windows io_uring-like (IoRing)**: no.
7. **Upstream proposals:** AnyIO backend registry; a CPython `asyncio` hook to let foreign task implementations register a current task cheaply.

## 3. Name candidates if collisions block release
Runtime: `porto`, `ribeira`, `marea`, `tejo`. Framework: `douro`, `cais`, `vinho`.
