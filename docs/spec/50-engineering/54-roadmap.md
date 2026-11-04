# 54 — Roadmap (Milestones with Exit Criteria)

Each milestone is shippable internally. The order is chosen so the riskiest assumption (ST-1: trap protocol speed) is validated first.

| M | Scope | Exit criteria |
|---|---|---|
| **M0 Spike (2–3 wks)** | Minimal porto-py: single lane, PyTask driver + Trap (`Yield`, `Sleep`), `run`, `scope.spawn`, `sleep`, `checkpoint`. No I/O. | §11 §11 micro targets met or within 30%. **If spawn/switch isn't ≥ 2× asyncio+uvloop, stop and redesign.** |
| **M1 Core runtime** | porto-core complete single-lane: scopes (full §15), timers wheel, sync primitives, channels, budget, registry; porto-io epoll/kqueue; TCP/UDP/Unix; blocking pool; signals; errors. | §15/§16 semantic tests green; TCP echo gate; loom suite green |
| **M2 Multi-lane** | lanes, stealing, inbox, LIFO slot, free-threaded attach policy, GIL-degraded mode, Rust lanes on GIL builds | Free-threaded scaling gates; TSAN clean |
| **M3 ABI + Rust facade** | porto-abi v1, porto-pyo3, `porto` crate API, hyper compat, tokio bridge | A sample PyO3 extension (in `examples/`) doing TCP via ABI passes tests; hyper client example works |
| **M4 Compat** | asyncio facade (loop, transports, subprocess, signals), bridging, auto-hosting, AnyIO backend, sniffio | S2 compat ≥ 95% |
| **M5 Observability + Deterministic** | inspect, watchdog, metrics, console TUI, OTel; porto-sim, sim net, pytest plugin, explore | S4; console demo |
| **M6 Douro core** | router, oxiform integration (§34: OX-1…OX-5 must be released in oxiform first), HandlerPlan, DI, responses, HTTP/1.1+h2 server, limits, errors, test client | HTTP gates (§53 §4) on JSON/path/POST |
| **M7 Douro complete** | WebSockets, SSE, multipart, files, compression, Rust layers, Python middleware, OpenAPI+docs, supervisor+CLI, reload, ASGI mount | h2spec + Autobahn; graceful-shutdown zero-loss test; docs complete |
| **M8 Hardening → 1.0** | fuzzing campaigns, soak, PGO, wheels matrix, security review, API freeze | S1–S7 |
| **M9+ Post-1.0** | HTTP/3 (quinn), RFC 8441 WS over h2, `select`, FastAPI codemod, layer ABI, subinterpreter research | — |
