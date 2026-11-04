# 14 — Time & Timers

## 1. Clocks
- **Runtime clock:** monotonic (`Instant`), exposed as `porto.current_time() -> float` (seconds since the runtime epoch, i.e. the runtime start). `porto.monotonic()` is an alias.
- **Virtual clock** in deterministic mode (§24): `current_time()` returns virtual seconds, and every timer uses it.
- Deadlines are absolute `float` values in runtime-clock seconds. `deadline = porto.current_time() + 5`.

## 2. Timer wheel
Hierarchical wheel: 6 levels × 64 slots. Level 0 slot = `resolution` (default 1 ms). Max span ≈ 2.2 years at 1 ms; longer deadlines are clamped and re-armed.

- One wheel per runtime, protected by a mutex sharded per lane. Each lane owns a wheel *shard* for timers registered on that lane. The shard is processed by its lane during maintenance and before parking. The park timeout is `min(next deadline across own shard, driver-provided)`.
- Entries are intrusive (`TimerEntry { deadline_tick, prev, next, waker/state }`). A PyTask embeds one entry for `Sleep` and one for its innermost scope deadline (§15). Rust `Sleep` futures embed their own.
- Firing: `deadline_tick` is rounded **up** (never fire early). Expired entries are fired in batches. Firing calls the waker (Sleep) or `scope.cancel_by_deadline()` (scope).
- Cancel: O(1) unlink.

## 3. API surface (Python)
```python
await porto.sleep(seconds: float)          # seconds <= 0 → pure checkpoint (Yield)
await porto.sleep_until(deadline: float)
await porto.sleep_forever()                # until cancelled
porto.current_time() -> float
porto.interval(period: float, *, missed: Literal["burst","skip","delay"]="skip") -> AsyncIterator[float]
```
`interval` follows Tokio `MissedTickBehavior` semantics, and yields the scheduled tick time.

## 4. Precision rules
- The park timeout is computed as `deadline - now` rounded **up** to the driver granularity (epoll: ms, or µs with `epoll_pwait2` on Linux ≥ 5.11).
- `timer_resolution` is configurable in `[100µs, 10ms]`.
