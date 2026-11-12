# 14 — Time & Timers

## 1. Clocks
- **Runtime clock:** monotonic (`Instant`), exposed as `porto.current_time() -> float` (seconds since the runtime epoch, i.e. the runtime start). `porto.monotonic()` is an alias.
- **Virtual clock** in deterministic mode (§24): `current_time()` returns virtual seconds, and every timer uses it.
- Deadlines are absolute `float` values in runtime-clock seconds. `deadline = porto.current_time() + 5`.

## 2. Timer wheel
Hierarchical wheel: 6 levels × 64 slots. Level 0 slot = `resolution` (default 1 ms). Max span ≈ 2.2 years at 1 ms; longer deadlines are clamped and re-armed.

- One wheel per runtime, protected by a mutex sharded per lane. Each lane owns a wheel *shard* for timers registered on that lane. The shard is processed by its lane during maintenance and before parking. The park timeout is `min(next deadline across own shard, driver-provided)`.
- Entries are intrusive (`TimerEntry { deadline_tick, prev, next, waker/state }`). A PyTask embeds one entry for `Sleep`; a scope with a deadline embeds one in its `ScopeNode` (`deadline_timer`, §15 §1.1), whose waker is a counted reference to the node, so the wheel keeps the scope alive while the entry is linked and the scope's exit disarms it. Rust `Sleep` futures embed their own.
- Firing: `deadline_tick` is rounded **up** (never fire early). Expired entries are fired in batches. Firing calls the waker: for a `Sleep` it wakes the task; for a scope it cancels the scope with `Deadline`, unless its deadline moved later meanwhile (the move re-linked the entry) or the scope closed. A scope's arm and disarm run under the scope's lock, taken before the shard's; the fire's check (not closed, deadline still reached) and its cancel are one step under that lock. A scope holds its runtime weakly. Once the runtime shut its wheel down (it is being dropped) arming links nothing, so a deadline set on a scope that outlived its run is recorded and inert.
- Cancel: O(1) unlink. Dropping a `Sleep` (on any thread) unlinks its entry, so it never wakes its task late or keeps it alive; this is why the shard is behind a mutex even though only its lane fires it.
- The park deadline is the earliest entry's tick (not a wake for each cascade on the way down), or the re-arm point of a clamped entry. Finding it is O(1) under the shard lock, however many entries share a slot: each slot keeps a lower bound of its entries' ticks, set on insert and reset when the slot empties or cascades. Cancelling a slot's earliest entry can leave the bound early; the lane then wakes once, early, to cascade that slot, and never late.

## 3. API surface (Python)
```python
await porto.sleep(seconds: float)          # seconds <= 0 → pure checkpoint (Yield)
await porto.sleep_until(deadline: float)
await porto.sleep_forever()                # until cancelled
porto.current_time() -> float
porto.interval(period: float, *, missed: Literal["burst","skip","delay"]="skip") -> AsyncIterator[float]
```
`interval` follows Tokio `MissedTickBehavior` semantics, and yields the scheduled tick time.

Arguments are `float` seconds, and the edge values are defined:
- A deadline already reached (`sleep(seconds <= 0)`, `-inf` included; `sleep_until(t)` with `t <= current_time()`) is a pure checkpoint: the task yields once, and a cancellation is delivered there (C2). It is not an error.
- An infinite one (`sleep(inf)`, `sleep_until(inf)`, or a deadline beyond the clock's ~584 years) never fires: like `sleep_forever()`, only a cancellation ends it. Its entry stays linked (in the top level's ring, re-armed about every 2.2 years at 1 ms), so cancelling it unlinks it like any other.
- NaN is no point in time: `sleep` and `sleep_until` raise `ValueError` saying what to pass (a number of seconds, `0` to yield, or `sleep_forever()`).
- `sleep_until(t)` rounds `t` up to the nanosecond, so "never early" holds on the `current_time()` reading it was computed from.

## 4. Precision rules
- The park timeout is computed as `deadline - now` rounded **up** to the driver granularity (epoll: ms, or µs with `epoll_pwait2` on Linux ≥ 5.11).
- `timer_resolution` is configurable in `[100µs, 10ms]`.
- "Never early" holds on the runtime clock: `current_time()` read after a `sleep(d)` is at least `d` past a reading taken before it. A coarser clock can read the same sleep as shorter. Python 3.11's `time.monotonic()` on Windows ticks every ~15.6 ms (`GetTickCount64`), so measure sleeps with `current_time()` or `time.perf_counter()`.
