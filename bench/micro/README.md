Micro-benchmarks: criterion (Rust) and pyperf (Python) scripts (see `docs/spec/50-engineering/53-benchmarks.md`).

## The pyperf suite (M0)

The same program runs on porto, on asyncio's default loop and on asyncio+uvloop:

| Benchmark | What one operation is | Spec |
|---|---|---|
| `checkpoint` | `await porto.checkpoint()` vs `await asyncio.sleep(0)`, one task | §11 row 1 |
| `switch` | one switch between two tasks that yield in turn | §11 row 2 |
| `spawn_one` | spawn `async def f(): return 1` and wait for it (porto: a scope per task; asyncio: `create_task` + `await`) | §11 row 3 and its comparison row |
| `spawn_join_100k` | spawn 100k trivial tasks into one nursery and join them (asyncio: `TaskGroup`, and `gather`) | 53 §2 |
| `ping_pong` | a round trip of two players hitting a ball in turn | 53 §2 "task switch (yield ping-pong)" |

There is no oneshot channel row: M0 has no channels.

Each program times itself from inside its main task, so runtime start-up is not measured. A
row is gated against its fastest *baseline* variant (for `spawn_join_100k`, the faster of
`TaskGroup` and `gather`). The *informative* variants are reported but never gate:

- `asyncio-nopoll`: the default loop over a selector that makes no system call. Every asyncio
  loop iteration polls the OS for I/O; porto M0 has no I/O polling at all. On macOS an empty
  zero-timeout `kevent` costs about 12 µs, so the poll dominates the per-switch rows.
- `uvloop-readyfd`: uvloop with an always-readable pipe registered, so its poll returns an event
  at once (about 0.3 µs) instead of taking the empty-poll path, plus one no-op callback.
- `*-eager` (3.12+): `asyncio.eager_task_factory`. It changes semantics (the child runs inside
  `create_task`), and porto's spawn does not do it.
- `spawn_one/*-taskgroup`: a `TaskGroup` per task, the same shape as porto's scope per task.

## Running it

```bash
uv sync
uv run python bench/micro/run.py                 # 3.14 (gating), 3.11, 3.14t: about 40 min
uv run python bench/micro/run.py --fast --select '^switch/'   # a quick smoke test
uv run python bench/micro/report.py bench/results/A.json --compare bench/results/B.json
```

`run.py` makes `bench/.venvs/py<tag>` per Python (uv's python-build-standalone, PGO + LTO),
installs a porto wheel built with `maturin build --release` plus the dev environment's pyperf
and uvloop, runs a discarded warm-up pass (`--fast`), then runs `suite.py` under pyperf
(default calibration: 20 worker processes x 3 values). It refuses to run on battery or in Low
Power Mode (53 §1) and writes `bench/results/<date>-<commit>.{json,md}`; pyperf's raw files go
to `bench/results/raw/` (not committed). Run it from a clean tree, since the result records the
commit. `pyperf system tune` has no operation on macOS, so it is not used.

To run one Python by hand: `bench/.venvs/py3.14/bin/python bench/micro/suite.py -o out.json`.
