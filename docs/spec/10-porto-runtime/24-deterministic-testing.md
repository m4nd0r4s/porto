# 24 — Deterministic Testing

## 1. Goal
Any concurrency bug in user code (or Porto) can be reproduced from `(seed, test)` alone.

## 2. Activation
```python
porto.testing.run(main, *, seed: int = 0, clock: Literal["virtual","real"] = "virtual",
                  autojump: bool = True, network: porto.testing.net.Network | None = None,
                  max_steps: int | None = 10_000_000)
```
or `RuntimeConfig(deterministic=DeterministicConfig(...))`. Env var `PORTO_SEED` overrides the seed. On failure, the pytest plugin prints `Reproduce with PORTO_SEED=<seed>`.

## 3. What is forced
| Aspect | Deterministic behavior |
|---|---|
| Lanes | exactly 1 (Python and Rust tasks share it); lane count settings ignored |
| Scheduling | at each scheduling decision, choose uniformly (seeded `ChaCha8Rng`) among **all** runnable tasks, not FIFO. This explores interleavings |
| Time | virtual clock. With autojump, when no task is runnable and only timers are pending, the clock jumps to the next deadline instantly |
| I/O | `sim` driver. Real sockets are refused (`porto.net.connect_tcp` to a non-sim address raises `RuntimeError("real I/O in deterministic mode")`) unless `allow_real_io=True` (which breaks determinism and is flagged in the report) |
| Blocking pool | jobs run synchronously on the lane at the scheduling point (deterministic). Wall time is not advanced |
| DNS | sim resolver backed by `Network.hosts` |
| Hash randomization | warns if `PYTHONHASHSEED` is not set (set-iteration order affects user code) |
| asyncio facade | supported; its callbacks are scheduled through the same seeded chooser |

## 4. Simulated network (`porto.testing.net`)
```python
net = Network()
net.add_host("db", "10.0.0.2")
net.link("app", "db", latency=(0.001, 0.005), loss=0.0, bandwidth=None)   # latency drawn from seeded RNG
net.partition({"app"}, {"db"}, at=2.0, heal_at=5.0)
net.fault("db", kind="reset_connections", at=3.0)
```
Sim sockets implement the same `ByteStream`/listener APIs. Douro's test client also runs over the sim network (§39).

## 5. Fuzzing schedules
`porto.testing.explore(main, seeds=range(1000), stop_on_first_failure=True)` runs many seeds and reports failing seeds with a minimal step trace (task-switch log). It integrates with `hypothesis` via `porto.testing.strategies.seeds()`.

## 6. Pytest plugin
```python
@pytest.mark.porto                 # runs the async test under porto.run
@pytest.mark.porto(deterministic=True, seeds=50)   # runs 50 seeds
async def test_x(porto_clock): ...
```
Fixtures: `porto_clock` (virtual clock with `advance(s)`), `porto_net`, `porto_runtime`. Registered via the `pytest11` entry point.
