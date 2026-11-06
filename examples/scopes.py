"""Structured concurrency: scopes, ``spawn``, ``Task.result`` and ``ExceptionGroup``.

    uv run python examples/scopes.py

A scope is a nursery: every task spawned into it finishes before its ``async with`` block
exits, so no task outlives the code that started it. Failures come out of the block as an
``ExceptionGroup``; handle them with ``except*``.
"""

import porto

LATENCY = {
    "/": 0.02,
    "/about": 0.03,
    "/blog": 0.01,
    "/contact": 0.02,
    "/docs": 0.03,
    "/faq": 0.01,
}


async def fetch(path: str) -> str:
    """Pretends to download ``path``."""
    await porto.sleep(LATENCY[path])
    return f"<html>{path}</html>"


async def fetch_all() -> dict[str, str]:
    """Downloads every page at once: the scope waits for the slowest, not for the sum."""
    started = porto.current_time()
    tasks: dict[str, porto.Task[str]] = {}
    async with porto.scope() as s:
        for path in LATENCY:
            tasks[path] = s.spawn(fetch, path)  # an async function and its arguments
        # Tasks run concurrently with the rest of the block; none has finished yet.
        assert not any(task.done() for task in tasks.values())
    # Every child finished when the block exited, so result() never waits.
    elapsed = porto.current_time() - started
    assert elapsed < sum(LATENCY.values()), f"pages were fetched one by one ({elapsed:.3f} s)"
    return {path: task.result() for path, task in tasks.items()}


async def nested_scopes() -> list[str]:
    """Scopes nest: the inner block waits for its own tasks only."""
    log: list[str] = []

    async def step(name: str, seconds: float) -> None:
        await porto.sleep(seconds)
        log.append(name)

    async with porto.scope() as outer:
        outer.spawn(step, "outer child", 0.05)
        async with porto.scope() as inner:
            inner.spawn(step("inner child", 0.01))  # a coroutine object works too
        log.append("inner scope exited")
    log.append("outer scope exited")
    return log


async def validate(record: dict[str, object]) -> None:
    """Rejects a bad record at once, before its first await."""
    if "id" not in record:
        raise KeyError("id")
    if not isinstance(record.get("age"), int):
        raise TypeError(f"age must be an int, got {record.get('age')!r}")
    await porto.checkpoint()


async def validate_all(records: list[dict[str, object]]) -> list[str]:
    """Every failure is reported: the block raises them together in an ExceptionGroup."""
    problems: list[str] = []
    try:
        async with porto.scope() as s:
            for record in records:
                s.spawn(validate, record)
    except* KeyError as group:
        problems += [f"missing {error}" for error in group.exceptions]
    except* TypeError as group:
        problems += [str(error) for error in group.exceptions]
    return sorted(problems)


async def task_lifecycle() -> None:
    task: porto.Task[str] | None = None
    async with porto.scope() as s:
        task = s.spawn(fetch, "/blog")
        # Reading a result before the task finished is a bug the runtime reports.
        try:
            task.result()
        except porto.InvalidStateError:
            pass
        else:
            raise AssertionError("result() of a running task did not raise")
    assert task is not None and task.result() == "<html>/blog</html>"

    # The scope is closed once its block exited: spawn there is an error, with a hint.
    try:
        s.spawn(fetch, "/")
    except RuntimeError:
        pass
    else:
        raise AssertionError("spawn on an exited scope did not raise")


async def main() -> None:
    pages = await fetch_all()
    assert pages == {path: f"<html>{path}</html>" for path in LATENCY}, pages

    log = await nested_scopes()
    # Usually: inner child, inner scope exited, outer child, outer scope exited. Only the order
    # below is guaranteed: a block exits after its own children, whatever the timing.
    assert log.index("inner child") < log.index("inner scope exited"), log
    assert log[-1] == "outer scope exited", log
    assert sorted(log) == ["inner child", "inner scope exited", "outer child", "outer scope exited"]

    problems = await validate_all(
        [
            {"id": 1, "age": 30},
            {"age": 41},
            {"id": 3, "age": "forty"},
            {"id": 4, "age": 25},
        ]
    )
    assert problems == ["age must be an int, got 'forty'", "missing 'id'"], problems

    await task_lifecycle()
    print("scopes: ok")


if __name__ == "__main__":
    porto.run(main)
