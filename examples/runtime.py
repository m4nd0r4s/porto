"""Running a program with an explicit ``porto.Runtime``.

``porto.run(main)`` is the usual entry point. A ``Runtime`` is the same thing as an object:
it runs once, ``porto.runtime()`` finds it from inside, and one runtime runs per process at a
time. Using what belongs to a runtime after it stopped raises ``porto.RuntimeClosed``.
"""

import porto


async def main() -> str:
    assert porto.in_runtime()
    assert porto.runtime() is runtime
    answer = 0  # bound before the block: Scope.__aexit__ may suppress, as far as pyright knows
    async with porto.scope() as scope:
        answer = await scope.spawn(double, 21)
    return f"answer: {answer}"


async def double(value: int) -> int:
    await porto.sleep(0)
    return value * 2


async def inner() -> None:
    raise AssertionError("never runs")


async def nested() -> None:
    # One runtime runs per process: starting another from a task is refused, and the
    # coroutine it was given is closed, so there is no "never awaited" warning either.
    try:
        porto.run(inner())
    except RuntimeError as error:
        assert "inside a running porto runtime" in str(error)
    else:
        raise AssertionError("a nested run must be refused")


assert not porto.in_runtime()

with porto.Runtime() as runtime:
    assert runtime.block_on(main) == "answer: 42"
    # A runtime runs once: it is stopped for good now.
    try:
        runtime.block_on(main)
    except porto.RuntimeClosed as error:
        assert "create a new porto.Runtime" in str(error)
    else:
        raise AssertionError("a stopped runtime must not run again")

porto.run(nested)
print("ok")
