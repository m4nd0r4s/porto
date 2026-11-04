"""The public API as a user's type checker sees it (ADR-028, pyright strict).

The ``check_*`` functions are only type-checked; ``test_*`` also run them.
"""

from typing import assert_type

import porto


def check_version_is_str() -> None:
    assert_type(porto.__version__, str)


async def fetch(host: str, port: int) -> bytes:
    await porto.sleep(0)
    return f"{host}:{port}".encode()


async def total() -> int:
    await porto.checkpoint()
    return 3


async def main(hosts: list[str]) -> list[bytes]:
    async with porto.scope() as s:
        assert_type(s, porto.Scope)
        from_function = [s.spawn(fetch, host, 443) for host in hosts]
        from_coroutine = s.spawn(total())
        assert_type(from_function[0], porto.Task[bytes])
        assert_type(from_coroutine, porto.Task[int])
    assert_type(from_coroutine.result(), int)
    assert_type(from_coroutine.done(), bool)
    return [task.result() for task in from_function]


def check_run_infers_the_result_type() -> None:
    assert_type(porto.run(main, ["a"]), list[bytes])
    assert_type(porto.run(main(["a"])), list[bytes])
    assert_type(porto.run(total), int)


def check_wrong_arguments_are_type_errors() -> None:
    porto.run(main, 1)  # pyright: ignore[reportArgumentType]
    porto.run(main)  # pyright: ignore[reportArgumentType]


async def check_spawn_checks_the_arguments() -> None:
    async with porto.scope() as s:
        s.spawn(fetch, "a")  # pyright: ignore[reportArgumentType]
        s.spawn(fetch, "a", "443")  # pyright: ignore[reportArgumentType]


def test_the_sample_runs() -> None:
    assert porto.run(main, ["a", "b"]) == [b"a:443", b"b:443"]
