"""``Scope.spawn``'s keyword options: ``name=``, ``pin=`` and ``context=`` (spec 11 §8, spec 20
``Scope.spawn`` and ``Task``), and the errors for options it does not take.
"""

import contextvars
import functools
import gc
import weakref
from typing import Any

import pytest

import porto

request_id: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="unset")


async def work(n: int = 0) -> int:
    await porto.checkpoint()
    return n


# name=


def test_a_task_is_named_after_its_function_by_default() -> None:
    async def main() -> tuple[str, str]:
        async with porto.scope() as s:
            by_function = s.spawn(work, 1)
            by_coroutine = s.spawn(work(2))
        return by_function.name, by_coroutine.name

    assert porto.run(main) == ("work", "work")


def test_a_nested_function_is_named_by_its_qualname() -> None:
    async def child() -> None:
        pass

    async def main() -> str:
        async with porto.scope() as s:
            task = s.spawn(child)
        return task.name

    assert porto.run(main) == "test_a_nested_function_is_named_by_its_qualname.<locals>.child"


def test_name_names_the_task_and_shows_in_its_repr() -> None:
    async def main() -> tuple[str, str, str]:
        async with porto.scope() as s:
            task = s.spawn(work, 1, name="fetch user 42")
            running = repr(task)
        return task.name, running, repr(task)

    name, running, finished = porto.run(main)
    assert name == "fetch user 42"
    assert running == "<porto.Task 'fetch user 42' running>"
    assert finished == "<porto.Task 'fetch user 42' returned>"


def test_a_partial_without_a_qualname_is_named_by_its_type() -> None:
    async def main() -> str:
        async with porto.scope() as s:
            task = s.spawn(functools.partial(work, 3))
        return task.name

    assert porto.run(main) == "partial"


def test_a_name_that_is_not_a_str_raises_and_says_what_to_do() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(work, name=42)  # pyright: ignore[reportArgumentType]

    with pytest.raises(TypeError, match=r"name= must be a str, got int; pass name=str\(\.\.\.\)"):
        porto.run(main)


# pin=


def test_tasks_are_not_pinned_by_default_and_pin_true_pins() -> None:
    async def main() -> tuple[bool, bool, bool]:
        async with porto.scope() as s:
            default = s.spawn(work)
            pinned = s.spawn(work, pin=True)
            unpinned = s.spawn(work, pin=False)
        return default.pinned, pinned.pinned, unpinned.pinned

    assert porto.run(main) == (False, True, False)


def test_a_pinned_task_shows_it_in_its_repr() -> None:
    async def main() -> str:
        async with porto.scope() as s:
            task = s.spawn(work, name="w", pin=True)
        return repr(task)

    assert porto.run(main) == "<porto.Task 'w' returned, pinned>"


def test_pinning_is_inherited_by_the_tasks_a_pinned_task_spawns() -> None:
    async def parent() -> tuple[bool, bool]:
        async with porto.scope() as s:
            inherited = s.spawn(work)
            opted_out = s.spawn(work, pin=False)
        return inherited.pinned, opted_out.pinned

    async def main() -> tuple[bool, bool]:
        async with porto.scope() as s:
            task = s.spawn(parent, pin=True)
        return task.result()

    assert porto.run(main) == (True, False)


def test_a_pin_that_is_not_a_bool_raises_and_says_what_to_do() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(work, pin=1)  # pyright: ignore[reportArgumentType]

    with pytest.raises(TypeError, match="pin= must be True, False or None, got int; pass pin=True"):
        porto.run(main)


# context=


def test_context_runs_the_task_in_that_context_itself() -> None:
    async def child() -> str:
        seen = request_id.get()
        request_id.set("written by the child")
        await porto.checkpoint()
        return seen

    async def main() -> tuple[str, str, str]:
        request_id.set("parent")
        context = contextvars.Context()
        context.run(request_id.set, "given")
        async with porto.scope() as s:
            task = s.spawn(child, context=context)
        return task.result(), context[request_id], request_id.get()

    # The child saw the given context and wrote into it; the parent's is untouched.
    assert porto.run(main) == ("given", "written by the child", "parent")


def test_tasks_given_the_same_context_share_it() -> None:
    async def writer() -> None:
        request_id.set("shared")
        await porto.checkpoint()

    async def reader() -> str:
        await porto.checkpoint()
        await porto.checkpoint()
        return request_id.get()

    async def main() -> str:
        context = contextvars.copy_context()
        async with porto.scope() as s:
            s.spawn(writer, context=context)
            read = s.spawn(reader, context=context)
        return read.result()

    assert porto.run(main) == "shared"


def test_a_context_that_is_not_a_context_raises_and_says_what_to_do() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(work, context={})  # pyright: ignore[reportArgumentType]

    with pytest.raises(
        TypeError,
        match=r"must be a contextvars.Context, got dict; pass contextvars.copy_context\(\)",
    ):
        porto.run(main)


# Options spawn does not take


def test_keyword_arguments_meant_for_the_function_raise_and_say_what_to_do() -> None:
    called: list[int] = []

    async def fetch(n: int, *, timeout: float) -> None:
        called.append(n)

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(fetch, 1, timeout=3)  # pyright: ignore[reportCallIssue]

    with pytest.raises(TypeError, match=r"does not take: \['timeout'\].*functools.partial"):
        porto.run(main)
    assert called == [], "the function is not called"


def test_none_options_are_the_defaults() -> None:
    async def main() -> tuple[str, bool]:
        async with porto.scope() as s:
            task = s.spawn(work, 1, name=None, pin=None, context=None)
        return task.name, task.pinned

    assert porto.run(main) == ("work", False)


def test_spawning_something_that_is_not_awaitable_raises() -> None:
    def sync() -> int:
        return 1

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(sync)  # pyright: ignore[reportArgumentType]

    with pytest.raises(TypeError, match="not awaitable; define it with `async def`"):
        porto.run(main)


# The name's source and the cycle collector


def test_a_task_whose_function_refers_back_to_it_is_collected() -> None:
    class Payload:
        pass

    async def spawn_a_cycle(s: porto.Scope) -> weakref.ref[Payload]:
        tasks: list[Any] = []
        payload = Payload()

        async def child() -> None:
            # The closure reaches the task through `tasks`: task -> child -> cell -> tasks.
            assert tasks
            assert payload

        tasks.append(s.spawn(child))
        return weakref.ref(payload)

    async def main() -> None:
        async with porto.scope() as s:
            alive = await spawn_a_cycle(s)
        gc.collect()
        assert alive() is None

    porto.run(main)
