"""Runtime start and the process-level invariants I1 and I4 (10-architecture-overview.md §5,
20-python-api-reference.md Running, 26-errors-config-lifecycle.md §2).

I1: one runtime runs per process at a time. I4: what belongs to a runtime answers
``porto.RuntimeClosed`` once that runtime stopped.
"""

import contextlib
import sys
import threading
import warnings
from typing import Any

import pytest

import porto
from porto import _core
from porto._config import is_gil_build

# Runs and results


async def double(value: int) -> int:
    await porto.sleep(0)
    return value * 2


def test_run_returns_the_result_of_main() -> None:
    assert porto.run(double, 21) == 42


def test_run_raises_the_exception_of_main_as_it_is() -> None:
    async def main() -> None:
        raise KeyError("missing")

    with pytest.raises(KeyError, match="missing"):
        porto.run(main)


def test_runtime_block_on_returns_the_result_and_raises_the_exception_of_main() -> None:
    async def main() -> int:
        return 7

    async def failing() -> None:
        raise ValueError("no")

    assert porto.Runtime().block_on(main) == 7
    with pytest.raises(ValueError, match="no"):
        porto.Runtime().block_on(failing)


def test_block_on_takes_a_coroutine_object_too() -> None:
    assert porto.Runtime().block_on(double(4)) == 8


def test_sequential_runs_in_one_process_work() -> None:
    results = [porto.run(double, n) for n in range(3)]
    results.append(porto.Runtime().block_on(double(10)))
    with porto.Runtime() as runtime:
        results.append(runtime.block_on(double(11)))
    assert results == [0, 2, 4, 20, 22]


def test_sequential_runs_on_different_threads_work() -> None:
    results: list[int] = []

    def target(value: int) -> None:
        results.append(porto.run(double, value))

    for value in (1, 2):
        thread = threading.Thread(target=target, args=(value,))
        thread.start()
        thread.join()
    assert results == [2, 4]


def test_a_runtime_configuration_error_raises_when_it_is_made() -> None:
    with pytest.raises(ValueError, match="coop_budget"):
        porto.Runtime(coop_budget=0)


def test_a_runtime_uses_its_configuration() -> None:
    async def main() -> bool:
        return porto.current_task() is not None

    assert porto.Runtime(coop_budget=4).block_on(main)


# I1: one runtime per process


def test_a_nested_run_raises_and_closes_the_coroutine() -> None:
    async def inner() -> None:
        raise AssertionError("must not run")

    async def main() -> str:
        coro = inner()
        with pytest.raises(RuntimeError, match="inside a running porto runtime") as raised:
            porto.run(coro)
        assert not isinstance(raised.value, porto.RuntimeClosed)
        with pytest.raises(RuntimeError, match="inside a running porto runtime"):
            porto.Runtime().block_on(inner())
        return "survived"

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert porto.run(main) == "survived"


def test_a_run_on_a_blocking_pool_worker_raises_and_says_what_to_do() -> None:
    def start_a_runtime() -> str:
        try:
            porto.run(double, 1)
        except RuntimeError as error:
            return str(error)
        return "started"

    async def main() -> str:
        return await porto.to_thread(start_a_runtime)

    message = porto.run(main)
    assert "blocking-pool thread" in message
    assert "porto.from_thread" in message


def test_a_run_on_a_plain_thread_while_a_runtime_runs_raises() -> None:
    errors: list[BaseException] = []

    def start_a_runtime() -> None:
        try:
            porto.run(double, 1)
        except RuntimeError as error:
            errors.append(error)

    async def main() -> None:
        thread = threading.Thread(target=start_a_runtime)
        thread.start()
        # The thread holds no lane: wait for it without blocking this one.
        while thread.is_alive():
            await porto.sleep(0.001)
        thread.join()

    porto.run(main)
    assert len(errors) == 1
    assert "another porto runtime is already running" in str(errors[0])
    assert porto.run(double, 1) == 2, "the refused start left nothing behind"


def test_a_runtime_that_ran_cannot_run_again() -> None:
    runtime = porto.Runtime()
    runtime.block_on(double(1))
    with pytest.raises(porto.RuntimeClosed, match=r"create a new porto\.Runtime"):
        runtime.block_on(double(1))


def test_a_runtime_shut_down_before_it_ran_cannot_run() -> None:
    runtime = porto.Runtime()
    runtime.shutdown()
    runtime.shutdown()
    with pytest.raises(porto.RuntimeClosed):
        runtime.block_on(double(1))


def test_leaving_the_context_manager_shuts_the_runtime_down() -> None:
    with porto.Runtime() as runtime:
        assert repr(runtime) == "<porto.Runtime new>"
    assert repr(runtime) == "<porto.Runtime stopped>"
    with pytest.raises(porto.RuntimeClosed):
        runtime.block_on(double(1))


def test_a_runtime_that_runs_cannot_be_shut_down_yet() -> None:
    async def main() -> str:
        with pytest.raises(RuntimeError, match="not supported yet"):
            porto.runtime().shutdown()
        return str(porto.runtime())

    assert porto.run(main) == "<porto.Runtime running>"


def test_shutdown_refuses_a_bad_timeout() -> None:
    with pytest.raises(ValueError, match="timeout"):
        porto.Runtime().shutdown(timeout=-1)


# runtime(), in_runtime(), root_scope


def test_runtime_and_in_runtime_are_for_code_that_runs_in_a_runtime() -> None:
    assert not porto.in_runtime()
    with pytest.raises(porto.NoRuntimeError, match=r"porto\.runtime\(\)"):
        porto.runtime()
    seen: list[Any] = []

    async def main() -> None:
        seen.append((porto.in_runtime(), porto.runtime()))
        async with porto.scope() as scope:
            scope.spawn(child)

    async def child() -> None:
        seen.append((porto.in_runtime(), porto.runtime()))

    porto.run(main)
    assert [inside for inside, _ in seen] == [True, True]
    assert seen[0][1] is seen[1][1]
    assert not porto.in_runtime()


def test_runtime_is_the_object_block_on_was_called_on() -> None:
    runtime = porto.Runtime()

    async def main() -> bool:
        return porto.runtime() is runtime

    assert runtime.block_on(main)


def test_a_blocking_pool_thread_is_not_in_the_runtime() -> None:
    def probe() -> bool:
        return porto.in_runtime()

    async def main() -> bool:
        return await porto.to_thread(probe)

    assert porto.run(main) is False


def test_root_scope_is_valid_only_while_the_runtime_runs() -> None:
    runtime = porto.Runtime()
    with pytest.raises(porto.NoRuntimeError, match="root_scope"):
        _ = runtime.root_scope
    seen: list[porto.Scope] = []

    async def main() -> None:
        seen.append(runtime.root_scope)
        assert runtime.root_scope is seen[0]

    runtime.block_on(main)
    with pytest.raises(porto.RuntimeClosed, match="root_scope"):
        _ = runtime.root_scope


# I4: use after the runtime stopped


def test_a_task_the_runtime_dropped_raises_runtime_closed_when_used_later() -> None:
    kept: list[porto.Task[None]] = []

    async def stubborn() -> None:
        while True:
            with contextlib.suppress(porto.Cancelled):
                await porto.sleep(1000)

    async def main() -> None:
        kept.append(porto.runtime().root_scope.spawn(stubborn))
        await porto.sleep(0)

    with pytest.warns(RuntimeWarning, match="abandoned"):
        porto.run(main, cancel_timeout=0.05)
    task = kept[0]
    with pytest.raises(porto.RuntimeClosed, match="runtime stopped"):
        task.result()

    async def next_run() -> None:
        with pytest.raises(porto.RuntimeClosed):
            await task

    porto.run(next_run)


def test_a_finished_task_keeps_its_result_after_the_runtime_stopped() -> None:
    kept: list[porto.Task[int]] = []

    async def main() -> None:
        async with porto.scope() as scope:
            kept.append(scope.spawn(double, 4))

    porto.run(main)
    assert kept[0].result() == 8


def test_a_scope_of_a_stopped_runtime_raises_runtime_closed() -> None:
    kept: list[porto.Scope] = []

    async def main() -> None:
        async with porto.scope() as scope:
            kept.append(scope)

    porto.run(main)
    scope = kept[0]

    with pytest.raises(porto.RuntimeClosed, match="belongs to a runtime that stopped"):
        scope.spawn(double, 1)


def test_a_scope_that_never_ran_is_a_no_runtime_error_not_a_closed_runtime() -> None:
    scope = porto.scope()
    with pytest.raises(porto.NoRuntimeError):
        scope.spawn(double, 1)


@pytest.mark.skipif(sys.platform == "win32", reason="porto has no sockets on Windows yet (IOCP)")
def test_a_socket_of_a_stopped_runtime_raises_runtime_closed() -> None:
    kept: list[Any] = []

    async def main() -> None:
        listener = await porto.net.listen_tcp("127.0.0.1", 0)
        kept.append(listener)

    porto.run(main)

    async def use() -> None:
        with pytest.raises(porto.RuntimeClosed, match="runtime that stopped"):
            await kept[0].accept()

    with pytest.raises(porto.RuntimeClosed, match="runtime that stopped"):
        kept[0].accept()
    porto.run(use)


def test_channels_and_primitives_hold_no_runtime_and_work_after_it_stopped() -> None:
    """Runtime-free objects have nothing to close: what needs a runtime to park raises
    ``NoRuntimeError``, which says to use it inside one."""
    kept: list[Any] = []

    async def main() -> None:
        kept.extend(porto.channel(4))
        kept.append(porto.Event())

    porto.run(main)
    event = kept[2]
    event.set()
    assert event.is_set()
    with pytest.raises(porto.NoRuntimeError):
        event.wait()


# The start steps (26 §2 step 1)


def _trace() -> list[str]:
    return _core._startup_trace()


def test_the_start_steps_install_in_spec_order_and_uninstall_in_reverse() -> None:
    async def main() -> None:
        return None

    porto.run(main)
    steps = ["asyncgen_hooks", "fork_hooks", "signal_receiver"]
    if not is_gil_build():
        steps.append("import_hook")
    expected = [f"install:{step}" for step in steps] + [
        f"uninstall:{step}" for step in reversed(steps)
    ]
    assert _trace() == expected


def test_the_signal_receiver_is_only_installed_on_the_main_thread_and_when_asked() -> None:
    async def main() -> None:
        return None

    porto.run(main, handle_signals=False)
    assert "install:signal_receiver" not in _trace()

    traces: list[list[str]] = []

    def on_a_thread() -> None:
        porto.run(main)
        traces.append(_trace())

    thread = threading.Thread(target=on_a_thread)
    thread.start()
    thread.join()
    assert "install:signal_receiver" not in traces[0]
    assert "install:asyncgen_hooks" in traces[0]
