"""Tasks: the ``porto.Task`` handle, ``current_task()``, ``Task.cancel()`` and ``Scope.start()``.

    uv run python examples/tasks.py

``Scope.spawn`` returns a ``Task``: a handle to read the result, to wait for the task
(``await task``) and to cancel it alone. ``Scope.start`` is ``spawn`` for a task that must be
ready before the caller goes on (a server that has to bind its port first): the task reports
with ``started.set(value)`` and ``start`` returns that value.
"""

import porto
from porto import lowlevel


async def double(n: int) -> int:
    await porto.sleep(0.005)
    return n * 2


async def handle_basics() -> None:
    async with porto.scope() as s:
        task = s.spawn(double, 21, name="doubler")
        assert task.name == "doubler"  # without name=: the function's __qualname__, "double"
        assert s.spawn(double, 1).name == "double"
        assert isinstance(task.id, int)  # unique in the process, increasing in spawn order
        assert not task.done()
        # Awaiting a task waits for it and evaluates to its result...
        assert await task == 42
        # ...and it is done now, so result() and exception() read it without waiting.
        assert task.done() and not task.cancelled()
        assert task.result() == 42 and task.exception() is None
        # Awaiting it again, or from a second task, gives the same result at once.
        assert await task == 42


async def failing_task() -> None:
    """A task that raised: ``await`` and ``result()`` raise the same exception again."""

    async def boom() -> None:
        await porto.checkpoint()
        raise ValueError("bad input")

    task: porto.Task[None] | None = None
    s = porto.scope()
    try:
        async with s:
            task = s.spawn(boom)
            try:
                await task
            except ValueError as error:
                assert str(error) == "bad input"
            else:
                raise AssertionError("awaiting a failed task did not raise")
    except* ValueError:
        pass  # the scope reports the failure too: it was nobody's job to swallow it
    assert task is not None and task.done()
    assert isinstance(task.exception(), ValueError)


async def who_am_i() -> None:
    """``current_task()`` is the very handle ``spawn`` returned."""
    seen: list[porto.Task[None]] = []

    async def worker() -> None:
        seen.append(porto.current_task())

    task: porto.Task[None] | None = None
    async with porto.scope() as s:
        task = s.spawn(worker, name="worker")
    assert task is not None and seen == [task] and seen[0].name == "worker"

    # The main task has a handle too, the same on every call, named after the function.
    me = porto.current_task()
    assert me is porto.current_task() and me.name == "main"


async def cancel_one_task() -> None:
    """``Task.cancel()`` stops that task only: its siblings and its scope go on."""
    log: list[str] = []

    async def worker(name: str, seconds: float) -> None:
        try:
            await porto.sleep(seconds)
            log.append(f"{name} finished")
        finally:
            log.append(f"{name} cleaned up")

    sibling: porto.Task[None] | None = None
    async with porto.scope() as s:
        doomed = s.spawn(worker, "doomed", 10)
        sibling = s.spawn(worker, "sibling", 0.05)
        await porto.sleep(0.005)  # both are sleeping now
        doomed.cancel()
        await porto.checkpoint()  # it gets Cancelled at its await, runs its finally, ends
        assert doomed.done() and doomed.cancelled()
        assert not s.cancel_called  # the scope is not cancelled; the sibling runs on
        try:
            doomed.result()
        except porto.Cancelled:
            pass
        else:
            raise AssertionError("a cancelled task has a result")
        doomed.cancel()  # cancelling a task that finished does nothing
    # The Cancelled that ended it is not a failure: the block exits without an error.
    assert sibling is not None and sibling.result() is None and not s.cancel_called
    assert log == ["doomed cleaned up", "sibling finished", "sibling cleaned up"], log


async def serve(port: int, *, started: porto.Started[int]) -> None:
    """A server-style task: get ready, say so, then run until cancelled."""
    await porto.sleep(0.005)  # say, bind a socket
    started.set(port)  # ``start`` returns here, and this task becomes a child of the scope
    await porto.sleep_forever()


async def start_then_use() -> None:
    log: list[str] = []

    async def failing_setup(*, started: porto.Started[None]) -> None:
        raise OSError("address already in use")

    async with porto.scope() as s:
        port = await s.start(serve, 8080)  # waits until serve calls started.set(...)
        assert port == 8080
        log.append(f"listening on {port}")
        s.cancel()  # stop the server; its exit would wait for it forever otherwise

    assert log == ["listening on 8080"]

    # A failure before ``set`` is raised by start itself, as it is, not in an ExceptionGroup.
    async with porto.scope() as s:
        try:
            await s.start(failing_setup)
        except OSError as error:
            assert "address already in use" in str(error)
        else:
            raise AssertionError("start did not raise the setup failure")

    # A task that ends without calling ``set`` is a bug, reported with a hint.
    async def forgetful(*, started: porto.Started[None]) -> None:
        return None

    async with porto.scope() as s:
        try:
            await s.start(forgetful)
        except RuntimeError as error:
            assert "set" in str(error)
        else:
            raise AssertionError("start accepted a task that never reported")


async def lowlevel_traps() -> None:
    """``porto.lowlevel`` is for library authors: the traps behind the awaitables you know."""
    assert lowlevel.current_lane() == 0  # a runtime has one lane until M2
    await lowlevel.trap_yield()  # what ``porto.checkpoint()`` awaits

    notify = porto.Notify()
    woken: list[str] = []

    async def wait_by_hand() -> None:
        await lowlevel.trap_wait_notify(notify)  # what ``await notify.notified()`` awaits
        woken.append("woken")

    async with porto.scope() as s:
        s.spawn(wait_by_hand)
        await porto.checkpoint()  # let it start waiting
        notify.notify_one()
    assert woken == ["woken"]


async def main() -> None:
    await handle_basics()
    await failing_task()
    await who_am_i()
    await cancel_one_task()
    await start_then_use()
    await lowlevel_traps()
    print("tasks: ok")


if __name__ == "__main__":
    porto.run(main)
