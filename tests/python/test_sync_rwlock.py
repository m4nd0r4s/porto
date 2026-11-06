"""porto.RwLock (16-sync-primitives-and-channels.md §1).

Shared readers, one writer, writer preference (a reader never takes the lock while a writer
waits), arrival order between readers and writers, cancel safety.
"""

import random
import threading
from collections.abc import Awaitable

import pytest

import porto
from porto import _core


def completes_at_once(awaitable: Awaitable[object]) -> bool:
    """Resumes ``awaitable`` once by hand: whether it completed without suspending."""
    steps = awaitable.__await__()
    try:
        steps.send(None)
    except StopIteration:
        return True
    steps.close()
    return False


async def settle() -> None:
    """Lets every other ready task run until it waits."""
    for _ in range(10):
        await porto.checkpoint()


def test_readers_share_and_a_writer_excludes_them() -> None:
    async def main() -> None:
        lock = porto.RwLock()
        async with lock.read():
            assert completes_at_once(lock.acquire_read())
            assert lock.readers == 2
            assert repr(lock) == "<porto.RwLock read-locked by 2>"
            assert not lock.try_acquire_write()
            lock.release_read()
        assert lock.readers == 0
        async with lock.write():
            assert lock.write_locked()
            assert repr(lock) == "<porto.RwLock write-locked>"
            assert not lock.try_acquire_read()
        assert repr(lock) == "<porto.RwLock unlocked>"

    porto.run(main)


def test_a_waiting_writer_blocks_new_readers() -> None:
    lock = porto.RwLock()
    order: list[str] = []

    async def writer() -> None:
        async with lock.write():
            order.append("writer")

    async def reader() -> None:
        async with lock.read():
            order.append("late reader")

    async def main() -> None:
        async with porto.scope() as s, lock.read():
            s.spawn(writer)
            await settle()
            # Readable (a reader holds it), but a writer waits: the reader queues.
            assert not lock.try_acquire_read()
            s.spawn(reader)
            await settle()
            assert order == []
        assert order == ["writer", "late reader"]

    porto.run(main)


def test_a_cancelled_writer_unblocks_the_readers_queued_behind_it() -> None:
    lock = porto.RwLock()
    tokens: list[_core._TaskToken] = []
    got: list[str] = []

    async def writer() -> None:
        tokens.append(_core._current_task_token())
        try:
            async with lock.write():
                got.append("writer")
        except KeyError:
            got.append("writer interrupted")

    async def reader(n: int) -> None:
        async with lock.read():
            got.append(f"reader {n}")
            await settle()

    async def main() -> None:
        async with porto.scope() as s:
            await lock.acquire_read()
            s.spawn(writer)
            await settle()
            s.spawn(reader, 1)
            s.spawn(reader, 2)
            await settle()
            assert got == []
            _core._interrupt(tokens[0], KeyError("cancelled"))
            await settle()
            assert got == ["writer interrupted", "reader 1", "reader 2"]
            assert lock.readers == 3
            lock.release_read()

    porto.run(main)


def test_waiters_are_served_in_arrival_order_with_readers_together() -> None:
    lock = porto.RwLock()
    order: list[str] = []

    async def take(name: str, write: bool) -> None:
        async with lock.write() if write else lock.read():
            order.append(name)
            await settle()

    async def main() -> None:
        async with porto.scope() as s, lock.write():
            for name, write in [("r1", False), ("r2", False), ("w", True), ("r3", False)]:
                s.spawn(take, name, write)
            await settle()
        assert order == ["r1", "r2", "w", "r3"]

    porto.run(main)


def test_a_waiter_cancelled_after_its_grant_passes_the_lock_on() -> None:
    lock = porto.RwLock()

    async def writer() -> None:
        await lock.acquire_write()

    async def main() -> None:
        await lock.acquire_write()
        _core._spawn_joinable(writer())
        _core._spawn_joinable(writer())
        await settle()
        # Hands the lock to the first writer, which never runs: the run ends and cancels
        # both; each passes the lock on, and the last releases it.
        lock.release_write()

    _core._run(main())
    assert not lock.write_locked(), "the lock leaked"


def test_releasing_what_is_not_held_raises() -> None:
    lock = porto.RwLock()
    with pytest.raises(RuntimeError, match=r"release_read\(\) was called while no reader"):
        lock.release_read()
    with pytest.raises(RuntimeError, match=r"release_write\(\) was called while no writer"):
        lock.release_write()
    assert lock.try_acquire_read()
    with pytest.raises(RuntimeError, match=r"no writer holds the lock"):
        lock.release_write()
    lock.release_read()


def test_an_entry_exited_without_being_entered_says_so() -> None:
    async def main() -> None:
        lock = porto.RwLock()
        entry = lock.read()
        with pytest.raises(RuntimeError, match=r"RwLock\.read\(\)\.__aexit__\(\) was called"):
            await entry.__aexit__(None, None, None)
        entry.close()
        async with lock.write() as _:
            pass
        reused = lock.write()
        async with reused:
            pass
        with pytest.raises(RuntimeError, match=r"already exited"):
            await reused.__aexit__(None, None, None)
        assert not lock.write_locked()

    porto.run(main)


def test_read_and_write_do_nothing_until_entered() -> None:
    async def main() -> None:
        lock = porto.RwLock()
        entry = lock.write()
        with pytest.warns(RuntimeWarning, match=r"RwLock\.write\(\) was never awaited"):
            del entry
        waiting = lock.acquire_read()
        with pytest.warns(RuntimeWarning, match=r"RwLock\.acquire_read\(\) was never awaited"):
            del waiting
        assert lock.readers == 0
        assert not lock.write_locked()

    porto.run(main)


def test_entering_a_free_lock_takes_it_when_awaited_without_suspending() -> None:
    async def main() -> None:
        lock = porto.RwLock()
        entry = lock.write()
        assert entry.__aenter__() is entry
        assert not lock.write_locked()
        assert completes_at_once(entry)
        assert lock.write_locked()
        await entry.__aexit__(None, None, None)
        assert not lock.write_locked()

    porto.run(main)


def test_a_semaphore_entry_is_not_a_context_manager() -> None:
    async def main() -> None:
        entering = porto.Semaphore(1).__aenter__()
        with pytest.raises(TypeError, match=r"not a context manager"):
            entering.__aenter__()  # pyright: ignore[reportAttributeAccessIssue]
        entering.close()  # pyright: ignore[reportAttributeAccessIssue]

    porto.run(main)


def test_blocking_acquires_from_a_thread_and_refused_on_a_lane() -> None:
    lock = porto.RwLock()
    acquired = threading.Event()

    def take() -> None:
        lock.blocking_acquire_write()
        acquired.set()
        lock.release_write()

    async def main() -> None:
        with pytest.raises(RuntimeError, match=r"RwLock\.blocking_acquire_read\(\)"):
            lock.blocking_acquire_read()
        async with lock.read():
            threading.Thread(target=take).start()
            await porto.sleep(0.02)
            assert not acquired.is_set()

    porto.run(main)
    assert acquired.wait(5)


def test_a_mixed_load_of_1000_iterations_starves_nobody() -> None:
    lock = porto.RwLock()
    shared = [0]
    done: list[str] = []
    rng = random.Random(16)

    async def writer(n: int) -> None:
        for _ in range(250):
            async with lock.write():
                value = shared[0]
                await porto.checkpoint()
                shared[0] = value + 1
        done.append(f"w{n}")

    async def reader(n: int) -> None:
        for _ in range(250):
            async with lock.read():
                value = shared[0]
                if rng.random() < 0.5:
                    await porto.checkpoint()
                assert shared[0] == value, "a writer ran inside a read lock"
        done.append(f"r{n}")

    async def main() -> None:
        async with porto.scope() as s:
            for n in range(2):
                s.spawn(writer, n)
            for n in range(2):
                s.spawn(reader, n)

    porto.run(main)
    assert shared[0] == 500
    assert sorted(done) == ["r0", "r1", "w0", "w1"]
