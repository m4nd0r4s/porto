"""``porto.Buffer`` and the per-lane buffer pool (13-io-driver.md §6).

The pool's use shows in ``_core._buffer_pool_stats()``: one ``(capacity, free, hits, misses,
discarded)`` tuple per size class, for the lane running the caller (cleared when a run ends).
"""

import socket
import sys
import sysconfig
import threading

import pytest

import porto
from porto import _core

KIB = 1024
CLASSES = [4 * KIB, 16 * KIB, 64 * KIB, 256 * KIB]


def stats_of(capacity: int) -> tuple[int, int, int, int]:
    """``(free, hits, misses, discarded)`` of the class with this capacity."""
    for row in _core._buffer_pool_stats():
        if row[0] == capacity:
            return row[1], row[2], row[3], row[4]
    raise AssertionError(f"no size class of {capacity} bytes")


def test_a_buffer_starts_zeroed_and_copies_on_bytes() -> None:
    buffer = porto.Buffer(5)
    assert len(buffer) == 5
    assert bytes(buffer) == b"\0" * 5
    assert porto.Buffer().capacity == 0
    assert repr(porto.Buffer(3)).startswith("porto.Buffer(len=3, capacity=")


def test_a_memoryview_over_a_buffer_is_readable_and_writable() -> None:
    buffer = porto.Buffer(4)
    with memoryview(buffer) as view:
        assert not view.readonly
        assert view.format == "B"
        assert view.nbytes == 4
        view[:] = b"porto"[:4]
        assert view.tobytes() == b"port"
    assert bytes(buffer) == b"port"


def test_a_buffer_grows_across_size_classes_and_keeps_its_bytes() -> None:
    buffer = porto.Buffer(10)
    assert buffer.capacity == CLASSES[0]
    buffer.extend(b"x" * 5000)
    assert len(buffer) == 5010
    assert buffer.capacity == CLASSES[1]
    buffer.resize(3)
    assert bytes(buffer) == b"\0\0\0"
    buffer.extend(buffer)
    assert len(buffer) == 6
    large = porto.Buffer(1 << 20)
    assert large.capacity == 1 << 20


def test_a_buffer_cannot_change_size_while_a_view_is_exported() -> None:
    buffer = porto.Buffer(8)
    view = memoryview(buffer)
    with pytest.raises(BufferError, match="release the memoryview"):
        buffer.resize(16)
    with pytest.raises(BufferError, match="view of it is exported"):
        buffer.extend(b"more")
    view.release()
    buffer.resize(16)
    assert len(buffer) == 16


def test_extend_takes_only_bytes_like_objects() -> None:
    with pytest.raises(TypeError, match="bytes-like object"):
        porto.Buffer().extend("text")  # pyright: ignore[reportArgumentType]


def test_a_negative_size_is_refused() -> None:
    with pytest.raises(OverflowError):
        porto.Buffer(-1)


def test_an_impossible_size_raises_memory_error() -> None:
    with pytest.raises((MemoryError, OverflowError)):
        porto.Buffer(sys.maxsize)


def test_the_pool_reuses_a_freed_buffer_on_the_lane() -> None:
    async def main() -> tuple[tuple[int, int, int, int], tuple[int, int, int, int]]:
        first = porto.Buffer(1000)
        del first
        after_free = stats_of(CLASSES[0])
        second = porto.Buffer(2000)
        after_reuse = stats_of(CLASSES[0])
        del second
        return after_free, after_reuse

    (free, hits, misses, _), (free_after, hits_after, misses_after, _) = porto.run(main())
    assert (free, hits, misses) == (1, 0, 1)
    assert (free_after, hits_after, misses_after) == (0, 1, 1)
    # The run's pool is emptied when it ends.
    assert stats_of(CLASSES[0]) == (0, 0, 0, 0)


def test_100_buffers_in_a_row_reuse_one_allocation_per_class() -> None:
    async def main() -> list[tuple[int, int, int, int]]:
        for _ in range(100):
            for capacity in CLASSES:
                buffer = porto.Buffer(capacity)
                buffer.extend(b"x")  # one past the class: moves to the next one
                del buffer
        return [stats_of(capacity) for capacity in CLASSES]

    rows = porto.run(main())
    # Each class allocated at most once (the one past 256 KiB is exact, unpooled), and the
    # free lists did not grow: zero growth over 100 rounds.
    for free, hits, misses, discarded in rows:
        assert misses <= 1
        assert free <= 1
        assert hits >= 99
        assert discarded == 0


def test_the_pool_keeps_a_bounded_amount_of_free_storage() -> None:
    async def main() -> tuple[int, int, int, int]:
        buffers = [porto.Buffer(CLASSES[3]) for _ in range(10)]
        del buffers
        return stats_of(CLASSES[3])

    free, _, misses, discarded = porto.run(main())
    assert misses == 10
    # 1 MiB per class: four 256 KiB buffers are kept, the rest freed.
    assert (free, discarded) == (4, 6)


@pytest.mark.skipif(sys.platform == "win32", reason="registering raw sockets needs AFD (Windows)")
def test_read_buffer_and_read_into_fill_a_buffer_without_a_copy() -> None:
    async def main() -> tuple[bytes, int, bytes, int]:
        left, right = socket.socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            left.send(b"zero-copy")
            filled = await handle.read_buffer(64)
            assert type(filled) is porto.Buffer
            left.send(b"into")
            target = porto.Buffer(8)
            count = await handle.read_into(target)
            handle.close()
            return bytes(filled), filled.capacity, bytes(target), count

    data, capacity, target, count = porto.run(main())
    assert data == b"zero-copy"
    assert capacity == CLASSES[0]
    assert (target, count) == (b"into\0\0\0\0", 4)


@pytest.mark.skipif(sys.platform == "win32", reason="registering raw sockets needs AFD (Windows)")
def test_a_pending_read_into_holds_its_buffer_at_a_fixed_size() -> None:
    async def main() -> None:
        left, right = socket.socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            target = porto.Buffer(8)
            awaitable = handle.read_into(target)
            with pytest.raises(BufferError):
                target.resize(1)
            del awaitable
            target.resize(1)
            handle.close()

    porto.run(main())


needs_buffer_dunder = pytest.mark.skipif(
    sys.version_info < (3, 12), reason="__buffer__ in Python classes needs 3.12 (PEP 688)"
)


@needs_buffer_dunder
def test_extend_with_data_whose_buffer_exports_this_buffer_is_refused() -> None:
    """``data.__buffer__`` runs Python code: if it exports ``buf`` and keeps the view, growing
    ``buf`` would free the memory that view points to."""
    buffer = porto.Buffer(4)
    with memoryview(buffer) as view:
        view[:] = b"keep"
    kept: list[memoryview] = []

    class Evil:
        def __buffer__(self, flags: int, /) -> memoryview:
            kept.append(memoryview(buffer))
            return memoryview(b"x" * 100_000)

    with pytest.raises(BufferError, match="view of it is exported"):
        buffer.extend(Evil())  # pyright: ignore[reportArgumentType]
    assert bytes(kept[0]) == b"keep"
    assert len(buffer) == 4
    kept[0].release()
    buffer.extend(b"!")
    assert bytes(buffer) == b"keep!"


@needs_buffer_dunder
def test_extend_with_data_whose_buffer_is_this_buffer_is_refused() -> None:
    buffer = porto.Buffer(4)

    class Mirror:
        def __buffer__(self, flags: int, /) -> memoryview:
            return memoryview(buffer)

    with pytest.raises(BufferError, match="view of it is exported"):
        buffer.extend(Mirror())  # pyright: ignore[reportArgumentType]
    assert bytes(buffer) == b"\0\0\0\0"
    buffer.extend(b"ok")
    assert len(buffer) == 6


FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


@pytest.mark.skipif(not FREE_THREADED, reason="needs a free-threaded build (3.14t)")
def test_views_and_reads_race_on_threads_without_failing() -> None:
    """On a free-threaded build, exporting and releasing views while other threads read the
    buffer never fails, and the export count ends at zero (the buffer can resize again)."""
    buffer = porto.Buffer(64)
    errors: list[BaseException] = []
    barrier = threading.Barrier(4)

    def export_views() -> None:
        barrier.wait()
        try:
            for _ in range(20_000):
                with memoryview(buffer) as view:
                    view[0] = 1
        except BaseException as error:
            errors.append(error)

    def read() -> None:
        barrier.wait()
        try:
            for _ in range(20_000):
                assert len(buffer) == 64
                bytes(buffer)
                repr(buffer)
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=f) for f in (export_views, export_views, read, read)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    buffer.resize(1)
