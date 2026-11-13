"""The default resolver, ``porto.net.resolve`` (17-networking-tls-dns.md §4): ``getaddrinfo`` on
the blocking pool, a 30 s positive cache and the coalescing of concurrent identical lookups.

The blocking lookup and the cache's clock are replaced (``porto._dns._lookup``, ``_clock``), so
nothing here depends on a DNS server. Waits have generous limits.
"""

import socket
import threading
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

import porto
from porto import _core, _dns, net  # pyright: ignore[reportPrivateUsage]

Address = net.SockAddr


class Lookups:
    """A counting ``getaddrinfo`` that can be held until a test releases it."""

    def __init__(self, answer: list[Address] | None = None, *, hold: bool = False) -> None:
        self.answer = answer if answer is not None else [("192.0.2.1", 0)]
        self.calls: list[tuple[str, int, int]] = []
        self.error: Exception | None = None
        self.started = threading.Event()
        self.release = threading.Event()
        if not hold:
            self.release.set()

    def __call__(self, host: str, port: int, family: int) -> list[Address]:
        self.calls.append((host, port, family))
        self.started.set()
        assert self.release.wait(20), "the test never released the lookup"
        if self.error is not None:
            raise self.error
        return [(address[0], port, *address[2:]) for address in self.answer]  # type: ignore[misc]


@pytest.fixture
def lookups(monkeypatch: pytest.MonkeyPatch) -> Lookups:
    fake = Lookups()
    monkeypatch.setattr(_dns, "_lookup", fake)
    return fake


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    now = [1000.0]
    monkeypatch.setattr(_dns, "_clock", lambda: now[0])
    return now


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return porto.run(coro)


async def until(condition: Callable[[], bool], what: str, seconds: float = 20.0) -> None:
    deadline = porto.current_time() + seconds
    while not condition():
        assert porto.current_time() < deadline, f"timed out waiting for {what}"
        await porto.sleep(0.005)


def test_a_hundred_concurrent_resolves_of_one_host_issue_one_lookup(lookups: Lookups) -> None:
    lookups.release.clear()

    async def main() -> list[list[Address]]:
        async with porto.scope() as s:
            tasks = [s.spawn(net.resolve, "Example.test", 443) for _ in range(100)]
            await until(lookups.started.is_set, "the lookup to start")
            await porto.sleep(0.1)  # let the other 99 reach the wait
            lookups.release.set()
        return [task.result() for task in tasks]

    answers = run(main())
    assert lookups.calls == [("example.test", 443, 0)]
    assert answers == [[("192.0.2.1", 443)]] * 100
    # Each caller owns its list.
    assert len({id(answer) for answer in answers}) == 100


def test_the_answer_is_cached_and_expires_after_the_ttl(
    lookups: Lookups, clock: list[float]
) -> None:
    async def main() -> None:
        await net.resolve("example.test")
        await net.resolve("example.test")
        assert len(lookups.calls) == 1
        clock[0] += porto._dns.POSITIVE_TTL - 0.1  # pyright: ignore[reportPrivateUsage]
        await net.resolve("example.test")
        assert len(lookups.calls) == 1, "still fresh just before the TTL"
        clock[0] += 0.2
        await net.resolve("example.test")
        assert len(lookups.calls) == 2, "expired after the TTL"
        await net.resolve("example.test")
        assert len(lookups.calls) == 2, "the new entry is cached again"

    run(main())


def test_the_ttl_is_thirty_seconds() -> None:
    assert _dns.POSITIVE_TTL == 30.0


def test_host_port_and_family_are_part_of_the_key(lookups: Lookups) -> None:
    async def main() -> None:
        await net.resolve("a.test", 1)
        await net.resolve("A.TEST", 1)  # names are case-insensitive: the same entry
        await net.resolve("a.test", 2)
        await net.resolve("a.test", 1, family=socket.AF_INET)
        await net.resolve("b.test", 1)

    run(main())
    assert lookups.calls == [
        ("a.test", 1, 0),
        ("a.test", 2, 0),
        ("a.test", 1, int(socket.AF_INET)),
        ("b.test", 1, 0),
    ]


def test_a_caller_cannot_change_the_cached_answer(lookups: Lookups) -> None:
    async def main() -> list[Address]:
        first = await net.resolve("example.test", 80)
        first.append(("203.0.113.9", 80))
        return await net.resolve("example.test", 80)

    assert run(main()) == [("192.0.2.1", 80)]


def test_the_cache_does_not_outlive_the_run(lookups: Lookups) -> None:
    async def main() -> None:
        await net.resolve("example.test")

    run(main())
    run(main())
    assert len(lookups.calls) == 2


def test_an_ip_literal_is_not_looked_up_and_takes_no_thread(lookups: Lookups) -> None:
    async def main() -> tuple[Any, ...]:
        before = _core._blocking_stats()  # pyright: ignore[reportPrivateUsage]
        v4 = await net.resolve("127.0.0.1", 8080)
        v6 = await net.resolve("::1", 8080)
        only_v4 = await net.resolve("10.1.2.3", 1, family=socket.AF_INET)
        after = _core._blocking_stats()  # pyright: ignore[reportPrivateUsage]
        return before, after, v4, v6, only_v4

    before, after, v4, v6, only_v4 = run(main())
    assert lookups.calls == []
    assert before == after, "an IP literal must not touch the blocking pool"
    assert v4 == [("127.0.0.1", 8080)]
    assert v6 == [("::1", 8080, 0, 0)]
    assert only_v4 == [("10.1.2.3", 1)]


def test_an_ip_literal_of_the_wrong_family_is_a_gaierror(lookups: Lookups) -> None:
    async def main() -> None:
        await net.resolve("127.0.0.1", 1, family=socket.AF_INET6)

    with pytest.raises(socket.gaierror):
        run(main())
    assert lookups.calls == []


def test_a_failed_lookup_raises_gaierror_and_is_not_cached(lookups: Lookups) -> None:
    lookups.error = socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    async def main() -> None:
        for _ in range(2):
            with pytest.raises(socket.gaierror) as caught:
                await net.resolve("nowhere.test", 80)
            assert caught.value.errno == socket.EAI_NONAME
        lookups.error = None
        assert await net.resolve("nowhere.test", 80) == [("192.0.2.1", 80)]

    run(main())
    assert len(lookups.calls) == 3


def test_every_waiter_of_a_failing_lookup_gets_its_own_gaierror(lookups: Lookups) -> None:
    lookups.release.clear()
    lookups.error = socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    async def attempt() -> BaseException:
        try:
            await net.resolve("nowhere.test")
        except BaseException as error:
            return error
        raise AssertionError("resolve returned")

    async def main() -> list[BaseException]:
        async with porto.scope() as s:
            tasks = [s.spawn(attempt) for _ in range(10)]
            await until(lookups.started.is_set, "the lookup to start")
            await porto.sleep(0.05)
            lookups.release.set()
        return [task.result() for task in tasks]

    errors = run(main())
    assert len(lookups.calls) == 1
    assert all(isinstance(error, socket.gaierror) for error in errors)
    assert len({id(error) for error in errors}) == 10


def test_cancelling_the_task_that_started_the_lookup_leaves_the_others_their_answer(
    lookups: Lookups,
) -> None:
    lookups.release.clear()

    async def main() -> list[Address]:
        async with porto.scope() as s:
            first = s.spawn(net.resolve, "example.test")
            await until(lookups.started.is_set, "the lookup to start")
            second = s.spawn(net.resolve, "example.test")
            await porto.sleep(0.05)
            first.cancel()
            await porto.sleep(0.05)
            lookups.release.set()
        return second.result()

    assert run(main()) == [("192.0.2.1", 0)]
    assert len(lookups.calls) == 1


def test_a_cancelled_resolve_still_fills_the_cache(lookups: Lookups) -> None:
    lookups.release.clear()

    async def main() -> None:
        with porto.move_on_after(0.05):
            await net.resolve("example.test")
        lookups.release.set()
        await until(lambda: lookups.calls != [] and _dns._current().cache != {}, "the cache")  # pyright: ignore[reportPrivateUsage]
        assert await net.resolve("example.test") == [("192.0.2.1", 0)]

    run(main())
    assert len(lookups.calls) == 1


def test_a_name_that_exists_resolves_with_the_real_getaddrinfo() -> None:
    async def main() -> list[Address]:
        return await net.resolve("localhost", 4242)

    addresses = run(main())
    assert addresses
    assert all(address[1] == 4242 for address in addresses)
    assert {address[0] for address in addresses} <= {"127.0.0.1", "::1"}


@pytest.mark.parametrize(
    ("args", "kwargs", "error"),
    [
        ((b"example.test",), {}, TypeError),
        ((42,), {}, TypeError),
        (("",), {}, ValueError),
        (("example.test", "80"), {}, TypeError),
        (("example.test", True), {}, TypeError),
        (("example.test", -1), {}, ValueError),
        (("example.test", 65536), {}, ValueError),
        (("example.test", 80), {"family": 99}, ValueError),
    ],
)
def test_a_wrong_argument_says_what_to_pass(
    lookups: Lookups, args: tuple[Any, ...], kwargs: dict[str, Any], error: type[Exception]
) -> None:
    async def main() -> None:
        await net.resolve(*args, **kwargs)

    with pytest.raises(error, match="porto: resolve's"):
        run(main())
    assert lookups.calls == []


@pytest.mark.parametrize("host", ["example..com", "a" * 64 + ".test"])
def test_a_malformed_host_name_is_a_gaierror(host: str) -> None:
    async def main() -> None:
        with pytest.raises(socket.gaierror, match="not a valid host name"):
            await net.resolve(host, 80)

    run(main())


def test_a_failure_while_storing_the_answer_still_wakes_every_waiter(
    lookups: Lookups, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_store(self: Any, key: Any, addresses: Any) -> None:
        raise RuntimeError("dictionary changed size during iteration")

    monkeypatch.setattr(_dns._Resolver, "store", broken_store)  # pyright: ignore[reportPrivateUsage]

    async def main() -> list[list[net.SockAddr]]:
        results: list[list[net.SockAddr]] = []

        async def one() -> None:
            results.append(await net.resolve("host.test", 80))

        async with porto.move_on_after(5), porto.scope() as s:
            for _ in range(3):
                s.spawn(one)
        return results

    assert len(run(main())) == 3
