"""Deadlines and shields: ``timeout``, ``move_on_after``, ``Scope.deadline`` and ``shield``.

    uv run python examples/deadlines_and_shields.py

A deadline is a cancellation that fires by itself: every scope can have one, and a scope past it
is cancelled like any other (see ``cancellation.py``). ``porto.timeout(s)`` raises the builtin
``TimeoutError`` when the block was cut short; ``porto.move_on_after(s)`` absorbs the cut and
lets the program go on. A shield is the opposite: a scope that the cancels and deadlines of the
scopes around it do not reach, for cleanup that must finish.
"""

import contextlib

import porto


class Connection:
    """A pretend database connection whose rollback takes a little while."""

    def __init__(self) -> None:
        self.rolled_back = False

    async def rollback(self) -> None:
        await porto.sleep(0.01)
        self.rolled_back = True


async def timeout_raises() -> None:
    """``porto.timeout`` turns "too slow" into the builtin ``TimeoutError``."""
    started = porto.current_time()
    s = porto.timeout(0.02)
    try:
        async with s:
            await porto.sleep(10)  # cut short after 20 ms, not 10 s
    except TimeoutError:
        pass
    else:
        raise AssertionError("the deadline did not raise TimeoutError")
    assert porto.current_time() - started < 1.0
    assert s.timed_out and s.cancel_called

    # A block that finishes in time raises nothing and reports nothing.
    async with porto.timeout(10) as quick:
        await porto.sleep(0.001)
    assert not quick.timed_out and not quick.cancel_called


async def move_on() -> list[str]:
    """``porto.move_on_after`` gives up quietly: ``timed_out`` tells whether it had to."""
    log: list[str] = []
    async with porto.move_on_after(0.02) as s:
        await porto.sleep(10)
        log.append("never reached")  # the cancel ended the block at the await above
    # No exception: the scope absorbed its own Cancelled.
    assert s.timed_out and s.cancelled_caught
    log.append("moved on")

    async with porto.move_on_after(10) as fast:
        await porto.sleep(0.001)
    assert not fast.timed_out and not fast.cancelled_caught
    return log


async def deadline_on_children() -> None:
    """The deadline covers the tasks spawned into the scope, not only its body."""
    cleaned_up: list[str] = []

    async def slow_download() -> None:
        try:
            await porto.sleep(10)
        finally:
            cleaned_up.append("download")

    s = porto.scope(timeout=0.02, raise_on_timeout=False)  # what move_on_after(0.02) is
    async with s:
        s.spawn(slow_download)
    assert cleaned_up == ["download"] and s.timed_out


async def moving_the_deadline() -> None:
    """``Scope.deadline`` is a time on the runtime clock; assigning to it re-arms the timer."""
    async with porto.move_on_after(0.01) as s:
        assert s.deadline > porto.current_time() - 0.01  # a time on the runtime clock
        s.deadline = porto.current_time() + 0.5  # more time, say after a sign of progress
        await porto.sleep(0.03)  # would have been cut at 10 ms
        assert not s.cancel_called
    assert not s.timed_out

    # A deadline already passed cancels the scope now.
    async with porto.move_on_after(10) as sooner:
        sooner.deadline = porto.current_time()
        await porto.checkpoint()
        raise AssertionError("the scope was not cancelled")
    assert sooner.timed_out

    # inf removes the deadline.
    async with porto.move_on_after(0.01) as forever:
        forever.deadline = float("inf")
        await porto.sleep(0.03)
    assert not forever.timed_out and forever.deadline == float("inf")


async def nested_deadlines() -> None:
    """The earliest deadline wins, whichever scope it belongs to."""
    outer = porto.timeout(0.02)
    inner = porto.move_on_after(10)  # far later than the outer deadline
    try:
        async with outer:
            async with inner:
                await porto.sleep(10)
            raise AssertionError("the inner scope absorbed the outer deadline")
    except TimeoutError:
        pass
    # The cancel came from the outer scope, so it counts for the outer scope only.
    assert outer.timed_out and not inner.timed_out

    # The other way round, the inner deadline fires first and the outer block goes on.
    async with porto.timeout(10) as outer_scope:
        async with porto.move_on_after(0.01) as inner_scope:
            await porto.sleep(10)
        assert inner_scope.timed_out
        await porto.sleep(0.001)  # the outer scope is untouched
    assert not outer_scope.timed_out


async def shielded_cleanup() -> None:
    """A cancelled task can still clean up: in ``finally``, enter a shield with sync ``with``.

    This is the rollback pattern (spec 15, ST-9): once a scope is cancelled every await in it
    raises ``Cancelled``, so a cleanup that awaits would be cut short without the shield. The
    shield's own timeout bounds it, so a hung database cannot hold the shutdown forever.
    """
    safe, careless = Connection(), Connection()

    async def transaction() -> None:
        try:
            await porto.sleep(10)
        finally:
            with porto.shield(timeout=5):  # the cancel of the enclosing scope stops here
                await safe.rollback()

    async def hasty_transaction() -> None:
        try:
            await porto.sleep(10)
        finally:
            with contextlib.suppress(porto.Cancelled):
                await careless.rollback()  # cancelled already: raises at its first await

    shielded: porto.Task[None] | None = None
    async with porto.move_on_after(0.02) as s:
        shielded = s.spawn(transaction)
        s.spawn(hasty_transaction)
    assert s.timed_out
    assert safe.rolled_back  # the shielded rollback ran to the end
    assert not careless.rolled_back  # the unshielded one was cut at its first await
    # The shield only delays the cancel: the task still ends cancelled.
    assert shielded is not None and shielded.cancelled()


async def shield_scope_rules() -> None:
    """A shield is a cancel scope: it holds outer cancels off, but its own timeout counts."""
    async with porto.move_on_after(0.01) as outer:
        shield = porto.shield(timeout=0.1)
        try:
            with shield:  # sync ``with``: a cancel scope only, no await at entry or exit
                await porto.sleep(0.02)  # the outer deadline passed 10 ms ago: not delivered
                assert outer.cancel_called and not shield.cancel_called
                await porto.sleep(10)  # the shield's own deadline ends this wait
        except TimeoutError:  # like porto.timeout(), a shield with a timeout raises it
            pass
        else:
            raise AssertionError("the shield's own timeout did not fire")
        assert shield.timed_out
        # Back outside the shield, the outer cancel reaches the block again.
        await porto.checkpoint()
        raise AssertionError("the outer deadline never reached the block")
    assert outer.timed_out

    # A scope entered with ``with`` is a cancel scope only: it has no children.
    async def child() -> None: ...

    with porto.shield() as plain:
        try:
            plain.spawn(child)
        except RuntimeError:
            pass
        else:
            raise AssertionError("spawned into a scope entered with sync with")


async def main() -> None:
    await timeout_raises()
    assert await move_on() == ["moved on"]
    await deadline_on_children()
    await moving_the_deadline()
    await nested_deadlines()
    await shielded_cleanup()
    await shield_scope_rules()
    print("deadlines_and_shields: ok")


if __name__ == "__main__":
    porto.run(main)
