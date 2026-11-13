"""Shutting down, and receiving signals: ``Runtime.shutdown``, ``porto.lifecycle``,
``porto.signals.receive``.

    uv run python examples/shutdown_and_signals.py

A shutdown starts with ``Runtime.shutdown(timeout=...)`` or a signal (SIGINT, SIGTERM; the runtime
handles those by default on the main thread). ``porto.lifecycle.shutdown_requested()`` is an
``Event`` that code serving requests waits on to stop taking work; main has ``timeout`` seconds
(``graceful_timeout`` for SIGTERM) to finish by itself, then the root scope is cancelled, and
what still runs after ``cancel_timeout`` is abandoned with a warning. After a SIGINT
``porto.run`` raises ``KeyboardInterrupt``, after a SIGTERM ``porto.Terminated``; a second
Ctrl-C forces the exit (``tests/python/test_signal_defaults.py`` runs those in subprocesses).
``porto.signals.receive`` hands any other signal to a task, on Unix.
"""

import os
import signal
import sys
import warnings

import porto


async def graceful_drain() -> list[str]:
    """main asks for the shutdown, drains, and finishes: ``porto.run`` returns normally."""
    log: list[str] = []
    requested = porto.lifecycle.shutdown_requested()
    assert not requested.is_set()
    porto.runtime().shutdown(timeout=5)  # from a task: returns at once, the drain goes on
    await requested.wait()
    log.append("shutdown requested")
    await porto.sleep(0.01)  # finishing the work in hand
    log.append("drained")
    return log


async def main_that_ignores_the_request() -> None:
    """After the graceful period main is cancelled, and ``porto.run`` raises ``Cancelled``."""
    porto.runtime().shutdown(timeout=0.02)
    await porto.sleep(30)


async def stuck_task() -> None:
    """Shielded for longer than ``cancel_timeout``: abandoned, with a warning that counts it."""
    with porto.shield():
        await porto.sleep(30)


async def abandon_it() -> None:
    porto.runtime().root_scope.spawn(stuck_task, name="stuck")
    await porto.sleep(0.01)


async def receive_a_signal() -> int:
    async with porto.signals.receive(signal.SIGUSR1) as signals:
        os.kill(os.getpid(), signal.SIGUSR1)
        async for signum in signals:
            return signum
    raise AssertionError("the iterator ended")


def main() -> None:
    assert porto.run(graceful_drain) == ["shutdown requested", "drained"]

    try:
        porto.run(main_that_ignores_the_request)
    except porto.Cancelled:
        pass
    else:
        raise AssertionError("main was cancelled after the graceful period")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        porto.run(abandon_it, cancel_timeout=0.02)
    assert any("1 task(s) of the root scope" in str(w.message) for w in caught)

    if sys.platform != "win32":
        assert porto.run(receive_a_signal) == signal.SIGUSR1
    print("ok")


if __name__ == "__main__":
    main()
