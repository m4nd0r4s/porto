"""The runtime's lifecycle, as libraries see it (26-errors-config-lifecycle.md §2).

A shutdown starts when ``Runtime.shutdown()`` is called or a signal the runtime handles
arrives (SIGINT, SIGTERM). From then on main has ``graceful_timeout`` to finish by itself, and
code that serves requests uses that time to stop taking new work and finish what it has::

    async def serve() -> None:
        async with porto.scope() as s:
            s.spawn(accept_loop)
            await porto.lifecycle.shutdown_requested().wait()
            await stop_accepting_and_drain()
"""

from porto import _core
from porto._core import Event

__all__ = ["shutdown_requested"]


def shutdown_requested() -> Event:
    """The event that is set once the running runtime was asked to shut down.

    Raises ``porto.NoRuntimeError`` outside a running runtime.
    """
    return _core._shutdown_requested()  # pyright: ignore[reportPrivateUsage]
