"""Receiving signals (18-fs-process-signals.md §3)::

    async with porto.signals.receive(signal.SIGHUP, signal.SIGUSR1) as signals:
        async for signum in signals:
            ...

The runtime installs its own handler for each signal received, which writes the number to a
pipe the I/O driver watches: no Python-level handler runs, and the signal is delivered whichever
thread the OS chose. Leaving the ``async with`` puts the previous handler back (the last
receiver of a signal restores it). Several receivers of one signal all get it. Signals that
arrive close together may be delivered once, as the OS coalesces them.

Not available on Windows yet: ``receive`` raises ``NotImplementedError`` there.
"""

import contextlib
import sys
from types import TracebackType

from porto import _core
from porto._channel import unbounded_channel
from porto._core import Receiver
from porto.errors import PortoError

__all__ = ["receive"]


class receive:
    """``async with porto.signals.receive(*signums) as signals``: ``async for signum in signals``
    yields the number of each signal that arrives while the block runs.

    Raises ``ValueError`` for no signals or one that cannot be caught (``SIGKILL``, ``SIGSTOP``),
    ``porto.NoRuntimeError`` outside a running runtime, and ``NotImplementedError`` on Windows.
    """

    def __init__(self, *signums: int) -> None:
        if not signums:
            raise ValueError(
                "porto.signals.receive() needs at least one signal: pass e.g. "
                "porto.signals.receive(signal.SIGHUP, signal.SIGUSR1)"
            )
        if sys.platform == "win32":
            raise NotImplementedError(
                "porto.signals.receive() is not available on Windows yet: run on Linux or macOS"
            )
        self._signums = tuple(int(signum) for signum in signums)
        self._subscription: _core._SignalSubscription | None = None  # pyright: ignore[reportPrivateUsage]
        self._sender: _core.Sender[int] | None = None

    async def __aenter__(self) -> Receiver[int]:
        if self._subscription is not None:
            raise PortoError(
                "porto: this porto.signals.receive(...) is already entered; make another with "
                "porto.signals.receive(...) for each `async with`"
            )
        sender, receiver = unbounded_channel[int]()

        def deliver(signum: int) -> None:
            # The receiver may have gone away between the signal and now: nobody to tell.
            with contextlib.suppress(PortoError):
                sender.try_send(signum)

        self._subscription = _core._signals_subscribe(self._signums, deliver)  # pyright: ignore[reportPrivateUsage]
        self._sender = sender
        return receiver

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        subscription, sender = self._subscription, self._sender
        self._subscription = self._sender = None
        if subscription is not None:
            subscription.close()
        if sender is not None:
            sender.close()
