"""The native extension module, built from crates/porto-py.

Everything except ``__version__`` is internal to porto: the trap protocol's awaitables
(docs/spec/10-porto-runtime/11-task-model-and-trap-protocol.md §5-§6) and test hooks.
"""

from collections.abc import Callable, Generator
from types import TracebackType
from typing import Generic, Literal, NoReturn, TypeVar, final, overload

_T_co = TypeVar("_T_co", covariant=True)

__version__: str

@final
class _Trap:
    """A request to the driver. Awaiting it yields the trap, then returns what the driver sends.

    A trap is awaited exactly once: resuming it again raises ``RuntimeError("trap reused")``.
    """

    @property
    def kind(self) -> Literal["yield", "sleep"]:
        """What the trap asks the driver for."""

    @property
    def deadline(self) -> float | None:
        """For a sleep, when it ends, in seconds on the runtime clock (``_monotonic()``)."""

    def __await__(self) -> Generator[_Trap, object, object]: ...
    def __iter__(self) -> Generator[_Trap, object, object]: ...
    def __next__(self) -> _Trap: ...
    def send(self, value: object, /) -> _Trap: ...
    @overload
    def throw(
        self, typ: BaseException, val: None = None, tb: TracebackType | None = None, /
    ) -> NoReturn: ...
    @overload
    def throw(
        self,
        typ: type[BaseException],
        val: object = None,
        tb: TracebackType | None = None,
        /,
    ) -> NoReturn: ...
    def close(self) -> None: ...

@final
class _Ready(Generic[_T_co]):
    """An awaitable that already holds its value: awaiting it never suspends."""

    def __await__(self) -> Generator[_Trap, object, _T_co]: ...
    def __iter__(self) -> Generator[_Trap, object, _T_co]: ...
    def __next__(self) -> NoReturn: ...
    def send(self, value: object, /) -> NoReturn: ...
    @overload
    def throw(
        self, typ: BaseException, val: None = None, tb: TracebackType | None = None, /
    ) -> NoReturn: ...
    @overload
    def throw(
        self,
        typ: type[BaseException],
        val: object = None,
        tb: TracebackType | None = None,
        /,
    ) -> NoReturn: ...
    def close(self) -> None: ...

def _monotonic() -> float:
    """The runtime clock, in seconds: what sleep deadlines are measured on."""

def _yield_trap() -> _Trap:
    """A trap that yields to the scheduler once."""

def _sleep_trap(delay: float, /) -> _Trap:
    """A trap that resumes the task ``delay`` seconds from now, never earlier."""

def _make_ready(value: _T_co, /) -> _Ready[_T_co]:
    """Test hook: a ready awaitable holding ``value``."""

def _send(iterator: object, value: object, /) -> tuple[bool, object]:
    """Test hook: ``PyIter_Send``. ``(True, result)`` on return, ``(False, yielded)`` on yield."""

def _count_allocations(function: Callable[[], object], /) -> int:
    """Test hook: allocations CPython makes (MEM and OBJ domains) while ``function()`` runs."""
