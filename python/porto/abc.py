"""Abstract interfaces of porto's I/O types (17-networking-tls-dns.md §1.1).

``ByteStream`` is the protocol ``porto.net.TcpStream`` implements (and, as they land,
``UnixStream`` and ``TlsStream``), so code that reads and writes bytes can take any of them.
The buffered reads (``read_exact``, ``read_until``, ``readline``, ``read_to_end``) and
``sendfile`` join it with the stream types that implement them.
"""

from collections.abc import Awaitable, Iterable
from types import TracebackType
from typing import Protocol, Self, TypeAlias, runtime_checkable

from porto._core import Buffer

__all__ = ["ByteStream"]

_Bytes: TypeAlias = bytes | bytearray | memoryview | Buffer


@runtime_checkable
class ByteStream(Protocol):
    """A reliable, ordered stream of bytes with a reading and a writing side, closed once."""

    def read(self, max: int = 65536, /) -> Awaitable[bytes]:
        """Receives at most ``max`` bytes; ``b""`` at the end of the stream."""
        ...

    def read_buffer(self, max: int = 65536, /) -> Awaitable[Buffer]:
        """Like ``read``, into a pooled ``porto.Buffer`` with no ``bytes`` copy."""
        ...

    def read_into(self, buffer: bytearray | memoryview | Buffer, /) -> Awaitable[int]:
        """Receives into a writable buffer; the number of bytes, 0 at the end of the stream."""
        ...

    def write(self, data: _Bytes, /) -> Awaitable[None]:
        """Sends all of ``data``, waiting while the stream cannot take more."""
        ...

    def writev(self, buffers: Iterable[_Bytes], /) -> Awaitable[None]:
        """Sends all of ``buffers`` in order."""
        ...

    def shutdown_write(self) -> Awaitable[None]:
        """Half-close: the peer reads the end of the stream; this side can still read."""
        ...

    def aclose(self) -> Awaitable[None]:
        """Closes the stream. Closing again does nothing."""
        ...

    def __aenter__(self) -> Awaitable[Self]: ...

    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...
