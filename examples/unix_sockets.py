"""Unix domain sockets: ``porto.net.listen_unix``, ``connect_unix``, ``UnixListener`` and
``UnixStream``.

    uv run python examples/unix_sockets.py

A Unix socket is a stream like a TCP connection (a ``UnixStream`` is a ``porto.abc.ByteStream``,
so the same code reads and writes either) but names its endpoint with a path, not a port.
``listen_unix(path, mode=0o660)`` creates the socket file with exactly that mode, and who may
connect is who may write to it. Closing the listener removes the file it created. The path must
not exist: a file a crashed server left behind is ``EADDRINUSE``, never silently removed.

On Linux a name that starts with a NUL byte is in the abstract namespace: no file, no mode,
and the name is freed when the listener closes.

Unix sockets are Unix-only: on Windows ``listen_unix`` raises ``NotImplementedError`` saying to
use TCP on the loopback.
"""

import os
import shutil
import stat
import sys
import tempfile

import porto
from porto import abc, net


async def shout(stream: abc.ByteStream) -> None:
    """One request/response on any ``ByteStream``: reads a line, answers it upper-cased."""
    async with stream:
        await stream.write((await stream.readline()).upper())


async def serve_one(listener: net.UnixListener) -> None:
    """Accepts one connection (a ``UnixStream``) and serves it."""
    await shout(await listener.accept())


async def file_socket(path: str) -> None:
    async with await net.listen_unix(path, mode=0o600) as listener:
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600  # exactly the mode asked for
        assert listener.sockname == path
        async with porto.scope() as s:
            s.spawn(serve_one(listener))  # accepts a UnixStream, like a TcpStream
            async with await net.connect_unix(path) as client:
                await client.write(b"hello unix\n")
                assert await client.readline() == b"HELLO UNIX\n"
                assert client.peername == path  # a client that bound no name has sockname ""
    assert not os.path.exists(path)  # closing the listener removed its socket file


async def mistakes(path: str) -> None:
    try:
        await net.connect_unix(path)  # nothing is there yet
    except FileNotFoundError as error:
        assert any("start the server first" in note for note in error.__notes__)
    async with await net.listen_unix(path):
        try:
            await net.listen_unix(path)  # the path is taken
        except OSError as error:
            assert error.errno == 98 or error.errno == 48  # EADDRINUSE (Linux, macOS)
            assert any("remove it (os.unlink)" in note for note in error.__notes__)


async def abstract_socket() -> None:
    name = f"\0porto-example-{os.getpid()}"  # Linux only: no file anywhere
    async with await net.listen_unix(name) as listener:
        assert listener.sockname == name.encode()
        async with porto.scope() as s:
            s.spawn(serve_one(listener))
            async with await net.connect_unix(name) as client:
                await client.write(b"abstract\n")
                assert await client.readline() == b"ABSTRACT\n"


async def main() -> None:
    # A short path: a socket's path must fit in about 100 bytes.
    directory = tempfile.mkdtemp(prefix="porto-", dir="/tmp")
    try:
        path = os.path.join(directory, "echo.sock")
        await file_socket(path)
        await mistakes(path)
        if sys.platform.startswith("linux"):
            await abstract_socket()
    finally:
        shutil.rmtree(directory, ignore_errors=True)
    print("unix_sockets: ok")


if __name__ == "__main__":
    if sys.platform == "win32":
        # Unix sockets are Unix-only; the Windows I/O backend (IOCP) is not written yet either.
        print("unix_sockets: skipped, porto has no Unix sockets on Windows")
    else:
        porto.run(main)
