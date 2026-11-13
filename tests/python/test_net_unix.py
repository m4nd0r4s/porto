"""``porto.net.listen_unix``, ``connect_unix``, ``UnixListener`` and ``UnixStream``
(17-networking-tls-dns.md §2): the socket file and its mode, the abstract namespace on Linux, the
errors for a path that cannot work. The ``ByteStream`` behavior of ``UnixStream`` is tested for
every stream type in ``test_net_byte_stream.py``. Unix only: porto has no sockets on Windows yet
(``test_net_unix_windows.py`` checks the error it gives there).
"""

import errno
import gc
import os
import pathlib
import socket
import stat
import subprocess
import sys
import textwrap
import threading
from collections.abc import Coroutine
from typing import Any

import pytest

import porto
from porto import net

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="Unix domain sockets are not available on Windows"
)

is_linux = sys.platform.startswith("linux")


TEST_DEADLINE = 60  # seconds a test may take: none comes near it


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    """Runs ``coro`` in ``porto.run`` under a deadline: a test that would wait forever (a datagram
    or connection that never comes) fails after a minute with ``TimeoutError`` instead of
    hanging the suite."""

    async def bounded() -> Any:
        async with porto.timeout(TEST_DEADLINE):
            return await coro

    return porto.run(bounded())


def mode_of(path: str) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


async def connected(path: str) -> tuple[net.UnixStream, net.UnixStream, net.UnixListener]:
    listener = await net.listen_unix(path)
    client = await net.connect_unix(path)
    return client, await listener.accept(), listener


# Talking over the socket


def test_a_client_and_a_server_exchange_bytes(unix_socket_path: str) -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await connected(unix_socket_path)
        async with client, server, listener:
            await client.write(b"ping")
            ping = await server.read_exact(4)
            await server.write(b"pong")
            return ping, await client.read_exact(4)

    assert run(main()) == (b"ping", b"pong")


def test_the_types_are_unix_streams_and_listeners(unix_socket_path: str) -> None:
    async def main() -> None:
        client, server, listener = await connected(unix_socket_path)
        async with client, server, listener:
            assert isinstance(client, net.UnixStream)
            assert isinstance(server, net.UnixStream)
            assert isinstance(listener, net.UnixListener)
            assert not isinstance(client, net.TcpStream)

    run(main())


def test_addresses_are_paths(unix_socket_path: str) -> None:
    async def main() -> None:
        client, server, listener = await connected(unix_socket_path)
        async with client, server, listener:
            assert listener.sockname == unix_socket_path
            assert client.peername == unix_socket_path
            assert server.sockname == unix_socket_path
            # A client that bound no name has none: the socket module's empty string.
            assert client.sockname == ""
            assert server.peername == ""
            fd = client.fileno()
            with socket.socket(fileno=socket.dup(fd)) as duplicate:
                assert duplicate.family == socket.AF_UNIX
                assert duplicate.getpeername() == unix_socket_path
            assert isinstance(listener.fileno(), int)
        with pytest.raises(porto.ClosedResourceError):
            listener.fileno()
        # The addresses a closed stream knew stay known.
        assert client.peername == unix_socket_path

    run(main())


def test_a_path_may_be_str_bytes_or_pathlike(unix_socket_path: str) -> None:
    class Wrapper:
        def __fspath__(self) -> str:
            return unix_socket_path

    async def main() -> list[str]:
        names: list[str] = []
        for path in (
            unix_socket_path,
            os.fsencode(unix_socket_path),
            pathlib.Path(unix_socket_path),
        ):
            async with (
                await net.listen_unix(path) as listener,
                await net.connect_unix(Wrapper()) as client,
            ):
                names.append(client.peername)
                await listener.accept()
        return names

    assert run(main()) == [unix_socket_path] * 3


def test_a_relative_path_is_relative_to_the_current_directory(
    unix_socket_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory, name = os.path.split(unix_socket_path)
    monkeypatch.chdir(directory)

    async def main() -> None:
        client, server, listener = await connected(name)
        async with client, server, listener:
            await client.write(b"x")
            assert await server.read_exact(1) == b"x"
        assert not os.path.exists(unix_socket_path)

    run(main())


def test_a_foreign_client_and_a_foreign_server_talk_to_porto(unix_socket_path: str) -> None:
    """The peer need not be porto: a plain ``socket`` connects to a ``UnixListener``, and a
    ``UnixStream`` connects to a server that is a plain ``socket`` in a thread."""
    answer: list[bytes] = []

    def foreign_server(listening: socket.socket) -> None:
        # Bounded everywhere: a failing connect must end the thread, not hang the suite.
        try:
            connection, _ = listening.accept()
        except OSError:
            return
        with connection:
            connection.settimeout(5)
            answer.append(connection.recv(100))
            connection.sendall(b"hello porto")

    async def main() -> tuple[bytes, bytes]:
        async with await net.listen_unix(unix_socket_path) as listener:
            with socket.socket(socket.AF_UNIX) as foreign:
                foreign.connect(unix_socket_path)
                foreign.sendall(b"hello from foreign")
                accepted = await listener.accept()
                async with accepted:
                    received = await accepted.read_exact(18)
        other = unix_socket_path + ".foreign"
        with socket.socket(socket.AF_UNIX) as listening:
            listening.bind(other)
            listening.listen()
            listening.settimeout(5)
            thread = threading.Thread(target=foreign_server, args=(listening,), daemon=True)
            thread.start()
            try:
                async with porto.timeout(10), await net.connect_unix(other) as client:
                    await client.write(b"hello foreign")
                    reply = await client.read_exact(11)
            finally:
                thread.join(timeout=10)
        os.unlink(other)
        return received, reply

    received, reply = run(main())
    assert (received, reply) == (b"hello from foreign", b"hello porto")
    assert answer == [b"hello foreign"]


# The file's mode


def test_the_default_mode_lets_the_owner_and_the_group_connect(unix_socket_path: str) -> None:
    async def main() -> int:
        async with await net.listen_unix(unix_socket_path):
            return mode_of(unix_socket_path)

    assert run(main()) == 0o660


@pytest.mark.parametrize("mode", [0o600, 0o640, 0o666, 0o700, 0o777, 0o000])
def test_the_file_has_exactly_the_mode_asked_for(unix_socket_path: str, mode: int) -> None:
    async def main() -> int:
        async with await net.listen_unix(unix_socket_path, mode=mode):
            return mode_of(unix_socket_path)

    assert run(main()) == mode


def test_the_mode_is_exact_whatever_the_processs_umask(unix_socket_path: str) -> None:
    async def main() -> int:
        async with await net.listen_unix(unix_socket_path, mode=0o664):
            return mode_of(unix_socket_path)

    previous = os.umask(0o077)  # would remove the group's and the others' bits
    try:
        assert run(main()) == 0o664
        assert os.umask(previous) == 0o077, "listen_unix leaves the process's umask as it was"
    finally:
        os.umask(previous)


@pytest.mark.skipif(
    getattr(os, "geteuid", lambda: 1)() == 0, reason="root ignores file permissions"
)
def test_a_socket_without_write_permission_cannot_be_connected_to(unix_socket_path: str) -> None:
    async def main() -> None:
        async with await net.listen_unix(unix_socket_path, mode=0o400):
            with pytest.raises(PermissionError, match=r"write permission on the socket file") as c:
                await net.connect_unix(unix_socket_path)
            assert c.value.errno == errno.EACCES
            assert c.value.filename == unix_socket_path

    run(main())


@pytest.mark.parametrize("mode", [-1, 0o1000, 2**40, 2**70])
def test_a_mode_out_of_range_is_a_value_error_that_says_what_it_is(
    unix_socket_path: str, mode: int
) -> None:
    async def main() -> None:
        with pytest.raises(
            ValueError, match=r"mode must be an int in 0\.\.=511 \(0o777 at most.*permission bits"
        ):
            await net.listen_unix(unix_socket_path, mode=mode)
        with pytest.raises(TypeError):
            await net.listen_unix(unix_socket_path, mode="0o660")  # pyright: ignore[reportArgumentType]
        assert not os.path.exists(unix_socket_path)

    run(main())


# The socket file's life


def test_closing_the_listener_removes_the_socket_file(unix_socket_path: str) -> None:
    async def main() -> None:
        listener = await net.listen_unix(unix_socket_path)
        assert os.path.exists(unix_socket_path)
        await listener.aclose()
        assert not os.path.exists(unix_socket_path)
        await listener.aclose()  # closing again does nothing
        with pytest.raises(FileNotFoundError):
            await net.connect_unix(unix_socket_path)

    run(main())


def test_async_with_removes_the_file_even_for_an_exception(unix_socket_path: str) -> None:
    async def main() -> None:
        with pytest.raises(KeyError):
            async with await net.listen_unix(unix_socket_path):
                raise KeyError("boom")
        assert not os.path.exists(unix_socket_path)

    run(main())


def test_a_listener_that_is_dropped_still_removes_its_file(unix_socket_path: str) -> None:
    async def main() -> None:
        listener = await net.listen_unix(unix_socket_path)
        assert os.path.exists(unix_socket_path)
        del listener
        gc.collect()
        assert not os.path.exists(unix_socket_path)

    run(main())


def test_a_forked_child_dropping_the_listener_does_not_remove_the_parents_file(
    unix_socket_path: str,
) -> None:
    """The child of a fork holds a copy of the listener; letting it go must leave the socket file
    to the process that made it."""
    code = textwrap.dedent(
        """
        import gc, os, sys, warnings
        import porto
        from porto import net

        warnings.simplefilter("ignore")
        path = sys.argv[1]

        async def main():
            listener = await net.listen_unix(path)
            pid = os.fork()
            if pid == 0:
                del listener
                gc.collect()
                os._exit(0 if os.path.exists(path) else 1)
            _, status = os.waitpid(pid, 0)
            print("child exit:", os.waitstatus_to_exitcode(status), flush=True)
            print("file after the child:", os.path.exists(path), flush=True)
            await listener.aclose()
            print("file after the parent:", os.path.exists(path), flush=True)

        porto.run(main)
        """
    )
    done = subprocess.run(
        [sys.executable, "-c", code, unix_socket_path],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert "child exit: 0" in done.stdout, done.stdout
    assert "file after the child: True" in done.stdout
    assert "file after the parent: False" in done.stdout


def test_a_file_that_something_else_replaced_is_left_alone(unix_socket_path: str) -> None:
    async def main() -> None:
        listener = await net.listen_unix(unix_socket_path)
        os.unlink(unix_socket_path)
        pathlib.Path(unix_socket_path).write_text("not mine")
        await listener.aclose()
        assert pathlib.Path(unix_socket_path).read_text() == "not mine"

    run(main())


def test_an_existing_path_is_address_in_use_with_a_hint_and_is_left_alone(
    unix_socket_path: str,
) -> None:
    async def main() -> None:
        async with await net.listen_unix(unix_socket_path) as first:
            with pytest.raises(OSError, match="Errno") as caught:
                await net.listen_unix(unix_socket_path)
            assert caught.value.errno == errno.EADDRINUSE
            assert caught.value.filename == unix_socket_path
            assert any("remove it (os.unlink)" in note for note in caught.value.__notes__)
            assert os.path.exists(unix_socket_path), "the first listener's file is not touched"
            async with await net.connect_unix(unix_socket_path) as client:
                await first.accept()
                assert client.peername == unix_socket_path

    run(main())


def test_a_stale_file_is_address_in_use_and_a_refused_connect(unix_socket_path: str) -> None:
    """The file a crashed server left: listening fails (it is not removed for you), connecting is
    refused, and both say what to do."""
    with socket.socket(socket.AF_UNIX) as stale:
        stale.bind(unix_socket_path)  # never listens, and leaves the file at close

    async def main() -> None:
        with pytest.raises(ConnectionRefusedError, match="Errno") as refused:
            await net.connect_unix(unix_socket_path)
        assert any("nothing listens on it" in note for note in refused.value.__notes__)
        with pytest.raises(OSError, match="Errno") as taken:
            await net.listen_unix(unix_socket_path)
        assert taken.value.errno == errno.EADDRINUSE
        os.unlink(unix_socket_path)  # what the hint says to do
        async with await net.listen_unix(unix_socket_path):
            pass

    run(main())


def test_connecting_to_nothing_is_file_not_found_with_a_hint(unix_socket_path: str) -> None:
    async def main() -> None:
        with pytest.raises(FileNotFoundError, match="Errno") as caught:
            await net.connect_unix(unix_socket_path)
        assert caught.value.filename == unix_socket_path
        assert any("start the server first" in note for note in caught.value.__notes__)

    run(main())


def test_a_missing_directory_is_file_not_found_with_a_hint(unix_socket_path: str) -> None:
    missing = os.path.join(os.path.dirname(unix_socket_path), "no", "such", "dir", "s.sock")

    async def main() -> None:
        with pytest.raises(FileNotFoundError) as caught:
            await net.listen_unix(missing)
        assert any("create it first" in note for note in caught.value.__notes__)

    run(main())


# Accept


def test_async_for_accepts_until_the_listener_is_closed(unix_socket_path: str) -> None:
    async def main() -> list[bytes]:
        listener = await net.listen_unix(unix_socket_path)
        seen: list[bytes] = []

        async def clients() -> None:
            for number in range(3):
                async with await net.connect_unix(unix_socket_path) as client:
                    await client.write(b"client %d" % number)
            await porto.sleep(0.01)
            await listener.aclose()

        async with porto.scope() as s:
            s.spawn(clients())
            async for stream in listener:
                async with stream:
                    seen.append(await stream.read_to_end(limit=100))
        return seen

    assert run(main()) == [b"client 0", b"client 1", b"client 2"]


def test_closing_the_listener_fails_a_waiting_accept(unix_socket_path: str) -> None:
    async def main() -> None:
        listener = await net.listen_unix(unix_socket_path)

        async def close_later() -> None:
            await porto.sleep(0.01)
            await listener.aclose()

        async with porto.scope() as s:
            s.spawn(close_later())
            with pytest.raises(porto.ClosedResourceError) as caught:
                await listener.accept()
        assert caught.value.errno == errno.EBADF

    run(main())


def test_a_cancelled_accept_loses_no_connection(unix_socket_path: str) -> None:
    async def main() -> bytes:
        async with await net.listen_unix(unix_socket_path) as listener:
            async with porto.move_on_after(0.02) as scope:
                await listener.accept()
            assert scope.timed_out
            async with await net.connect_unix(unix_socket_path) as client:
                await client.write(b"after the cancel")
                async with await listener.accept() as server:
                    return await server.read_exact(16)

    assert run(main()) == b"after the cancel"


def test_several_tasks_may_accept_from_one_listener(unix_socket_path: str) -> None:
    async def main() -> list[bytes]:
        async with await net.listen_unix(unix_socket_path) as listener, porto.scope() as s:

            async def serve_one() -> bytes:
                async with await listener.accept() as stream:
                    return await stream.read_to_end(limit=100)

            servers = [s.spawn(serve_one) for _ in range(3)]
            await porto.sleep(0.01)
            for number in range(3):
                async with await net.connect_unix(unix_socket_path) as client:
                    await client.write(b"connection %d" % number)
            return sorted([await server for server in servers])

    assert run(main()) == [b"connection 0", b"connection 1", b"connection 2"]


@pytest.mark.skipif(not is_linux, reason="only Linux makes a connect to a full backlog wait")
def test_connects_beyond_the_backlog_wait_and_complete_as_the_listener_accepts(
    unix_socket_path: str,
) -> None:
    async def main() -> int:
        streams: list[net.UnixStream] = []
        async with await net.listen_unix(unix_socket_path, backlog=1) as listener:
            async with porto.scope() as s:
                connecting = [s.spawn(net.connect_unix, unix_socket_path) for _ in range(6)]
                await porto.sleep(0.05)  # the backlog holds one or two: the rest are retrying
                streams.extend([await listener.accept() for _ in range(6)])
            streams.extend(task.result() for task in connecting)
        for stream in streams:
            await stream.aclose()
        return len(streams)

    assert run(main()) == 12


def test_connect_unix_cancelled_while_waiting_opens_nothing(unix_socket_path: str) -> None:
    async def main() -> None:
        async with porto.move_on_after(0) as scope:
            await porto.checkpoint()
            await net.connect_unix(unix_socket_path)
        assert scope.timed_out

    run(main())


# Names that cannot work


@pytest.mark.parametrize(
    ("path", "message"),
    [
        ("", r"it is empty"),
        (b"", r"it is empty"),
        ("/tmp/with\0nul", r"NUL byte inside the path"),
        ("/tmp/" + "x" * 200, r"at most \d+.*shorter directory"),
    ],
)
def test_a_path_that_cannot_name_a_socket_is_a_value_error_that_says_why(
    path: str | bytes, message: str
) -> None:
    async def main() -> None:
        for call in (net.listen_unix, net.connect_unix):
            with pytest.raises(ValueError, match=message):
                await call(path)

    run(main())


@pytest.mark.parametrize("path", [None, 5, ("/tmp/x",), 1.5])
def test_a_path_of_another_type_is_a_type_error_that_says_what_to_pass(path: Any) -> None:
    async def main() -> None:
        for call in (net.listen_unix, net.connect_unix):
            with pytest.raises(TypeError, match=r"str, bytes or os\.PathLike"):
                await call(path)

    run(main())


def test_unix_sockets_outside_a_run_say_to_open_them_in_one(unix_socket_path: str) -> None:
    async def make() -> net.UnixListener:
        return await net.listen_unix(unix_socket_path)

    with pytest.raises(RuntimeError, match=r"inside a running porto runtime|porto\.run"):
        make().send(None)
    assert not os.path.exists(unix_socket_path)


def test_a_cancelled_task_opens_no_listener(unix_socket_path: str) -> None:
    async def main() -> None:
        async with porto.move_on_after(0) as scope:
            await porto.checkpoint()
            await net.listen_unix(unix_socket_path)
        assert scope.timed_out
        assert not os.path.exists(unix_socket_path)

    run(main())


def test_a_listener_of_an_earlier_run_is_refused_at_the_call(unix_socket_path: str) -> None:
    holder: list[net.UnixListener] = []

    async def first() -> None:
        holder.append(await net.listen_unix(unix_socket_path))

    run(first())

    async def second() -> None:
        with pytest.raises(porto.RuntimeClosed, match="runtime that stopped"):
            holder[0].accept()

    run(second())
    holder.clear()


# The abstract namespace (Linux)


@pytest.mark.skipif(not is_linux, reason="the abstract namespace is Linux's")
def test_an_abstract_socket_works_without_a_file() -> None:
    name = f"\0porto-test-{os.getpid()}"

    async def main() -> tuple[bytes, bytes, object, object]:
        async with await net.listen_unix(name) as listener:
            assert listener.sockname == name.encode()
            async with await net.connect_unix(name) as client, await listener.accept() as server:
                await client.write(b"abstract")
                got = await server.read_exact(8)
                await server.write(b"reply")
                return got, await client.read_exact(5), client.peername, server.sockname

    got, reply, client_peer, server_name = run(main())
    assert (got, reply) == (b"abstract", b"reply")
    assert client_peer == name.encode() == server_name


@pytest.mark.skipif(not is_linux, reason="the abstract namespace is Linux's")
def test_an_abstract_name_may_be_bytes_and_the_mode_means_nothing_for_it() -> None:
    name = f"\0porto-bytes-{os.getpid()}".encode()

    async def main() -> bytes:
        async with (
            await net.listen_unix(name, mode=0o000) as listener,
            await net.connect_unix(name) as client,
        ):
            await client.write(b"ok")
            async with await listener.accept() as server:
                return await server.read_exact(2)

    assert run(main()) == b"ok"


@pytest.mark.skipif(not is_linux, reason="the abstract namespace is Linux's")
def test_an_abstract_name_in_use_is_address_in_use() -> None:
    name = f"\0porto-taken-{os.getpid()}"

    async def main() -> None:
        async with await net.listen_unix(name):
            with pytest.raises(OSError, match="Errno") as caught:
                await net.listen_unix(name)
            assert caught.value.errno == errno.EADDRINUSE
        # Closing frees the name: there is no file to leave behind.
        async with await net.listen_unix(name):
            pass

    run(main())


@pytest.mark.skipif(is_linux, reason="Linux has the abstract namespace")
def test_an_abstract_name_off_linux_says_it_is_linux_only(unix_socket_path: str) -> None:
    async def main() -> None:
        for call in (net.listen_unix, net.connect_unix):
            with pytest.raises(
                ValueError, match=r"abstract namespace, which this OS does not have"
            ):
                await call("\0name")

    run(main())
