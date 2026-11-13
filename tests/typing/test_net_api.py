"""``porto.net`` and ``porto.abc`` as a user's type checker sees them (ADR-028, pyright strict).

The ``check_*`` functions are only type-checked: they would need a network.
"""

from typing import Any, assert_type

import porto
from porto import abc, net


async def check_listen_and_accept() -> None:
    listener = await net.listen_tcp("127.0.0.1", 0, backlog=16, reuse_port=False, dual_stack=True)
    assert_type(listener, net.TcpListener)
    assert_type(listener.port, int)
    assert_type(listener.sockname, tuple[str, int] | tuple[str, int, int, int])
    stream = await listener.accept()
    assert_type(stream, net.TcpStream)
    async for connection in listener:
        assert_type(connection, net.TcpStream)
    async with listener as same:
        assert_type(same, net.TcpListener)
    await net.listen_tcp()
    await net.listen_tcp(None, 8080, tls=None)


async def check_stream_methods(stream: net.TcpStream) -> None:
    assert_type(await stream.read(), bytes)
    assert_type(await stream.read(10), bytes)
    assert_type(await stream.read_buffer(), porto.Buffer)
    assert_type(await stream.read_into(bytearray(8)), int)
    assert_type(await stream.read_into(porto.Buffer(8)), int)
    assert_type(await stream.read_exact(4), bytes)
    assert_type(await stream.read_until(b"\r\n"), bytes)
    assert_type(await stream.read_until(bytearray(b"\n"), limit=1024), bytes)
    assert_type(await stream.readline(), bytes)
    assert_type(await stream.readline(limit=100), bytes)
    assert_type(await stream.read_to_end(limit=1 << 20), bytes)
    assert_type(await stream.write(b"data"), None)
    assert_type(await stream.write(memoryview(b"data")), None)
    assert_type(await stream.writev([b"a", bytearray(b"b"), porto.Buffer(1)]), None)
    assert_type(await stream.shutdown_write(), None)
    assert_type(stream.peername, tuple[str, int] | tuple[str, int, int, int])
    assert_type(stream.sockname, tuple[str, int] | tuple[str, int, int, int])
    assert_type(stream.fileno(), int)
    stream.set_nodelay(True)
    stream.set_keepalive(30.0, 10.0, 5)
    async with stream as same:
        assert_type(same, net.TcpStream)
    assert_type(await stream.aclose(), None)


def check_a_tcp_stream_is_a_byte_stream(stream: net.TcpStream) -> abc.ByteStream:
    return stream


async def check_code_over_any_byte_stream(stream: abc.ByteStream) -> bytes:
    await stream.write(b"ping")
    data = await stream.read(4)
    data += await stream.read_exact(2) + await stream.readline()
    data += await stream.read_until(b"\0") + await stream.read_to_end(limit=100)
    await stream.shutdown_write()
    await stream.aclose()
    return data


def check_wrong_calls_are_errors(stream: net.TcpStream) -> None:
    stream.read("10")  # pyright: ignore[reportArgumentType]
    stream.write("text")  # pyright: ignore[reportArgumentType]
    stream.set_keepalive(1.0)  # pyright: ignore[reportCallIssue]
    stream.read_exact("4")  # pyright: ignore[reportArgumentType]
    stream.read_until("\n")  # pyright: ignore[reportArgumentType]
    stream.read_until(b"\n", 10)  # pyright: ignore[reportCallIssue]
    stream.read_to_end()  # pyright: ignore[reportCallIssue]
    net.listen_tcp(port="80")  # pyright: ignore[reportArgumentType, reportUnusedCoroutine]


async def check_serve_tcp(scope: porto.Scope) -> None:
    async def handler(stream: net.TcpStream) -> None:
        await stream.write(await stream.readline())

    async def returns_a_value(stream: net.TcpStream) -> int:
        return len(await stream.read())

    port = await scope.start(net.serve_tcp, handler, "127.0.0.1")
    assert_type(port, Any)  # Scope.start returns what `started.set` was given
    await net.serve_tcp(returns_a_value, max_connections=100)
    await net.serve_tcp(handler, None, 8080, tls=None, max_connections=None, started=None)


async def check_serve_tcp_started(started: porto.Started[int]) -> None:
    await net.serve_tcp(lambda stream: stream.aclose(), "127.0.0.1", 0, started=started)


async def check_serve_tcp_wrong_calls(started: porto.Started[str]) -> None:
    await net.serve_tcp("not a handler")  # pyright: ignore[reportArgumentType]
    await net.serve_tcp(lambda stream: stream.aclose(), max_connections="2")  # pyright: ignore[reportArgumentType]
    await net.serve_tcp(lambda stream: stream.aclose(), started=started)  # pyright: ignore[reportArgumentType]


async def check_udp(address: tuple[str, int]) -> None:
    sock = await net.bind_udp("127.0.0.1", 0, reuse_port=False, broadcast=False)
    assert_type(sock, net.UdpSocket)
    assert_type(sock.port, int)
    assert_type(sock.sockname, tuple[str, int] | tuple[str, int, int, int])
    assert_type(sock.peername, tuple[str, int] | tuple[str, int, int, int])
    assert_type(await sock.send_to(b"datagram", address), None)
    assert_type(await sock.send_to(bytearray(b"datagram"), ("::1", 9, 0, 0)), None)
    data, sender = await sock.recv_from()
    assert_type(data, bytes)
    assert_type(sender, tuple[str, int] | tuple[str, int, int, int])
    assert_type(
        await sock.recv_from(512), tuple[bytes, tuple[str, int] | tuple[str, int, int, int]]
    )
    assert_type(await sock.connect(address), None)
    assert_type(await sock.send(porto.Buffer(4)), None)
    assert_type(await sock.recv(), bytes)
    assert_type(await sock.recv(512), bytes)
    assert_type(sock.fileno(), int)
    async with sock as same:
        assert_type(same, net.UdpSocket)
    assert_type(await sock.aclose(), None)
    await net.bind_udp()
    await net.bind_udp(None, 5353, reuse_port=True)


async def check_udp_wrong_calls(sock: net.UdpSocket) -> None:
    sock.send_to("text", ("127.0.0.1", 9))  # pyright: ignore[reportArgumentType, reportUnusedCoroutine]
    sock.send_to(b"data")  # pyright: ignore[reportCallIssue, reportUnusedCoroutine]
    sock.recv_from("10")  # pyright: ignore[reportArgumentType, reportUnusedCoroutine]
    sock.connect("127.0.0.1")  # pyright: ignore[reportArgumentType, reportUnusedCoroutine]
    net.bind_udp("127.0.0.1", 0, True)  # pyright: ignore[reportCallIssue, reportUnusedCoroutine]


async def check_unix(path: str, name: bytes) -> None:
    import os
    import pathlib

    listener = await net.listen_unix(path, mode=0o600, backlog=16)
    assert_type(listener, net.UnixListener)
    assert_type(listener.sockname, str | bytes)
    stream = await listener.accept()
    assert_type(stream, net.UnixStream)
    async for connection in listener:
        assert_type(connection, net.UnixStream)
    async with listener as same:
        assert_type(same, net.UnixListener)
    client = await net.connect_unix(pathlib.Path(path))
    assert_type(client, net.UnixStream)
    assert_type(client.peername, str | bytes)
    assert_type(client.sockname, str | bytes)
    assert_type(await client.read_exact(4), bytes)
    assert_type(await client.write(b"data"), None)
    assert_type(client.fileno(), int)
    await net.listen_unix(name)
    await net.connect_unix(os.fsencode(path))
    await net.connect_unix("\0abstract")


def check_a_unix_stream_is_a_byte_stream(stream: net.UnixStream) -> abc.ByteStream:
    return stream


async def check_unix_wrong_calls() -> None:
    await net.listen_unix(5)  # pyright: ignore[reportArgumentType]
    await net.listen_unix("/tmp/s", 0o660)  # pyright: ignore[reportCallIssue]
    await net.connect_unix()  # pyright: ignore[reportCallIssue]
