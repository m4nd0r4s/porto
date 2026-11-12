"""``porto.net`` and ``porto.abc`` as a user's type checker sees them (ADR-028, pyright strict).

The ``check_*`` functions are only type-checked: they would need a network.
"""

from typing import assert_type

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
