"""On Windows, ``listen_unix`` and ``connect_unix`` raise ``NotImplementedError`` that says what to
do (17-networking-tls-dns.md §2): porto has Unix domain sockets on Linux and macOS only, until its
Windows I/O backend lands. Runs on Windows only; ``test_net_unix.py`` covers the other platforms.
"""

import sys

import pytest

import porto
from porto import net

pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="the error is what Windows gets; other platforms have the sockets",
)


def test_listen_unix_on_windows_says_to_use_tcp_on_the_loopback() -> None:
    async def main() -> None:
        with pytest.raises(NotImplementedError, match=r"listen_unix\(\).*listen_tcp"):
            await net.listen_unix("C:\\temp\\app.sock")

    porto.run(main())


def test_connect_unix_on_windows_says_to_use_tcp_on_the_loopback() -> None:
    async def main() -> None:
        with pytest.raises(NotImplementedError, match=r"connect_unix\(\).*connect_tcp"):
            await net.connect_unix("C:\\temp\\app.sock")

    porto.run(main())
