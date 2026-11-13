"""Fixtures shared by the test modules."""

import shutil
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture
def unix_socket_path() -> Iterator[str]:
    """A fresh path for a Unix socket in a new directory, removed with its contents afterwards.

    Short on purpose: a socket path must fit ``sockaddr_un.sun_path`` (104 bytes on macOS, 108 on
    Linux), which pytest's ``tmp_path`` on macOS (``/private/var/folders/...``) does not.
    """
    if sys.platform == "win32":
        pytest.skip("Unix domain sockets are not available on Windows")
    directory = tempfile.mkdtemp(prefix="porto-", dir="/tmp")
    try:
        yield str(Path(directory) / "s.sock")
    finally:
        shutil.rmtree(directory, ignore_errors=True)
