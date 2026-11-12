"""Byte buffers: ``porto.Buffer``, a growable buffer that porto owns.

    uv run python examples/buffers.py

A ``Buffer`` is the memory porto's I/O reads into: its storage is pooled per lane in size
classes (4, 16, 64 and 256 KiB) and handed to the kernel without a copy. You use it through the
buffer protocol: ``memoryview(buf)`` is the zero-copy window, ``bytes(buf)`` is a copy. While a
view is out the buffer cannot move its storage, so ``resize`` and ``extend`` refuse
(``BufferError``) instead of leaving the view dangling.
"""

import porto


def create_and_read() -> None:
    buf = porto.Buffer(5)  # five zero bytes
    assert len(buf) == 5 and bytes(buf) == bytes(5)
    assert len(porto.Buffer()) == 0  # the default size is 0
    # The capacity is the room before the contents move to larger storage: a size class.
    assert buf.capacity == 4096
    assert porto.Buffer(5000).capacity == 16384


def write_through_a_view() -> None:
    buf = porto.Buffer(11)
    with memoryview(buf) as view:  # leaving the block releases the view
        assert view.nbytes == 11 and not view.readonly
        view[:5] = b"hello"  # writes the buffer's own memory: no copy
        view[5:] = b" porto"
    assert bytes(buf) == b"hello porto"
    # bytes(buf) copied it: changing the buffer later leaves the copy alone.
    copy = bytes(buf)
    with memoryview(buf) as view:
        view[0:1] = b"J"
    assert copy == b"hello porto" and bytes(buf) == b"Jello porto"


def grow_and_shrink() -> None:
    buf = porto.Buffer()
    buf.extend(b"GET / ")  # bytes, bytearray, memoryview or another Buffer
    buf.extend(bytearray(b"HTTP/1.1"))
    buf.extend(memoryview(b"\r\n"))
    other = porto.Buffer()
    other.extend(b"Host: porto")
    buf.extend(other)
    assert bytes(buf) == b"GET / HTTP/1.1\r\nHost: porto"

    buf.resize(5)  # truncates...
    assert bytes(buf) == b"GET /"
    buf.resize(8)  # ...or appends zero bytes
    assert bytes(buf) == b"GET /\0\0\0"


def a_view_pins_the_size() -> None:
    buf = porto.Buffer(4)
    view = memoryview(buf)  # exported, and not released yet
    for change in (lambda: buf.resize(2), lambda: buf.extend(b"more")):
        try:
            change()
        except BufferError as error:
            # The message names the way out.
            assert "release the memoryview" in str(error), error
        else:
            raise AssertionError("the buffer changed size under a live view")
    assert len(buf) == 4  # nothing changed

    view.release()  # now it is free to change
    buf.resize(2)
    buf.extend(b"ok")
    assert bytes(buf) == b"\0\0ok"


def only_bytes_like_data() -> None:
    buf = porto.Buffer()
    try:
        buf.extend("text")  # pyright: ignore[reportArgumentType]  # a str is not bytes: encode it first
    except TypeError as error:
        assert "bytes-like" in str(error), error
    else:
        raise AssertionError("extend accepted a str")
    buf.extend(b"text")
    assert bytes(buf) == b"text"


def main() -> None:
    create_and_read()
    write_through_a_view()
    grow_and_shrink()
    a_view_pins_the_size()
    only_bytes_like_data()
    print("buffers: ok")


if __name__ == "__main__":
    main()
