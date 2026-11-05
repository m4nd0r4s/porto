"""The CPython layer of porto-py (crates/porto-py/src/py_compat, spec 25 §1, §4): exceptions come
out normalized on every version, and the send wrapper keeps reference counts balanced."""

import sys
from collections.abc import Generator
from types import TracebackType

import pytest

from porto import _core


def a_traceback() -> TracebackType:
    with pytest.raises(LookupError) as raised:
        raise LookupError("for its traceback")
    traceback = raised.value.__traceback__
    assert traceback is not None
    return traceback


# On 3.11 `_take_unnormalized_exception` leaves the triple raw, so these exercise
# PyErr_NormalizeException; 3.12+ normalizes in PyErr_Restore and takes the other path.
@pytest.mark.parametrize(
    ("value", "args"),
    [
        (None, ()),
        ("a message", ("a message",)),
        (("two", 2), ("two", 2)),
    ],
)
def test_a_raw_exception_triple_comes_out_normalized(
    value: object, args: tuple[object, ...]
) -> None:
    exception = _core._take_unnormalized_exception(ValueError, value, None)
    assert type(exception) is ValueError
    assert exception.args == args


def test_an_instance_of_a_subclass_survives_normalization() -> None:
    raised = KeyError("missing")
    exception = _core._take_unnormalized_exception(LookupError, raised, None)
    if sys.version_info >= (3, 12):
        # PyErr_Restore only keeps a value whose type is exactly exc_type; it calls
        # LookupError(raised) for anything else.
        assert type(exception) is LookupError
        assert exception.args == (raised,)
    else:
        # PyErr_NormalizeException keeps any instance of exc_type, subclasses included.
        assert exception is raised


def test_the_traceback_is_attached_to_the_exception() -> None:
    traceback = a_traceback()
    exception = _core._take_unnormalized_exception(RuntimeError, "with a traceback", traceback)
    assert type(exception) is RuntimeError
    assert exception.__traceback__ is traceback


def test_a_constructor_that_fails_yields_its_own_exception() -> None:
    class Strict(Exception):
        def __init__(self, *args: object) -> None:
            raise TypeError("Strict takes no arguments")

    exception = _core._take_unnormalized_exception(Strict, "an argument", None)
    assert type(exception) is TypeError
    assert exception.args == ("Strict takes no arguments",)


def test_the_hook_rejects_what_pyerr_restore_cannot_take() -> None:
    with pytest.raises(TypeError, match="exc_type must be an exception class, not int"):
        _core._take_unnormalized_exception(int, None, None)
    with pytest.raises(TypeError, match="traceback must be a traceback or None"):
        _core._take_unnormalized_exception(ValueError, None, "not a traceback")


def echo() -> Generator[object, object, object]:
    received = yield "started"
    return received


def test_send_releases_the_value_it_was_given() -> None:
    value = object()
    before = sys.getrefcount(value)
    for _ in range(100):
        generator = echo()
        assert _core._send(generator, None) == (False, "started")
        assert _core._send(generator, value) == (True, value)
    del generator
    assert sys.getrefcount(value) == before


def test_send_raises_what_the_iterator_raised() -> None:
    def failing() -> Generator[None, object, None]:
        yield
        raise ArithmeticError("from the generator")

    generator = failing()
    _core._send(generator, None)
    with pytest.raises(ArithmeticError, match="from the generator") as raised:
        _core._send(generator, None)
    assert raised.value.__traceback__ is not None
