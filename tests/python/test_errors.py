"""``porto.errors`` (20-python-api-reference.md, Exceptions) and errors crossing from Rust
(26-errors-config-lifecycle.md §3, ST-19)."""

import asyncio
import pickle

import pytest

import porto
from porto import _core, errors

# The hierarchy

PORTO_ERRORS: dict[type[BaseException], tuple[type[BaseException], ...]] = {
    errors.NoRuntimeError: (errors.PortoError, RuntimeError),
    errors.RuntimeClosed: (errors.PortoError, RuntimeError),
    errors.WouldBlock: (errors.PortoError,),
    errors.EndOfChannel: (errors.PortoError,),
    errors.BrokenResourceError: (errors.PortoError,),
    errors.ClosedResourceError: (errors.PortoError, OSError),
    errors.IncompleteReadError: (errors.PortoError, EOFError),
    errors.LimitOverrunError: (errors.PortoError,),
    errors.Lagged: (errors.PortoError,),
    errors.InvalidStateError: (errors.PortoError,),
    errors.RustPanic: (errors.PortoError,),
}


def test_porto_error_is_an_exception() -> None:
    assert errors.PortoError.__bases__ == (Exception,)


@pytest.mark.parametrize("cls", list(PORTO_ERRORS), ids=lambda cls: cls.__name__)
def test_each_error_has_exactly_the_bases_the_spec_gives(cls: type[BaseException]) -> None:
    assert cls.__bases__ == PORTO_ERRORS[cls]


def test_cancelled_is_an_asyncio_cancelled_error_and_not_a_porto_error() -> None:
    assert errors.Cancelled.__bases__ == (asyncio.CancelledError,)
    assert not issubclass(errors.Cancelled, Exception)
    assert not issubclass(errors.Cancelled, errors.PortoError)
    with pytest.raises(asyncio.CancelledError):
        raise errors.Cancelled


def test_terminated_is_a_system_exit_and_not_a_porto_error() -> None:
    assert errors.Terminated.__bases__ == (SystemExit,)
    assert not issubclass(errors.Terminated, errors.PortoError)
    with pytest.raises(SystemExit) as raised:
        raise errors.Terminated(143)
    assert raised.value.code == 143


def test_runtime_errors_are_caught_by_except_runtime_error() -> None:
    with pytest.raises(RuntimeError):
        raise errors.NoRuntimeError("porto: no runtime")
    with pytest.raises(OSError, match="closed"):
        raise errors.ClosedResourceError("porto: closed")


def test_every_class_is_re_exported_at_the_top_level() -> None:
    for name in errors.__all__:
        assert getattr(porto, name) is getattr(errors, name)
        assert name in porto.__all__


def test_every_class_lives_in_porto_errors() -> None:
    for name in errors.__all__:
        assert getattr(errors, name).__module__ == "porto.errors"


# Attributes, messages and pickling


def test_incomplete_read_error_keeps_the_bytes_that_arrived() -> None:
    error = errors.IncompleteReadError(b"abc", 10)
    assert (error.partial, error.expected) == (b"abc", 10)
    assert "ended after 3 bytes, before 10 bytes" in str(error)
    assert ".partial" in str(error)
    assert "before the separator" in str(errors.IncompleteReadError(b""))


def test_lagged_says_how_many_messages_were_skipped_and_what_to_do() -> None:
    error = errors.Lagged(5)
    assert error.skipped == 5
    assert "missed 5 messages; keep receiving" in str(error)


def test_rust_panic_keeps_the_message_and_location() -> None:
    error = errors.RustPanic("index out of bounds", "src/lib.rs:1:2")
    assert (error.message, error.location) == ("index out of bounds", "src/lib.rs:1:2")
    assert "panicked at src/lib.rs:1:2: index out of bounds; " in str(error)
    assert "please report it" in str(error)


@pytest.mark.parametrize(
    "error",
    [
        errors.IncompleteReadError(b"abc", 10),
        errors.Lagged(5),
        errors.RustPanic("boom", None),
        errors.NoRuntimeError("porto: no runtime"),
    ],
    ids=lambda error: type(error).__name__,
)
def test_errors_survive_pickling(error: BaseException) -> None:
    copy = pickle.loads(pickle.dumps(error))
    assert type(copy) is type(error)
    assert str(copy) == str(error)
    assert vars(copy) == vars(error)


# From Rust


def test_a_rust_error_chain_becomes_one_note_per_level() -> None:
    with pytest.raises(errors.PortoError) as raised:
        _core._raise_rust_error_chain(3)
    assert str(raised.value) == "level 0"
    assert raised.value.__notes__ == [
        "caused by: level 1",
        "caused by: level 2",
        "caused by: level 3",
    ]


def test_a_rust_error_without_a_source_has_no_notes() -> None:
    with pytest.raises(errors.PortoError) as raised:
        _core._raise_rust_error_chain(0)
    assert not hasattr(raised.value, "__notes__")


def test_a_rust_panic_becomes_rust_panic_with_its_message() -> None:
    with pytest.raises(errors.RustPanic, match="please report it") as raised:
        _core._raise_rust_panic("the answer is 42")
    assert raised.value.message == "the answer is 42"
    assert raised.value.location is not None
    assert raised.value.location.endswith("errors.rs")


# Raised by porto


def test_scope_spawn_outside_run_raises_no_runtime_error() -> None:
    async def child() -> None:
        pass

    coro = child()
    with pytest.raises(errors.NoRuntimeError, match=r"Scope.spawn\(\) must be called from a task"):
        porto.scope().spawn(coro)
    coro.close()


def test_entering_a_scope_outside_run_raises_no_runtime_error() -> None:
    with pytest.raises(errors.NoRuntimeError, match=r"only works in tasks of a porto runtime"):
        porto.scope().__aenter__()  # pyright: ignore[reportUnusedCoroutine]
