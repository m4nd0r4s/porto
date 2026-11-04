"""The installed porto imports and reports its version."""

import porto


def test_version_is_a_string() -> None:
    assert isinstance(porto.__version__, str)
    assert porto.__version__
