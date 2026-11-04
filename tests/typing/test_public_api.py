"""The public API as a user's type checker sees it (ADR-028, pyright strict)."""

from typing import assert_type

import porto


def check_version_is_str() -> None:
    assert_type(porto.__version__, str)
