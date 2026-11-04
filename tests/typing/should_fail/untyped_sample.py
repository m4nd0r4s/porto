"""Deliberately untyped: pyright strict MUST report errors here.

Proves the strict configuration is live; see tests/typing/test_should_fail.py.
"""


def add(left, right):
    return left + right
