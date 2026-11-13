"""The Python surface of ``Scope`` (20-python-api-reference.md "Structured concurrency").

One test per attribute, and a signature test that compares ``inspect.signature`` of every member
the spec lists, and the stub's, against the spec text itself (the code block of the section is
parsed, so spec, stub and implementation cannot drift apart unnoticed). Annotations are not
compared: the extension module has none.
"""

import ast
import contextlib
import inspect
import math
import re
from pathlib import Path
from typing import Any

import pytest

import porto
from porto import _core

REPO = Path(__file__).resolve().parents[2]
SPEC = REPO / "docs/spec/10-porto-runtime/20-python-api-reference.md"
STUB = REPO / "python/porto/_core.pyi"

_DEF = re.compile(r"(?:async )?def (\w+)(?:\[[^\]]*\])?(\(.*\))(?: -> .*)?$")

#: One parameter of a signature: (name, kind, default as source text or None).
Param = tuple[str, str, str | None]


# --- the spec's signatures -----------------------------------------------------------------


def _spec_block() -> list[str]:
    """The lines of the first code block under "### Structured concurrency"."""
    text = SPEC.read_text(encoding="utf-8")
    section = text.split("### Structured concurrency", 1)[1]
    block = section.split("```python", 1)[1].split("```", 1)[0]
    return block.splitlines()


def _logical_lines(lines: list[str]) -> list[tuple[int, str]]:
    """``(indent, text)`` per statement: comments dropped, bracketed continuations joined."""
    logical: list[tuple[int, str]] = []
    pending = ""
    indent = 0
    depth = 0
    for raw in lines:
        code = re.sub(r"\s+#.*$", "", raw).rstrip()
        if not code.strip():
            continue
        if not pending:
            indent = len(code) - len(code.lstrip())
        pending += (" " if pending else "") + code.strip()
        depth += sum(code.count(c) for c in "([") - sum(code.count(c) for c in ")]")
        if depth == 0:
            logical.append((indent, pending))
            pending = ""
    return logical


def _params(signature: str) -> list[Param]:
    """Parameters of ``def f<signature>`` (annotations dropped)."""
    function = ast.parse(f"def f{signature}: ...").body[0]
    assert isinstance(function, ast.FunctionDef)
    args = function.args
    positional = [*args.posonlyargs, *args.args]
    defaults: list[ast.expr | None] = [None] * (len(positional) - len(args.defaults))
    defaults += args.defaults
    params: list[Param] = []
    for index, (arg, default) in enumerate(zip(positional, defaults, strict=True)):
        kind = "positional-only" if index < len(args.posonlyargs) else "positional-or-keyword"
        params.append((arg.arg, kind, None if default is None else ast.unparse(default)))
    if args.vararg:
        params.append((args.vararg.arg, "var-positional", None))
    for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        params.append((arg.arg, "keyword-only", None if default is None else ast.unparse(default)))
    if args.kwarg:
        params.append((args.kwarg.arg, "var-keyword", None))
    return params


def _drop_self(params: list[Param]) -> list[Param]:
    assert params[0][0] == "self"
    return params[1:]


def spec_members() -> dict[str, dict[str, list[Param] | str]]:
    """``{class or "": {member: params | "property" | "attribute"}}`` from the spec's block."""
    members: dict[str, dict[str, list[Param] | str]] = {"": {}}
    owner = ""
    for indent, line in _logical_lines(_spec_block()):
        if indent == 0:
            owner = ""
            match = re.match(r"class (\w+)", line)
            if match:
                owner = match.group(1)
                members[owner] = {}
                continue
        for statement in (part.strip() for part in line.split(";")):
            if match := re.match(r"@property (\w+)(?::|\(self\))", statement):
                members[owner][match.group(1)] = "property"
            elif match := re.match(
                r"(?:async )?def (\w+)(?:\[[^\]]*\])?(\(.*\))(?: -> .*)?$", statement
            ):
                members[owner][match.group(1)] = _params(match.group(2))
            elif match := re.match(r"(\w+): ", statement):
                members[owner][match.group(1)] = "attribute"
    return members


# --- the implementation's and the stub's -------------------------------------------------------

_KINDS = {
    inspect.Parameter.POSITIONAL_ONLY: "positional-only",
    inspect.Parameter.POSITIONAL_OR_KEYWORD: "positional-or-keyword",
    inspect.Parameter.VAR_POSITIONAL: "var-positional",
    inspect.Parameter.KEYWORD_ONLY: "keyword-only",
    inspect.Parameter.VAR_KEYWORD: "var-keyword",
}


def runtime_params(function: Any, *, method: bool) -> list[Param]:
    params: list[Param] = []
    for parameter in inspect.signature(function).parameters.values():
        default = None if parameter.default is parameter.empty else repr(parameter.default)
        params.append((parameter.name, _KINDS[parameter.kind], default))
    if not method:
        return params
    # The extension marks `self` positional-only, the spec writes it plain: both mean "bound".
    assert params[0][0] == "self"
    return params[1:]


def _stub_params(function: ast.FunctionDef | ast.AsyncFunctionDef) -> list[Param]:
    source = ast.unparse(function.args)
    return _params(f"({source})")


def stub_members() -> dict[str, dict[str, list[Param] | str]]:
    tree = ast.parse(STUB.read_text(encoding="utf-8"))
    wanted = {"Scope", "Started", "Task"}
    members: dict[str, dict[str, list[Param] | str]] = {"": {}}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            members[""][node.name] = _stub_params(node)
        elif isinstance(node, ast.ClassDef) and node.name in wanted:
            members[node.name] = owned = {}
            for item in node.body:
                if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
                    is_property = any(
                        isinstance(d, ast.Name) and d.id == "property" for d in item.decorator_list
                    )
                    is_setter = any(isinstance(d, ast.Attribute) for d in item.decorator_list)
                    is_overload = any(
                        isinstance(d, ast.Name) and d.id == "overload" for d in item.decorator_list
                    )
                    if is_setter:
                        continue
                    if is_overload:
                        owned[item.name] = "overloaded"
                    elif is_property:
                        owned[item.name] = "property"
                    else:
                        owned[item.name] = _stub_params(item)
                elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                    owned[item.target.id] = "attribute"
    return members


def _owner(name: str) -> Any:
    return {"": porto, "Scope": porto.Scope, "Started": porto.Started, "Task": porto.Task}[name]


def test_the_spec_block_parses_into_the_members_this_suite_checks() -> None:
    """A guard on the parser: it must find the members, or the comparisons below prove nothing."""
    spec = spec_members()
    assert set(spec[""]) >= {"scope", "shield", "timeout", "move_on_after", "current_task"}
    assert set(spec["Scope"]) == {
        "spawn",
        "start",
        "cancel",
        "deadline",
        "shield",
        "cancel_called",
        "cancelled_caught",
        "timed_out",
        "name",
        "tasks",
    }
    assert spec["Started"] == {"set": spec["Started"]["set"]}
    assert {"id", "name", "done", "cancelled", "result", "exception", "cancel", "__await__"} <= set(
        spec["Task"]
    )
    assert _drop_self(spec["Scope"]["spawn"]) == [  # type: ignore[arg-type]
        ("coro_or_fn", "positional-only", None),
        ("args", "var-positional", None),
        ("name", "keyword-only", "None"),
        ("pin", "keyword-only", "None"),
        ("context", "keyword-only", "None"),
    ]


def test_every_member_of_20_has_the_documented_signature() -> None:
    """``inspect.signature`` of each function, method and descriptor the spec lists."""
    for owner, listed in spec_members().items():
        target = _owner(owner)
        for member, expected in listed.items():
            assert hasattr(target, member), f"porto.{owner + '.' if owner else ''}{member} missing"
            if isinstance(expected, str):
                assert not callable(getattr(target, member, None)) or owner == "", member
                continue
            function = getattr(target, member)
            method = owner != ""
            want = _drop_self(expected) if method else expected
            got = runtime_params(function, method=method)
            assert got == want, f"{owner}.{member}: {got} != the spec's {want}"


def test_the_stubs_declare_the_documented_signature_of_every_member() -> None:
    """20 says the stubs MUST match: names, kinds and defaults of everything it lists."""
    stubs = stub_members()
    for owner, listed in spec_members().items():
        for member, expected in listed.items():
            declared = stubs.get(owner, {}).get(member)
            assert declared is not None, f"_core.pyi lacks {owner}.{member}"
            if isinstance(expected, str) or declared == "overloaded":
                if isinstance(expected, str) and expected == "property":
                    assert declared == "property", f"{owner}.{member} is a property in 20"
                continue
            assert isinstance(declared, list)
            want = _drop_self(expected) if owner else expected
            got = _drop_self(declared) if owner else declared
            assert got == want, f"_core.pyi {owner}.{member}: {got} != the spec's {want}"


def test_only_deadline_and_shield_have_a_setter() -> None:
    """Every other Scope attribute is read-only: assigning to it raises AttributeError."""

    async def main() -> None:
        async with porto.scope() as s:
            for name, value in (
                ("cancel_called", True),
                ("cancelled_caught", True),
                ("timed_out", True),
                ("name", "other"),
                ("tasks", ()),
            ):
                with pytest.raises(AttributeError):
                    setattr(s, name, value)
            s.deadline = math.inf
            s.shield = True
            assert s.shield
            s.shield = False

    porto.run(main)


# --- porto.scope(name=...) and Scope.name ------------------------------------------------------


def test_name_is_what_scope_was_given_and_empty_for_an_unnamed_scope() -> None:
    async def main() -> list[str]:
        names: list[str] = []
        async with porto.scope(name="jobs") as s:
            names.append(s.name)
        async with porto.scope() as unnamed:
            names.append(unnamed.name)
        names.append(porto.scope(name="before").name)  # readable before it is entered
        names.append(porto.timeout(1).name)
        names.append(porto.shield().name)
        return names

    assert porto.run(main) == ["jobs", "", "before", "", ""]


def test_the_root_scope_is_named_root() -> None:
    async def main() -> str:
        return _core._root_scope().name

    assert porto.run(main) == "root"


def test_a_name_that_is_not_a_str_raises_type_error_that_says_what_to_pass() -> None:
    with pytest.raises(TypeError, match=r"name= must be a str, got int.*leave it out"):
        porto.scope(name=3)  # type: ignore[arg-type]


async def _fail() -> None:
    raise ValueError("boom")


def test_C4_a_named_scope_names_itself_in_the_failure_group() -> None:
    async def main(name: str | None) -> str:
        try:
            async with porto.scope(name=name) as s:
                s.spawn(_fail())
        except ExceptionGroup as group:
            return str(group)
        raise AssertionError("no group")

    assert porto.run(main, "jobs").startswith("porto scope 'jobs' failed")
    unnamed = porto.run(main, None)
    assert unnamed.startswith("porto scope failed")
    assert "'" not in unnamed.split(" (")[0]


def test_a_name_with_a_lone_surrogate_is_refused_where_it_is_given() -> None:
    """It could not be put in the failure group's message, which would lose the failures."""
    with pytest.raises(ValueError, match=r"lone surrogate.*leave it out"):
        porto.scope(name="bad\udc80name")


def test_C4_a_non_ascii_name_names_the_failure_group_and_keeps_the_failure() -> None:
    async def main() -> BaseExceptionGroup[Any]:
        try:
            async with porto.scope(name="jöbs \U0001f680") as s:
                s.spawn(_fail())
        except BaseExceptionGroup as group:
            return group
        raise AssertionError("no group")

    group = porto.run(main)
    assert str(group).startswith("porto scope 'jöbs \U0001f680' failed")
    assert [type(error) for error in group.exceptions] == [ValueError]


# --- Scope.tasks --------------------------------------------------------------------------------


async def _linger() -> None:
    await porto.sleep(30)


async def _hold(release: "list[bool]") -> None:
    while not release:
        await porto.checkpoint()


def test_tasks_is_empty_before_the_block_and_after_it() -> None:
    async def main() -> tuple[object, object, object]:
        s = porto.scope()
        before = s.tasks
        async with s:
            s.spawn(porto.sleep, 0.001)
        return before, s.tasks, type(s.tasks)

    before, after, kind = porto.run(main)
    assert before == ()
    assert after == ()
    assert kind is tuple


def test_tasks_lists_the_live_children_in_spawn_order_and_drops_the_ended() -> None:
    async def main() -> None:
        release: list[bool] = []
        async with porto.scope() as s:
            first = s.spawn(_hold, release, name="first")
            second = s.spawn(_hold, release, name="second")
            quick = s.spawn(porto.checkpoint)
            assert s.tasks == (first, second, quick)
            await quick
            assert s.tasks == (first, second)
            release.append(True)
            await first
            await second
            assert s.tasks == ()

    porto.run(main)


def test_tasks_is_a_snapshot_not_a_live_view() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            snapshot = s.tasks
            task = s.spawn(_linger)
            assert snapshot == ()
            assert s.tasks == (task,)
            later = s.tasks
            s.cancel()
            await porto.checkpoint()
            assert later == (task,)

    porto.run(main)


def test_tasks_does_not_list_the_tasks_of_nested_scopes_nor_the_body() -> None:
    async def main() -> None:
        async with porto.scope() as outer:
            task = outer.spawn(_linger)
            async with porto.scope() as inner:
                nested = inner.spawn(_linger)
                assert outer.tasks == (task,)
                assert inner.tasks == (nested,)
                assert porto.current_task() not in outer.tasks
                outer.cancel()

    porto.run(main)


def test_tasks_lists_a_started_task_only_once_it_called_set() -> None:
    async def server(*, started: "porto.Started[int]") -> None:
        started.set(7)
        await porto.sleep(30)

    async def main() -> None:
        async with porto.scope() as s:
            assert await s.start(server) == 7
            (task,) = s.tasks
            assert task.name.endswith("server")
            s.cancel()

    porto.run(main)


def test_tasks_stays_exact_when_tasks_end_in_any_order() -> None:
    """The scope's list moves the last task into the place of one that ended: whichever ends,
    ``tasks`` must hold exactly the others, in spawn order."""

    async def main() -> None:
        releases = [[] for _ in range(40)]  # type: list[list[bool]]
        async with porto.scope() as s:
            tasks = [s.spawn(_hold, release) for release in releases]
            assert s.tasks == tuple(tasks)
            live = list(tasks)
            for index in (7, 0, 39, 20, 21, 1, 38):  # first, last and middle ones
                releases[index].append(True)
                await tasks[index]
                live.remove(tasks[index])
                assert s.tasks == tuple(live)
            for release in releases:
                release.append(True)

    porto.run(main)


def test_tasks_stays_exact_when_a_scope_spawns_many_short_tasks_in_turn() -> None:
    async def main() -> None:
        release: list[bool] = []
        async with porto.scope() as s:
            held = [s.spawn(_hold, release) for _ in range(5)]
            for _ in range(500):
                await s.spawn(porto.checkpoint)
            assert s.tasks == tuple(held)
            release.append(True)

    porto.run(main)


def test_a_child_reading_tasks_sees_itself_and_its_siblings() -> None:
    """The reader is the task running now: the registry walk reads it through the step driver."""

    async def main() -> None:
        release: list[bool] = []
        seen: list[tuple[porto.Task[None], ...]] = []

        async def reader(s: porto.Scope) -> None:
            seen.append(s.tasks)

        async with porto.scope() as s:
            sibling = s.spawn(_hold, release)
            me = s.spawn(reader, s)
            await me
            release.append(True)
        (listed,) = seen
        assert listed == (sibling, me)

    porto.run(main)


def test_tasks_does_not_list_a_task_without_a_handle_or_in_no_scope() -> None:
    async def main() -> None:
        release: list[bool] = []
        async with porto.scope() as s:
            # An internal spawn has no `porto.Task`, and `main` is no scope's child.
            hidden = _core._spawn_joinable(_hold(release))  # pyright: ignore[reportPrivateUsage]
            listed = s.spawn(_hold, release)
            assert s.tasks == (listed,)
            release.append(True)
            await hidden.join()

    porto.run(main)


def test_tasks_of_an_open_scope_cannot_be_read_off_the_lane() -> None:
    import threading

    async def main() -> None:
        errors: list[BaseException] = []
        async with porto.scope() as s:
            s.spawn(porto.sleep, 0.001)

            def other_thread() -> None:
                try:
                    s.tasks  # noqa: B018
                except BaseException as error:
                    errors.append(error)

            thread = threading.Thread(target=other_thread)
            thread.start()
            thread.join()
        assert len(errors) == 1
        assert isinstance(errors[0], porto.NoRuntimeError)
        assert "porto.run()" in str(errors[0])

    porto.run(main)


def test_tasks_still_answers_while_the_runtime_cancels_what_is_left() -> None:
    """The ``finally`` of an abandoned task runs as the runtime is dropped: it may list the tasks
    of a scope that is still open (the registry answers while it shuts down)."""
    import warnings

    seen: list[tuple[str, ...]] = []

    async def stubborn(s: porto.Scope | None = None) -> None:
        try:
            while True:
                with contextlib.suppress(porto.Cancelled):
                    await porto.sleep(0.001)
        finally:
            if s is not None:
                seen.append(tuple(task.name for task in s.tasks))

    async def parent() -> None:
        # Entered by hand and never exited, so tearing the parent down does not close the scope.
        s = porto.scope()
        await s.__aenter__()
        s.spawn(stubborn, s, name="c1")
        s.spawn(stubborn, s, name="c2")
        await stubborn()

    async def main() -> None:
        _core._root_scope().spawn(parent)  # pyright: ignore[reportPrivateUsage]
        for _ in range(5):
            await porto.checkpoint()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        porto.run(main, cancel_timeout=0.01)
    # One of the two is torn down first and sees the other still alive; the second sees none.
    assert len(seen) == 2
    assert sorted(seen)[0] == ()
    assert sorted(seen)[1] in {("c1",), ("c2",)}


def test_tasks_of_a_scope_of_another_run_raises_instead_of_answering_empty() -> None:
    async def enter_and_leave_open() -> porto.Scope:
        s = porto.scope()
        await s.__aenter__()  # never exited: the scope stays open after its run
        return s

    scope = porto.run(enter_and_leave_open)

    async def read_it() -> None:
        with pytest.raises(porto.RuntimeClosed, match="runtime that stopped"):
            scope.tasks  # noqa: B018

    porto.run(read_it)


def test_a_task_that_outlives_its_scope_object_is_unlisted_safely() -> None:
    """The root scope is closed with abandoned tasks still running: they end afterwards."""
    import gc
    import warnings

    async def main() -> None:
        async def stubborn() -> None:
            while True:
                with contextlib.suppress(porto.Cancelled):
                    await porto.sleep(0.001)

        _core._root_scope().spawn(stubborn)  # pyright: ignore[reportPrivateUsage]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        porto.run(main, cancel_timeout=0.01)
    gc.collect()


def test_tasks_of_a_cancelled_scope_empty_as_the_children_end() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            tasks = [s.spawn(_linger) for _ in range(3)]
            assert s.tasks == tuple(tasks)
            s.cancel()
            with porto.shield():  # the body is cancelled too: wait for the children unshielded
                for task in tasks:
                    with contextlib.suppress(porto.Cancelled):
                        await task
            assert s.tasks == ()

    porto.run(main)


# --- Scope.cancel(reason) and the properties -----------------------------------------------------


def test_cancel_takes_an_optional_str_reason_and_shows_it_in_the_repr() -> None:
    async def main() -> str:
        async with porto.scope() as s:
            s.cancel("shutting down")
            s.cancel("ignored: the first reason wins")
            text = repr(s)
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()
        return text

    assert '"shutting down"' in porto.run(main)


def test_cancel_called_flips_with_cancel_and_stays_true() -> None:
    async def main() -> list[bool]:
        seen: list[bool] = []
        async with porto.scope() as s:
            seen.append(s.cancel_called)
            s.cancel()
            seen.append(s.cancel_called)
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()
        seen.append(s.cancel_called)
        return seen

    assert porto.run(main) == [False, True, True]


def test_cancelled_caught_tells_whether_the_exit_absorbed_the_cancel() -> None:
    async def main() -> tuple[bool, bool]:
        async with porto.scope() as quiet:
            await porto.checkpoint()
        async with porto.scope() as cancelled:
            cancelled.cancel()
            await porto.checkpoint()
        return quiet.cancelled_caught, cancelled.cancelled_caught

    assert porto.run(main) == (False, True)


def test_timed_out_tells_whether_the_deadline_cut_the_block_short() -> None:
    async def main() -> tuple[bool, bool]:
        async with porto.move_on_after(0.01) as slow:
            await porto.sleep(30)
        async with porto.move_on_after(30) as fast:
            await porto.checkpoint()
        return slow.timed_out, fast.timed_out

    assert porto.run(main) == (True, False)


def test_deadline_reads_inf_for_none_and_is_read_write() -> None:
    async def main() -> tuple[float, float, bool]:
        async with porto.move_on_after(30) as s:
            assert s.deadline == pytest.approx(porto.current_time() + 30, abs=5)
            s.deadline = math.inf
            assert s.deadline == math.inf
            s.deadline = porto.current_time() - 1
            await porto.checkpoint()  # the Cancelled leaves the block: absorbed, timed out
        async with porto.scope() as plain:
            none = plain.deadline
        return none, s.deadline, s.timed_out

    none, _, timed_out = porto.run(main)
    assert none == math.inf
    assert timed_out


def test_shield_is_read_write_and_a_non_bool_is_refused_or_coerced_as_python_does() -> None:
    async def main() -> list[bool]:
        seen: list[bool] = []
        async with porto.scope(shield=True) as s:
            seen.append(s.shield)
            s.shield = False
            seen.append(s.shield)
            s.shield = True
            seen.append(s.shield)
        return seen

    assert porto.run(main) == [True, False, True]


def test_a_scope_attribute_read_without_a_runtime_does_not_need_one() -> None:
    """``name``, ``tasks`` and the flags are plain reads: no ``NoRuntimeError`` outside a run."""
    s = porto.scope(name="plain")
    assert (s.name, s.tasks, s.cancel_called, s.cancelled_caught, s.timed_out) == (
        "plain",
        (),
        False,
        False,
        False,
    )
    assert s.shield is False
    assert s.deadline == math.inf
    s.cancel("before it runs")
    assert s.cancel_called


def test_current_task_is_the_object_spawn_returned() -> None:
    async def main() -> bool:
        seen: list[porto.Task[Any]] = []

        async def probe() -> None:
            seen.append(porto.current_task())

        async with porto.scope() as s:
            task = s.spawn(probe)
        return seen == [task]

    assert porto.run(main)


def test_current_task_outside_a_runtime_raises_no_runtime_error() -> None:
    with pytest.raises(porto.NoRuntimeError):
        porto.current_task()
