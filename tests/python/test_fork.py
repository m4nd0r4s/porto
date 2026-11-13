"""``fork()`` and the runtime (25-cpython-integration-and-memory.md §6, ST-14).

A forked child gets only the forking thread: the runtime that was running does not survive in it.
Each test runs a program in a subprocess that forks.
"""

import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="no fork on Windows")


def run_program(body: str) -> subprocess.CompletedProcess[str]:
    code = "import os, signal, threading, warnings\nimport porto\n" + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_a_fork_inside_a_running_runtime_warns() -> None:
    done = run_program(
        """
        async def main():
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                pid = os.fork()
            if pid == 0:
                os._exit(0)
            os.waitpid(pid, 0)
            for warning in caught:
                print(warning.category.__name__, warning.message, flush=True)

        porto.run(main)
        """
    )
    assert done.returncode == 0, done.stderr
    assert "RuntimeWarning fork() while porto runtime is running is unsafe" in done.stdout


def test_a_fork_without_a_runtime_does_not_warn() -> None:
    done = run_program(
        """
        async def main():
            await porto.sleep(0)

        porto.run(main)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            pid = os.fork()
        if pid == 0:
            os._exit(0)
        os.waitpid(pid, 0)
        print("warnings:", len(caught), flush=True)
        """
    )
    assert done.returncode == 0, done.stderr
    assert "warnings: 0" in done.stdout


def test_the_child_of_a_fork_inside_a_task_cannot_use_the_inherited_runtime_but_can_start_one() -> (
    None
):
    done = run_program(
        """
        warnings.simplefilter("ignore")

        async def fresh():
            await porto.sleep(0)
            return 41

        async def main():
            runtime = porto.runtime()
            pid = os.fork()
            if pid == 0:
                checks = []
                for name, use in [
                    ("root_scope", lambda: runtime.root_scope),
                    ("shutdown", lambda: runtime.shutdown()),
                    ("block_on", lambda: runtime.block_on(fresh)),
                ]:
                    try:
                        use()
                    except porto.RuntimeClosed as error:
                        checks.append(name if "does not survive fork()" in str(error) else "?")
                try:
                    porto.runtime()
                except porto.NoRuntimeError:
                    checks.append("runtime() is none")
                print("inherited:", checks, repr(runtime), flush=True)
                print("fresh:", porto.run(fresh), flush=True)
                os._exit(0)
            _, status = os.waitpid(pid, 0)
            print("child status", os.waitstatus_to_exitcode(status), flush=True)

        porto.run(main)
        """
    )
    assert done.returncode == 0, done.stderr
    assert (
        "inherited: ['root_scope', 'shutdown', 'block_on', "
        "'runtime() is none'] "
        "<porto.Runtime poisoned by fork()>" in done.stdout
    )
    assert "fresh: 41" in done.stdout
    assert "child status 0" in done.stdout


def test_the_child_of_a_fork_from_another_thread_cannot_use_the_runtime_either() -> None:
    done = run_program(
        """
        warnings.simplefilter("ignore")
        runtime = porto.Runtime()
        started = threading.Event()
        release = threading.Event()

        async def main():
            started.set()
            while not release.is_set():
                await porto.sleep(0.005)

        async def fresh():
            return 7

        thread = threading.Thread(target=runtime.block_on, args=(main,))
        thread.start()
        started.wait()
        pid = os.fork()
        if pid == 0:
            try:
                runtime.root_scope
            except porto.RuntimeClosed as error:
                print("closed:", "does not survive fork()" in str(error), flush=True)
            print("fresh:", porto.run(fresh), flush=True)
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        release.set()
        thread.join()
        print("child status", os.waitstatus_to_exitcode(status), flush=True)
        """
    )
    assert done.returncode == 0, done.stderr
    assert "closed: True" in done.stdout
    assert "fresh: 7" in done.stdout
    assert "child status 0" in done.stdout


def test_a_runtime_that_never_started_is_usable_in_the_child() -> None:
    done = run_program(
        """
        runtime = porto.Runtime()
        pid = os.fork()
        if pid == 0:
            async def main():
                return 5
            print("child:", runtime.block_on(main), flush=True)
            os._exit(0)
        os.waitpid(pid, 0)
        """
    )
    assert done.returncode == 0, done.stderr
    assert "child: 5" in done.stdout


def test_a_signal_in_the_child_is_not_delivered_to_the_parents_runtime() -> None:
    done = run_program(
        """
        warnings.simplefilter("ignore")
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        async def main():
            pid = os.fork()
            if pid == 0:
                os.kill(os.getpid(), signal.SIGTERM)
                os._exit(3)
            _, status = os.waitpid(pid, 0)
            await porto.sleep(0.2)
            requested = porto.lifecycle.shutdown_requested().is_set()
            print("child:", os.waitstatus_to_exitcode(status), "requested:", requested, flush=True)

        porto.run(main)
        """
    )
    assert done.returncode == 0, done.stderr
    assert f"child: {-15} requested: False" in done.stdout


def test_the_child_of_a_fork_from_a_blocking_pool_worker_can_start_a_runtime() -> None:
    done = run_program(
        """
        warnings.simplefilter("ignore")

        async def fresh():
            return 9

        def forker():
            pid = os.fork()
            if pid == 0:
                print("fresh:", porto.run(fresh), flush=True)
                os._exit(0)
            _, status = os.waitpid(pid, 0)
            return os.waitstatus_to_exitcode(status)

        async def main():
            print("child status", await porto.to_thread(forker), flush=True)

        porto.run(main)
        """
    )
    assert done.returncode == 0, done.stderr
    assert "fresh: 9" in done.stdout
    assert "child status 0" in done.stdout
