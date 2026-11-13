"""The runtime's default handling of SIGINT and SIGTERM (18-fs-process-signals.md §3,
26-errors-config-lifecycle.md §2): `handle_signals=True` on the main thread.

Each test runs a program in a subprocess and signals it, as a shell or a supervisor would.
Timeouts are generous: the machine may be loaded, and nothing here waits for them when it works.
"""

import queue
import signal
import subprocess
import sys
import threading
import time

import pytest

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Unix signals")

WAIT = 60.0


class Child:
    """A Python program under test: its stdout lines arrive in order."""

    def __init__(self, program: str, *, reset_signals: bool = True) -> None:
        # A runner started with `cmd &` or `nohup` has SIGINT ignored, which children inherit.
        prefix = (
            "import signal\n"
            "signal.signal(signal.SIGINT, signal.default_int_handler)\n"
            "signal.signal(signal.SIGTERM, signal.SIG_DFL)\n"
            if reset_signals
            else ""
        )
        self.process = subprocess.Popen(
            [sys.executable, "-c", prefix + program],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.lines: queue.Queue[str | None] = queue.Queue()
        self.seen: list[str] = []
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.lines.put(line.strip())
        self.lines.put(None)

    def wait_for(self, expected: str) -> None:
        deadline = time.monotonic() + WAIT
        while time.monotonic() < deadline:
            try:
                line = self.lines.get(timeout=1)
            except queue.Empty:
                continue
            if line is None:
                break
            self.seen.append(line)
            if line == expected:
                return
        self.process.kill()
        raise AssertionError(f"never saw {expected!r}; saw {self.seen}\n{self.stderr()}")

    def signal(self, signum: int) -> None:
        self.process.send_signal(signum)

    def finish(self) -> int:
        try:
            code = self.process.wait(WAIT)
        except subprocess.TimeoutExpired:
            self.process.kill()
            raise AssertionError(f"did not exit; saw {self.seen}\n{self.stderr()}") from None
        while (line := self.lines.get(timeout=WAIT)) is not None:
            self.seen.append(line)
        return code

    def stderr(self) -> str:
        assert self.process.stderr is not None
        if self.process.poll() is None:
            return "(still running)"
        return self.process.stderr.read()


SLEEPING_MAIN = """
import porto

async def main():
    print("ready", flush=True)
    try:
        await porto.sleep(60)
    finally:
        print("finally", flush=True)
"""


def test_sigint_ends_porto_run_with_keyboard_interrupt_after_the_finally_blocks() -> None:
    child = Child(
        SLEEPING_MAIN
        + """
try:
    porto.run(main)
except KeyboardInterrupt:
    print("KeyboardInterrupt", flush=True)
"""
    )
    child.wait_for("ready")
    child.signal(signal.SIGINT)
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["ready", "finally", "KeyboardInterrupt"]


def test_an_uncaught_sigint_exits_like_python_does_for_ctrl_c() -> None:
    child = Child(SLEEPING_MAIN + "\nporto.run(main)\n")
    child.wait_for("ready")
    child.signal(signal.SIGINT)
    code = child.finish()
    assert code in {-signal.SIGINT, 128 + signal.SIGINT, 1}, (code, child.stderr())
    assert "finally" in child.seen


def test_sigterm_ends_porto_run_with_terminated_after_the_graceful_timeout() -> None:
    child = Child(
        SLEEPING_MAIN
        + """
started = None
try:
    porto.run(main, graceful_timeout=0.2)
except porto.Terminated as terminated:
    print("Terminated", terminated.code, isinstance(terminated, SystemExit), flush=True)
"""
    )
    child.wait_for("ready")
    child.signal(signal.SIGTERM)
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["ready", "finally", "Terminated 143 True"]


def test_a_main_that_drains_on_sigterm_finishes_before_the_graceful_timeout() -> None:
    child = Child(
        """
import porto

async def main():
    requested = porto.lifecycle.shutdown_requested()
    print("ready", flush=True)
    await requested.wait()
    print("draining", flush=True)
    await porto.sleep(0.2)
    print("drained", flush=True)

try:
    porto.run(main, graceful_timeout=30, cancel_timeout=30)
except porto.Terminated as terminated:
    print("Terminated", terminated.code, flush=True)
"""
    )
    child.wait_for("ready")
    started = time.monotonic()
    child.signal(signal.SIGTERM)
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["ready", "draining", "drained", "Terminated 143"]
    assert time.monotonic() - started < 25, "the drain was cut short by neither timeout"


def test_a_second_sigint_forces_the_exit_while_a_shielded_task_hangs() -> None:
    child = Child(
        """
import porto

async def announce():
    with porto.shield():
        await porto.lifecycle.shutdown_requested().wait()
        print("requested", flush=True)

async def main():
    async with porto.scope() as scope:
        scope.spawn(announce)
        print("ready", flush=True)
        with porto.shield():
            await porto.sleep(120)

try:
    porto.run(main, cancel_timeout=120)
except KeyboardInterrupt:
    print("KeyboardInterrupt", flush=True)
"""
    )
    child.wait_for("ready")
    child.signal(signal.SIGINT)
    child.wait_for("requested")
    time.sleep(0.3)  # the first interrupt cancelled the scope; the shield holds main
    assert child.process.poll() is None, "the first Ctrl-C alone must not end a shielded main"
    started = time.monotonic()
    child.signal(signal.SIGINT)
    assert child.finish() == 0, child.stderr()
    assert child.seen[-1] == "KeyboardInterrupt"
    assert time.monotonic() - started < 30, "the second Ctrl-C did not force the exit"


def test_handle_signals_false_leaves_pythons_default_for_sigint() -> None:
    child = Child(
        SLEEPING_MAIN
        + """
try:
    porto.run(main, handle_signals=False)
except KeyboardInterrupt:
    print("KeyboardInterrupt", flush=True)
"""
    )
    child.wait_for("ready")
    child.signal(signal.SIGINT)
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["ready", "finally", "KeyboardInterrupt"]


def test_handle_signals_false_leaves_the_default_disposition_of_sigterm() -> None:
    child = Child(SLEEPING_MAIN + "\nporto.run(main, handle_signals=False)\n")
    child.wait_for("ready")
    child.signal(signal.SIGTERM)
    assert child.finish() == -signal.SIGTERM
    assert "finally" not in child.seen, "the process was killed, not shut down"


def test_a_runtime_off_the_main_thread_does_not_take_the_signals() -> None:
    child = Child(
        """
import os, signal, threading
import porto

ready = threading.Event()
stop = threading.Event()
result = []

async def main():
    ready.set()
    await porto.to_thread(stop.wait, 60)
    return "stopped"

thread = threading.Thread(target=lambda: result.append(porto.run(main)))
thread.start()
assert ready.wait(60)
try:
    os.kill(os.getpid(), signal.SIGINT)
    thread.join(60)
except KeyboardInterrupt:
    print("python's own handler", flush=True)
stop.set()
thread.join(60)
print(result, flush=True)
"""
    )
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["python's own handler", "['stopped']"]


def test_the_default_handlers_are_gone_after_the_run() -> None:
    import os

    import porto

    async def main() -> None:
        await porto.sleep(0)

    signal.signal(signal.SIGINT, signal.default_int_handler)

    def interrupt_and_wait() -> None:
        os.kill(os.getpid(), signal.SIGINT)
        time.sleep(5)

    porto.run(main)
    with pytest.raises(KeyboardInterrupt):
        interrupt_and_wait()


def test_a_second_sigint_interrupts_a_main_that_runs_synchronous_code() -> None:
    child = Child(
        """
import time
import porto

async def main():
    print("ready", flush=True)
    started = time.time()
    while time.time() - started < 60:
        pass

try:
    porto.run(main)
except KeyboardInterrupt:
    print("KeyboardInterrupt", flush=True)
"""
    )
    child.wait_for("ready")
    started = time.monotonic()
    child.signal(signal.SIGINT)
    time.sleep(0.3)  # the first cannot act: the lane is busy
    assert child.process.poll() is None
    child.signal(signal.SIGINT)
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["ready", "KeyboardInterrupt"]
    assert time.monotonic() - started < 30


def test_a_second_sigint_interrupts_a_main_blocked_in_time_sleep() -> None:
    child = Child(
        """
import time
import porto

async def main():
    print("ready", flush=True)
    time.sleep(60)

try:
    porto.run(main)
except KeyboardInterrupt:
    print("KeyboardInterrupt", flush=True)
"""
    )
    child.wait_for("ready")
    started = time.monotonic()
    child.signal(signal.SIGINT)
    time.sleep(0.3)
    assert child.process.poll() is None
    child.signal(signal.SIGINT)
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["ready", "KeyboardInterrupt"]
    assert time.monotonic() - started < 30


def test_a_second_sigint_forces_the_exit_while_the_root_scope_drains_after_main_ended() -> None:
    child = Child(
        """
import porto

async def hang():
    with porto.shield():
        await porto.sleep(120)

async def main():
    porto.runtime().root_scope.spawn(hang)
    await porto.sleep(0.05)
    print("ready", flush=True)

try:
    porto.run(main, cancel_timeout=120)
except KeyboardInterrupt:
    print("KeyboardInterrupt", flush=True)
"""
    )
    child.wait_for("ready")
    time.sleep(0.3)
    child.signal(signal.SIGINT)
    time.sleep(0.3)
    assert child.process.poll() is None, "the first Ctrl-C leaves the shielded task its time"
    started = time.monotonic()
    child.signal(signal.SIGINT)
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["ready", "KeyboardInterrupt"]
    assert time.monotonic() - started < 30


def test_an_ignored_sigint_stays_ignored() -> None:
    child = Child(
        """
import signal, time
import porto

signal.signal(signal.SIGINT, signal.SIG_IGN)

async def main():
    print("ready", flush=True)
    await porto.sleep(1.0)
    print("finished", flush=True)

porto.run(main)
print("done", flush=True)
""",
        reset_signals=False,
    )
    child.wait_for("ready")
    child.signal(signal.SIGINT)
    assert child.finish() == 0, child.stderr()
    assert child.seen[-2:] == ["finished", "done"]


def test_a_sigint_handler_the_program_installed_stays() -> None:
    child = Child(
        """
import signal
import porto

def custom(signum, frame):
    print("custom handler", flush=True)

signal.signal(signal.SIGINT, custom)

async def main():
    print("ready", flush=True)
    await porto.sleep(1.0)

porto.run(main)
print("done", flush=True)
""",
        reset_signals=False,
    )
    child.wait_for("ready")
    child.signal(signal.SIGINT)
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["ready", "custom handler", "done"]


def test_a_sigterm_handler_the_program_installed_stays() -> None:
    child = Child(
        """
import signal
import porto

signal.signal(signal.SIGTERM, lambda signum, frame: print("custom term", flush=True))

async def main():
    print("ready", flush=True)
    await porto.sleep(1.0)

porto.run(main)
print("done", flush=True)
""",
        reset_signals=False,
    )
    child.wait_for("ready")
    child.signal(signal.SIGTERM)
    assert child.finish() == 0, child.stderr()
    assert child.seen == ["ready", "custom term", "done"]
