"""Porto: an async runtime for Python, written in Rust.

```python
import porto


async def work(n: int) -> int:
    await porto.sleep(0.1)
    return n * 2


async def main() -> int:
    async with porto.scope() as s:
        tasks = [s.spawn(work, n) for n in range(10)]
    return sum(task.result() for task in tasks)


print(porto.run(main))
```
"""

from porto import abc, lifecycle, lowlevel, net
from porto._channel import broadcast, channel, oneshot, unbounded_channel, watch
from porto._config import DeterministicConfig, RuntimeConfig
from porto._core import (
    Barrier,
    BroadcastReceiver,
    BroadcastSender,
    Buffer,
    CapacityLimiter,
    Condition,
    Event,
    Lock,
    Mutex,
    MutexGuard,
    Notify,
    OneshotReceiver,
    OneshotSender,
    Receiver,
    Runtime,
    RwLock,
    Scope,
    Semaphore,
    Sender,
    Started,
    Task,
    WatchReceiver,
    WatchSender,
    __version__,
    checkpoint,
    current_task,
    current_time,
    in_runtime,
    monotonic,
    move_on_after,
    run,
    runtime,
    scope,
    shield,
    sleep,
    timeout,
)
from porto._threads import to_thread
from porto.errors import (
    BrokenResourceError,
    BusyResourceError,
    Cancelled,
    ClosedResourceError,
    EndOfChannel,
    IncompleteReadError,
    InvalidStateError,
    Lagged,
    LimitOverrunError,
    NoRuntimeError,
    PortoError,
    RuntimeClosed,
    RustPanic,
    Terminated,
    WouldBlock,
)

__all__ = [
    "Barrier",
    "BroadcastReceiver",
    "BroadcastSender",
    "BrokenResourceError",
    "Buffer",
    "BusyResourceError",
    "Cancelled",
    "CapacityLimiter",
    "ClosedResourceError",
    "Condition",
    "DeterministicConfig",
    "EndOfChannel",
    "Event",
    "IncompleteReadError",
    "InvalidStateError",
    "Lagged",
    "LimitOverrunError",
    "Lock",
    "Mutex",
    "MutexGuard",
    "NoRuntimeError",
    "Notify",
    "OneshotReceiver",
    "OneshotSender",
    "PortoError",
    "Receiver",
    "Runtime",
    "RuntimeClosed",
    "RuntimeConfig",
    "RustPanic",
    "RwLock",
    "Scope",
    "Semaphore",
    "Sender",
    "Started",
    "Task",
    "Terminated",
    "WatchReceiver",
    "WatchSender",
    "WouldBlock",
    "__version__",
    "abc",
    "broadcast",
    "channel",
    "checkpoint",
    "current_task",
    "current_time",
    "in_runtime",
    "lifecycle",
    "lowlevel",
    "monotonic",
    "move_on_after",
    "net",
    "oneshot",
    "run",
    "runtime",
    "scope",
    "shield",
    "sleep",
    "timeout",
    "to_thread",
    "unbounded_channel",
    "watch",
]

# Time: sleep_until and sleep_forever (porto-szb.33)
from porto._core import sleep_forever, sleep_until

__all__ += ["sleep_forever", "sleep_until"]
