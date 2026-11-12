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

from porto import lowlevel
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
    monotonic,
    move_on_after,
    run,
    scope,
    shield,
    sleep,
    timeout,
)
from porto.errors import (
    BrokenResourceError,
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
    "broadcast",
    "channel",
    "checkpoint",
    "current_task",
    "current_time",
    "lowlevel",
    "monotonic",
    "move_on_after",
    "oneshot",
    "run",
    "scope",
    "shield",
    "sleep",
    "timeout",
    "unbounded_channel",
    "watch",
]

# Time: sleep_until and sleep_forever (porto-szb.33)
from porto._core import sleep_forever, sleep_until

__all__ += ["sleep_forever", "sleep_until"]
