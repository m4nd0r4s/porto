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

from porto._channel import channel, unbounded_channel
from porto._config import DeterministicConfig, RuntimeConfig
from porto._core import (
    Event,
    Lock,
    Notify,
    Receiver,
    Scope,
    Sender,
    Task,
    __version__,
    checkpoint,
    current_time,
    monotonic,
    run,
    scope,
    sleep,
)
from porto._sync import Mutex, MutexGuard
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
    "BrokenResourceError",
    "Cancelled",
    "ClosedResourceError",
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
    "PortoError",
    "Receiver",
    "RuntimeClosed",
    "RuntimeConfig",
    "RustPanic",
    "Scope",
    "Sender",
    "Task",
    "Terminated",
    "WouldBlock",
    "__version__",
    "channel",
    "checkpoint",
    "current_time",
    "monotonic",
    "run",
    "scope",
    "sleep",
    "unbounded_channel",
]
