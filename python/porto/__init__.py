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

from porto._core import Scope, Task, __version__, checkpoint, run, scope, sleep

__all__ = ["Scope", "Task", "__version__", "checkpoint", "run", "scope", "sleep"]
