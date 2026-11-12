"""Mutual exclusion: ``porto.Lock`` and ``porto.Mutex[T]``.

    uv run python examples/locks.py

A ``Lock`` keeps a read-modify-write that awaits in the middle from interleaving with
another task's. It is fair (waiters get it in arrival order) and not reentrant. A ``Mutex[T]``
is a lock that owns its value: the value is reachable only through the guard ``async with``
gives, so it cannot be touched without holding the lock. For several holders at once see
``semaphores.py``; for readers and writers, ``coordination.py``.
"""

import porto


class Account:
    def __init__(self, balance: int) -> None:
        self.balance = balance


async def deposit(account: Account, lock: porto.Lock | None, amount: int) -> None:
    async def update() -> None:
        current = account.balance
        await porto.checkpoint()  # say, a database round trip
        account.balance = current + amount

    if lock is None:
        await update()
    else:
        async with lock:
            await update()


async def deposits(lock: porto.Lock | None) -> int:
    account = Account(0)
    async with porto.scope() as s:
        for _ in range(100):
            s.spawn(deposit, account, lock, 1)
    return account.balance


async def fair_order() -> list[int]:
    """Waiters get the lock in the order they asked for it."""
    lock = porto.Lock()
    order: list[int] = []

    async def worker(index: int) -> None:
        async with lock:
            order.append(index)
            await porto.sleep(0.001)

    async with porto.scope() as s:
        await lock.acquire()  # hold it while the workers line up
        for index in range(5):
            s.spawn(worker, index)
            await porto.checkpoint()  # let each one start waiting before the next
        assert lock.locked() and not lock.try_acquire()
        lock.release()
    assert not lock.locked()
    return order


async def bank_transfers() -> dict[str, int]:
    """Concurrent transfers between accounts kept in a Mutex[dict]."""
    ledger = porto.Mutex({"alice": 100, "bob": 50, "carol": 0})

    async def transfer(source: str, target: str, amount: int) -> None:
        async with ledger as guard:
            balances = dict(guard.value)
            if balances[source] < amount:
                return  # declined
            await porto.checkpoint()  # an audit log write, while the lock is held
            balances[source] -= amount
            balances[target] += amount
            guard.set(balances)

    async with porto.scope() as s:
        for _ in range(10):
            s.spawn(transfer, "alice", "bob", 10)
            s.spawn(transfer, "bob", "carol", 5)

    escaped: porto.MutexGuard[dict[str, int]] | None = None
    async with ledger as guard:
        result = dict(guard.value)
        escaped = guard
    # The guard stops working once its block exits, with an error that says why.
    try:
        _ = escaped.value
    except RuntimeError:
        pass
    else:
        raise AssertionError("the guard worked after its block")
    return result


async def main() -> None:
    # Without the lock, the deposits read the same balance and overwrite each other.
    assert await deposits(lock=None) < 100
    assert await deposits(lock=porto.Lock()) == 100
    assert await fair_order() == [0, 1, 2, 3, 4]
    balances = await bank_transfers()
    assert balances == {"alice": 0, "bob": 100, "carol": 50}, balances
    assert sum(balances.values()) == 150  # no money was created or lost
    print("locks: ok")


if __name__ == "__main__":
    porto.run(main)
