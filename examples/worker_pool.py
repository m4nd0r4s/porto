"""A worker pool pipeline: producer -> bounded job channel -> N workers -> result channel.

    uv run python examples/worker_pool.py [--jobs 200] [--workers 8]

The workers share one receiver through clones, so each job goes to exactly one worker; each
worker has its own clone of the result sender and closes it when the jobs run out, so the
collector's ``async for`` ends once the last worker is done. The bounded job channel keeps
the producer from running far ahead of the workers.
"""

import argparse
from dataclasses import dataclass

import porto


@dataclass(frozen=True)
class Done:
    job: int
    square: int
    worker: int


async def produce(jobs: porto.Sender[int], count: int) -> None:
    async with jobs:  # closing it ends the workers' loops once the jobs are drained
        for job in range(count):
            await jobs.send(job)


async def work(worker: int, jobs: porto.Receiver[int], results: porto.Sender[Done]) -> None:
    async with results:
        async for job in jobs:
            await porto.sleep(0.001 * (job % 3))  # uneven work
            await results.send(Done(job, job * job, worker))


async def collect(results: porto.Receiver[Done]) -> list[Done]:
    return [done async for done in results]


async def pipeline(count: int, workers: int) -> list[Done]:
    job_tx, job_rx = porto.channel[int](workers * 2)
    result_tx, result_rx = porto.channel[Done](workers * 2)
    collector: porto.Task[list[Done]] | None = None
    async with porto.scope() as s:
        s.spawn(produce, job_tx, count)
        for worker in range(workers):
            s.spawn(work, worker, job_rx.clone(), result_tx.clone())
        # Close our own ends: the clones are what keeps each channel open.
        job_rx.close()
        result_tx.close()
        collector = s.spawn(collect, result_rx)
    assert collector is not None
    return collector.result()


def main() -> None:
    parser = argparse.ArgumentParser(description="Square numbers on a pool of workers.")
    parser.add_argument("--jobs", type=int, default=200)
    parser.add_argument("--workers", type=int, default=8)
    options = parser.parse_args()
    jobs: int = options.jobs
    workers: int = options.workers

    results = porto.run(pipeline, jobs, workers)
    squares = {done.job: done.square for done in results}
    assert len(results) == len(squares), "a job was done twice"
    assert squares == {job: job * job for job in range(jobs)}, "jobs were lost or wrong"
    used = {done.worker for done in results}
    assert len(used) > 1 or min(workers, jobs) == 1, f"only worker {used} did any work"
    print(f"worker_pool: ok ({jobs} jobs on {len(used)} workers)")


if __name__ == "__main__":
    main()
