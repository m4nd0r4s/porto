"""A quick, interleaved look at the TCP echo servers, for investigating ``tcp_echo`` (the pyperf
row is the number that counts; this is not).

    <venv>/bin/python bench/micro/echo_probe.py [--workers 8] [--rounds 2000] [--reps 5]

Runs each server ``--reps`` times in turn (porto, uvloop streams, uvloop protocol, asyncio
streams) against the same load generator, printing the median and minimum microseconds per
round trip and the throughput. Interleaving puts the same machine noise on every server. With
``--serve porto|uvloop`` it runs only that server, for a long run you can attach a profiler to
(``sample <pid> 3`` on macOS, ``perf record -p <pid>`` on Linux): it prints its pid first.
"""

from __future__ import annotations

import argparse
import os
import statistics
from collections.abc import Callable

import net_programs
import programs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=8, help="load generator processes")
    parser.add_argument("--rounds", type=int, default=2000, help="round trips per connection")
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--serve", choices=("porto", "uvloop"), help="run one server only")
    options = parser.parse_args()
    net_programs.CLIENT_WORKERS = options.workers
    rounds: int = options.rounds
    servers: dict[str, Callable[[], float]] = {
        "porto": lambda: programs.run_porto(net_programs.porto_tcp_echo, rounds),
        "uvloop": lambda: programs.runasyncio_program(
            "uvloop", net_programs.asyncio_tcp_echo, rounds
        ),
        "uvloop-protocol": lambda: programs.runasyncio_program(
            "uvloop", net_programs.protocol_tcp_echo, rounds
        ),
        "asyncio": lambda: programs.runasyncio_program(
            "asyncio", net_programs.asyncio_tcp_echo, rounds
        ),
    }
    if options.serve:
        print("pid", os.getpid(), flush=True)
        servers[options.serve]()
        return
    per_round_trip = 1e6 / net_programs.CONNECTIONS / rounds
    microseconds: dict[str, list[float]] = {name: [] for name in servers}
    for _ in range(options.reps):
        for name, run in servers.items():
            microseconds[name].append(run() * per_round_trip)
    for name, values in microseconds.items():
        median = statistics.median(values)
        print(
            f"{name:16s} median {median:6.2f} us  min {min(values):6.2f} us  "
            f"({1e6 / median:8.0f} round trips/s)  {[round(v, 1) for v in values]}"
        )
    print("load average", [round(load, 1) for load in os.getloadavg()])


if __name__ == "__main__":
    main()
