"""Every pyperf benchmark of the suite, in report order: the task rows of ``programs`` (M0),
then channels, sync primitives, scopes and cancellation, timers, the worker pool and the TCP
echo."""

from __future__ import annotations

import channel_programs
import net_programs
import programs
import scope_programs
import sync_programs
import timer_programs
import workload_programs
from programs import Benchmark

BENCHMARKS: list[Benchmark] = [
    *programs.BENCHMARKS,
    *channel_programs.BENCHMARKS,
    *sync_programs.BENCHMARKS,
    *scope_programs.BENCHMARKS,
    *timer_programs.BENCHMARKS,
    *workload_programs.BENCHMARKS,
    *net_programs.BENCHMARKS,
]
