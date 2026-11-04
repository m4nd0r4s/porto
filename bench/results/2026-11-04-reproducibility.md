# Run-to-run spread: 2026-11-04-0164596a5 vs 2026-11-04-fdb28bb69

Difference of the second run's mean from the first's, per benchmark.

## Python 3.14

| Benchmark | role | first | second | difference |
|---|---|---:|---:|---:|
| `checkpoint/porto` | porto | 67.4 ns | 67.7 ns | +0.3% |
| `checkpoint/asyncio` | baseline | 14.4 µs | 14.4 µs | -0.5% |
| `checkpoint/uvloop` | baseline | 12.9 µs | 13 µs | +1.2% |
| `checkpoint/asyncio-nopoll` | informative | 577 ns | 581 ns | +0.7% |
| `checkpoint/uvloop-readyfd` | informative | 547 ns | 553 ns | +1.1% |
| `switch/porto` | porto | 66.4 ns | 66.7 ns | +0.4% |
| `switch/asyncio` | baseline | 7.19 µs | 7.42 µs | +3.3% |
| `switch/uvloop` | baseline | 6.74 µs | 6.88 µs | +2.1% |
| `switch/asyncio-nopoll` | informative | 493 ns | 494 ns | +0.1% |
| `switch/uvloop-readyfd` | informative | 396 ns | 398 ns | +0.6% |
| `spawn_one/porto` | porto | 710 ns | 714 ns | +0.6% |
| `spawn_one/asyncio` | baseline | 28.8 µs | 29.4 µs | +1.8% |
| `spawn_one/uvloop` | baseline | 26.4 µs | 25.9 µs | -2.0% |
| `spawn_one/asyncio-nopoll` | informative | 1.26 µs | 1.28 µs | +1.3% |
| `spawn_one/uvloop-readyfd` | informative | 1.19 µs | 1.2 µs | +0.5% |
| `spawn_one/asyncio-taskgroup` | informative | 42.6 µs | 42.4 µs | -0.6% |
| `spawn_one/uvloop-taskgroup` | informative | 40.2 µs | 40.7 µs | +1.2% |
| `spawn_one/asyncio-nopoll-taskgroup` | informative | 2.5 µs | 2.5 µs | +0.3% |
| `spawn_one/asyncio-eager` | informative | 327 ns | 327 ns | -0.0% |
| `spawn_one/uvloop-eager` | informative | 26.9 µs | 26.2 µs | -2.6% |
| `spawn_join_100k/porto` | porto | 41.4 ms | 41.3 ms | -0.1% |
| `spawn_join_100k/asyncio-taskgroup` | baseline | 169 ms | 170 ms | +0.5% |
| `spawn_join_100k/uvloop-taskgroup` | baseline | 131 ms | 131 ms | +0.2% |
| `spawn_join_100k/asyncio-nopoll-taskgroup` | informative | 169 ms | 169 ms | -0.0% |
| `spawn_join_100k/asyncio-gather` | baseline | 171 ms | 172 ms | +0.2% |
| `spawn_join_100k/uvloop-gather` | baseline | 134 ms | 134 ms | +0.1% |
| `spawn_join_100k/asyncio-nopoll-gather` | informative | 172 ms | 171 ms | -0.3% |
| `spawn_join_100k/asyncio-eager-taskgroup` | informative | 130 ms | 130 ms | +0.3% |
| `spawn_join_100k/uvloop-eager-taskgroup` | informative | 139 ms | 139 ms | +0.4% |
| `ping_pong/porto` | porto | 162 ns | 163 ns | +0.3% |
| `ping_pong/asyncio` | baseline | 14.8 µs | 14 µs | -5.2% |
| `ping_pong/uvloop` | baseline | 13.6 µs | 13.9 µs | +1.7% |
| `ping_pong/asyncio-nopoll` | informative | 1.02 µs | 1.02 µs | +0.0% |
| `ping_pong/uvloop-readyfd` | informative | 834 ns | 842 ns | +1.0% |

- baseline: largest difference 5.2%, median 1.7%
- informative: largest difference 2.6%, median 0.5%
- porto: largest difference 0.6%, median 0.3%
- Gate speedups vs uvloop: spawn+join 100k trivial tasks: 3.2× then 3.2×; task switch (two tasks yielding): 101.5× then 103.1×; task switch (yield ping-pong, the 53 §2 row): 83.9× then 85.1×

## Python 3.11

| Benchmark | role | first | second | difference |
|---|---|---:|---:|---:|
| `checkpoint/porto` | porto | 59.5 ns | 59.3 ns | -0.3% |
| `checkpoint/asyncio` | baseline | 14.4 µs | 14.3 µs | -0.5% |
| `checkpoint/uvloop` | baseline | 12.7 µs | 13 µs | +1.8% |
| `checkpoint/asyncio-nopoll` | informative | 716 ns | 713 ns | -0.4% |
| `checkpoint/uvloop-readyfd` | informative | 596 ns | 603 ns | +1.1% |
| `switch/porto` | porto | 58 ns | 58.2 ns | +0.4% |
| `switch/asyncio` | baseline | 7.44 µs | 7.57 µs | +1.8% |
| `switch/uvloop` | baseline | 7.04 µs | 7.09 µs | +0.7% |
| `switch/asyncio-nopoll` | informative | 593 ns | 594 ns | +0.3% |
| `switch/uvloop-readyfd` | informative | 428 ns | 430 ns | +0.6% |
| `spawn_one/porto` | porto | 605 ns | 609 ns | +0.7% |
| `spawn_one/asyncio` | baseline | 29.1 µs | 28.9 µs | -0.8% |
| `spawn_one/uvloop` | baseline | 27.1 µs | 27.1 µs | -0.1% |
| `spawn_one/asyncio-nopoll` | informative | 1.62 µs | 1.63 µs | +0.5% |
| `spawn_one/uvloop-readyfd` | informative | 1.3 µs | 1.31 µs | +0.5% |
| `spawn_one/asyncio-taskgroup` | informative | 42.5 µs | 44 µs | +3.4% |
| `spawn_one/uvloop-taskgroup` | informative | 39.7 µs | 41.9 µs | +5.5% |
| `spawn_one/asyncio-nopoll-taskgroup` | informative | 2.93 µs | 2.95 µs | +0.9% |
| `spawn_join_100k/porto` | porto | 50.7 ms | 51.3 ms | +1.3% |
| `spawn_join_100k/asyncio-taskgroup` | baseline | 231 ms | 231 ms | +0.1% |
| `spawn_join_100k/uvloop-taskgroup` | baseline | 180 ms | 180 ms | +0.2% |
| `spawn_join_100k/asyncio-nopoll-taskgroup` | informative | 232 ms | 231 ms | -0.2% |
| `spawn_join_100k/asyncio-gather` | baseline | 252 ms | 252 ms | -0.0% |
| `spawn_join_100k/uvloop-gather` | baseline | 200 ms | 200 ms | -0.0% |
| `spawn_join_100k/asyncio-nopoll-gather` | informative | 255 ms | 254 ms | -0.4% |
| `ping_pong/porto` | porto | 154 ns | 154 ns | -0.3% |
| `ping_pong/asyncio` | baseline | 16.5 µs | 15.7 µs | -5.0% |
| `ping_pong/uvloop` | baseline | 14.3 µs | 14 µs | -2.2% |
| `ping_pong/asyncio-nopoll` | informative | 1.24 µs | 1.26 µs | +0.9% |
| `ping_pong/uvloop-readyfd` | informative | 907 ns | 911 ns | +0.4% |

- baseline: largest difference 5.0%, median 0.7%
- informative: largest difference 5.5%, median 0.5%
- porto: largest difference 1.3%, median 0.4%
- Gate speedups vs uvloop: spawn+join 100k trivial tasks: 3.5× then 3.5×; task switch (two tasks yielding): 121.3× then 121.8×; task switch (yield ping-pong, the 53 §2 row): 92.8× then 91.1×

## Python 3.14t

| Benchmark | role | first | second | difference |
|---|---|---:|---:|---:|
| `checkpoint/porto` | porto | 74.3 ns | 74.4 ns | +0.2% |
| `checkpoint/asyncio` | baseline | 14.3 µs | 14.8 µs | +3.6% |
| `checkpoint/uvloop` | baseline | 13.2 µs | 13.1 µs | -0.8% |
| `checkpoint/asyncio-nopoll` | informative | 597 ns | 598 ns | +0.3% |
| `checkpoint/uvloop-readyfd` | informative | 558 ns | 561 ns | +0.7% |
| `switch/porto` | porto | 74.2 ns | 74.5 ns | +0.3% |
| `switch/asyncio` | baseline | 7.29 µs | 7.2 µs | -1.2% |
| `switch/uvloop` | baseline | 6.75 µs | 6.82 µs | +0.9% |
| `switch/asyncio-nopoll` | informative | 501 ns | 502 ns | +0.1% |
| `switch/uvloop-readyfd` | informative | 402 ns | 403 ns | +0.2% |
| `spawn_one/porto` | porto | 682 ns | 687 ns | +0.7% |
| `spawn_one/asyncio` | baseline | 29 µs | 28.3 µs | -2.4% |
| `spawn_one/uvloop` | baseline | 25.5 µs | 25.4 µs | -0.4% |
| `spawn_one/asyncio-nopoll` | informative | 1.32 µs | 1.33 µs | +0.4% |
| `spawn_one/uvloop-readyfd` | informative | 1.2 µs | 1.19 µs | -0.8% |
| `spawn_one/asyncio-taskgroup` | informative | 42.8 µs | 42.7 µs | -0.2% |
| `spawn_one/uvloop-taskgroup` | informative | 40.4 µs | 39.8 µs | -1.6% |
| `spawn_one/asyncio-nopoll-taskgroup` | informative | 2.65 µs | 2.66 µs | +0.3% |
| `spawn_one/asyncio-eager` | informative | 357 ns | 357 ns | -0.1% |
| `spawn_one/uvloop-eager` | informative | 26.8 µs | 27.3 µs | +2.0% |
| `spawn_join_100k/porto` | porto | 31.4 ms | 31.5 ms | +0.3% |
| `spawn_join_100k/asyncio-taskgroup` | baseline | 131 ms | 130 ms | -0.2% |
| `spawn_join_100k/uvloop-taskgroup` | baseline | 94.7 ms | 95 ms | +0.3% |
| `spawn_join_100k/asyncio-nopoll-taskgroup` | informative | 131 ms | 131 ms | -0.0% |
| `spawn_join_100k/asyncio-gather` | baseline | 142 ms | 141 ms | -0.0% |
| `spawn_join_100k/uvloop-gather` | baseline | 103 ms | 103 ms | -0.1% |
| `spawn_join_100k/asyncio-nopoll-gather` | informative | 142 ms | 142 ms | +0.0% |
| `spawn_join_100k/asyncio-eager-taskgroup` | informative | 104 ms | 104 ms | +0.3% |
| `spawn_join_100k/uvloop-eager-taskgroup` | informative | 100 ms | 105 ms | +4.5% |
| `ping_pong/porto` | porto | 181 ns | 182 ns | +0.2% |
| `ping_pong/asyncio` | baseline | 14.4 µs | 14.4 µs | -0.5% |
| `ping_pong/uvloop` | baseline | 14.1 µs | 14.1 µs | +0.4% |
| `ping_pong/asyncio-nopoll` | informative | 1.05 µs | 1.05 µs | -0.1% |
| `ping_pong/uvloop-readyfd` | informative | 849 ns | 853 ns | +0.4% |

- baseline: largest difference 3.6%, median 0.5%
- informative: largest difference 4.5%, median 0.3%
- porto: largest difference 0.7%, median 0.3%
- Gate speedups vs uvloop: spawn+join 100k trivial tasks: 3.0× then 3.0×; task switch (two tasks yielding): 91.0× then 91.5×; task switch (yield ping-pong, the 53 §2 row): 77.5× then 77.7×
