# minimonarch round-trip benchmarks

Each cell is the median over all samples for that message size.

## Topologies

- **(a) in-process, common inproc parent** — s and r live in one process/context, both inproc children of a common parent p.
- **(b) two processes, common unix parent** — s and r are in separate processes, each an inproc-free child of a common parent p (third process) over unix://.
- **(c) process/host manager** — s -inproc-> p0 -unix-> h <-unix- p1 <-inproc- r. h is a host manager; p0/p1 are process managers.

## Round-trip latency (us)

_median us per size_

| size | inproc | unix | manager |
| --- | --- | --- | --- |
| 64 B | 37 | 228 | 203 |
| 1 KiB | 36 | 205 | 233 |
| 16 KiB | 32.2 | 227 | 204 |
| 256 KiB | 31.5 | 498 | 502 |
| 1 MiB | 35.4 | 1.33e+03 | 1.3e+03 |
| 16 MiB | 32.2 | 1.6e+04 | 1.64e+04 |

## Round-trip throughput (GB/s)

_median GB/s per size_

| size | inproc | unix | manager |
| --- | --- | --- | --- |
| 64 B | 0.00643 | 0.00204 | 0.00137 |
| 1 KiB | 0.0963 | 0.0308 | 0.0288 |
| 16 KiB | 2.3 | 0.33 | 0.318 |
| 256 KiB | 36.8 | 0.749 | 0.733 |
| 1 MiB | 141 | 0.981 | 0.871 |
| 16 MiB | 1.01e+03 | 0.56 | 0.594 |

## Round-trip latency w/ subscribe+unsubscribe (us)

_median us per size_

| size | inproc | unix | manager |
| --- | --- | --- | --- |
| 64 B | 43 | 226 | 215 |
| 1 KiB | 41.1 | 200 | 249 |
| 16 KiB | 36.1 | 208 | 213 |
| 256 KiB | 34.1 | 510 | 524 |
| 1 MiB | 45.1 | 1.28e+03 | 1.3e+03 |
| 16 MiB | 35.3 | 1.68e+04 | 1.51e+04 |

## Round-trip throughput w/ subscribe+unsubscribe (GB/s)

_median GB/s per size_

| size | inproc | unix | manager |
| --- | --- | --- | --- |
| 64 B | 0.00401 | 0.00144 | 0.00102 |
| 1 KiB | 0.0779 | 0.0213 | 0.0159 |
| 16 KiB | 1.32 | 0.266 | 0.255 |
| 256 KiB | 14.5 | 0.83 | 0.782 |
| 1 MiB | 78.6 | 0.918 | 0.906 |
| 16 MiB | 634 | 0.52 | 0.577 |
