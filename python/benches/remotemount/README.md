# remotemount benchmark

Benchmarks remotemount transfer throughput: cold transfer, no-change skip, and
incremental re-transfer. Compares `actor` vs `rust_tls` transfer modes and
sweeps parallel TLS stream counts.

## Setup

`setup_bench_env.sh` is a thin wrapper around `examples/remotemount/setup_conda_env.sh`.
It creates matching client (x86) and worker (target arch) conda envs with
monarch built from the current source. The four slow operations (2 fbpkg
fetches + 2 buck2 builds) run in parallel.

```bash
bash python/benches/remotemount/setup_bench_env.sh [target] [DEST_DIR]
```

- `target`: `gb200`, `gb300` (aarch64) or `grandteton`, `h100` (x86). Default: `gb200`
- `DEST_DIR`: where to create the envs. Default: `$HOME/monarch_conda_envs`

## Running

### Local (single host, no MAST)

```bash
buck run fbcode//monarch/python/benches/remotemount:bench_tls_packing
```

### MAST (multi-host, GB200)

```bash
CONDA_PREFIX=~/monarch_conda_envs/worker/conda \
~/monarch_conda_envs/client/conda/bin/python3.12 \
  python/benches/remotemount/bench_tls_packing.py \
  --backend=mast --host_type=gb200 --num_hosts=2 --data_size_mb=1024
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--backend` | `local` | `local` or `mast` |
| `--num_hosts` | `1` | Number of worker hosts |
| `--gpus_per_host` | `1` | GPUs per host |
| `--host_type` | `gb300` | MAST host type (`gb200`, `gb300`) |
| `--data_size_mb` | `50` | Size of test payload in MB |
| `--locality_constraints` | `""` | Semicolon-separated locality constraints |

## Results

### Benchmark setup

- 2 GB200 hosts, 1024 MB payload, 8 parallel TLS streams
- Client-to-worker transfer uses TCP (client has no ibverbs)
- Worker-to-worker fan-out uses native ibverbs RDMA

### Transfer mode comparison (cold transfer)

| Mode | Streams | Cold | Throughput |
|------|---------|------|------------|
| actor | 1 | 8.06s | 127 MB/s |
| rust_tls | 8 | 1.35s | 757 MB/s |

### rust_tls stream count sweep (cold transfer)

| Streams | Cold | Throughput |
|---------|------|------------|
| 1 | 1.32s | 778 MB/s |
| 2 | 1.35s | 757 MB/s |
| 4 | 1.47s | 697 MB/s |
| 8 | 1.29s | 792 MB/s |
| 16 | 1.30s | 788 MB/s |

### Incremental cycle (cold / skip / re-transfer)

| Mode | Cold | Skip | Retransfer | Throughput |
|------|------|------|------------|------------|
| actor | 1.31s | 0.42s | 7.21s | 784 MB/s |
| rust_tls | 4.62s | 0.42s | 3.54s | 222 MB/s |

Key observations:

- **rust_tls is 6x faster** than actor for cold transfers (757 vs 127 MB/s)
- **Stream count has minimal impact** — a single TLS stream already saturates
  the link at ~780 MB/s
- **No-change skip** is 0.42s for both modes (hash match, no network transfer)
- **rust_tls re-transfer** is 2x faster than actor (3.54s vs 7.21s)

## How it works

The benchmark creates a temp directory with test files (`data.bin` + small
Python/JSON files), then runs three sections:

1. **Transfer mode comparison** — cold transfer with `actor` vs `rust_tls`
2. **Stream count sweep** — `rust_tls` with 1, 2, 4, 8, 16 parallel streams
3. **Incremental cycle** — cold transfer, no-change skip (hash match), and
   modified re-transfer for each mode
