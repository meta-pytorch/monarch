# EFA RDMA Engineering Guide for Monarch

> Distilled from 198 instincts (89 EFA/RDMA-specific, 52 hardware-verified) accumulated
> during the CUCo optimization campaign on AWS P5.48xlarge (H100, 4x EFA) and
> P4d.24xlarge (A100, 4x EFA). Each section below links to the source instinct IDs
> in `knowledge_base/learned_instincts/instincts.json`.

---

## 1. SRD Is Not InfiniBand — What Breaks

EFA uses the Scalable Reliable Datagram (SRD) protocol. Three InfiniBand assumptions
fail silently on SRD:

### 1.1 No message ordering

SRD delivers RDMA writes out of order. Posting writes A, B, C may deliver C, A, B.
Any protocol that chains operations assuming order-of-completion will silently corrupt
data or deadlock.

**Monarch impact**: `IbvBackend::execute_op` processes ops sequentially within a batch,
which is safe. But if future work pipelines overlapping writes to adjacent memory regions,
the lack of ordering becomes a correctness hazard.

### 1.2 CQ completion is local-only

A send CQ completion means the local NIC accepted the WQE into its send queue. It does
**not** mean the remote side received the data. `ibv_post_send` can even return success
for packets that SRD will silently drop (e.g., loopback).

> *I203 [confidence 10, ground-truth]*: `ibv_post_send` returns 0 even when SRD will
> drop the packet. The return code only confirms the NIC accepted the WQE.

**Monarch impact**: The `wait_for_completion` loop polls the send CQ, which is correct
for confirming the local side is done. But callers must not treat this as proof that the
remote buffer was written.

### 1.3 No RDMA atomics

EFA does not support `IBV_ACCESS_REMOTE_ATOMIC`. Monarch's `efa::mr_access_flags()`
correctly omits this. The `RDMAAction.fetch_add` and `compare_and_swap` methods will
fail on EFA without a software fallback.

> *I047 [confidence 11, ground-truth]*: All 7 attempts to replace barriers with
> alternative synchronization (fence, quiet, int_p, epoch) failed on EFA.

### 1.4 FI_MORE is broken

The libfabric `FI_MORE` hint (batch operations before triggering the NIC) does not work
on the `efa-direct` provider. Every attempt to use it — counter-based flush, progress-based
flush, timer-based flush — caused hangs. 15+ attempts, zero successes.

> *I010 [confidence 3, ground-truth]*: FI_MORE does NOT work on EFA efa-direct provider.
> All patches hang.

### 1.5 fi_writedata is emulated and exhaustible

EFA RDM emulates `fi_writedata` (RDMA write with immediate) as a send/recv pair.
After ~240 writes, the receiver buffer pool exhausts and operations stall. Use
`fi_write` or `fi_writemsg` instead.

> *I128 [confidence 11, ground-truth]*: fi_writedata receiver buffer pool exhausts
> after ~240 writes.

**Monarch impact**: The `WriteWithImm` operation in `post_op_efa` maps to `fi_writedata`
under the hood (via the EFA provider). The existing warning in `post_op_efa` for
`WriteWithImm` is correct — it should not be used in hot loops on EFA.

---

## 2. Same-Node Loopback

**The #1 production gotcha on EFA.**

SRD silently drops packets destined for the same node. The full QP lifecycle completes
without error (INIT → RTR → RTS), `ibv_post_send` returns success, but the CQ completion
never arrives.

### 2.1 How it manifests

```
error: broken link: failed to enqueue in MailboxClient
  channel closed with reason Some("delivery timeout")
```

Or from the Rust layer:

```
[buffer] rdma operation did not complete in time (expected wr_ids=[0])
```

Everything *looks* connected. The QP is RTS. The post returns 0. But `poll_cq` returns
0 forever because SRD dropped the packet at the NIC.

### 2.2 Current fix (PR #3391)

`IbvBackend::execute_op` detects same-actor loopback and uses `memcpy`:

```rust
if crate::efa::is_efa_device()
    && self.actor_id() == op.remote_manager.actor_id()
{
    // CPU buffers: direct memcpy
    std::ptr::copy_nonoverlapping(src, dst, size);
}
```

For CUDA device memory, the check falls through with a warning (needs `cudaMemcpy`).

### 2.3 Remaining gap: cross-process same-node

Two actors in **different** processes on the same node have different `IbvManagerActor`
instances. The actor-ID check misses this case. Known solutions:

1. Compare GIDs at QP connection time (same GID = same node)
2. Shared-memory handshake between node-local processes
3. Fall back to TCP when EFA loopback is detected at connect time

> *I204 [confidence 9, ground-truth]*: Cross-process same-node EFA loopback is unsolved
> in Monarch as of PR #3391.

> *I176 [confidence 10, ground-truth]*: Memcpy fallback pattern for EFA same-process
> loopback.

---

## 3. Memory Registration

### 3.1 CPU tensors: allocator can pull the rug

PyTorch's caching allocator may reclaim and reallocate the underlying memory between
`ibv_reg_mr` and the RDMA operation. This causes `IBV_WC_LOC_LEN_ERR` (status=1,
vendor_err=104) on large tensors (≥10 MB) under memory pressure.

The MR was registered pointing at virtual address X with size S. By the time the RDMA
write executes, the allocator freed and reused that memory. The NIC tries to DMA from
an address that no longer holds the expected data — or worse, the MR itself was
invalidated.

**Mitigations**:

- Hold a strong reference to the tensor for the entire RDMA operation lifetime
- Use `torch.Tensor.pin_memory()` for buffers involved in RDMA
- Re-register the MR immediately before the operation if the tensor might have moved
- Enable `expandable_segments` so the caching allocator uses stable large blocks

> *I202 [confidence 7, hypothesis]*: Monarch #1136 large tensor flakiness likely stems
> from MR/allocator mismatch under memory pressure.

### 3.2 GPU memory: DMA-BUF constraints

DMA-BUF registration works on EFA P5 instances (kernel 6.17+). Monarch already implements
this path in `register_mr` via `ibv_reg_dmabuf_mr`. Constraints discovered through testing:

| Constraint | Detail |
|-----------|--------|
| Separate `cudaMalloc` per NIC | Sub-ranges of one allocation → CQ err=22 (EINVAL) |
| `nvshmem_malloc` incompatible | DMA-BUF requires `cudaMalloc` memory specifically |
| `efa_nv_peermem` module required | Must be loaded; check `/proc/modules` |
| `FI_EFA_USE_DEVICE_RDMA=1` | Required env var for GPU Direct RDMA |
| libfabric ≥ 1.18 | For FI_HMEM_CUDA support |

> *I080 [confidence 3, hypothesis]*: DMA-BUF for multi-NIC GPU RDMA requires SEPARATE
> cudaMalloc per NIC.
>
> *I122 [confidence 10, ground-truth]*: cudaMalloc staging buffers bypass the
> nvshmem_malloc MR issue.
>
> *I076 [confidence 3, hypothesis]*: DMA-BUF GPU memory registration works on EFA P5
> kernel 6.17.

### 3.3 GPU L2 cache coherence after RDMA

GPU L2 cache is **not** coherent with incoming RDMA writes. After remote data arrives
via RDMA, the GPU must call `cuFlushGPUDirectRDMAWrites` before reading it. Without the
flush, the GPU may read stale L2-cached data from before the RDMA transfer.

> *I129 [confidence 11, ground-truth]*: cuFlushGPUDirectRDMAWrites required after RDMA
> data arrives, before GPU reads.

**Monarch impact**: If Monarch adds GPU tensor RDMA support, the completion path must
insert a GPU-side flush before returning the buffer to the caller.

---

## 4. Completion Queue Polling

### 4.1 Never skip CQ polls

On EFA (via libfabric), `fi_cq_read` is not just a status check — it drives the
provider's internal progress engine. Skipping polls when you think nothing is pending
prevents the provider from completing other in-flight operations.

Monarch's `wait_for_completion` (poll every 1ms) is correct. Do not "optimize" by
reducing poll frequency or skipping empty polls.

> *I020 [confidence 8, ground-truth]*: Adaptive CQ polling breaks dispatch by starving
> the progress engine.

### 4.2 CQ iteration sweet spot

For NCCL GIN on EFA, `CQ_ITER=64` is a verified local optimum. The curve is U-shaped:

| CQ_ITER | BW (GB/s) | Delta |
|---------|-----------|-------|
| 32 | 2.048 | -2.7% |
| **64** | **2.308** | **baseline** |
| 128 | 1.906 | -17.4% |

> *I186 [confidence 8, ground-truth]*: CQ_ITER=128 regresses LL by -17.4%. 64 is the
> sweet spot.

### 4.3 Adding CQ-only workers does not help

The EFA bottleneck is `fi_write` issuance (the actual RDMA post), not CQ polling. Adding
dedicated CQ polling threads gives 0% throughput improvement.

> *I057 [confidence 10, ground-truth]*: CQ-only proxy workers give 0% throughput gain.

### 4.4 fi_cntr is thread-local on EFA

EFA's hardware completion counter (`fi_cntr`) is thread-local. Calling `fi_write` from
thread A and `fi_cntr_read` from thread B returns invisible completions. If Monarch ever
uses multi-threaded RDMA submission, completions must be polled from the **same thread**
that posted the operation.

> *I056 [confidence 11, ground-truth]*: EFA fi_cntr is thread-local.

---

## 5. GPU Direct RDMA on EFA

### 5.1 It works — and it's fast

FI_HMEM_CUDA (GPU Direct RDMA via libfabric) is functional on EFA P5 instances.
Standalone benchmarks show EFA **matches or exceeds** InfiniBand:

| Scenario | EFA | InfiniBand | Ratio |
|----------|-----|------------|-------|
| 4-NIC standalone, 64 MB writes | 44 GB/s | 43 GB/s | 1.02x |
| Dispatch simulation (4096×7168 BF16) | 45.1 GB/s | 43 GB/s | 1.05x |
| Single NIC, 256 KB writes | 8.2 GB/s | — | — |

The hardware is not the bottleneck. The gap between EFA and InfiniBand in real workloads
comes from the CPU proxy architecture, not the NIC itself.

> *I116 [confidence 11, ground-truth]*: FI_HMEM_CUDA WORKS on P5 EFA.
>
> *I083 [confidence 3, hypothesis]*: 45.1 GB/s dispatch simulation exceeds IB reference.

### 5.2 Requirements checklist

For Monarch to enable GPU Direct RDMA on EFA:

```
[ ] libfabric >= 1.18 installed (current EFA installer includes 2.4.0+)
[ ] efa_nv_peermem kernel module loaded (check: grep efa_nv_peermem /proc/modules)
[ ] FI_EFA_USE_DEVICE_RDMA=1 in environment
[ ] Kernel >= 6.17 for DMA-BUF fd support
[ ] GPU memory allocated via cudaMalloc (not nvshmem_malloc or managed memory)
[ ] Separate cudaMalloc per NIC for multi-NIC configs
[ ] cuFlushGPUDirectRDMAWrites after RDMA receive, before GPU read
```

### 5.3 The peermem symbol version issue (Monarch #2672)

`efa_nv_peermem` can fail to load after NVIDIA driver updates because DKMS rebuilds the
NVIDIA modules but not `efa_nv_peermem`. Fix: rebuild `efa_nv_peermem` after any NVIDIA
driver change, or pin both driver and peermem to compatible versions in your container image.

> *I071 [confidence 11, ground-truth]*: FI_HMEM_CUDA registration requires
> efa_nv_peermem module loaded.

---

## 6. The Proxy Architecture Gap

### 6.1 Why EFA needs a CPU proxy

InfiniBand supports IBGDA (InfiniBand GPU Direct Async) — the GPU posts RDMA operations
directly to the NIC via PCIe MMIO writes to hardware doorbells. EFA does not support IBGDA.

On EFA, a CPU thread must mediate every RDMA operation:
1. Read data source address from GPU (or CPU) memory
2. Post `ibv_post_send` / `fi_writemsg` to the EFA NIC
3. Poll CQ for completion
4. Signal the requestor that the operation finished

This CPU round-trip is where the EFA-vs-InfiniBand latency gap lives. The NIC itself is
comparably fast (45 GB/s EFA vs 43 GB/s IB at the hardware level).

### 6.2 EFA uses PROXY_MINIMAL

On EFA, NVSHMEM's proxy mode is `PROXY_MINIMAL`. There are no proxy channels. All code
that optimizes multi-proxy channel distribution is dead code on EFA. Enhanced proxy
(adding `progress_channels` to the minimal loop) has zero effect.

> *I118 [confidence 11, ground-truth]*: EFA uses PROXY_MINIMAL.
>
> *I119 [confidence 10, ground-truth]*: Enhanced proxy has zero effect on throughput.

### 6.3 The proven high-performance pattern (pplx)

The fastest known EFA RDMA implementation (pplx-garden, 63 GB/s) uses:

1. **GPU kernel** writes tokens to a registered send buffer
2. **GPU signals CPU** via GDRCopy `st_mmio_b8` (MMIO write, ~1 μs)
3. **CPU worker** calls `fi_writemsg` from the registered staging memory
4. **CPU polls CQ** for completion
5. **CPU signals GPU** via `nvshmem_signal` that the transfer finished

The key insight: the CPU reads a ring buffer **after** the kernel completes, not during.
Concurrent GPU-CPU access to the same memory causes hangs because workers cannot access
GPU memory while CUDA kernels are executing on the same device.

> *I073 [confidence 10, ground-truth]*: pplx achieves 63 GB/s with this post-dispatch
> architecture.
>
> *I112 [confidence 3, hypothesis]*: Workers CANNOT access GPU memory while CUDA kernels
> are executing on the same device.
>
> *I113 [confidence 3, hypothesis]*: Post-dispatch architecture WORKS — kernel writes to
> device ring during execution, CPU reads ring AFTER cudaStreamSynchronize.

### 6.4 Batched writes dominate throughput

RDMA chunk size is the single most important tuning variable for normal-mode throughput.
Transfer size matters more than batch count:

| Chunk size | BW (GB/s) | Speedup |
|-----------|-----------|---------|
| 4 KB | 3.7 | 1.0x |
| 116 KB | 31.4 | 8.5x |
| 256 KB | 40+ | 10.8x |

> *I133 [confidence 10, ground-truth]*: RDMA chunk size is the dominant normal-mode
> tuning variable (8.5x range).
>
> *I130 [confidence 10, ground-truth]*: Batched 256KB writes: 16MB → 40 GB/s,
> 64MB → 44 GB/s. At 64MB, EFA exceeds InfiniBand.

**Monarch impact**: The current `MAX_RDMA_MSG_SIZE = 1 GiB` with chunking in `put()`/`get()`
is fine for correctness. But for performance-sensitive paths, experimenting with chunk
sizes in the 256 KB–16 MB range would likely improve throughput.

---

## 7. EFA Configuration Reference

### 7.1 IbverbsConfig defaults for EFA

Monarch applies these via `efa::apply_efa_defaults()`:

| Parameter | EFA Value | Mellanox Default | Why |
|-----------|----------|-----------------|-----|
| `gid_index` | 0 | 3 | EFA uses GID index 0 |
| `max_send_sge` | 1 | 30 | EFA limit |
| `max_recv_sge` | 1 | 30 | EFA limit |
| `max_dest_rd_atomic` | 0 | 16 | No RDMA atomics on EFA |
| `max_rd_atomic` | 0 | 16 | No RDMA atomics on EFA |

### 7.2 RDMA buffer sizing constraints

| Parameter | Safe Range | Failure Mode |
|-----------|-----------|--------------|
| `rdma_chunk_size` | ≤ `buffer_size / 2` | Config assertion crash |
| `rdma_buffer_size` | 512 optimal (with 1 GB backing) | 1024+ regresses |
| `num_rdma_bytes` | ≤ 1 GB | 2 GB → OOM on H100 80 GB |
| `OFI_NCCL_EAGER_MAX_SIZE` | Default only | EFA provider rejects 32768 |

> *I017, I018, I019 [confidence 10, ground-truth]*: Buffer sizing constraints verified
> on P5 hardware.
>
> *I151 [confidence 11, ground-truth]*: EFA rejects eager max 32768.

### 7.3 EFA provider selection

Two providers exist: `efa` and `efa-direct`. On P5:

- `efa` (aws-patches): 10% **better** throughput for normal dispatch
- `efa-direct` (aws-patches-2): Higher raw bandwidth but worse dispatch latency

> *I054 [confidence 9, ground-truth]*: efa-direct gives 10% WORSE throughput than efa
> for normal dispatch.

### 7.4 P5.48xlarge NUMA topology

```
NUMA 0: CPU cores 0-47, 16 EFA NICs (rdmap0s*-rdmap15s*)
NUMA 1: CPU cores 48-95, 16 EFA NICs (rdmap16s*-rdmap31s*)
```

Pin RDMA proxy threads to the same NUMA node as the NIC they drive. Cross-NUMA NIC
access adds measurable latency.

> *I132 [confidence 10, ground-truth]*: P5.48xlarge has 2 NUMA nodes, not 4.

---

## 8. What This Means for Monarch

### 8.1 Current EFA support status

EFA ibverbs support merged in PR #2638 (Feb 2026). Key components:

- **Auto-detection**: `efadv_query_device()` → `is_efa_device()` cached in `OnceLock`
- **QP connect**: `rdmaxcel_efa_connect()` handles INIT → RTR → RTS + address handle
- **Post ops**: `rdmaxcel_qp_post_op()` for write/read/recv
- **Loopback fix**: PR #3391 adds memcpy fallback for same-process loopback

> *I178 [confidence 10, ground-truth]*

### 8.2 CUDA decoupling is already mostly done

Three coupling points exist, all of which gracefully degrade without CUDA:

| Coupling Point | What It Does | Without CUDA |
|---------------|-------------|-------------|
| `cuPointerGetAttribute` | Detect CUDA vs CPU memory | Returns "not CUDA" → CPU path used |
| `pytorch_segment_scanner` | `torch.cuda.memory._snapshot()` | Returns 0 segments → skipped |
| `send_wqe` / `db_ring` CUDA kernels | mlx5dv GPU-initiated RDMA | Not used on EFA or standard ibverbs |

The CPU + EFA path works today with zero CUDA dependency.

> *I179 [confidence 9, ground-truth]*

### 8.3 Pluggable backend architecture

The `RdmaBackend` trait with `IbvBackend` (ibverbs NIC) and `TcpBackend` (TCP fallback)
already supports pluggable transports. Adding a new backend (AMD Broadcom, UCX, etc.)
requires implementing the trait — no core code changes needed.

> *I180 [confidence 9, ground-truth]*

---

## 9. Actionable Recommendations for Open Issues

| Issue | Problem | Recommendation | Key Instincts |
|-------|---------|---------------|---------------|
| **#3376** | `read_into()` hangs same node | **Fixed** in PR #3391 (memcpy fallback) | I174, I176, I203 |
| **#1136** | Flaky large tensors (≥10 MB) | Investigate MR lifetime vs allocator; pin memory during RDMA | I202 |
| **#1215** | GPU-GPU RDMA support | DMA-BUF path exists; test on EFA with peermem + per-NIC cudaMalloc | I071, I076, I080, I116, I129 |
| **#1658** | Does Monarch work on EFA? | **Yes** since PR #2638; answer and close | I178 |
| **#2296** | Decouple from CUDA | CPU+EFA path is already CUDA-free; document the 3 coupling points | I179 |
| **#3239** | AMD pluggable NICs | `RdmaBackend` trait already supports this; implement new backend | I180 |

### Follow-up work

1. **Cross-process same-node detection**: Compare GIDs at connect time or add a
   shared-memory node-identity handshake. (Unblocks multi-proc EFA on same node.)

2. **GPU RDMA on EFA**: Enable and test the DMA-BUF path with `efa_nv_peermem`.
   Add `cuFlushGPUDirectRDMAWrites` to the completion path. Per-NIC `cudaMalloc`.

3. **MR pinning for large tensors**: Hold strong tensor reference or use
   `pin_memory()` for the RDMA operation duration. (Fixes #1136.)

4. **Chunk size tuning**: Experiment with 256 KB–16 MB chunks in `put()`/`get()`
   for throughput-sensitive workloads. Current 1 GiB max is correct but not optimized.

5. **Document EFA limitations**: No atomics, no ordering, no loopback, `WriteWithImm`
   exhausts receiver pool. Add to contributor onboarding docs.

---

## Appendix: Instinct Index

89 EFA/RDMA instincts organized by topic. `GT` = verified on hardware.
Full details in `knowledge_base/learned_instincts/instincts.json`.

**SRD protocol**:
I010 (GT), I020 (GT), I047 (GT), I056 (GT), I075, I120 (GT), I128 (GT), I203 (GT)

**Same-node loopback**:
I174 (GT), I176 (GT), I204 (GT)

**Memory registration**:
I076, I080, I087, I088, I106 (GT), I121 (GT), I122 (GT), I202

**Completion queue**:
I020 (GT), I046 (GT), I057 (GT), I153 (GT), I186 (GT)

**GPU Direct RDMA**:
I071 (GT), I077, I081, I083, I089, I116 (GT), I129 (GT)

**Proxy architecture**:
I073 (GT), I102, I110 (GT), I112, I113, I118 (GT), I119 (GT), I126 (GT)

**Configuration**:
I017 (GT), I018 (GT), I019 (GT), I054 (GT), I130 (GT), I132 (GT), I133 (GT),
I151 (GT), I186 (GT)

**Monarch-specific**:
I178 (GT), I179 (GT), I180 (GT), I202, I203 (GT), I204 (GT)

---

*Generated from CUCo knowledge base (198 instincts, 89 EFA-relevant) on 2026-04-10.
Source: `knowledge_base/learned_instincts/instincts.json`*
