# EFA RDMA notes

Field notes for running `monarch_rdma` over AWS Elastic Fabric Adapter. EFA
carries RDMA over the Scalable Reliable Datagram (SRD) protocol rather than
InfiniBand's reliable connection, and several assumptions that hold on Mellanox
hardware fail on SRD — some of them silently, which is what makes them worth
writing down.

Everything below was observed on 2x p5.48xlarge (H100 x8, EFA x32 per node) with
one p4d.24xlarge (A100 x8, EFA x4) as a capability-delta reference, on Ubuntu
24.04 with the EFA installer 1.43+, libfabric 2.x, and `efa_nv_peermem` loaded.
Where a claim is about hardware rather than about this code, it was verified by
reading the RDMA hardware counters under
`/sys/class/infiniband/rdmap*/ports/1/hw_counters/` before and after the run,
because a counter delta is ground truth and an `ibv_post_send` return code is
not.

## How to reproduce the environment

`fi_pingpong` ships with libfabric and exercises the whole provider stack
without application code, which makes it the fastest way to tell a Monarch bug
from a fabric bug:

```
FI_PROVIDER=efa FI_LOG_LEVEL=debug FI_EFA_USE_DEVICE_RDMA=1 fi_pingpong -e rdm
```

Useful when narrowing something down:

- `efa-info` and `show_gids` to confirm the device, GID index 0, and link state.
  A wrong GID index looks exactly like a fabric failure.
- `FI_LOG_LEVEL=trace` is verbose but is the ground truth for what the provider
  decided to do.
- The `tx_pkts` / `rx_pkts` hardware counters. If `tx_pkts` climbs and the peer's
  `rx_pkts` does not, the packets are being dropped below libfabric and no amount
  of application-level debugging will show it.

## What SRD does not do

### No message ordering

SRD delivers RDMA writes out of order: posting A, B, C may deliver C, A, B. Any
protocol that infers "B arrived" from "C arrived" is unsound on EFA.

This is safe in `monarch_rdma` today because a `QueuePairActor` replies per op
and holds an op's reply until every work request for it has completed
(`queue_pair.rs`), so no caller observes a partial transfer. It is a live hazard
for any future change that pipelines overlapping writes into adjacent regions and
treats one completion as evidence about another.

### A send completion is local-only

A send-CQ completion means the local NIC accepted the work request. It does not
mean the remote side received the data, and `ibv_post_send` returns 0 even for
packets SRD will drop. Polling the send CQ is the right way to know the local
side is finished with a buffer; it is not proof the remote buffer was written.

### No RDMA atomics

EFA does not support `IBV_ACCESS_REMOTE_ATOMIC`. `efa::mr_access_flags`
(`efa.rs`) omits it, and `EfaDevice::apply_config_defaults` (`efa_device.rs`)
zeroes `max_rd_atomic` and `max_dest_rd_atomic`. Fetch-add and compare-and-swap
need a software fallback on this fabric; there is no way to get them from the
NIC.

The absence of atomics is an EFA property rather than a p4d quirk — p4d reports
`max_qp_rd_atom = 0`, and p5 zeroes the same config knobs.

### `FI_MORE` does not work on `efa-direct`

The libfabric `FI_MORE` hint (batch work requests, then trigger the NIC once)
hangs on the `efa-direct` provider — counter-based, progress-based, and
timer-based flush strategies all hang. This does not affect the ibverbs backend,
which posts to the NIC directly, but it rules out an obvious batching strategy
for anything built on libfabric.

### Write-with-immediate is emulated and exhaustible

EFA RDM emulates `fi_writedata` as a send/recv pair, so the receiver's buffer
pool drains: after roughly 240 writes, operations stall. Plain writes do not have
this problem.

`EfaQueuePair` cannot express write-with-immediate at all — it posts only
`IbvOperation::Write` and `IbvOperation::Read` through `WrSession`
(`efa_queue_pair.rs`) — so the current EFA path cannot hit this. The hazard
applies to `IbvOperation::WriteWithImm`, which survives for the legacy
GPU-initiated path in `queue_pair/legacy.rs`; do not wire it to EFA without
addressing the receiver pool.

## Same-node loopback is silently dropped

This is the EFA gotcha most likely to cost someone a day.

SRD drops packets addressed to the node that sent them. The whole queue-pair
lifecycle succeeds — INIT to RTR to RTS, `ibv_post_send` returns 0 — and then the
completion never arrives, because the payload was dropped at the NIC. It
surfaces as a timeout far from the cause:

```
[buffer] rdma operation did not complete in time (expected wr_ids=[0])
```

or, when the timeout propagates through the mailbox:

```
error: broken link: failed to enqueue in MailboxClient
  channel closed with reason Some("delivery timeout")
```

Note that this is not the same thing as the `is_loopback` flag in
`manager_actor.rs`, which only tells `QueuePairActor::init` to skip the
`CreatePeerQueuePair` round trip and connect a queue pair to its own endpoint.
That is a connection-setup shortcut; the data path underneath it still posts a
real work request, and on EFA that work request is dropped.

The fix in this crate is in the `SubmitOps` handler in `manager_actor.rs`: when
the EFA manager is asked for a transfer whose peer is itself and whose memory on
both ends is host memory, it copies directly instead of posting a work request.
Three conditions gate it, and each one matters:

- **EFA only**, decided per backend via `I::backend_name()`, not by asking
  whether the process has an EFA device anywhere. Both `IbvManagerActor<MlxDevice>`
  and `IbvManagerActor<EfaDevice>` can be spawned on a host with both NICs, and a
  process-wide predicate would divert working Mellanox loopback RDMA into a copy.
- **Same manager**, by `ActorRef::actor_addr()` equality, matching what
  `ensure_qp_actor` already does.
- **Host memory on both ends**, decided from `MemoryLocation` rather than by
  probing the address. The remote side's `addr` is an offset into a zero-based MR
  address space, not a pointer: for a dmabuf-registered GPU region it is
  frequently literally `0`, so a pointer probe would classify device memory as
  host memory and copy to a near-null address. `IbvRemoteMemoryRegionView`
  carries `location` on the wire for exactly this reason.

Two actors in *different processes* on the same node are still exposed. They have
different manager actors, so the address comparison does not fire, and the
transfer between them hits the silent drop. Options, none implemented: compare
GIDs at connect time (same GID means same node), add a shared-memory node-identity
handshake, or route node-local peers over TCP.

## p4d cannot do RDMA at all

EFA generations differ in whether they can carry RDMA. p5 and p5en advertise
`EFADV_DEVICE_ATTR_CAPS_RDMA_READ` and `..._RDMA_WRITE`; p4d advertises neither
and offers only send/recv. `EfaQueuePair::create` requires both and rejects the
device, so on p4d every transfer would fail at queue-pair creation — that is, on
the first op, deep inside `ensure_qp_actor`, as a per-op error rather than as a
transport decision.

`EfaDevice::is_instance` therefore declines a device that cannot serve RDMA, via
`efa::supports_rdma`. An unclaimed device leaves
`IbvBackend::<EfaDevice>::available()` false, so `RdmaBackends::spawn_available`
falls through to the next registered backend. One consequence worth knowing:
`is_instance` is also what makes a device *appear* as EFA, so a p4d NIC becomes
invisible to `list_all_devices()`. The warning logged at that point is the only
place it is visible.

## Memory registration

### Host memory

The failure worth recognizing is `IBV_WC_LOC_LEN_ERR` (status 1, vendor_err 104),
which means the receiving region is smaller than the incoming message. It reads
like a lifetime bug and is not one: size the MR for the largest message you
intend to send.

For pages that must not move underneath a registration, `pin_memory()` on the
tensor. With the PyTorch caching allocator, `expandable_segments` tends to produce
stable large blocks that are better registration candidates.

### GPU memory

dmabuf registration works on EFA (kernel 6.17+) and is the path
`register_host_or_dmabuf_mr` takes for `MemoryLocation::Gpu` in `domain.rs`, via
`ibv_reg_dmabuf_mr`. Constraints found the hard way:

| Constraint | Detail |
|---|---|
| One `cudaMalloc` per NIC | Sub-ranges of a single allocation fail with CQ error 22 (EINVAL) |
| `cudaMalloc` memory specifically | `nvshmem_malloc` memory cannot be dmabuf-registered; stage through `cudaMalloc` |
| `efa_nv_peermem` loaded | Check `/proc/modules` |
| `FI_EFA_USE_DEVICE_RDMA=1` | Required for GPU-direct RDMA |
| libfabric >= 1.18 | For `FI_HMEM_CUDA` |

`efa_nv_peermem` can stop loading after an NVIDIA driver update, because DKMS
rebuilds the NVIDIA modules and not this one. Rebuild it after any driver change,
or pin both in the image.

### GPU L2 is not coherent with incoming RDMA

Only CPU-initiated CUDA APIs order GPU-direct memory operations as the GPU
observes them, so a kernel that reads a destination buffer immediately after a
remote write may see stale L2-cached data; `cuFlushGPUDirectRDMAWrites` is what
resolves it.

This crate does not call it. `RDMABuffer` has no defined CUDA stream semantics
yet, so there is no correct place to put the flush — that has to be decided
first, and then it becomes a choice between flushing on the completion path and
relying on a stream-synchronized read. Until then, a caller doing GPU RDMA should
flush before the first kernel read of the buffer.

## Completion queues

**Never stop polling.** On the libfabric path `fi_cq_read` drives the provider's
progress engine, so code that stops polling when it believes nothing is pending
will prevent unrelated in-flight operations from completing. Tuning how long to
wait between polls is fine; skipping polls is not. The ibverbs backend has no
provider progress engine to starve, since it posts to the NIC directly.

**`fi_cntr` is thread-local on EFA.** Posting from one thread and reading the
counter from another shows no completions. Any multi-threaded submission scheme
has to poll completions on the thread that posted them.

**More polling threads do not help.** The EFA bottleneck is work-request
issuance, not completion polling; dedicated CQ-polling threads measured as no
improvement at all.

## Throughput

The NIC is not the bottleneck. Standalone 4-NIC writes at 64 MB reach ~44 GB/s,
which is at or slightly above the InfiniBand reference on the same test, and a
single NIC does ~8.2 GB/s at 256 KB. The EFA-versus-InfiniBand gap in real
workloads comes from the CPU round trip, not the hardware: EFA has no equivalent
of IBGDA, so a CPU thread must post every work request rather than the GPU
posting to a doorbell itself.

Transfer size dominates every other tuning variable — roughly 3.7 GB/s at 4 KB,
31 GB/s at 116 KB, and 40+ GB/s at 256 KB, an 8.5x range from chunk size alone.
`MAX_RDMA_MSG_SIZE` is 1 GiB and correctness does not depend on it, so a
throughput-sensitive path is worth testing in the 256 KB to 16 MB range rather
than assuming the maximum is best.

For reference, the fastest EFA RDMA implementation we know of (pplx-garden,
~63 GB/s) has the GPU kernel write into a registered buffer, signals the CPU via a
GDRCopy MMIO write, and only then has a CPU worker post from that memory. The
ordering is the point: the CPU reads the ring *after* the kernel finishes, because
concurrent CPU access to GPU memory while kernels run on the same device hangs.

## Configuration notes

`EfaDevice::apply_config_defaults` (`efa_device.rs`) sets what EFA requires:
`max_send_sge` and `max_recv_sge` to 1 (EFA accepts exactly one scatter/gather
entry per RDMA work request, and `EfaQueuePair::create` re-applies this rather
than trusting the caller), and `max_rd_atomic` / `max_dest_rd_atomic` to 0. EFA
also uses GID index 0, where Mellanox commonly uses 3.

Queue depths are rejected rather than clamped when they exceed the device's
`max_sq_wr` / `max_rq_wr`, because `QueuePairActor` budgets send-queue credits
against the configured depth and would over-commit if it silently got fewer.

Two providers exist. For dispatch-shaped traffic, `efa` measured about 10% better
throughput than `efa-direct`, despite `efa-direct` having higher raw bandwidth —
worth knowing before reaching for the lower-level one.

p5.48xlarge has two NUMA nodes, not four: cores 0-47 with 16 NICs, cores 48-95
with the other 16. Pin a thread to the same NUMA node as the NIC it drives.

## One-time setup that looks like a code bug

A fresh cluster with a security group that has no self-referencing egress rule
will show `tx_pkts` climbing while the peer's `rx_pkts` stays at zero. SRD needs
that rule. Relatedly, SRD is silently dropped on secondary VPC CIDRs — use the
primary CIDR.
