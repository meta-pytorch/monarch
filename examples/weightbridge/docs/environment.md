# Environment variables

WeightBridge uses environment variables for deployment and performance experiments that must be selected
before worker construction. The public Python API remains the preferred place for topology and endpoint
configuration; these variables control buffer planning, scheduling, transport tuning, and diagnostics.

Export the variables **before importing `wbridge` and before starting Ray or Monarch workers**. Several
defaults are captured at module import (`WBRIDGE_ROUND_CAP_BYTES`, `WBRIDGE_SAME_NODE_IPC`, Gantt switches,
and the transport chunk sizes), while the remaining values are read when an endpoint is constructed. Unless
a row says otherwise, give every sender and receiver the same value. A mismatched round plan or receive depth
is a protocol error, not a supported heterogeneous configuration.

## Library defaults

The library defaults enable the GPU-direct pipeline:

```bash
export WBRIDGE_DEDUP_PAIR_BYTES=inf
export WBRIDGE_RECV_PIPELINE=1
export WBRIDGE_RECV_3STAGE=1
export WBRIDGE_TOPO_EXCHANGE=1
export WBRIDGE_SAME_NODE_IPC=1
```

The library's round cap is 2 GiB. Override memory and scheduling values only as a complete, recorded
deployment profile: changing a cap can also change the number of rounds, buffer footprint, and overlap.

## Planning, buffers, and scheduling

| variable | library default | meaning |
|---|---:|---|
| `WBRIDGE_SPECGEN_I64_CAP_BYTES` | `536870912` (512 MiB) | Maximum int64 structured-index slice used while inferring a LoadSpec. Oversized checkpoint sources and worker probe tensors are divided on dimension 0, inferred independently, translated back to physical coordinates, and merged before specgen returns. The loader still sees the original checkpoint names and tensor shapes. `inf` disables logical splitting for equivalence/debug runs; it does not disable the smaller arithmetic tiles used while constructing an index tensor. |
| `WBRIDGE_LOGICAL_TENSOR_CAP_BYTES` | `134217728` (128 MiB) | Maximum full size of one logical checkpoint-source tensor. Larger tensors are divided into deterministic first-dimension intervals before routing and round planning; model parameters and checkpoint files remain unchanged. `0` disables the translation. A single row larger than the cap or a non-rectangular mapping is rejected rather than copied incorrectly. |
| `WBRIDGE_REPLICA_RELAY` | `0` | Experimental replica-group relay data plane. Trainers pack once per replication group and send to its node-head; one representative per rollout node relays the canonical assembled payload down a chain while local members consume its depth-2 PREP buffers over CUDA IPC. Adjacent writes, assembles, and consumes are completion-ordered per group/edge; depth-2 parity reuse additionally waits for the exact previous parity ACK/free fence. Requires depth-2 sender/receiver buffers, same-node IPC, and no host staging. |
| `WBRIDGE_DIRECT_SAME_NODE` | `0` | Single-node direct-consume path. When every trainer and rollout worker is on one host and staging/relay are off, receiver replicas take complete trainer routes and one fused CUDA-IPC kernel reads sender pack buffers directly into live model parameters. The sender's DATA-ready sequence acts as READY after packing and the receiver ACK after the kernel is the source-lifetime fence. RECV/PREP/GRECV/DOFF payload buffers and rollout exchange are skipped. |
| `WBRIDGE_ROUND_CAP_BYTES` | `2147483648` (2 GiB) | Hard per-receiver byte cap used to divide weights into balanced global rounds. Larger values normally improve fabric efficiency and reduce handshakes, but increase sender pack buffers and receiver ingress/exchange memory. |
| `WBRIDGE_SENDER_NUM_BUF` | `2` | Number of reusable GPU pack buffers per sender/peer. `2` overlaps packing with the previous round's RDMA; `1` is the memory-saving, non-overlapped control. Values above two are experimental and increase trainer HBM. |
| `WBRIDGE_TCP_CONTROL` | `0` | Routes inter-node DATA-ready, ACK, READY, and CONS/OFFLOAD sequence records over one persistent full-duplex TCP connection per worker pair on the host-network interface. Bulk weights remain on the selected RDMA data plane, while same-node topology flags remain in shared memory. Each receiver has an independent socket lock and receive loop; set `1` to isolate control-message tail latency from data-plane submission traffic. |
| `WBRIDGE_COORDINATOR_IPC_DIR` | system temporary directory | Directory for the single-node coordinator and receiver-hub Unix sockets. Set it to a node-local writable directory when the system temporary directory is unavailable or has a restrictive path-length limit. |
| `WBRIDGE_LOCAL_SHM_DIR` | `/dev/shm` when available, otherwise system temporary directory | Directory for mmap-backed same-node replica flags. It must be visible to all local receiver processes and should reside on memory-backed storage. |
| `WBRIDGE_RECV_PIPELINE` | `1` | `1` gives the receiver two isolated trainer-ingress slots, two rollout `own/send/topo-send` prepare slots, and two slots per active external GRECV source; `0` gives one of each. At depth one, ACK immediately after A+R gates the trainer's next write into the lone RECV, while SEND and GRECV retain their independent data-plane reuse gates. |
| `WBRIDGE_RECV_3STAGE` | `1` | Runs external exchange, GRECV→DOFF offload, and internal consume on a progress worker. With depth two the main thread can prepare the opposite SEND parity; with depth one it can pre-land the next ACK-gated RECV and waits for the prior SEND to become free before A+R. The fixed-DOFF topology path requires this setting. |
| `WBRIDGE_DOFF` | `1` | Number of fixed non-RDMA internal-offload generations. Each generation contains one full SEND/PREP shadow plus one exclusive maximum-sized region per direct external source; differently sized external receives are never pooled. GRECV is OFFLOAD-released after its D2D copy, while this DOFF slot is reused only after all local readers return DONE. |
| `WBRIDGE_MERGED_RECV_PREP` | `0` | Experimental memory profile. Overlays each of the two trainer RECV slots with its rollout PREP slot, snapshots landed RECV into one epoch-scoped non-RDMA buffer, and performs A+R back into the merged slot. The scratch tensor is released after the final A+R. This lowers persistent and peak HBM at the cost of one D2D snapshot per round and whole-slot backpressure; it requires depth-2 topology-aware three-stage exchange and does not support receiver staging. |
| `WBRIDGE_DEDUP_PAIR_BYTES` | `inf` | Per-pair crossing-byte threshold for replication-group consolidation. `0` keeps natural tensor replication classes. A positive number dissolves eligible groups whose pair traffic is below the threshold; `inf` is the aggressive production setting and normally reduces each worker to one low-fanout exchange group. Consolidation is an optimization, not a topology eligibility requirement. |
| `WBRIDGE_TOPO_EXCHANGE` | `1` | Enables topology-aware external exchange + fused internal consume. Replication groups are split into one-worker-per-node columns. Incoming DATA is copied immediately from compact GRECV into the matching source-exclusive DOFF slot; OFFLOAD then frees GRECV independently of local readers. The common 1T2R layout has one consume lane per local GPU. |
| `WBRIDGE_SAME_NODE_IPC` | `1` | Enables CUDA-IPC for same-node bulk. Internal consume maps each peer's unregistered DOFF allocation and reads it directly from its descriptor kernel; READY/DONE use shared CPU sequences plus CUDA events. `0` is incompatible with a structurally selected compact topology. |
| `WBRIDGE_RECEIVER_STAGING` | `0` | SGLang-adapter switch that lands the complete update in pinned CPU memory while generation continues. After all local rounds arrive, the worker preloads the first active GPU ingress round; rank 0's host-side GPU-ready flag triggers GO. Host RDMA is slower, direct CUDA IPC is unavailable, and the GPU receiver schedule is serial. |

The receiver allocations for depth `D` are:

```text
D × isolated trainer RECV (stable lane per sender)
+ D × rollout own/send/topo-send prepare
+ up to D × each active external grecv source slot
+ WBRIDGE_DOFF × (one SEND/PREP shadow + one exclusive max region per external source)
```

The isolated RECV slot stride is `max_parity(sum_sender(max_round_bytes(sender, parity)))`: padding is paid
only when a sender's peak occurs in a different round from another sender's peak. In return, no trainer can
overwrite another trainer's live lane, and ACK dependencies remain local to one trainer/rollout pair.

Consequently `WBRIDGE_RECV_PIPELINE=1` means two RECV buffers, two fused send/prepare buffers, and two
GRECV parity slots per active external source. Trainer addresses point only into RECV; external RDMA addresses
point only into SEND/GRECV; internal CUDA-IPC readers map only DOFF. This prevents a later external transfer
from waiting for readers after the previous GRECV payload has been safely offloaded.

## Transport tuning

| variable | default | meaning |
|---|---:|---|
| `WBRIDGE_EFA_SUBSLICE_BYTES` | `16777216` (16 MiB) | Maximum Mooncake request sub-slice. WeightBridge splits a large write into contiguous requests so Mooncake round-robins them across the GPU's EFA paths. `0` disables WeightBridge striping and typically limits one large write to one NIC. Flags and transfers already below the threshold are unchanged. |
| `WBRIDGE_MONARCH_CHUNK_BYTES` | `67108864` (64 MiB) | Tile size used to publish and address exact Monarch RDMA regions. It is not a correctness limit; larger tiles reduce handle count and connect time. Both peers must use the same value. |
| `WBRIDGE_MONARCH_SWITCH_INTERVAL` | `0` | If positive, calls `sys.setswitchinterval(value)` in seconds inside a Monarch endpoint. A shorter interval can help synthetic workloads whose Python threads delay RDMA completion messages, but it did not improve the representative workload and is process-global, so it is off by default. |

For Mooncake/EFA deployments, configure these transport-owned variables before importing Mooncake:

| variable | typical value | meaning |
|---|---:|---|
| `FI_PROVIDER` | `efa` | Restricts libfabric to the EFA provider so a silent TCP-provider measurement cannot be mistaken for RDMA. |
| `FI_EFA_USE_DEVICE_RDMA` | `1` | Enables EFA device RDMA for GPU-direct transfers. |
| `FI_EFA_ENABLE_SHM_TRANSFER` | `0` | Disables libfabric's shared-memory transfer path. WeightBridge handles supported same-node bulk transfers with CUDA IPC. |
| `MC_PATH_ROUNDROBIN` | `1` | Tells Mooncake to distribute requests over available EFA paths. This works with `WBRIDGE_EFA_SUBSLICE_BYTES`: WeightBridge creates the subrequests and Mooncake assigns their paths. |
| `MC_NUM_QP_PER_EP` | library fallback `4` | Number of Mooncake queue pairs per endpoint. It must be set before Mooncake is imported and may need tuning for the target fabric. |
| `MC_EFA_CQ_THREADS` | `1` | Mooncake `>=0.3.12.post1` worker count for the pool that services all EFA completion queues in one transport endpoint. `0` restores the legacy one-busy-poller-per-device behavior and can create excessive CPU polling. It must be set before Mooncake is imported and forwarded to spawned workers. |
| `MC_FORCE_TCP` | automatic for `protocol="tcp"` | Prevents stock Mooncake from attempting an unsupported RC-QP transport in TCP/toy runs. Do not set it for EFA runs. |

These variables do not replace the deployment requirements: Mooncake must be built with EFA support, and
Mooncake and NCCL must resolve to a compatible libfabric installation. WeightBridge does not prepend a
vendor installation directory; configure the system linker or `LD_LIBRARY_PATH` before launching the
driver so spawned workers inherit the same libraries. See
[Assumptions, Limitations & Pitfalls](limitations.md#3-mooncake-on-efa).

## Instrumentation and correctness checks

All switches in this section default to off. They can perturb timing or generate substantial output and
should not be enabled in a production benchmark unless their overhead is being measured.

| variable | value when enabled | meaning |
|---|---:|---|
| `WBRIDGE_GANTT` | `1` | Records wall-clock spans for sender, RDMA, receiver, exchange, consume, and waits. Each process writes `gantt_pid<PID>.jsonl`. |
| `WBRIDGE_GANTT_DIR` | directory | Output directory for Gantt JSONL files. If unset, events go through the logger instead of assuming a shared filesystem path. |
| `WBRIDGE_PROFILE` | `1` | Adds `wbridge::<operation>` `torch.profiler.record_function` labels to every Gantt span. This is independent of wall-clock Gantt recording. |
| `WBRIDGE_RECV_PROFILE_WT` | zero-based integer | Captures a PyTorch CPU+CUDA trace around exactly that receiver update. Leave unset to disable. A value at least `1` avoids the cold update. |
| `WBRIDGE_RECV_PROFILE_DIR` | directory, default system temporary directory | Destination for targeted receiver profiler traces selected by `WBRIDGE_RECV_PROFILE_WT`. |
| `WBRIDGE_CTL_PROFILE` | `1` | Logs per-epoch control primitives (`w_ack`, `p_recv`, `w_ready`, `p_ready`, `w_cons`, `p_cons`) and sender bulk submit/wait bandwidth. It adds host timers to these operations. |
| `WBRIDGE_DUMP_LOADSPEC` | directory | Pickles each rank's inferred metadata and tensor shapes for the frameworkless replay harness. Dumps are diagnostic and failures are logged rather than made fatal. |
| `WBRIDGE_FUSE_SELFCHECK` | `1` | At connect, compares every fused sender pack against the two-stage reference. It allocates temporary full-size scratch buffers and can cause OOM at large scale. |
| `WBRIDGE_DEDUP_DIAG` | `1` | Prints consolidation and byte-savings summaries and adds per-tensor detail when a fused-copy self-check fails. |
| `WBRIDGE_XCHECK` | `1` | Computes and logs per-slice `WXCHK` fingerprints on both sides of receiver exchange. The reductions synchronize GPUs and substantially perturb performance. Use `RAY_DEDUP_LOGS=0` so Ray does not collapse records. |
| `WBRIDGE_TOPO_DEBUG` | `1` | Emits topology-resolution reasons, protocol steps, and machine-readable `WBSTATE` transitions with rank and thread. It is intended for deadlock-cycle reconstruction. |
| `WBRIDGE_TIMING` | `1` | Logs `wbridge-snap` sender wire-byte maps per rank and round during plan construction. |

Use `WBRIDGE_DEDUP_PAIR_BYTES=0` to disable consolidation. Use `WBRIDGE_RECV_3STAGE=0` to run external
exchange and internal consume serially inline on the receiver thread; the default progress worker overlaps
adjacent prepared rounds.
