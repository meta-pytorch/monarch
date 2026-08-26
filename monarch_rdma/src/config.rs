/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RDMA configuration attributes.

use std::time::Duration;

use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::NonZeroUsize;
use hyperactor_config::attrs::declare_attrs;

declare_attrs! {
    /// Maximum chunk size in MiB for TCP-based RDMA transfers.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_MAX_CHUNK_SIZE_MB".to_string()),
        Some("rdma_max_chunk_size_mb".to_string()),
    ))
    pub attr RDMA_MAX_CHUNK_SIZE_MB: usize = 64;

    /// Allow TCP fallback when ibverbs hardware is unavailable.
    ///
    /// When true (the default), RDMA operations fall back to chunked
    /// hyperactor messaging over the default channel transport. When
    /// false, operations fail if no ibverbs backend is available.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_ALLOW_TCP_FALLBACK".to_string()),
        Some("rdma_allow_tcp_fallback".to_string()),
    ))
    pub attr RDMA_ALLOW_TCP_FALLBACK: bool = true;

    /// Disable ibverbs even when hardware is present.
    ///
    /// When true, `RdmaManagerActor` skips ibverbs initialization and
    /// relies on the TCP fallback (if enabled). Useful for testing the
    /// TCP transport on machines that have RDMA hardware.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_DISABLE_IBVERBS".to_string()),
        Some("rdma_disable_ibverbs".to_string()),
    ))
    pub attr RDMA_DISABLE_IBVERBS: bool = false;

    /// Number of parallel channels for TCP fallback transfers.
    ///
    /// When greater than 1, each [`TcpManagerActor`] serves this many
    /// direct `hyperactor::channel` connections for bulk data transfer,
    /// bypassing the single-socket actor mailbox. Default is 1
    /// (sequential, existing behavior).
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_TCP_FALLBACK_PARALLELISM".to_string()),
        Some("rdma_tcp_fallback_parallelism".to_string()),
    ))
    pub attr RDMA_TCP_FALLBACK_PARALLELISM: usize = 1;

    /// How long a `CompletionQueueActor` keeps polling a completion queue that
    /// comes back empty before it starts sleeping between rounds. Within the
    /// window it arms the next round immediately; past it, rounds fall into an
    /// exponential backoff (1ms initial, x2, capped at 10ms). `None` (the
    /// default) disables the cutoff entirely: the loop only ever yields, never
    /// sleeps.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_CQ_BUSY_POLL_WINDOW".to_string()),
        Some("rdma_cq_busy_poll_window".to_string()),
    ))
    pub attr RDMA_CQ_BUSY_POLL_WINDOW: Option<Duration> = None;

    /// Per-side budget for the `QueuePairInitializer` handshake. The
    /// timer arms once when we send `EnsureQueuePair` and is rearmed
    /// after we hit RTS while still waiting for the peer's
    /// `NotifyRts`. If it fires the entry is tombstoned with a
    /// `qp_initializer_failed` so further `RequestQueuePair` calls
    /// for the same key surface the same error rather than hanging.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_QP_INIT_TIMEOUT".to_string()),
        Some("rdma_qp_init_timeout".to_string()),
    ))
    pub attr RDMA_QP_INIT_TIMEOUT: Duration = Duration::from_secs(30);

    /// Default ibverbs device target for managers without an explicit target.
    ///
    /// Accepted forms are `cpu:<numa>`, `gpu:<ordinal>`, and `nic:<name>`.
    /// The empty default preserves automatic device selection. Non-empty value
    /// syntax is validated when the RDMA manager starts.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_IBVERBS_TARGET".to_string()),
        Some("rdma_ibverbs_target".to_string()),
    ))
    pub attr RDMA_IBVERBS_TARGET: String = String::new();

    /// Which peer NICs each local NIC may pair with for a transfer.
    ///
    /// Accepted forms are `any`, `match_name`, and `groups:` followed by any
    /// number of `|`-separated groups, each naming any number of devices,
    /// comma-separated — `groups:mlx5_0,mlx5_1|mlx5_2,mlx5_3|mlx5_4,mlx5_5` and
    /// `groups:mlx5_0` are both valid. Groups must be disjoint. Parsed into a
    /// [`PeerDeviceAffinityPolicy`](crate::backend::ibverbs::device_selection::PeerDeviceAffinityPolicy).
    /// The empty default means `any`, since a `String` attribute cannot default
    /// to anything else. Value syntax is validated when the RDMA manager starts.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_PEER_DEVICE_AFFINITY".to_string()),
        Some("rdma_peer_device_affinity".to_string()),
    ))
    pub attr RDMA_PEER_DEVICE_AFFINITY: String = String::new();

    /// How many NICs a buffer is registered on, at most. `None`, which an
    /// empty environment value parses to, sets no limit: every equally good
    /// NIC serves the buffer.
    ///
    /// Depending on where in memory a buffer lives, there may be many
    /// NICs that would be equally good for serving it. Registering the
    /// buffer on each NIC improves flexibility and provides performance
    /// optimization opportunities, but comes at the cost of an expensive
    /// memory registration per NIC. This config attribute lets the user
    /// tune this tradeoff.
    ///
    /// When multiple optimal NICs are available for a buffer, selecting
    /// among them depends on [`RDMA_PEER_DEVICE_AFFINITY`]:
    /// - `any`: first chosen at random, then the rest are taken
    ///    lexicographically starting from the first.
    /// - `match_name`: taken lexicographically.
    /// - `groups`: round robin across groups, taken lexicographically
    ///    within each group (so `groups:nic0,nic1|nic2,nic3`
    ///    with `RDMA_MAX_NICS_PER_BUFFER = 2` would choose
    ///    `nic0` and `nic2`).
    ///
    /// A manager pinned to one device by [`RDMA_IBVERBS_TARGET`] registers
    /// there and ignores this.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_MAX_NICS_PER_BUFFER".to_string()),
        Some("rdma_max_nics_per_buffer".to_string()),
    ))
    pub attr RDMA_MAX_NICS_PER_BUFFER: Option<NonZeroUsize> =
        Some(NonZeroUsize::new(1).expect("1 is non-zero"));

    /// How many queue pairs share one completion queue.
    ///
    /// Sharing is what lets one poller reap for several queue pairs. Each
    /// completion queue is sized to hold every sharer's work requests at once
    /// (`rdma_qps_per_cq * max_send_wr` entries), so raising this trades
    /// completion-queue memory for fewer completion queues to poll. Opening a
    /// device fails outright if it cannot hold a completion queue that large.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_QPS_PER_CQ".to_string()),
        Some("rdma_qps_per_cq".to_string()),
    ))
    pub attr RDMA_QPS_PER_CQ: NonZeroUsize =
        NonZeroUsize::new(1).expect("1 is non-zero");

    /// Whether each device gets its own completion-queue poller.
    ///
    /// When true (the default) the manager spawns one `CompletionQueueActor` per
    /// RDMA device, on demand, so each device's completions are polled
    /// independently of every other device's and a poller that fails takes down
    /// only that device's queue pairs. When false, one poller serves every device
    /// in the process.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_CQ_POLLER_PER_DEVICE".to_string()),
        Some("rdma_cq_poller_per_device".to_string()),
    ))
    pub attr RDMA_CQ_POLLER_PER_DEVICE: bool = true;

    /// Worker-thread count for the shared rdma data-plane runtime, which every
    /// `CompletionQueueActor` and `QueuePairActor` runs on.
    ///
    /// The runtime is built once, lazily, so this value is latched at the
    /// first RDMA use in a process and later changes have no effect.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_RUNTIME_WORKER_THREADS".to_string()),
        Some("rdma_runtime_worker_threads".to_string()),
    ))
    pub attr RDMA_RUNTIME_WORKER_THREADS: usize = 16;
}
