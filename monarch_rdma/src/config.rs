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

    /// Cooperative-yield window for the ibverbs CQ poll loop. While
    /// the policy is within this window it calls
    /// `tokio::task::yield_now` between polls; past it, polls fall
    /// into an exponential backoff sleep (1ms initial, x2, capped at
    /// 10ms). `None` (the default) disables the cutoff entirely:
    /// the loop only ever yields, never sleeps.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_RDMA_CQ_BUSY_POLL_WINDOW".to_string()),
        Some("rdma_cq_busy_poll_window".to_string()),
    ))
    pub attr RDMA_CQ_BUSY_POLL_WINDOW: Option<Duration> = None;
}
