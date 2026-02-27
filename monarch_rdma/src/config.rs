/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RDMA configuration attributes.

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
}
