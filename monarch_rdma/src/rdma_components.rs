/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # RDMA Components
//!
//! This module provides the core RDMA building blocks for establishing and managing RDMA connections.
//!
//! ## Core Components
//!
//! * `IbvDomain` - Manages RDMA resources including context, protection domain, and memory region
//! * `IbvQueuePair` - Handles communication between endpoints via queue pairs and completion queues
//!
//! ## RDMA Overview
//!
//! Remote Direct Memory Access (RDMA) allows direct memory access from the memory of one computer
//! into the memory of another without involving either computer's operating system. This permits
//! high-throughput, low-latency networking with minimal CPU overhead.
//!
//! ## Connection Architecture
//!
//! The module manages the following ibverbs primitives:
//!
//! 1. **Queue Pairs (QP)**: Each connection has a send queue and a receive queue
//! 2. **Completion Queues (CQ)**: Events are reported when operations complete
//! 3. **Memory Regions (MR)**: Memory must be registered with the RDMA device before use
//! 4. **Protection Domains (PD)**: Provide isolation between different connections
//!
//! ## Connection Lifecycle
//!
//! 1. Create an `IbvDomain` with `new()`
//! 2. Create an `IbvQueuePair` from the domain
//! 3. Exchange connection info with remote peer (application must handle this)
//! 4. Connect to remote endpoint with `connect()`
//! 5. Perform RDMA operations (read/write)
//! 6. Poll for completions
//! 7. Resources are cleaned up when dropped

/// Maximum size for a single RDMA operation in bytes (1 GiB)
use std::fs;
use std::result::Result;
use std::sync::Arc;
use std::time::Duration;

use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::context;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::RdmaLocalMemory;
use crate::RdmaManagerActor;
use crate::RdmaOp;
use crate::RdmaOpType;
use crate::ReleaseBufferClient;
use crate::backend::RdmaBackend;
use crate::backend::RdmaRemoteBackendContext;
use crate::backend::ibverbs::IbvBuffer;
use crate::backend::ibverbs::manager_actor::IbvManagerActor;
use crate::backend::ibverbs::manager_actor::IbvManagerMessageClient;
use crate::backend::tcp::manager_actor::TcpManagerActor;

/// Lightweight handle representing a registered RDMA buffer.
///
/// Contains an id for the buffer registration, the buffer size, a reference
/// to the owning [`RdmaManagerActor`], and backend-specific contexts for
/// performing RDMA operations.
#[derive(Debug, Named, Clone, Serialize, Deserialize)]
pub struct RdmaRemoteBuffer {
    pub id: usize,
    pub size: usize,
    pub owner: ActorRef<RdmaManagerActor>,
    pub backends: Vec<RdmaRemoteBackendContext>,
}
wirevalue::register_type!(RdmaRemoteBuffer);

/// Backend handle returned by [`RdmaRemoteBuffer::choose_backend`].
///
/// `RdmaBackend` is not object-safe (associated type + generic parameter
/// on `submit`), so we use an enum that delegates to the concrete handle.
#[derive(Debug)]
pub enum RdmaLocalBackend {
    Ibv(ActorHandle<IbvManagerActor>),
    Tcp(ActorHandle<TcpManagerActor>),
}

impl RdmaLocalBackend {
    async fn submit(
        &mut self,
        cx: &(impl context::Actor + Send + Sync),
        ops: Vec<RdmaOp>,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        match self {
            RdmaLocalBackend::Ibv(h) => h.submit(cx, ops, timeout).await,
            RdmaLocalBackend::Tcp(h) => h.submit(cx, ops, timeout).await,
        }
    }
}

impl RdmaRemoteBuffer {
    /// Choose the best available backend for this buffer.
    ///
    /// Prefers ibverbs when both the local and remote sides support it.
    /// Falls back to TCP when ibverbs is unavailable and
    /// [`RDMA_ALLOW_TCP_FALLBACK`](crate::config::RDMA_ALLOW_TCP_FALLBACK)
    /// is enabled.
    pub async fn choose_backend(
        &self,
        client: &(impl context::Actor + Send + Sync),
    ) -> Result<RdmaLocalBackend, anyhow::Error> {
        if self.has_ibverbs_backend() {
            if let Ok(ibv_backend) = IbvManagerActor::local_handle(client).await {
                return Ok(RdmaLocalBackend::Ibv(ibv_backend));
            }

            return self
                .tcp_fallback_or_bail("no ibverbs backend on the local side", client)
                .await;
        }

        self.tcp_fallback_or_bail(
            &format!(
                "no ibverbs backend on the remote side (owner={})",
                self.owner.actor_id()
            ),
            client,
        )
        .await
    }

    /// Push data from local memory into this remote buffer (local->remote).
    pub async fn write_from_local(
        &self,
        client: &(impl context::Actor + Send + Sync),
        local: Arc<dyn RdmaLocalMemory>,
        timeout: u64,
    ) -> Result<bool, anyhow::Error> {
        let mut backend = self.choose_backend(client).await?;
        backend
            .submit(
                client,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local,
                    remote: self.clone(),
                }],
                Duration::from_secs(timeout),
            )
            .await?;
        Ok(true)
    }

    /// Pull data from this remote buffer into local memory (remote->local).
    pub async fn read_into_local(
        &self,
        client: &(impl context::Actor + Send + Sync),
        local: Arc<dyn RdmaLocalMemory>,
        timeout: u64,
    ) -> Result<bool, anyhow::Error> {
        let mut backend = self.choose_backend(client).await?;
        backend
            .submit(
                client,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local,
                    remote: self.clone(),
                }],
                Duration::from_secs(timeout),
            )
            .await?;
        Ok(true)
    }

    /// Get a TCP backend handle, or bail if TCP fallback is disabled.
    async fn tcp_fallback_or_bail(
        &self,
        reason: &str,
        client: &(impl context::Actor + Send + Sync),
    ) -> Result<RdmaLocalBackend, anyhow::Error> {
        if !hyperactor_config::global::get(crate::config::RDMA_ALLOW_TCP_FALLBACK) {
            anyhow::bail!(
                "{reason}, and TCP fallback is disabled; \
                 enable it with monarch.configure(rdma_allow_tcp_fallback=True)"
            );
        }

        tracing::warn!("falling back to TCP transport ({reason})");

        let tcp_backend = TcpManagerActor::local_handle(client).await?;
        Ok(RdmaLocalBackend::Tcp(tcp_backend))
    }

    /// Drop the buffer and release remote handles.
    pub async fn drop_buffer(&self, client: &impl context::Actor) -> Result<(), anyhow::Error> {
        tracing::debug!("[buffer] dropping buffer id={}", self.id);
        self.owner.release_buffer(client, self.id).await?;
        Ok(())
    }

    /// Whether this buffer has an ibverbs backend context.
    fn has_ibverbs_backend(&self) -> bool {
        self.backends
            .iter()
            .any(|b| matches!(b, RdmaRemoteBackendContext::Ibverbs(..)))
    }

    /// Resolve the ibverbs backend context for this buffer.
    ///
    /// Returns `None` if the buffer has no ibverbs backend context (i.e.,
    /// the remote side was created without ibverbs). Returns `Some(Err(...))`
    /// if the context exists but lazy MR resolution fails. Returns
    /// `Some(Ok(...))` on success.
    pub async fn resolve_ibv(
        &self,
        client: &impl context::Actor,
    ) -> Option<Result<(ActorRef<IbvManagerActor>, IbvBuffer), anyhow::Error>> {
        let (remote_ibv_mgr, remote_ibv_buf) = self.backends.iter().find_map(|b| match b {
            RdmaRemoteBackendContext::Ibverbs(mgr, buf) => Some((mgr, buf)),
            _ => None,
        })?;

        Some(
            remote_ibv_buf
                .get_or_try_init(async || {
                    remote_ibv_mgr
                        .request_buffer(client, self.id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("buffer {} not found", self.id))
                })
                .await
                .cloned()
                .map(|buf| (remote_ibv_mgr.clone(), buf)),
        )
    }

    /// Extract the TCP backend context from this buffer.
    ///
    /// Unlike [`resolve_ibv`], no lazy initialization is needed -- the
    /// TCP backend only needs the remote actor ref and the buffer id.
    pub fn resolve_tcp(&self) -> Result<(ActorRef<TcpManagerActor>, usize), anyhow::Error> {
        self.backends
            .iter()
            .find_map(|b| match b {
                RdmaRemoteBackendContext::Tcp(tcp_ref) => Some((tcp_ref.clone(), self.id)),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("tcp backend not found for buffer: {:?}", self))
    }
}

/// Utility to validate execution context.
///
/// Remote Execution environments do not always have access to the nvidia_peermem module
/// and/or set the PeerMappingOverride parameter due to security. This function can be
/// used to validate that the execution context when running operations that need this
/// functionality (ie. cudaHostRegisterIoMemory).
///
/// # Returns
///
/// * `Ok(())` if the execution context is valid
/// * `Err(anyhow::Error)` if the execution context is invalid
pub async fn validate_execution_context() -> Result<(), anyhow::Error> {
    // Check for nvidia peermem
    match fs::read_to_string("/proc/modules") {
        Ok(contents) => {
            if !contents.contains("nvidia_peermem") {
                return Err(anyhow::anyhow!(
                    "nvidia_peermem module not found in /proc/modules"
                ));
            }
        }
        Err(e) => {
            return Err(anyhow::anyhow!(e));
        }
    }

    // Test file access to nvidia params
    match fs::read_to_string("/proc/driver/nvidia/params") {
        Ok(contents) => {
            if !contents.contains("PeerMappingOverride=1") {
                return Err(anyhow::anyhow!(
                    "PeerMappingOverride=1 not found in /proc/driver/nvidia/params"
                ));
            }
        }
        Err(e) => {
            return Err(anyhow::anyhow!(e));
        }
    }
    Ok(())
}

/// Get all segments that have been registered with MRs
///
/// # Returns
/// * `Vec<SegmentInfo>` - Vector containing all registered segment information
pub fn get_registered_cuda_segments() -> Vec<rdmaxcel_sys::rdma_segment_info_t> {
    unsafe {
        let segment_count = rdmaxcel_sys::rdma_get_active_segment_count();
        if segment_count <= 0 {
            return Vec::new();
        }

        let mut segments = vec![
            std::mem::MaybeUninit::<rdmaxcel_sys::rdma_segment_info_t>::zeroed()
                .assume_init();
            segment_count as usize
        ];
        let actual_count =
            rdmaxcel_sys::rdma_get_all_segment_info(segments.as_mut_ptr(), segment_count);

        if actual_count > 0 {
            segments.truncate(actual_count as usize);
            segments
        } else {
            Vec::new()
        }
    }
}

/// Segment scanner callback type alias for convenience.
pub type SegmentScannerFn = rdmaxcel_sys::RdmaxcelSegmentScannerFn;

/// Register a segment scanner callback.
///
/// The scanner callback is called during RDMA segment registration to discover
/// CUDA memory segments. The callback should fill the provided buffer with
/// segment information and return the total count of segments found.
///
/// If the returned count exceeds the buffer size, the caller will allocate
/// a larger buffer and retry.
///
/// Pass `None` to unregister the scanner.
///
/// # Safety
///
/// The provided callback function must be safe to call from C code and must
/// properly handle the segment buffer.
pub fn register_segment_scanner(scanner: SegmentScannerFn) {
    // SAFETY: We are registering a callback function pointer with rdmaxcel.
    unsafe { rdmaxcel_sys::rdmaxcel_register_segment_scanner(scanner) }
}
