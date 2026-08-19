/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Batched RDMA action layer.
//!
//! [`RdmaAction`] accumulates [`crate::RdmaOp`]s through `add_read_into_local`
//! / `add_write_from_local`, validates the per-op sizes as ops are added,
//! and then dispatches the queued ops across the available backends in
//! parallel on [`RdmaAction::submit`].

use std::time::Duration;

use hyperactor::context;

use crate::RdmaManagerActor;
use crate::RdmaManagerMessageClient;
use crate::RdmaOp;
use crate::RdmaOpType;
use crate::backend::RdmaBackendHandle;
use crate::local_memory::KeepaliveLocalMemory;
use crate::rdma_components::RdmaRemoteBuffer;

/// A batch of RDMA operations submitted as a single unit.
///
/// `RdmaAction` is a builder that accumulates read-into-local and
/// write-from-local ops, checking each op's sizes as it is added, and then
/// runs them concurrently across the available backends on
/// [`Self::submit`].
///
/// Overlap between the queued ops' memory ranges is not tracked, on either
/// the local or the remote side. Two ops in one action that write the same
/// range race, and it is the caller's job not to queue them.
pub struct RdmaAction {
    entries: Vec<RdmaOp>,
}

impl Default for RdmaAction {
    fn default() -> Self {
        Self::new()
    }
}

impl RdmaAction {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Queue a read from `remote` into `local`.
    pub fn add_read_into_local(
        &mut self,
        remote: RdmaRemoteBuffer,
        local: KeepaliveLocalMemory,
    ) -> Result<&mut Self, anyhow::Error> {
        if local.size() < remote.size {
            anyhow::bail!(
                "destination local memory size ({}) must be >= remote buffer size ({})",
                local.size(),
                remote.size,
            );
        }
        self.entries.push(RdmaOp {
            op_type: RdmaOpType::ReadIntoLocal,
            local,
            remote,
        });
        Ok(self)
    }

    /// Queue a write from `local` into `remote`.
    pub fn add_write_from_local(
        &mut self,
        remote: RdmaRemoteBuffer,
        local: KeepaliveLocalMemory,
    ) -> Result<&mut Self, anyhow::Error> {
        if local.size() > remote.size {
            anyhow::bail!(
                "source local memory size ({}) must be <= remote buffer size ({})",
                local.size(),
                remote.size,
            );
        }
        self.entries.push(RdmaOp {
            op_type: RdmaOpType::WriteFromLocal,
            local,
            remote,
        });
        Ok(self)
    }

    /// Submit all queued ops. Ops are grouped by backend and each group
    /// is submitted in parallel. Safe to call more than once on the same
    /// action — the queued ops are left intact.
    ///
    /// Takes `&mut self` so the borrow checker prevents two submit futures
    /// from being alive on the same action at once, which would put two
    /// in-flight dispatches over the same local memory.
    pub async fn submit(
        &mut self,
        client: &(impl context::Actor + Send + Sync),
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        if self.entries.is_empty() {
            return Ok(());
        }

        // This proc's spawned backends, in priority order.
        let handles = RdmaManagerActor::local_handle(client)
            .get_backend_handles(client)
            .await?;
        let mut buckets: Vec<(RdmaBackendHandle, Vec<RdmaOp>)> =
            handles.into_iter().map(|h| (h, Vec::new())).collect();

        // Route each op to the first backend the local proc runs that the
        // remote buffer also advertises.
        for entry in &self.entries {
            let op = RdmaOp {
                op_type: entry.op_type,
                local: entry.local.clone(),
                remote: entry.remote.clone(),
            };
            match buckets
                .iter_mut()
                .find(|(handle, _)| entry.remote.is_compatible_with(handle))
            {
                Some((_, ops)) => ops.push(op),
                None => anyhow::bail!("no compatible RDMA backend for buffer: {:?}", entry.remote),
            }
        }

        // Submit each non-empty backend group in parallel, waiting for all
        // groups to finish and accumulating every failure.
        let pending = buckets.into_iter().filter(|(_, ops)| !ops.is_empty()).map(
            |(handle, ops)| async move {
                let name = handle.backend_name();
                handle
                    .submit(client, ops, timeout)
                    .await
                    .map_err(|e| format!("({name}) {e}"))
            },
        );
        let errors: Vec<String> = futures::future::join_all(pending)
            .await
            .into_iter()
            .filter_map(Result::err)
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(
                "RDMA submit failed on {} backend(s):\n{}",
                errors.len(),
                errors.join("\n")
            )
        }
    }
}
