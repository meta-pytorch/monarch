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
//! / `add_write_from_local`, validates the per-op sizes and the local
//! memory ranges as ops are added, and then dispatches the queued ops
//! across the available backends in parallel on [`RdmaAction::submit`].

use std::collections::HashMap;
use std::time::Duration;

use hyperactor::context;

use crate::RdmaManagerActor;
use crate::RdmaManagerMessageClient;
use crate::RdmaOp;
use crate::RdmaOpType;
use crate::backend::RdmaBackend;
use crate::backend::tcp::manager_actor::TcpBackend;
use crate::backend::tcp::manager_actor::TcpManagerActor;
use crate::local_memory::KeepaliveLocalMemory;
use crate::nic::NicBackendHandle;
use crate::rdma_components::RdmaRemoteBuffer;

/// A batch of RDMA operations submitted as a single unit.
///
/// `RdmaAction` is a builder that accumulates read-into-local and
/// write-from-local ops, performs eager validation (size check + local
/// memory range race detection), and then runs them concurrently across
/// the available backends on [`Self::submit`].
///
/// Local-memory race detection treats an `add_read_into_local` claim as a
/// write to the local range and an `add_write_from_local` claim as a
/// read; two reads of the same range merge into one claim, anything else
/// errors. Remote-side ranges are deliberately not tracked.
pub struct RdmaAction {
    entries: Vec<RdmaOp>,
    // Claimed local address ranges keyed by `[start, end)`. The stored
    // [`RdmaOpType`] doubles as the op-kind tag: `ReadIntoLocal` is a
    // local write, `WriteFromLocal` is a local read.
    local_claims: HashMap<(usize, usize), RdmaOpType>,
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
            local_claims: HashMap::new(),
        }
    }

    /// True if this op writes to the local address range
    /// (`ReadIntoLocal` reads remote data *into* local memory).
    fn is_local_write(op_type: RdmaOpType) -> bool {
        matches!(op_type, RdmaOpType::ReadIntoLocal)
    }

    /// Queue a read from `remote` into `local`. Records a local *write*
    /// claim over the local memory range.
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
        self.record_claim(local.addr(), local.size(), RdmaOpType::ReadIntoLocal)?;
        self.entries.push(RdmaOp {
            op_type: RdmaOpType::ReadIntoLocal,
            local,
            remote,
        });
        Ok(self)
    }

    /// Queue a write from `local` into `remote`. Records a local *read*
    /// claim over the local memory range.
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
        self.record_claim(local.addr(), local.size(), RdmaOpType::WriteFromLocal)?;
        self.entries.push(RdmaOp {
            op_type: RdmaOpType::WriteFromLocal,
            local,
            remote,
        });
        Ok(self)
    }

    fn record_claim(
        &mut self,
        addr: usize,
        size: usize,
        op_type: RdmaOpType,
    ) -> Result<(), anyhow::Error> {
        let mut start = addr;
        let mut end = addr.saturating_add(size);
        let new_is_write = Self::is_local_write(op_type);

        // In one pass, we merge all entries that overlap with the new claim into a single entry.
        // We then remove all the merged entries.
        let mut to_remove: Vec<(usize, usize)> = Vec::new();
        for (&(s, e), &existing) in self.local_claims.iter() {
            if end <= s || e <= start {
                continue;
            }
            if new_is_write || Self::is_local_write(existing) {
                anyhow::bail!(
                    "RdmaAction data race: existing {:?} claim at [{:#x}, {:#x}) overlaps new {:?} claim at [{:#x}, {:#x})",
                    existing,
                    s,
                    e,
                    op_type,
                    start,
                    end,
                );
            }
            start = start.min(s);
            end = end.max(e);
            to_remove.push((s, e));
        }

        for k in &to_remove {
            self.local_claims.remove(k);
        }
        self.local_claims.insert((start, end), op_type);
        Ok(())
    }

    /// Submit all queued ops. Ops are grouped by their local backend
    /// (NIC or TCP); each group is submitted in parallel. Safe to
    /// call more than once on the same action — the queued ops and
    /// overlap claims are left intact.
    ///
    /// Takes `&mut self` so the borrow checker prevents two submit
    /// futures from being alive on the same action simultaneously;
    /// otherwise the local-range overlap detection would be meaningless
    /// (two in-flight dispatches over the same claimed local memory).
    pub async fn submit(
        &mut self,
        client: &(impl context::Actor + Send + Sync),
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        // The nic cell's inner `Option` distinguishes "lookup tried and
        // failed" (`Some(None)`) from "never tried" (cell unset), so the
        // lookup doesn't repeat when the local proc lacks a NIC.
        let nic_cell: tokio::sync::OnceCell<Option<NicBackendHandle>> =
            tokio::sync::OnceCell::new();
        // The tcp cell needs no such sentinel: the init closure either
        // caches a handle or bails.
        let tcp_cell: tokio::sync::OnceCell<hyperactor::ActorHandle<TcpManagerActor>> =
            tokio::sync::OnceCell::new();

        let mut nic_ops: Vec<RdmaOp> = Vec::new();
        let mut tcp_ops: Vec<RdmaOp> = Vec::new();

        for entry in &self.entries {
            let op = RdmaOp {
                op_type: entry.op_type,
                local: entry.local.clone(),
                remote: entry.remote.clone(),
            };
            // Route over the NIC only when the remote advertises a NIC
            // backend and the local proc runs a compatible one.
            if let Some(backend_ctx) = entry.remote.resolve_nic() {
                let nic = nic_cell
                    .get_or_init(|| async {
                        RdmaManagerActor::local_handle(client)
                            .get_nic_backend_handle(client)
                            .await
                            .ok()
                            .flatten()
                    })
                    .await;
                if nic
                    .as_ref()
                    .is_some_and(|h| backend_ctx.is_compatible_with(h))
                {
                    nic_ops.push(op);
                    continue;
                }
            }
            // Fall back to TCP — refusing if fallback is disabled.
            tcp_cell
                .get_or_try_init(|| async {
                    if !hyperactor_config::global::get(crate::config::RDMA_ALLOW_TCP_FALLBACK) {
                        anyhow::bail!(
                            "no usable NIC backend, and TCP fallback is disabled; \
                             enable it with monarch.configure(rdma_allow_tcp_fallback=True)"
                        );
                    }
                    tracing::warn!("falling back to TCP transport (no usable NIC backend)");
                    TcpManagerActor::local_handle(client).await
                })
                .await?;
            tcp_ops.push(op);
        }

        let nic_fut = async {
            match nic_cell.into_inner().flatten() {
                Some(h) => h.submit(client, nic_ops, timeout).await,
                None => {
                    assert!(nic_ops.is_empty());
                    Ok(())
                }
            }
        };
        let tcp_fut = async {
            match tcp_cell.into_inner() {
                Some(h) => TcpBackend(h).submit(client, tcp_ops, timeout).await,
                None => {
                    assert!(tcp_ops.is_empty());
                    Ok(())
                }
            }
        };
        let (nic_res, tcp_res) = tokio::join!(nic_fut, tcp_fut);
        nic_res?;
        tcp_res?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Convenience: WRITE-kind claim (i.e. simulates `add_read_into_local`).
    fn write_claim(action: &mut RdmaAction, addr: usize, size: usize) -> Result<(), anyhow::Error> {
        action.record_claim(addr, size, RdmaOpType::ReadIntoLocal)
    }

    /// Convenience: READ-kind claim (i.e. simulates `add_write_from_local`).
    fn read_claim(action: &mut RdmaAction, addr: usize, size: usize) -> Result<(), anyhow::Error> {
        action.record_claim(addr, size, RdmaOpType::WriteFromLocal)
    }

    #[test]
    fn disjoint_writes_ok() {
        let mut a = RdmaAction::new();
        write_claim(&mut a, 0x1000, 0x100).unwrap();
        write_claim(&mut a, 0x2000, 0x100).unwrap();
        assert_eq!(a.local_claims.len(), 2);
    }

    #[test]
    fn overlapping_writes_error() {
        let mut a = RdmaAction::new();
        write_claim(&mut a, 0x1000, 0x100).unwrap();
        let err = write_claim(&mut a, 0x1080, 0x100).unwrap_err();
        assert!(err.to_string().contains("data race"));
    }

    #[test]
    fn overlapping_reads_merge() {
        let mut a = RdmaAction::new();
        read_claim(&mut a, 0x1000, 0x100).unwrap();
        read_claim(&mut a, 0x1080, 0x100).unwrap();
        assert_eq!(a.local_claims.len(), 1);
        let (&(start, end), kind) = a.local_claims.iter().next().unwrap();
        assert_eq!(start, 0x1000);
        assert_eq!(end, 0x1180);
        assert_eq!(*kind, RdmaOpType::WriteFromLocal);
    }

    #[test]
    fn cascading_reads_merge() {
        // Two disjoint reads, then a third that bridges them — the cascade
        // must absorb both pre-existing entries, not just the first.
        let mut a = RdmaAction::new();
        read_claim(&mut a, 0x1000, 0x100).unwrap();
        read_claim(&mut a, 0x1200, 0x100).unwrap();
        assert_eq!(a.local_claims.len(), 2);
        read_claim(&mut a, 0x1080, 0x200).unwrap();
        assert_eq!(a.local_claims.len(), 1);
        let (&(start, end), _) = a.local_claims.iter().next().unwrap();
        assert_eq!(start, 0x1000);
        assert_eq!(end, 0x1300);
    }

    #[test]
    fn write_vs_read_errors() {
        let mut a = RdmaAction::new();
        read_claim(&mut a, 0x1000, 0x100).unwrap();
        let err = write_claim(&mut a, 0x1080, 0x100).unwrap_err();
        assert!(err.to_string().contains("data race"));
    }

    #[test]
    fn read_vs_write_errors() {
        let mut a = RdmaAction::new();
        write_claim(&mut a, 0x1000, 0x100).unwrap();
        let err = read_claim(&mut a, 0x1080, 0x100).unwrap_err();
        assert!(err.to_string().contains("data race"));
    }

    #[test]
    fn touching_ranges_do_not_overlap() {
        // `[0,100)` and `[100,200)` are adjacent, not overlapping.
        let mut a = RdmaAction::new();
        write_claim(&mut a, 0x1000, 0x100).unwrap();
        write_claim(&mut a, 0x1100, 0x100).unwrap();
        assert_eq!(a.local_claims.len(), 2);
    }
}
