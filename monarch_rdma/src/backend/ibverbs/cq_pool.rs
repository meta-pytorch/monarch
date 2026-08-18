/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A device's pool of completion queues, and the leases queue pairs hold on
//! them.
//!
//! One completion queue serves several queue pairs, so that one poller can reap
//! for all of them. How many share a completion queue is fixed by
//! [`crate::config::RDMA_QPS_PER_CQ`], and each completion queue is sized to
//! hold every sharer's work requests at once:
//!
//! ```text
//! cq_entries = queue_pairs_per_cq * max_send_wr
//! ```
//!
//! That is what lets completions be reaped without any runtime credit
//! accounting: a queue pair caps its own outstanding work requests at
//! `max_send_wr`, so it can never leave more than `max_send_wr` completion-queue
//! entries unreaped, and the completion queue is sized for exactly that much per
//! queue pair leasing it. Overflow -- which puts a completion queue into
//! an unrecoverable error state -- is therefore structurally impossible rather
//! than merely unlikely.
//!
//! One assumption holds that reasoning up, and breaking it means this sizing has
//! to be revisited: nothing posts receive work requests on these queue pairs. A
//! receive work request yields an entry too, but is bounded by `max_recv_wr`
//! rather than `max_send_wr`, so it would consume capacity this sizing does not
//! account for.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use super::primitives::IbvContext;
use super::primitives::IbvCq;
use super::primitives::IbvDeviceInfo;

/// Entries a completion queue needs in order to serve `queue_pairs` queue pairs,
/// each able to have `max_send_wr` work requests outstanding.
///
/// Fails when the device cannot hold a completion queue that large.
///
/// `max_cqe` of 0 means the device's limit was never queried -- true only of a
/// synthetic [`IbvDeviceInfo`] in a test -- and imposes no limit.
pub(super) fn cq_entries_for(
    queue_pairs: u32,
    max_send_wr: u32,
    max_cqe: i32,
) -> anyhow::Result<i32> {
    let needed = u64::from(queue_pairs) * u64::from(max_send_wr);
    anyhow::ensure!(
        needed > 0,
        "a completion queue serving {queue_pairs} queue pairs of {max_send_wr} \
         work requests each would hold no entries",
    );
    if max_cqe > 0 {
        anyhow::ensure!(
            needed <= max_cqe as u64,
            "{} queue pairs of {} work requests each need a completion queue of {} entries, \
             but this device holds at most {}; lower rdma_qps_per_cq",
            queue_pairs,
            max_send_wr,
            needed,
            max_cqe,
        );
    }
    i32::try_from(needed)
        .map_err(|_| anyhow::anyhow!("a completion queue of {needed} entries does not fit an i32"))
}

/// One completion queue in a [`CqPool`], with the number of live leases on it.
#[derive(Debug)]
struct CqEntry {
    cq: Arc<IbvCq>,
    /// How many queue pairs have claimed a lease on this CQ.
    /// Increments when a new `CqLease` is minted and decrements
    /// when that `CqLease` drops.
    leases: Arc<AtomicU32>,
}

/// One queue pair's lease on a completion queue.
///
/// Holding a lease is what entitles a queue pair to post: the completion queue
/// has room for its full `max_send_wr` of work requests, and -- while
/// `rdma_qps_per_cq` is 1 -- the leaseholder is the only queue pair polling it.
/// Dropping the lease returns that room to the pool, so a lease must outlive
/// every completion its queue pair can still produce -- including the flush
/// entries a teardown generates.
#[derive(Debug)]
pub(crate) struct CqLease {
    cq: Arc<IbvCq>,
    leases: Arc<AtomicU32>,
}

impl CqLease {
    /// The completion queue this lease is on, shareable as a keepalive.
    pub(super) fn cq(&self) -> &Arc<IbvCq> {
        &self.cq
    }

    /// A lease on no completion queue, for tests that build a queue pair without
    /// a device.
    #[cfg(test)]
    pub(super) fn for_test() -> Self {
        Self {
            cq: Arc::new(IbvCq::null()),
            leases: Arc::new(AtomicU32::new(1)),
        }
    }
}

impl Drop for CqLease {
    fn drop(&mut self) {
        self.leases.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A device's completion queues, leased to its queue pairs.
///
/// Completion queues are created on demand and never destroyed: one outlives the
/// queue pairs leasing it, and its slots are reused as queue pairs come and go.
#[derive(Debug)]
pub(super) struct CqPool {
    context: Arc<IbvContext>,
    /// Live leases each completion queue admits.
    queue_pairs_per_cq: u32,
    /// Entries each completion queue is created with.
    cq_entries: i32,
    completion_queues: Vec<CqEntry>,
}

impl CqPool {
    /// Builds an empty pool for queue pairs on `context`, sized from
    /// [`crate::config::RDMA_QPS_PER_CQ`], `max_send_wr` and the device's own
    /// per-completion-queue entry limit.
    ///
    /// Fails when the device cannot hold a completion queue that large; see
    /// [`cq_entries_for`].
    pub(super) fn new(
        context: Arc<IbvContext>,
        device_info: &IbvDeviceInfo,
        max_send_wr: u32,
    ) -> anyhow::Result<Self> {
        let configured: usize =
            hyperactor_config::global::get(crate::config::RDMA_QPS_PER_CQ).into();
        // Each queue pair polls its own completion queue, so two of them sharing
        // one would give it two pollers, splitting its completions between them.
        // The cap is temporary while each queue pair is responsible for polling
        // its CQ.
        anyhow::ensure!(
            configured == 1,
            "rdma_qps_per_cq is {configured}, but only 1 queue pair per completion \
             queue is supported: each polls its own",
        );
        let queue_pairs_per_cq = u32::try_from(configured)
            .map_err(|_| anyhow::anyhow!("rdma_qps_per_cq {configured} does not fit a u32"))?;
        let cq_entries = cq_entries_for(queue_pairs_per_cq, max_send_wr, device_info.max_cqe())?;
        Ok(Self {
            context,
            queue_pairs_per_cq,
            cq_entries,
            completion_queues: Vec::new(),
        })
    }

    /// A pool that admits `queue_pairs_per_cq` leases per completion queue,
    /// bypassing the config. Lets the sharing and reuse logic be exercised while
    /// the configured value is held at 1.
    #[cfg(test)]
    pub(super) fn for_test(
        context: Arc<IbvContext>,
        queue_pairs_per_cq: u32,
        max_send_wr: u32,
        max_cqe: i32,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            context,
            queue_pairs_per_cq,
            cq_entries: cq_entries_for(queue_pairs_per_cq, max_send_wr, max_cqe)?,
            completion_queues: Vec::new(),
        })
    }

    /// Leases capacity for one queue pair, on a completion queue that has room
    /// or on a newly created one.
    pub(super) fn acquire_one(&mut self) -> anyhow::Result<CqLease> {
        // Claim a slot with a compare-exchange rather than a load followed by an
        // increment: a lease is released from whichever thread drops it, so the
        // count can fall between reading it and acting on it. `Relaxed` suffices
        // because nothing is published through the count, and read-modify-writes
        // on one atomic are totally ordered regardless.
        let limit = self.queue_pairs_per_cq;
        let claimed = self.completion_queues.iter().find(|cq| {
            cq.leases
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                    (live < limit).then_some(live + 1)
                })
                .is_ok()
        });
        if let Some(cq) = claimed {
            return Ok(CqLease {
                cq: Arc::clone(&cq.cq),
                leases: Arc::clone(&cq.leases),
            });
        }

        // SAFETY: an `IbvContext` holds a null-or-live `ibv_context` for its own
        // lifetime per its construction contract, and `IbvCq::create` rejects a
        // null one.
        let cq = Arc::new(unsafe { IbvCq::create(Arc::clone(&self.context), self.cq_entries) }?);
        let leases = Arc::new(AtomicU32::new(1));
        self.completion_queues.push(CqEntry {
            cq: Arc::clone(&cq),
            leases: Arc::clone(&leases),
        });
        tracing::debug!(
            completion_queues = self.completion_queues.len(),
            cq_entries = self.cq_entries,
            queue_pairs_per_cq = self.queue_pairs_per_cq,
            "created a completion queue"
        );
        Ok(CqLease { cq, leases })
    }

    /// A completion queue for a queue pair that never posts.
    ///
    /// Such a queue pair still needs a completion queue to be created against,
    /// but produces no entries, so it consumes no capacity and takes no lease:
    /// it shares whichever queue is already there rather than pinning one of its
    /// own. It must not poll that queue either, which is what leaves the
    /// leaseholder as the only poller.
    pub(super) fn cq_without_lease(&mut self) -> anyhow::Result<Arc<IbvCq>> {
        if let Some(entry) = self.completion_queues.first() {
            return Ok(Arc::clone(&entry.cq));
        }
        // SAFETY: an `IbvContext` holds a null-or-live `ibv_context` for its own
        // lifetime per its construction contract, and `IbvCq::create` rejects a
        // null one.
        let cq = Arc::new(unsafe { IbvCq::create(Arc::clone(&self.context), self.cq_entries) }?);
        self.completion_queues.push(CqEntry {
            cq: Arc::clone(&cq),
            leases: Arc::new(AtomicU32::new(0)),
        });
        Ok(cq)
    }

    /// Completion queues this pool has created.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.completion_queues.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ibverbs::device::IbvDevice;
    use crate::backend::ibverbs::mlx_device::MlxDevice;
    use crate::backend::ibverbs::primitives::IbvConfig;

    #[test]
    fn entries_cover_every_sharer_at_full_send_depth() {
        // 32 queue pairs each able to have 512 work requests outstanding.
        assert_eq!(cq_entries_for(32, 512, 4_194_303).unwrap(), 16_384);
    }

    #[test]
    fn entries_fail_when_the_device_cannot_hold_them() {
        let err = cq_entries_for(32, 512, 1000)
            .expect_err("a device holding 1000 entries cannot serve 32 x 512");
        let message = err.to_string();
        assert!(
            message.contains("16384") && message.contains("1000"),
            "the error should name what was needed and what the device holds: {message}",
        );
        assert!(
            message.contains("rdma_qps_per_cq"),
            "the error should name the knob to lower: {message}",
        );
    }

    #[test]
    fn entries_fail_even_for_one_queue_pair_the_device_cannot_hold() {
        assert!(cq_entries_for(1, 512, 300).is_err());
    }

    #[test]
    fn entries_treat_an_unqueried_device_as_unlimited() {
        // A synthetic `IbvDeviceInfo` in a test reports 0.
        assert_eq!(cq_entries_for(8, 256, 0).unwrap(), 2048);
    }

    #[test]
    fn entries_reject_an_empty_completion_queue() {
        assert!(cq_entries_for(0, 512, 4_194_303).is_err());
        assert!(cq_entries_for(32, 0, 4_194_303).is_err());
    }

    /// A lease releases its slot on drop, so a later queue pair reuses it rather
    /// than forcing another completion queue.
    #[test]
    fn dropping_a_lease_frees_its_slot() {
        let leases = Arc::new(AtomicU32::new(1));
        let lease = CqLease {
            cq: Arc::new(IbvCq::null()),
            leases: Arc::clone(&leases),
        };
        assert_eq!(leases.load(Ordering::Relaxed), 1);
        drop(lease);
        assert_eq!(leases.load(Ordering::Relaxed), 0);
    }

    /// The configured value is held at 1 while each queue pair polls its own
    /// completion queue.
    #[test]
    fn pool_refuses_a_configured_value_above_one() {
        let info =
            IbvDeviceInfo::first_available().expect("test runs on machines with RDMA devices");
        let lock = hyperactor_config::global::lock();
        let _guard = lock.override_key(
            crate::config::RDMA_QPS_PER_CQ,
            std::num::NonZeroUsize::new(2)
                .expect("2 is non-zero")
                .into(),
        );
        let err = CqPool::new(Arc::new(IbvContext::null()), &info, 512)
            .expect_err("two queue pairs per completion queue is not supported");
        assert!(
            err.to_string().contains("rdma_qps_per_cq"),
            "the error should name the knob: {err}",
        );
    }

    /// Queue pairs share a completion queue up to `queue_pairs_per_cq`, the next
    /// one gets a new completion queue, and a released lease frees its slot for
    /// reuse.
    #[test]
    fn pool_fills_a_completion_queue_before_creating_another() {
        let info =
            IbvDeviceInfo::first_available().expect("test runs on machines with RDMA devices");
        let device = IbvDevice::<MlxDevice>::try_open(info.name(), IbvConfig::default())
            .expect("the first available device should open");
        let mut pool = CqPool::for_test(device.context(), 2, 512, device.device_info().max_cqe())
            .expect("this device holds two queue pairs' worth of entries");

        let first = pool.acquire_one().expect("first lease");
        assert_eq!(pool.len(), 1);

        let second = pool.acquire_one().expect("second lease");
        assert_eq!(
            pool.len(),
            1,
            "a second queue pair shares the first completion queue",
        );
        assert!(Arc::ptr_eq(first.cq(), second.cq()));

        let third = pool.acquire_one().expect("third lease");
        assert_eq!(
            pool.len(),
            2,
            "a third queue pair needs a second completion queue",
        );
        assert!(!Arc::ptr_eq(first.cq(), third.cq()));

        drop(second);
        let fourth = pool.acquire_one().expect("fourth lease");
        assert_eq!(pool.len(), 2, "the released slot is reused, not grown into");
        assert!(Arc::ptr_eq(first.cq(), fourth.cq()));
    }
}
