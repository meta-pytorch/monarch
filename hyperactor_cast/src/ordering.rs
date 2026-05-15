/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Sender-scoped ordering state for cast domains that share a stream.
//!
//! # Cast Ordering Model
//!
//! Consider the following case:
//!
//! ```text
//! root domain:
//! A rank=0
//! `-- B rank=1
//!     `-- C rank=2
//!
//! slice domain:
//! A rank=0
//! `-- C rank=2
//! ```
//!
//! Now send three casts and track the sequence each recipient observes:
//!
//! ```text
//! 1. cast seq=0 to root
//!    A seq=0
//!    B seq=0
//!    C seq=0
//!
//! 2. cast seq=1 to slice [A, C]
//!    A seq=1
//!    B seq=0  (unchanged; B is not in the slice)
//!    C seq=1
//!
//! 3. cast to root
//!    A seq=2
//!    B seq=1
//!    C seq=2
//! ```
//!
//! On the third cast, `C` receives its third sender-local message:
//!
//! ```text
//! C: root seq=0, slice seq=1, root seq=2
//! ```
//!
//! But `B` must receive only its second:
//!
//! ```text
//! B: root seq=0, root seq=1
//! ```
//!
//! This is the core problem: on the third cast, `B` needs to know that the
//! previous sequence it should have expected to receive was `0`, not `1`. The
//! next sequence number is a property of the recipient, not the transport edge.
//! A root cast reaches `C` through `B -> C`; a slice cast may reach `C` through
//! `A -> C`. Both still increment `C`'s sequence in the same stream, while `B`
//! is unaffected by the slice cast.
//!
//! Naively, we can replay this with a dense vector that maps each root/base
//! rank to the previous sequence it should expect to see:
//!
//! ```text
//! prev_by_base_rank[rank] = previous sequence for that recipient
//! ```
//!
//! In this example, with ranks `A=0`, `B=1`, and `C=2`, the dense vector would
//! contain:
//!
//! ```text
//! initial state:
//! rank: 0  1  2
//! node: A  B  C
//! prev: -  -  -
//!
//! after cast seq=0 to root:
//! rank: 0  1  2
//! node: A  B  C
//! prev: 0  0  0
//!
//! after cast seq=1 to slice [A, C]:
//! rank: 0  1  2
//! node: A  B  C
//! prev: 1  0  1
//!
//! snapshot carried by the next root cast:
//! rank: 0  1  2
//! node: A  B  C
//! prev: 1  0  1
//!
//! after that root cast is assigned:
//! rank: 0  1  2
//! node: A  B  C
//! prev: 2  1  2
//! ```
//!
//! A cast first snapshots that vector for the target domain, then increments
//! the selected ranks in the authoritative sender state. The snapshot is what
//! gets forwarded down the selected domain's routing tree. Each recipient then
//! delivers with `previous_for_base_rank(rank) + 1`.
//!
//! Example with four ranks after two casts to slice `[1, 2]`:
//!
//! ```text
//! 0 seq=0
//! |-- 1 seq=2
//! |-- 2 seq=2
//! `-- 3 seq=0
//! ```
//!
//! A literal dense vector is correct, but it requires sending a O(mesh size) payload
//!  and even when divergence is simple. Optimization 1 is
//! [`PreviousSeqs`]: encode the dense vector as one `default_prev` plus affine
//! regional deltas.
//!
//! ```text
//! conceptual dense state:
//! rank: 0 1 2 3
//! prev: 0 2 2 0
//!
//! compact encoding:
//! default_prev = 0
//! patches = [{ region = ranks [1, 2], delta = 2 }]
//! ```
//!
//! This is compact for common slice patterns such as rows, columns, tensor
//! parallel groups, pipeline stages, and repeated casts to the same slice. It
//! also keeps the wire format small because CastActors forward only a
//! [`PreviousSeqs`] snapshot.
//!
//! But this optimization creates the next problem. If users create many small
//! slices, especially n one-rank slices, the authoritative sender state can
//! contain O(p) patches. Without another index, every cast would have to clone
//! or scan all `p` patches just to build the snapshot, even if the cast targets
//! one 1x1 slice whose rank intersects only one patch.
//!
//! Optimization 2 is the per-domain patch relevance cache:
//! - this stream stores `domain_id -> domain base-rank Slice`
//! - each [`SenderOrderingState`] stores `domain_id -> relevant patch ids`
//!
//! Snapshotting a cast to `domain_id` materializes the same wire type,
//! [`PreviousSeqs`], but only clones the cached relevant patches. That changes
//! snapshot cost from O(total patches) to O(relevant patches). For n 1x1
//! slices, a cast to rank `k` can snapshot rank `k`'s patches without scanning
//! patches for every other rank.
//!
//! The tradeoff is moved to less-hot paths:
//! - caching a newly installed domain scans existing patches for each active
//!   sender
//! - appending a new patch checks installed domains once and adds that patch id
//!   to intersecting domains
//!
//! Finally, forwarding applies the same compact model recursively.
//! Forwarding still calls [`PreviousSeqs::for_slice`] per next hop. That method
//! works on the already-pruned snapshot and promotes patches that fully cover
//! the child subtree into the forwarded `default_prev`, so descendants do not
//! keep carrying ancestor patches that now apply uniformly to them.

use std::collections::HashMap;

use anyhow::Result;
use hyperactor::ActorAddr;
use hyperactor::Uid;
use ndslice::Region;
use ndslice::Slice;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Ordering state for one logical stream shared by a root domain and its
/// slices.
///
/// See the module docs for the full ordering model.
#[derive(Debug)]
#[allow(dead_code)] // The data plane introduced later in the stack consumes this state.
pub(crate) struct SharedStreamOrderingState {
    /// Root-domain view for this stream. All predecessor patches are expressed
    /// in this base-rank frame, even for compact slice domains that share the
    /// stream.
    root_view: Region,
    /// One sender-scoped predecessor state per originating sender on this
    /// stream.
    ///
    /// This preserves the old CommActor ordering contract more closely:
    /// messages from the same sender to the same destination actor share a
    /// session and monotonically increasing sequence numbers, even when they
    /// flow through different domains that share the stream.
    senders: HashMap<ActorAddr, SenderOrderingState>,
    /// Root/base-rank footprint of each installed domain on this stream.
    ///
    /// Sender-local patch indexes use this to snapshot only the predecessor
    /// patches that can affect the domain being cast to.
    domain_slices: HashMap<Uid, Slice>,
}

/// Sender-scoped ordering state for one shared stream.
///
/// See [`SharedStreamOrderingState`] for the full ordering model and relevance
/// cache algorithm.
#[derive(Debug, Clone)]
#[allow(dead_code)] // The data plane introduced later in the stack consumes this state.
struct SenderOrderingState {
    /// Stable session used in `SEQ_INFO` for this sender/stream.
    session_id: Uuid,
    /// Authoritative predecessor metadata containing all patches for this
    /// sender/stream.
    previous_seqs: PreviousSeqs,
    /// Per-domain relevance index into `previous_seqs.patches`.
    patch_ids_by_domain: HashMap<Uid, Vec<usize>>,
}

#[allow(dead_code)] // The data plane introduced later in the stack consumes these methods.
impl SharedStreamOrderingState {
    pub(crate) fn new(root_view: Region) -> Self {
        Self {
            root_view,
            senders: HashMap::new(),
            domain_slices: HashMap::new(),
        }
    }

    /// Cache which existing predecessor patches can affect an installed domain.
    ///
    /// `domain_view.slice()` is the domain's root/base-rank footprint. We keep
    /// that footprint in `domain_slices`, then ask every active sender to scan
    /// its current `previous_seqs.patches` and remember only the patch ids
    /// whose regions intersect this domain.
    ///
    /// Example:
    ///
    /// ```text
    /// domain D covers ranks [4, 5]
    ///
    /// sender S patches:
    /// patch 0 covers ranks [0, 1]
    /// patch 1 covers ranks [4, 5]
    /// patch 2 covers ranks [5, 7]
    ///
    /// cached for S/D: [1, 2]
    /// ```
    ///
    /// Later, a cast to `D` can build its [`PreviousSeqs`] snapshot by cloning
    /// patches `[1, 2]` directly, instead of scanning every patch on the hot
    /// path. New patches keep the cache up to date in
    /// [`SenderOrderingState::increment_region`].
    pub(crate) fn cache_domain_patch_relevance(&mut self, domain_id: Uid, domain_view: &Region) {
        let domain_slice = domain_view.slice().clone();
        self.domain_slices
            .insert(domain_id.clone(), domain_slice.clone());
        for sender_state in self.senders.values_mut() {
            sender_state.cache_domain_patch_relevance(domain_id.clone(), &domain_slice);
        }
    }

    /// Clear cached patch relevance for a domain during teardown.
    pub(crate) fn clear_domain_patch_relevance(&mut self, domain_id: &Uid) {
        self.domain_slices.remove(domain_id);
        for sender_state in self.senders.values_mut() {
            sender_state.patch_ids_by_domain.remove(domain_id);
        }
    }

    /// Snapshot predecessor metadata for `domain_id`, then advance this sender.
    ///
    /// The ordering matters: the returned snapshot describes the predecessor
    /// state recipients should use for this cast. Only after taking that
    /// snapshot do we advance the authoritative sender state for future casts.
    ///
    /// ```text
    /// before cast to domain D = ranks [1, 2]:
    /// rank: 0  1  2  3
    /// prev: 5  6  6  5
    ///
    /// snapshot returned for this cast:
    /// rank: 1  2
    /// prev: 6  6
    ///
    /// sender state after incrementing D:
    /// rank: 0  1  2  3
    /// prev: 5  7  7  5
    /// ```
    ///
    /// See [`SharedStreamOrderingState`] for the full snapshot/increment
    /// algorithm and complexity tradeoff.
    pub(crate) fn get_prev_seq_snapshot_and_increment(
        &mut self,
        domain_id: &Uid,
        sender: &ActorAddr,
        selected_region: &Region,
    ) -> Result<(Uuid, PreviousSeqs)> {
        let sender_state = self
            .senders
            .entry(sender.clone())
            .or_insert_with(|| SenderOrderingState::new(&self.domain_slices));
        let snapshot = sender_state.previous_seqs_for_domain(domain_id);
        sender_state.increment_region(&self.root_view, selected_region, &self.domain_slices)?;
        Ok((sender_state.session_id, snapshot))
    }
}

#[allow(dead_code)] // The data plane introduced later in the stack consumes these methods.
impl SenderOrderingState {
    /// Create sender state and build empty relevance entries for existing
    /// domains.
    ///
    /// A new sender has no patches yet, but installing the domain keys up
    /// front lets later patch creation append to the right domain indexes.
    fn new(domain_slices: &HashMap<Uid, Slice>) -> Self {
        let mut state = Self {
            session_id: Uuid::new_v4(),
            previous_seqs: PreviousSeqs::default(),
            patch_ids_by_domain: HashMap::new(),
        };
        for (domain_id, domain_slice) in domain_slices {
            state.cache_domain_patch_relevance(domain_id.clone(), domain_slice);
        }
        state
    }

    /// Backfill one domain's relevant patch ids for this sender.
    ///
    /// Patch ids are indexes into this sender's `previous_seqs.patches`, so the
    /// cache has to be stored per sender. A patch is relevant iff its
    /// root/base-rank region intersects the domain's root/base-rank slice; only
    /// relevant patches can change any recipient in that domain.
    fn cache_domain_patch_relevance(&mut self, domain_id: Uid, domain_slice: &Slice) {
        let patch_ids = self
            .previous_seqs
            .patches
            .iter()
            .enumerate()
            .filter_map(|(patch_id, patch)| {
                domain_slice
                    .intersects(patch.region.slice())
                    .then_some(patch_id)
            })
            .collect();
        self.patch_ids_by_domain.insert(domain_id, patch_ids);
    }

    /// Materialize a wire-ready [`PreviousSeqs`] snapshot for one domain.
    ///
    /// The authoritative sender state may contain patches for many unrelated
    /// domains. The domain relevance cache gives the subset of patch ids that
    /// can affect this domain, and the snapshot keeps only those patches while
    /// preserving the same `default_prev`.
    ///
    /// ```text
    /// authoritative:
    /// default_prev = 5
    /// patches = [
    ///   0: ranks [0, 1] += 1
    ///   1: ranks [4, 5] += 2
    ///   2: ranks [5, 7] += 3
    /// ]
    ///
    /// cached patch ids for domain D: [1, 2]
    ///
    /// snapshot for D:
    /// default_prev = 5
    /// patches = [
    ///   ranks [4, 5] += 2
    ///   ranks [5, 7] += 3
    /// ]
    /// ```
    ///
    /// See [`SharedStreamOrderingState`] for why this snapshots O(relevant
    /// patches) instead of O(total patches).
    fn previous_seqs_for_domain(&self, domain_id: &Uid) -> PreviousSeqs {
        self.patch_ids_by_domain
            .get(domain_id)
            .map(|patch_ids| self.previous_seqs.with_patch_ids(patch_ids))
            .unwrap_or_else(|| self.previous_seqs.clone())
    }

    /// Advance this sender's authoritative predecessor state.
    ///
    /// If the increment appends a new patch, this also updates the per-domain
    /// relevance indexes for domains intersecting that patch.
    ///
    /// ```text
    /// domain_slices:
    /// D0 -> ranks [0, 1]
    /// D1 -> ranks [1, 2]
    /// D2 -> ranks [3]
    ///
    /// patch_ids_by_domain before:
    /// D0 -> [0]
    /// D1 -> [1]
    /// D2 -> [2]
    ///
    /// new patch id 4 covers ranks [1]
    ///
    /// patch_ids_by_domain after:
    /// D0 -> [0, 4]   intersects ranks [1]
    /// D1 -> [1, 4]   intersects ranks [1]
    /// D2 -> [2]      no intersection
    /// ```
    ///
    /// Root increments and repeated increments of an existing patch do not
    /// append a patch, so there is no new patch id to add to the cache.
    ///
    /// See [`SharedStreamOrderingState`] for why new patch creation updates
    /// cached relevance for intersecting domains.
    fn increment_region(
        &mut self,
        root_view: &Region,
        region: &Region,
        domain_slices: &HashMap<Uid, Slice>,
    ) -> Result<()> {
        let Some(patch_id) = self.previous_seqs.increment_region(root_view, region)? else {
            return Ok(());
        };

        for (domain_id, domain_slice) in domain_slices {
            if domain_slice.intersects(region.slice()) {
                self.patch_ids_by_domain
                    .entry(domain_id.clone())
                    .or_default()
                    .push(patch_id);
            }
        }

        Ok(())
    }
}

/// One affine patch of predecessor metadata.
///
/// `delta` is additive: matching this region contributes `delta` to the
/// predecessor for every covered root/base rank. This keeps repeated casts to
/// the same slice compact:
///
/// ```text
/// default_prev = 1
/// patches = [{ region = tp0, delta = 3 }]
/// ```
///
/// means "members outside `tp0` have seen 1 prior messages, members inside
/// `tp0` have seen 4."
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
pub(crate) struct PreviousSeqPatch {
    /// Covered root/base-rank region.
    pub(crate) region: Region,
    /// Additive predecessor delta for every rank covered by `region`.
    pub(crate) delta: u64,
}
wirevalue::register_type!(PreviousSeqPatch);

/// Compact predecessor metadata for one sender-scoped stream.
///
/// See [`SharedStreamOrderingState`] for the full ordering model. This type is
/// both the in-memory patch representation and the wire snapshot forwarded
/// between CastActors.
///
/// Conceptually, this is a dense vector indexed by root/base rank:
///
/// ```text
/// prev_by_base_rank[rank] = previous sequence for that recipient
/// ```
///
/// `PreviousSeqs` is the compact encoding of that vector. `default_prev` stores
/// the value shared by most ranks, while `patches` store affine regions whose
/// ranks differ from the default:
///
/// ```text
/// conceptual dense state:
/// rank: 0 1 2 3
/// prev: 1 3 3 1
///
/// compact encoding:
/// default_prev = 1
/// patches = [{ region = ranks [1, 2], delta = 2 }]
/// ```
///
/// The field is represented as:
/// - one default predecessor value for the whole root domain
/// - plus additive affine patches for slices that have observed extra casts
///
/// Lookup is:
///
/// ```text
/// previous(base_rank) = default_prev + sum(delta for matching patches)
/// ```
///
/// This exploits the fact that real slices are usually affine `Region`s
/// (rows, columns, blocks, dp/pp/tp groups, etc.), so many interleavings stay
/// compact without shipping one integer per destination. If every rank diverges
/// independently, the encoding can still grow to O(mesh size); the
/// per-domain patch relevance cache described on [`SharedStreamOrderingState`]
/// avoids scanning unrelated patches for narrow slice casts.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    typeuri::Named
)]
pub(crate) struct PreviousSeqs {
    /// Predecessor count shared by all ranks unless modified by patches.
    pub(crate) default_prev: u64,
    /// Additive exceptions for affine regions that have seen extra slice casts.
    pub(crate) patches: Vec<PreviousSeqPatch>,
}
wirevalue::register_type!(PreviousSeqs);

#[allow(dead_code)] // The data plane introduced later in the stack consumes these methods.
impl PreviousSeqs {
    /// Previous sequence expected by the member at `base_rank`.
    ///
    /// The returned value is `default_prev` plus every patch whose region
    /// contains `base_rank`. For example, with `default_prev = 1` and one patch
    /// covering ranks `1` and `2` with `delta = 2`:
    ///
    /// ```text
    /// 0 seq=1
    /// |-- 1 seq=3
    /// |-- 2 seq=3
    /// `-- 3 seq=1
    /// ```
    ///
    /// In that state, `previous_for_base_rank(2)` returns `3`, while
    /// `previous_for_base_rank(3)` returns `1`.
    pub(crate) fn previous_for_base_rank(&self, base_rank: usize) -> u64 {
        self.default_prev
            + self
                .patches
                .iter()
                .filter(|patch| patch.region.point_of_base_rank(base_rank).is_ok())
                .map(|patch| patch.delta)
                .sum::<u64>()
    }

    /// Advance every member in `region` by one.
    ///
    /// The rank trees below show the conceptual dense predecessor state. The
    /// actual encoding is `default_prev` plus `patches`.
    ///
    /// Incrementing the root region advances `default_prev`, so every rank
    /// moves together and no patch is needed:
    ///
    /// ```text
    /// encoding:
    /// default_prev = 1
    /// patches = []
    ///
    /// conceptual dense state:
    /// 0 seq=1
    /// |-- 1 seq=1
    /// |-- 2 seq=1
    /// `-- 3 seq=1
    /// ```
    ///
    /// Incrementing a slice instead records a patch for only the covered ranks.
    /// Starting from the root state above, incrementing ranks `1` and `2`
    /// produces:
    ///
    /// ```text
    /// encoding:
    /// default_prev = 1
    /// patches = [{ region = ranks [1, 2], delta = 1 }]
    ///
    /// conceptual dense state:
    /// 0 seq=1
    /// |-- 1 seq=2
    /// |-- 2 seq=2
    /// `-- 3 seq=1
    /// ```
    ///
    /// Repeated casts to the same slice coalesce into that existing patch:
    ///
    /// ```text
    /// encoding:
    /// default_prev = 1
    /// patches = [{ region = ranks [1, 2], delta = 2 }]
    ///
    /// conceptual dense state:
    /// 0 seq=1
    /// |-- 1 seq=3
    /// |-- 2 seq=3
    /// `-- 3 seq=1
    /// ```
    ///
    /// Returns `Some(patch_id)` only when a brand-new patch was appended.
    /// Callers use that id to update relevance caches. Root increments and
    /// repeated casts to an existing slice return `None` because no new patch id
    /// needs to be indexed.
    fn increment_region(&mut self, root_view: &Region, region: &Region) -> Result<Option<usize>> {
        anyhow::ensure!(
            region.is_subset(root_view),
            "slice region must be contained in stream root view"
        );

        if region == root_view {
            self.default_prev += 1;
            return Ok(None);
        }

        if let Some(existing) = self
            .patches
            .iter_mut()
            .find(|patch| patch.region == *region)
        {
            existing.delta += 1;
            return Ok(None);
        }

        let patch_id = self.patches.len();
        self.patches.push(PreviousSeqPatch {
            region: region.clone(),
            delta: 1,
        });
        Ok(Some(patch_id))
    }

    /// Build a snapshot containing only selected patch ids.
    ///
    /// Used by [`SenderOrderingState::previous_seqs_for_domain`] to materialize
    /// the same wire type while avoiding a clone of unrelated patches.
    ///
    /// ```text
    /// source:
    /// default_prev = 5
    /// patches = [p0, p1, p2, p3]
    ///
    /// with_patch_ids([1, 3]):
    /// default_prev = 5
    /// patches = [p1, p3]
    /// ```
    fn with_patch_ids(&self, patch_ids: &[usize]) -> Self {
        Self {
            default_prev: self.default_prev,
            patches: patch_ids
                .iter()
                .map(|patch_id| {
                    self.patches
                        .get(*patch_id)
                        .expect("patch relevance cache should point to existing patches")
                        .clone()
                })
                .collect(),
        }
    }

    /// Keep only the patches that can affect members of `tile_base_slice`.
    ///
    /// The patch regions stay expressed in the shared root/base-rank frame.
    /// `tile_base_slice` must be expressed in that same frame.
    /// Patches that fully cover the tile are promoted into the forwarded
    /// `default_prev`, so descendants of a slice do not keep carrying that
    /// ancestor slice's patch.
    ///
    /// For example, forwarding into a tile containing ranks `[4, 5, 6, 7]` with
    /// `default_prev = 7` and a covering `row1 += 2` patch produces a new
    /// `default_prev = 9`; no `row1` patch has to be carried further:
    ///
    /// ```text
    /// 4 seq=9
    /// |-- 5 seq=9
    /// |-- 6 seq=9
    /// `-- 7 seq=9
    /// ```
    ///
    /// If a second `col0 += 3` patch intersects only rank `4`, the covering
    /// `row1` patch is still promoted, but the partial `col0` patch is kept:
    ///
    /// ```text
    /// input:
    /// default_prev = 7
    /// patches = [
    ///   row1 += 2       covers [4, 5, 6, 7]
    ///   col0 += 3       intersects [4]
    /// ]
    ///
    /// forwarded:
    /// default_prev = 9
    /// patches = [
    ///   col0 += 3
    /// ]
    ///
    /// 4 seq=12
    /// |-- 5 seq=9
    /// |-- 6 seq=9
    /// `-- 7 seq=9
    /// ```
    ///
    /// A patch with no overlap is dropped because it cannot change any rank in
    /// the forwarded tile.
    pub(crate) fn for_slice(&self, tile_base_slice: &Slice) -> Self {
        let mut default_prev = self.default_prev;
        let mut patches = Vec::new();

        for patch in &self.patches {
            if patch.region.slice().contains_slice(tile_base_slice) {
                default_prev += patch.delta;
            } else if tile_base_slice.intersects(patch.region.slice()) {
                patches.push(patch.clone());
            }
        }

        Self {
            default_prev,
            patches,
        }
    }
}

#[cfg(test)]
mod tests {
    use hyperactor::ActorAddr;
    use hyperactor::Label;
    use hyperactor::ProcAddr;
    use hyperactor::Uid;
    use ndslice::ViewExt;
    use ndslice::shape;
    use proptest::prelude::*;

    use super::*;

    fn sender() -> ActorAddr {
        ActorAddr::root(
            "sender@inproc://0".parse::<ProcAddr>().unwrap(),
            Label::strip("sender"),
        )
    }

    fn increment_sequence() -> impl Strategy<Value = (usize, Vec<(usize, usize)>)> {
        (1usize..=16).prop_flat_map(|rank_count| {
            let range =
                (0..rank_count).prop_flat_map(move |begin| (Just(begin), (begin + 1)..=rank_count));
            (Just(rank_count), prop::collection::vec(range, 0..32))
        })
    }

    /// Build a contiguous 1D region over a generated root view.
    ///
    /// Example for `root_view = shape!(rank = 6)` and `begin=2, end=5`:
    ///
    /// ```text
    /// root/base ranks:
    /// 0 1 2 3 4 5
    ///
    /// selected region:
    ///     2 3 4
    /// ```
    fn range_region(root_view: &Region, begin: usize, end: usize) -> Region {
        if begin == 0 && end == root_view.num_ranks() {
            root_view.clone()
        } else {
            root_view
                .range("rank", ndslice::Range(begin, Some(end), 1))
                .unwrap()
        }
    }

    fn increment_dense(dense: &mut [u64], region: &Region) {
        for rank in region.slice().iter() {
            dense[rank] += 1;
        }
    }

    #[test]
    fn test_previous_seqs_for_slice_promotes_covering_patch() {
        // row=0: 0 1 2 3
        // row=1: 4 5 6 7
        let root_view = Region::from(shape!(row = 2, col = 4));

        // row=1: 4 5 6 7
        let row1 = root_view.range("row", 1).unwrap();

        // row=0: 0
        // row=1: 4
        let col0 = root_view.range("col", 0).unwrap();

        // row=0: 4 5 6 7
        let row1_tile_base_slice = Slice::new(4, vec![1, 4], vec![4, 1]).unwrap();

        let previous = PreviousSeqs {
            default_prev: 7,
            patches: vec![
                PreviousSeqPatch {
                    region: row1,
                    delta: 2,
                },
                PreviousSeqPatch {
                    region: col0.clone(),
                    delta: 3,
                },
            ],
        };

        let filtered = previous.for_slice(&row1_tile_base_slice);

        assert_eq!(filtered.default_prev, 9);
        assert_eq!(
            filtered.patches,
            vec![PreviousSeqPatch {
                region: col0,
                delta: 3,
            }]
        );
        assert_eq!(filtered.previous_for_base_rank(4), 12);
        assert_eq!(filtered.previous_for_base_rank(5), 9);
    }

    #[test]
    fn test_previous_seqs_for_slice_root_promotes_ancestor_slice_patch() {
        // row=0: 0 1
        // row=1: 2 3
        // row=2: 4 5
        // row=3: 6 7
        let root_view = Region::from(shape!(row = 4, col = 2));

        // row=0: 2 3
        // row=1: 4 5
        let ancestor_slice_region = root_view
            .range("row", ndslice::Range(1, Some(3), 1))
            .unwrap();

        // row=0: 2 3
        // row=1: 4 5
        let slice_root_base_slice = Slice::new(2, vec![2, 2], vec![2, 1]).unwrap();
        let previous = PreviousSeqs {
            default_prev: 0,
            patches: vec![PreviousSeqPatch {
                region: ancestor_slice_region,
                delta: 1,
            }],
        };

        let filtered = previous.for_slice(&slice_root_base_slice);

        assert_eq!(filtered.default_prev, 1);
        assert!(filtered.patches.is_empty());
        assert_eq!(filtered.previous_for_base_rank(2), 1);
        assert_eq!(filtered.previous_for_base_rank(5), 1);
    }

    #[test]
    fn test_shared_stream_snapshots_only_domain_relevant_patches() {
        // 0 1 2 3
        let root_view = Region::from(shape!(rank = 4));

        // 0
        let rank0 = root_view.range("rank", 0).unwrap();

        // 1
        let rank1 = root_view.range("rank", 1).unwrap();

        let sender = sender();
        let root_domain_id = Uid::instance();
        let rank0_domain_id = Uid::instance();
        let rank1_domain_id = Uid::instance();

        let mut stream = SharedStreamOrderingState::new(root_view.clone());
        stream.cache_domain_patch_relevance(root_domain_id.clone(), &root_view);
        stream.cache_domain_patch_relevance(rank0_domain_id.clone(), &rank0);
        stream.cache_domain_patch_relevance(rank1_domain_id.clone(), &rank1);

        let (_, first_rank0_snapshot) = stream
            .get_prev_seq_snapshot_and_increment(&rank0_domain_id, &sender, &rank0)
            .unwrap();
        assert!(first_rank0_snapshot.patches.is_empty());

        let (_, first_rank1_snapshot) = stream
            .get_prev_seq_snapshot_and_increment(&rank1_domain_id, &sender, &rank1)
            .unwrap();
        assert!(first_rank1_snapshot.patches.is_empty());

        let (_, second_rank0_snapshot) = stream
            .get_prev_seq_snapshot_and_increment(&rank0_domain_id, &sender, &rank0)
            .unwrap();
        assert_eq!(second_rank0_snapshot.patches.len(), 1);
        assert_eq!(second_rank0_snapshot.previous_for_base_rank(0), 1);
        assert_eq!(second_rank0_snapshot.previous_for_base_rank(1), 0);

        let (_, root_snapshot) = stream
            .get_prev_seq_snapshot_and_increment(&root_domain_id, &sender, &root_view)
            .unwrap();
        assert_eq!(root_snapshot.patches.len(), 2);
        assert_eq!(root_snapshot.previous_for_base_rank(0), 2);
        assert_eq!(root_snapshot.previous_for_base_rank(1), 1);
        assert_eq!(root_snapshot.previous_for_base_rank(2), 0);
    }

    proptest! {
        #[test]
        fn prop_previous_seqs_matches_dense_vector_model((rank_count, ranges) in increment_sequence()) {
            // Example generated case:
            //
            // 0 1 2 3 4 5
            let root_view = Region::from(shape!(rank = rank_count));
            let mut previous = PreviousSeqs::default();
            let mut dense = vec![0; rank_count];

            for (begin, end) in ranges {
                // Example generated range:
                //
                // increment range [1, 4):
                //   1 2 3
                let region = range_region(&root_view, begin, end);
                previous.increment_region(&root_view, &region).unwrap();
                increment_dense(&mut dense, &region);

                for (rank, expected) in dense.iter().enumerate() {
                    prop_assert_eq!(previous.previous_for_base_rank(rank), *expected);
                }
            }
        }

        #[test]
        fn prop_previous_seqs_for_slice_matches_dense_projection(
            (rank_count, ranges) in increment_sequence(),
            target_begin in 0usize..16,
            target_len in 1usize..=16,
        ) {
            // Example generated case:
            //
            // 0 1 2 3 4 5
            let root_view = Region::from(shape!(rank = rank_count));
            let target_begin = target_begin % rank_count;
            let target_end = target_begin.saturating_add(target_len).min(rank_count);

            // Example generated target:
            //
            // target_region [2, 5):
            //     2 3 4
            let target_region = range_region(&root_view, target_begin, target_end);
            let mut previous = PreviousSeqs::default();
            let mut dense = vec![0; rank_count];

            for (begin, end) in ranges {
                // Example generated prior increments:
                //
                // [1, 4) -> 1 2 3
                // [3, 6) -> 3 4 5
                let region = range_region(&root_view, begin, end);
                previous.increment_region(&root_view, &region).unwrap();
                increment_dense(&mut dense, &region);
            }

            let filtered = previous.for_slice(target_region.slice());
            for rank in target_region.slice().iter() {
                prop_assert_eq!(filtered.previous_for_base_rank(rank), dense[rank]);
            }
        }
    }
}
