/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Pure tiling geometry for cast routing.
//!
//! A [`Tile`] is an affine footprint that can be recursively decomposed by a
//! [`Tiling`] implementation. The root tile is the affine slice of the view
//! being routed; child tiles are affine subspaces of their parents.
//!
//! A [`Tiling`] first produces structural [`TileNode`]s: children labeled by
//! their relationship to the parent split. Anchor children preserve the parent
//! representative and sibling children introduce a distinct representative.
//! Communication children are then derived by contracting anchor edges and
//! keeping sibling edges.
//!
//! The anchor/sibling vocabulary is shared by all tilers in this module, but
//! each tiler can use it differently. [`BlockPartitioning`] recursively splits
//! the first varying dimension; later dimensions are discovered by following
//! the anchor branch. [`BoundedFanout`] computes a bounded local frontier in
//! one step, emitting sibling frontier tiles plus a terminal root anchor.
//!
//! This [`BlockPartitioning`] example shows anchor contraction in the simple
//! recursive case:
//!
//! ```text
//! structural decomposition, rendered by tile-root rank:
//! T0 [ A B
//!      C D ] Root
//! |-- T1 [ A B ] Anchor  { dim=0, index=0 }
//! |   |-- T3 [ A ] Anchor  { dim=1, index=0 }
//! |   `-- T4 [ B ] Sibling { dim=1, index=1 }
//! `-- T2 [ C D ] Sibling { dim=0, index=1 }
//!     |-- T5 [ C ] Anchor  { dim=1, index=0 }
//!     `-- T6 [ D ] Sibling { dim=1, index=1 }
//!
//! communication tree after contracting anchors:
//! A
//! |-- B
//! `-- C
//!     `-- D
//! ```
//!
//! [`BoundedFanout`] uses the same structural vocabulary with a different
//! local rule. It partitions the non-root ranks into one frontier slab per
//! active dimension, splits those slabs into a bounded number of groups, emits
//! each group as a sibling, and emits the root point as a terminal anchor.
//!
//! The fanout parameter controls how finely each tile's frontier is split.
//! Small fanout produces fewer, larger child tiles and pushes work deeper into
//! the recursive send tree. Large fanout produces more, smaller child tiles. At
//! the high end, the immediate frontier becomes BlockPartitioning-like: the
//! current root sends to the fine-grained frontier pieces that anchor
//! contraction would expose. At the low end, the tree becomes more
//! bisection-like: each hop delegates larger subtiles to child roots, keeping
//! local branching small. These are related behaviors, not identical tiling
//! strategies. In practice, the goal is the useful middle ground: geometric,
//! roughly logarithmic-depth routing with explicit control over local
//! fan-out.

#![allow(dead_code)]

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use ndslice::Region;
use ndslice::Slice;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeStruct;

/// Decomposable affine tile.
///
/// A [`Tile`] wraps the affine [`Slice`] carried by one node of a tiling tree.
/// The tile root is the slice offset, the natural representative for the
/// tile's geometry.
///
/// Root tiles should be constructed with [`Tile::from_view`]. Descendant tiles
/// should be produced by a [`Tiling`] implementation by selecting affine
/// subspaces of a parent tile.
///
/// ```text
/// root tile ranks:
/// 0 1 2 3
/// 4 5 6 7
///
/// bottom-row child tile:
/// 4 5 6 7
///
/// child.root_rank() = 4
/// child.ranks() = 4 5 6 7
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
pub(crate) struct Tile(Slice);
wirevalue::register_type!(Tile);

impl Tile {
    /// Construct the root tile for a view.
    ///
    /// The root tile is exactly the view's affine slice.
    pub(crate) fn from_view(view: &Region) -> Self {
        Self(view.slice().clone())
    }

    fn from_space(space: Slice) -> Self {
        Self(space)
    }

    /// Affine footprint covered by this tile.
    pub(crate) fn space(&self) -> &Slice {
        &self.0
    }

    /// Ranks covered by this tile in affine iteration order.
    pub(crate) fn ranks(&self) -> impl Iterator<Item = usize> + '_ {
        self.space().iter()
    }

    /// Number of ranks covered by this tile.
    pub(crate) fn rank_count(&self) -> usize {
        self.space().len()
    }

    /// Natural representative rank for this tile's geometry.
    pub(crate) fn root_rank(&self) -> usize {
        self.space().offset()
    }
}

/// A pure [`Tile`] zipped with one concrete item for each tile cell.
///
/// [`Tile`] remains the geometry primitive. `MaterializedTile<T>` adds the
/// items that occupy the tile's ranks, keyed by rank so subtiles can share the
/// same backing map while narrowing only the tile footprint.
///
/// ```text
/// tile.ranks():
/// 4 5 6 7
///
/// items_by_rank:
/// 4 -> A4
/// 5 -> A5
/// 6 -> A6
/// 7 -> A7
///
/// root_item() = item_at(4) = A4
/// ```
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MaterializedTile<T> {
    tile: Tile,
    items_by_rank: Arc<HashMap<usize, T>>,
}

impl<T: Serialize> Serialize for MaterializedTile<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // In memory, child materialized tiles created by `subtile()` may share
        // a root-level `items_by_rank` map so that further subtiles are O(1).
        // On the wire, send only the active tile's items in `tile.space()`
        // iteration order. The serialized tile carries the ranks, so the
        // receiver can rebuild the compact rank map without index shifts.
        let mut state = serializer.serialize_struct("MaterializedTile", 2)?;
        state.serialize_field("tile", &self.tile)?;
        state.serialize_field("items", &self.items().collect::<Vec<_>>())?;
        state.end()
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for MaterializedTile<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct MaterializedTileWire<T> {
            tile: Tile,
            items: Vec<T>,
        }

        // The wire form stores only active-tile items. `new()` zips those
        // values with the serialized tile's ranks, rebuilding a rank-addressed
        // map scoped to this tile rather than the sender's full shared map.
        let wire = MaterializedTileWire::deserialize(deserializer)?;
        Ok(Self::new(wire.tile, wire.items))
    }
}

impl<T> MaterializedTile<T> {
    /// Pair `tile` with one item per rank in `tile.ranks()` order.
    ///
    /// The item at position `i` in `items` is associated with the `i`th rank
    /// yielded by `tile.ranks()`.
    pub(crate) fn new(tile: Tile, items: Vec<T>) -> Self {
        assert_eq!(tile.rank_count(), items.len());
        let items_by_rank = tile.ranks().zip(items).collect();
        Self {
            tile,
            items_by_rank: Arc::new(items_by_rank),
        }
    }

    /// Construct a materialized tile from items already keyed by rank.
    ///
    /// `items_by_rank` must contain exactly one item for every rank covered by
    /// `tile`.
    pub(crate) fn from_map(tile: Tile, items_by_rank: HashMap<usize, T>) -> Self {
        assert_eq!(tile.rank_count(), items_by_rank.len());
        assert!(tile.ranks().all(|rank| items_by_rank.contains_key(&rank)));
        Self {
            tile,
            items_by_rank: Arc::new(items_by_rank),
        }
    }

    /// Geometry carried by this materialized tile.
    pub(crate) fn tile(&self) -> &Tile {
        &self.tile
    }

    /// Natural representative rank for this tile's geometry.
    pub(crate) fn root_rank(&self) -> usize {
        self.tile.root_rank()
    }

    /// Item stored at `rank`, if `rank` is covered by this tile.
    pub(crate) fn item_at(&self, rank: usize) -> Option<&T> {
        if !self.tile.space().contains(rank) {
            return None;
        }
        self.items_by_rank.get(&rank)
    }

    /// Item at this tile's natural representative rank.
    pub(crate) fn root_item(&self) -> Option<&T> {
        self.item_at(self.root_rank())
    }

    /// Items for this tile, in `self.tile.ranks()` order.
    pub(crate) fn items(&self) -> impl Iterator<Item = &T> {
        self.tile.ranks().map(|rank| {
            self.item_at(rank)
                .expect("materialized tile must contain every rank in its tile")
        })
    }

    /// Number of ranks covered by this materialized tile.
    pub(crate) fn rank_count(&self) -> usize {
        self.tile.rank_count()
    }

    /// Materialize `tile` as a child of this materialized tile.
    ///
    /// `tile` must be covered by this tile. The returned materialized tile uses
    /// `tile` as its geometry and shares the same rank-addressed item map.
    ///
    /// ```text
    /// parent MaterializedTile:
    /// 0 -> A0  1 -> A1  2 -> A2  3 -> A3
    /// 4 -> A4  5 -> A5  6 -> A6  7 -> A7
    ///
    /// child Tile input space:
    /// Slice { offset: 4, sizes: [1, 4], strides: [4, 1] }
    ///
    /// subtile(child):
    /// tile.ranks() = 4 5 6 7
    /// root_item() = item_at(4) = A4
    /// ```
    pub(crate) fn subtile(&self, tile: Tile) -> Self {
        debug_assert!(
            tile.ranks()
                .all(|rank| self.items_by_rank.contains_key(&rank))
        );
        Self {
            tile,
            items_by_rank: Arc::clone(&self.items_by_rank),
        }
    }
}

/// Interface for tile decomposition.
pub(crate) trait Tiling {
    /// Return the immediate structural child nodes of `tile`.
    ///
    /// Structural children describe geometry. [`TileRelation::Sibling`]
    /// children introduce distinct roots and become communication children.
    /// [`TileRelation::Anchor`] children preserve the parent root. They are not
    /// returned by [`Tiling::children`]; instead, `children` is called on the
    /// anchor tile and those results are returned in its place.
    fn child_nodes(&self, tile: &Tile) -> Vec<TileNode>;

    /// Return the immediate communication child tiles of `tile`.
    ///
    /// Sibling child tiles are returned directly. Anchor child tiles are not
    /// returned; instead, `children` is called on the anchor tile and those
    /// results are returned in its place.
    ///
    /// For example, [`BlockPartitioning`] over a `2 x 2` tile produces:
    ///
    /// ```text
    /// structural decomposition, rendered by tile-root rank:
    /// T0 [ A B
    ///      C D ] Root
    /// |-- T1 [ A B ] Anchor  { dim=0, index=0 }
    /// |   |-- T3 [ A ] Anchor  { dim=1, index=0 }
    /// |   `-- T4 [ B ] Sibling { dim=1, index=1 }
    /// `-- T2 [ C D ] Sibling { dim=0, index=1 }
    ///     |-- T5 [ C ] Anchor  { dim=1, index=0 }
    ///     `-- T6 [ D ] Sibling { dim=1, index=1 }
    ///
    /// children(tile), rendered by tile-root rank:
    /// |-- T4 [ B ]
    /// `-- T2 [ C D ]
    /// ```
    fn children(&self, tile: &Tile) -> Vec<Tile> {
        self.child_nodes(tile)
            .into_iter()
            .flat_map(|node| match node.relation {
                TileRelation::Sibling(_) => vec![node.tile],
                TileRelation::Anchor(_) => self.children(&node.tile),
            })
            .collect()
    }
}

/// Dimensions of `space`, as (dim, extent) pairs in dimension order.
fn dimension_extents(space: &Slice) -> impl Iterator<Item = (usize, usize)> + '_ {
    space.sizes().iter().copied().enumerate()
}

/// Block-partitioning tiler.
///
/// `child_nodes` finds the first dimension with extent greater than one and
/// fixes that dimension to each index. The index `0` child keeps the parent
/// tile root and is an anchor; every other index shifts the tile root and is a
/// sibling.
///
/// Through [`Tiling::children`], the anchor child is replaced by its own
/// communication children. That is how later dimensions become communication
/// children of the original tile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockPartitioning;

/// First dimension whose extent is greater than one.
///
/// [`BlockPartitioning`] splits this dimension next; dimensions with extent 1
/// are already fixed.
fn first_non_singleton_dim(space: &Slice) -> Option<usize> {
    dimension_extents(space).find_map(|(dim, extent)| (extent > 1).then_some(dim))
}

impl Tiling for BlockPartitioning {
    fn child_nodes(&self, tile: &Tile) -> Vec<TileNode> {
        let space = tile.space();
        let Some(dim) = first_non_singleton_dim(space) else {
            return vec![];
        };

        let size = space.sizes()[dim];

        let siblings = (1..size).map(|index| TileNode {
            tile: Tile::from_space(
                space
                    .select(dim, index, index + 1, 1)
                    .expect("fixing a valid dimension should produce a valid child space"),
            ),
            relation: TileRelation::Sibling(Split { dim, index }),
        });

        let anchor = TileNode {
            tile: Tile::from_space(
                space
                    .select(dim, 0, 1, 1)
                    .expect("fixing the anchor dimension should produce a valid child space"),
            ),
            relation: TileRelation::Anchor(Split { dim, index: 0 }),
        };

        siblings.chain(std::iter::once(anchor)).collect()
    }
}

/// Rectangular tiler with bounded communication fan-out.
///
/// Peels one rectangular frontier slab per active dimension, left-to-right.
/// For a 2 x 4 tile, the row dim peels [E F G H]; the column dim is then
/// constrained to row 0 and peels [B C D]; the corner [A] is the terminal
/// anchor. [`Tiling::children`] does not return that anchor because calling
/// `children` on the singleton root tile yields no communication children.
/// The requested cap is honored when geometrically feasible; when below the
/// active-dimension count, [`effective_fanout`] raises it to the geometric
/// minimum.
///
/// The vocabulary used by this tiler is:
///
/// - an *active dimension* is a dimension whose extent is greater than 1.
///   Only active dimensions can contribute frontier slabs.
/// - a *slab* is the rectangular frontier an active dimension contributes
///   (one slab per active dim).
/// - [`bounded_intervals`] partitions a slab into *groups* (left-heavy
///   chunks).
/// - each group becomes one structural sibling tile via [`frontier_tile`];
///   [`Tiling::children`] returns sibling tiles as communication children.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundedFanout {
    /// Requested cap on the number of communication child tiles per tile.
    ///
    /// [`BoundedFanout`] raises this to [`minimum_fanout`] when a tile has
    /// more active dimensions than the requested cap.
    pub fanout: NonZeroUsize,
}

impl Tiling for BoundedFanout {
    fn child_nodes(&self, tile: &Tile) -> Vec<TileNode> {
        // Active dimensions are the dimensions that can contribute frontier
        // slabs. Each entry is (dim, extent), in dimension order. If none
        // remain, the tile is a singleton and has no children.
        let active_dims: Vec<(usize, usize)> = active_dimensions(tile.space());
        if active_dims.is_empty() {
            return vec![];
        }

        // One active dimension means one frontier slab, and each slab starts
        // with one group. If the effective fanout is larger than the number of
        // slabs, the surplus groups are assigned left-to-right.
        //
        // `groups_per_slab` is parallel to `active_dims`: groups_per_slab[i]
        // is the number of groups to cut from the slab for active_dims[i].
        //
        // For a 3 x 4 tile with active dims [row, col]:
        //   fanout 2 -> row: 1 group, col: 1 group
        //   fanout 3 -> row: 2 groups, col: 1 group
        //   fanout 5 -> row: 2 groups, col: 3 groups
        let groups_per_slab =
            allocate_groups_per_slab(effective_fanout(tile, self.fanout.get()), &active_dims);

        let mut structural_children: Vec<TileNode> = Vec::new();
        for (&(slab_dim, dim_extent), &group_count) in active_dims.iter().zip(&groups_per_slab) {
            // The outer loop walks slabs. `slab_dim` is the active dimension
            // whose slab we are cutting: in a 3 x 4 tile, slab_dim 0 is the
            // row slab and slab_dim 1 is the column slab.
            for (group_begin, group_end) in bounded_intervals(group_count, dim_extent) {
                // The inner loop walks this slab's bounded intervals. Each
                // interval is one away-from-root group, and each group becomes
                // one structural sibling tile.
                structural_children.push(TileNode {
                    tile: frontier_tile(tile, &active_dims, slab_dim, group_begin, group_end),
                    relation: TileRelation::Sibling(Split {
                        dim: slab_dim,
                        index: group_begin,
                    }),
                });
            }
        }

        // The sibling tiles above cover every rank away from the root. Add the
        // root point as a terminal anchor so `child_nodes` is a structural
        // cover of the whole tile. `Tiling::children` will not return this
        // anchor because the root point has no communication children.
        //
        // `TileRelation::Anchor` carries a `Split`, but this root point is the
        // intersection of every active dimension's index-0 slice, not a group
        // from one slab. Use the last active dimension as a stable structural
        // label.
        let (anchor_label_dim, _) = *active_dims.last().expect("active_dims is non-empty");
        structural_children.push(TileNode {
            tile: root_point_tile(tile, &active_dims),
            relation: TileRelation::Anchor(Split {
                dim: anchor_label_dim,
                index: 0,
            }),
        });
        structural_children
    }
}

/// Active dimensions of `space`, as (dim, extent) pairs in dimension order.
///
/// Dimensions with extent 1 cannot contribute a frontier slab.
fn active_dimensions(space: &Slice) -> Vec<(usize, usize)> {
    dimension_extents(space)
        .filter_map(|(dim, extent)| (extent > 1).then_some((dim, extent)))
        .collect()
}

/// Minimum fan-out needed to give each active dimension one group.
pub(crate) fn minimum_fanout(tile: &Tile) -> usize {
    active_dimensions(tile.space()).len()
}

/// BoundedFanout policy: raise a requested cap to the tile's minimum fan-out.
fn effective_fanout(tile: &Tile, requested: usize) -> usize {
    requested.max(minimum_fanout(tile))
}

/// Allocate frontier groups to slabs in dimension order.
///
/// `available_groups` is the effective fan-out for this tile: the requested
/// fan-out raised to at least one group per active dimension.
///
/// `active_dims[i]` identifies one frontier slab, and the returned
/// `groups_per_slab[i]` says how many groups to cut from that slab. Each slab
/// starts with one group because every active dimension must contribute at
/// least one child. Any remaining budget is assigned left-to-right, capped by
/// the number of away-from-root positions in that dimension.
///
/// Allocation is left-heavy across dimensions: earlier slabs receive surplus
/// groups up to capacity before later slabs receive any.
fn allocate_groups_per_slab(available_groups: usize, active_dims: &[(usize, usize)]) -> Vec<usize> {
    // Base allocation: one group per slab.
    let mut groups_per_slab: Vec<usize> = vec![1; active_dims.len()];
    // Surplus groups left after the one-group-per-slab baseline.
    let mut remaining_groups = available_groups.saturating_sub(active_dims.len());

    for (slab_index, &(_dim, dim_extent)) in active_dims.iter().enumerate() {
        // A dimension of extent n has n - 1 away-from-root positions, so its
        // slab cannot be split into more than n - 1 groups.
        let slab_capacity = dim_extent - 1;
        // Give this slab as many surplus groups as possible, but no more than
        // the gap between its capacity and how many groups it already has.
        let additional_groups = remaining_groups.min(slab_capacity - groups_per_slab[slab_index]);

        // Fill this slab by the amount just allocated.
        groups_per_slab[slab_index] += additional_groups;
        // Consume the same amount from the shared surplus budget.
        remaining_groups -= additional_groups;
    }

    groups_per_slab
}

/// Split one slab into left-heavy index intervals.
///
/// `extent` is the size of the active dimension for this slab. The slab covers
/// indices `1..extent` in that dimension; index 0 stays with the
/// root/anchor path. Each returned `(begin, end)` is one group interval that
/// the caller turns into a structural sibling tile with [`frontier_tile`].
///
/// For example, with `extent = 8`, the away-from-root indices are `1..8`.
/// Splitting into 3 groups yields `[1, 4)`, `[4, 6)`, and `[6, 8)`.
///
/// If the intervals do not divide evenly, earlier groups receive one extra
/// index before later groups. Returns `[]` when no groups are requested or
/// the dimension has no away-from-root coordinates.
fn bounded_intervals(group_count: usize, extent: usize) -> Vec<(usize, usize)> {
    if group_count == 0 || extent <= 1 {
        return vec![];
    }
    let remaining = extent - 1;
    let groups = group_count.min(remaining);
    let base = remaining / groups;
    let extra = remaining % groups;
    let mut out = Vec::with_capacity(groups);
    let mut begin = 1;
    for i in 0..groups {
        let size = base + if i < extra { 1 } else { 0 };
        let end = begin + size;
        out.push((begin, end));
        begin = end;
    }
    out
}

/// Prepare `base` for cutting the slab of `dim`.
///
/// Every earlier active dimension is fixed to index 0. For a 2 x 4 tile, this
/// returns the whole tile for the row slab, and the top row [A B C D] for the
/// column slab. [`frontier_tile`] then selects the away-from-root interval in
/// `dim`, such as columns 1..4 to produce [B C D].
fn anchor_prefix(base: &Tile, active_dims: &[(usize, usize)], dim: usize) -> Tile {
    let mut space = base.space().clone();
    for &(earlier_dim, _) in active_dims.iter().take_while(|&&(d, _)| d != dim) {
        // Stay in the index-0 slice of each earlier slab dimension so this
        // slab does not overlap ranks already covered by earlier slabs.
        // Index 0 is tile-local: it is this tile's anchor slice for
        // `earlier_dim`, not a global row or column. The select call keeps
        // only that index-0 slice.
        space = space
            .select(earlier_dim, 0, 1, 1)
            .expect("anchor_prefix: active dim must admit index 0");
    }
    Tile::from_space(space)
}

/// Build the child tile for one group in `slab_dim`'s slab.
///
/// [`anchor_prefix`] first fixes earlier active dimensions to their tile-local
/// index-0 slices. Then this function selects the group's index interval in
/// `slab_dim`. For a 3 x 4 tile, the column slab starts from row 0 and then
/// selects columns 1..4, yielding [B C D].
fn frontier_tile(
    base: &Tile,
    active_dims: &[(usize, usize)],
    slab_dim: usize,
    group_begin: usize,
    group_end: usize,
) -> Tile {
    let anchored = anchor_prefix(base, active_dims, slab_dim);
    // Keep this group's half-open interval in `slab_dim`, with unit stride.
    let space = anchored
        .space()
        .select(slab_dim, group_begin, group_end, 1)
        .expect("frontier_tile: bounded_intervals must produce valid intervals");
    Tile::from_space(space)
}

/// Build the root-point tile left after all frontier slabs are removed.
///
/// This fixes every active dimension to this tile's index 0, leaving the
/// singleton tile at the natural representative rank. `BoundedFanout` emits it
/// as a terminal anchor so `child_nodes` covers the whole parent tile; because
/// it is a singleton, [`Tiling::children`] returns no communication children
/// for it.
fn root_point_tile(base: &Tile, active_dims: &[(usize, usize)]) -> Tile {
    let mut space = base.space().clone();
    for &(active_dim, _) in active_dims {
        // Keep only this tile's index-0 slice in each active dimension.
        space = space
            .select(active_dim, 0, 1, 1)
            .expect("root_point_tile: active dim must admit index 0");
    }
    Tile::from_space(space)
}

/// Reference bisection tiler.
///
/// Splits the first non-singleton dimension into a lower anchor half
/// (`0..mid`) and an upper sibling half (`mid..n`), where `mid = n / 2`.
/// The structural split is binary, but communication children are computed
/// after anchor contraction: sibling descendants inside the lower half are
/// promoted to the current root. This yields a deeper send tree with less root
/// fan-out than block partitioning on wide dimensions.
///
/// This tiler is included for model completeness, tests, and comparison with
/// the reference implementation. [`BoundedFanout`] is the production-oriented
/// fan-out control policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Bisection;

impl Tiling for Bisection {
    fn child_nodes(&self, tile: &Tile) -> Vec<TileNode> {
        let space = tile.space();
        let Some(dim) = first_non_singleton_dim(space) else {
            return vec![];
        };
        let n = space.sizes()[dim];
        let lower = n / 2;

        let sibling = TileNode {
            tile: Tile::from_space(
                space
                    .select(dim, lower, n, 1)
                    .expect("Bisection: upper half must be a valid slice"),
            ),
            relation: TileRelation::Sibling(Split { dim, index: lower }),
        };
        let anchor = TileNode {
            tile: Tile::from_space(
                space
                    .select(dim, 0, lower, 1)
                    .expect("Bisection: lower half must be a valid slice"),
            ),
            relation: TileRelation::Anchor(Split { dim, index: 0 }),
        };
        vec![sibling, anchor]
    }
}

/// Serializable selector for a concrete tiling algorithm.
///
/// This keeps the tiling family open internally via [`Tiling`], while giving
/// cross-process setup messages a small data value that can be serialized and
/// reconstructed by remote actors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TilingPolicy {
    /// Recursively split the first varying dimension and contract anchor edges.
    BlockPartitioning,
    /// Cap communication fan-out per node. When `fanout` is below the
    /// active-dimension minimum, the minimum is used.
    BoundedFanout { fanout: NonZeroUsize },
    /// Recursively bisect the first non-singleton dimension at each step.
    Bisection,
}

impl TilingPolicy {
    pub(crate) fn children(&self, tile: &Tile) -> Vec<Tile> {
        match self {
            Self::BlockPartitioning => {
                let tiling = BlockPartitioning;
                tiling.children(tile)
            }
            Self::BoundedFanout { fanout } => {
                let tiling = BoundedFanout { fanout: *fanout };
                tiling.children(tile)
            }
            Self::Bisection => {
                let tiling = Bisection;
                tiling.children(tile)
            }
        }
    }
}

/// Coordinates one structural decomposition step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Split {
    dim: usize,
    index: usize,
}

/// Relationship between a structural child tile and its parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TileRelation {
    /// Child whose natural representative rank is the same as the parent's.
    ///
    /// Anchors are structural only. [`Tiling::children`] does not return them;
    /// it calls `children` on the anchor tile and returns those results
    /// instead.
    Anchor(Split),
    /// Child whose natural representative rank differs from the parent's.
    ///
    /// Siblings become direct communication children of the parent.
    Sibling(Split),
}

/// Structural child emitted by a [`Tiling`].
///
/// The `tile` is the child geometry. The `relation` records whether that child
/// is a communication sibling or an anchor that [`Tiling::children`] should
/// replace with the anchor tile's own communication children.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TileNode {
    tile: Tile,
    relation: TileRelation,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ndslice::Shape;
    use ndslice::ViewExt;
    use ndslice::shape;
    use ndslice::strategy::gen_region_strided;
    use proptest::prelude::*;

    use super::*;

    fn small_shape_sizes() -> impl Strategy<Value = Vec<usize>> {
        prop::collection::vec(1usize..=4, 1..=4).prop_filter("shape must stay small", |sizes| {
            sizes.iter().product::<usize>() <= 64
        })
    }

    fn shape_from_sizes(sizes: &[usize]) -> Shape {
        Shape::new(
            (0..sizes.len()).map(|dim| format!("d{dim}")).collect(),
            Slice::new_row_major(sizes.to_vec()),
        )
        .unwrap()
    }

    fn collect_roots<T: Tiling>(tiling: &T, tile: &Tile, out: &mut Vec<usize>) {
        out.push(tile.root_rank());
        for child in tiling.children(tile) {
            collect_roots(tiling, &child, out);
        }
    }

    fn validate_child_tiles_are_parent_subsets<T: Tiling>(tiling: &T, tile: &Tile) {
        let parent_ranks = tile.ranks().collect::<BTreeSet<_>>();
        let mut sibling_ranks = BTreeSet::new();

        for child in tiling.children(tile) {
            let child_ranks = child.ranks().collect::<BTreeSet<_>>();
            assert!(
                child_ranks.is_subset(&parent_ranks),
                "child {child:?} must be contained in parent {tile:?}",
            );

            for rank in &child_ranks {
                assert!(
                    sibling_ranks.insert(*rank),
                    "sibling child tiles must be disjoint at rank {rank}",
                );
            }

            validate_child_tiles_are_parent_subsets(tiling, &child);
        }
    }

    fn sliced_view(shape: &Shape) -> Region {
        let mut view = Region::from(shape.clone());
        for label in shape.labels() {
            let dim = view
                .labels()
                .iter()
                .position(|candidate| candidate == label)
                .unwrap();
            let size = view.slice().sizes()[dim];
            if size > 2 {
                view = view.range(label, ndslice::Range(1, Some(size), 1)).unwrap();
            }
        }
        view
    }

    /// Generates `NonZeroUsize` fan-out caps in `1..=8`.
    fn cap_strategy() -> impl Strategy<Value = NonZeroUsize> {
        (1usize..=8).prop_map(|k| NonZeroUsize::new(k).unwrap())
    }

    fn bounded_fanout(fanout: usize) -> BoundedFanout {
        BoundedFanout {
            fanout: NonZeroUsize::new(fanout).unwrap(),
        }
    }

    /// Maximum number of communication children the tile's immediate frontier
    /// can expose. Each active dimension contributes one possible child per
    /// away-from-root index.
    fn immediate_frontier_capacity(tile: &Tile) -> usize {
        active_dimensions(tile.space())
            .into_iter()
            .map(|(_dim, extent)| extent - 1)
            .sum()
    }

    /// One-level structural-subset check: every tile in `tiling.child_nodes(tile)`
    /// covers a subset of `tile.ranks()`. The communication-children helper
    /// (`validate_child_tiles_are_parent_subsets`) recurses; this one
    /// intentionally does not — structural children include the anchor, and
    /// structural cover is only meaningful at the immediate decomposition
    /// step.
    fn validate_structural_child_subsets<T: Tiling>(tiling: &T, tile: &Tile) {
        let parent_ranks = tile.ranks().collect::<BTreeSet<_>>();
        for node in tiling.child_nodes(tile) {
            let child_ranks = node.tile.ranks().collect::<BTreeSet<_>>();
            assert!(
                child_ranks.is_subset(&parent_ranks),
                "structural child {node:?} must be contained in parent {tile:?}",
            );
        }
    }

    #[test]
    fn test_block_partitioning_covers_each_rank_once() {
        // The recursive send tree should choose each rank as the root of
        // exactly one tile.
        //
        // ```text
        // view ranks:
        //  0  1  2  3
        //  4  5  6  7
        //  8  9 10 11
        // 12 13 14 15
        //
        // collected tile roots, sorted:
        // 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
        // ```
        let view = Region::from(shape!(a = 4, b = 4));
        let tiling = BlockPartitioning;
        let root = Tile::from_view(&view);

        let mut roots = Vec::new();
        collect_roots(&tiling, &root, &mut roots);
        roots.sort();

        assert_eq!(roots, view.slice().iter().collect::<Vec<_>>());
    }

    #[test]
    fn test_block_partitioning_child_nodes_expose_anchor_and_sibling_relations() {
        // `child_nodes` returns the immediate structural decomposition before
        // anchor contraction. Splitting dim=0 produces a sibling row and an
        // anchor row.
        //
        // ```text
        // root tile:
        // 0 1
        // 2 3
        //
        // child_nodes(root):
        // |-- [2 3] Sibling { dim=0, index=1 }
        // `-- [0 1] Anchor  { dim=0, index=0 }
        // ```
        let view = Region::from(shape!(row = 2, col = 2));
        let tiling = BlockPartitioning;
        let root = Tile::from_view(&view);

        let nodes = tiling.child_nodes(&root);

        assert_eq!(
            nodes.iter().map(|node| node.relation).collect::<Vec<_>>(),
            vec![
                TileRelation::Sibling(Split { dim: 0, index: 1 }),
                TileRelation::Anchor(Split { dim: 0, index: 0 }),
            ],
        );
        assert_eq!(
            nodes
                .iter()
                .map(|node| node.tile.ranks().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![2, 3], vec![0, 1]],
        );
    }

    #[test]
    fn test_block_partitioning_children_contract_anchor_nodes() {
        // `children` projects structural decomposition into communication
        // children by contracting anchors. The anchor row `[0 1]` is spliced
        // out, exposing its sibling descendant `[1]`.
        //
        // ```text
        // structural decomposition:
        // [0 1 2 3]
        // |-- [2 3] Sibling
        // `-- [0 1] Anchor
        //     |-- [1] Sibling
        //     `-- [0] Anchor
        //
        // children(root):
        // |-- [2 3]
        // `-- [1]
        // ```
        let view = Region::from(shape!(row = 2, col = 2));
        let tiling = BlockPartitioning;
        let root = Tile::from_view(&view);

        assert_eq!(
            tiling
                .children(&root)
                .iter()
                .map(|tile| tile.ranks().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![2, 3], vec![1]],
        );
    }

    #[test]
    fn test_block_partitioning_sliced_view_tiles_stay_in_affine_frame() {
        // Root tile construction preserves the sliced view's affine offset.
        // Children are affine subspaces of that root tile.
        //
        // ```text
        // root/base region R:
        // row=0: 0 1
        // row=1: 2 3
        // row=2: 4 5
        // row=3: 6 7
        //
        // selected view S = R[row=1..3]:
        // row=0: 2 3
        // row=1: 4 5
        //
        // Tile::from_view(S):
        // 2 3
        // 4 5
        //
        // children(root):
        // |-- [4 5]
        // `-- [3]
        // ```
        let root = Region::from(shape!(row = 4, col = 2));
        let view = root.range("row", ndslice::Range(1, Some(3), 1)).unwrap();
        let tiling = BlockPartitioning;
        let tile = Tile::from_view(&view);

        assert_eq!(tile.root_rank(), 2);
        assert_eq!(tile.ranks().collect::<Vec<_>>(), vec![2, 3, 4, 5]);
        assert_eq!(
            view.point_of_base_rank(tile.root_rank()).unwrap(),
            view.extent().point(vec![0, 0]).unwrap(),
        );
        assert_eq!(
            tiling
                .children(&tile)
                .iter()
                .map(|child| child.ranks().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![4, 5], vec![3]],
        );
    }

    #[test]
    fn test_materialized_tile_is_rank_addressed() {
        // `MaterializedTile` stores items by the ranks in its tile. A subtile
        // keeps the same map and narrows only the affine tile footprint.
        //
        // ```text
        // parent tile:
        // 0 1 2 3
        // 4 5 6 7
        //
        // items_by_rank:
        // 0 -> A0  1 -> A1  2 -> A2  3 -> A3
        // 4 -> A4  5 -> A5  6 -> A6  7 -> A7
        //
        // child tile:
        // 4 5 6 7
        //
        // child.root_item() = A4
        // child.items() = A4 A5 A6 A7
        // ```
        let view = Region::from(shape!(row = 2, col = 4));
        let root = Tile::from_view(&view);
        let materialized = MaterializedTile::new(
            root.clone(),
            root.ranks().map(|rank| format!("A{rank}")).collect(),
        );
        let child =
            materialized.subtile(Tile::from_space(root.space().select(0, 1, 2, 1).unwrap()));

        assert_eq!(materialized.item_at(4).map(String::as_str), Some("A4"));
        assert_eq!(child.root_item().map(String::as_str), Some("A4"),);
        assert_eq!(
            child.items().map(String::as_str).collect::<Vec<_>>(),
            vec!["A4", "A5", "A6", "A7"],
        );
    }

    proptest! {
        #[test]
        fn prop_block_partitioning_recursive_tiles_cover_each_rank_once(sizes in small_shape_sizes()) {
            // Example generalized by this property:
            //
            // ```text
            // view ranks:
            // 0 1 2
            // 3 4 5
            //
            // recursive tile roots, sorted:
            // 0 1 2 3 4 5
            // ```
            let view = Region::from(shape_from_sizes(&sizes));
            let tiling = BlockPartitioning;
            let root = Tile::from_view(&view);

            let mut roots = Vec::new();
            collect_roots(&tiling, &root, &mut roots);
            roots.sort();

            let mut expected = view.slice().iter().collect::<Vec<_>>();
            expected.sort();
            prop_assert_eq!(roots, expected);
        }

        #[test]
        fn prop_block_partitioning_child_tiles_are_disjoint_parent_subsets(sizes in small_shape_sizes()) {
            // Example generalized by this property:
            //
            // ```text
            // parent tile:
            // 0 1
            // 2 3
            //
            // communication children:
            // |-- [2 3]  subset of parent
            // `-- [1]    subset of parent, disjoint from [2 3]
            // ```
            let view = Region::from(shape_from_sizes(&sizes));
            let tiling = BlockPartitioning;
            let root = Tile::from_view(&view);

            validate_child_tiles_are_parent_subsets(&tiling, &root);
        }

        #[test]
        fn prop_block_partitioning_sliced_view_tiles_remain_in_view_frame(sizes in small_shape_sizes()) {
            // Example generalized by this property:
            //
            // ```text
            // root/base region R:
            // row=0: 0 1 2
            // row=1: 3 4 5
            // row=2: 6 7 8
            //
            // selected view S = R[row=1..3]:
            // row=0: 3 4 5
            // row=1: 6 7 8
            //
            // every recursive tile contains only ranks from S.
            // ```
            let shape = shape_from_sizes(&sizes);
            let view = sliced_view(&shape);
            let tiling = BlockPartitioning;
            let root = Tile::from_view(&view);
            let view_ranks = view.slice().iter().collect::<BTreeSet<_>>();

            validate_child_tiles_are_parent_subsets(&tiling, &root);

            let mut roots = Vec::new();
            collect_roots(&tiling, &root, &mut roots);
            for root in roots {
                prop_assert!(view_ranks.contains(&root));
            }
        }
    }

    // BoundedFanout unit tests.

    // Tests that `minimum_fanout` is exactly the number of active dimensions:
    // each active dimension needs one child group for its frontier slab, while
    // dimensions with extent 1 do not contribute a slab.
    #[test]
    fn test_bounded_fanout_minimum_counts_active_dims() {
        // `minimum_fanout` is one child group per active dimension. Dimensions
        // with extent 1 do not contribute a slab, so `1 x 8` has minimum 1
        // while `2 x 2 x 2` has minimum 3.
        let t1 = Tile::from_view(&Region::from(shape!(row = 1, col = 8)));
        assert_eq!(minimum_fanout(&t1), 1);

        let t2 = Tile::from_view(&Region::from(shape!(row = 2, col = 2)));
        assert_eq!(minimum_fanout(&t2), 2);

        let t3 = Tile::from_view(&Region::from(shape!(a = 2, b = 2, c = 2)));
        assert_eq!(minimum_fanout(&t3), 3);
    }

    // Tests BoundedFanout's immediate structural decomposition on a minimal
    // two-dimensional tile: one sibling frontier for each active dimension,
    // followed by the terminal root-point anchor.
    #[test]
    fn test_bounded_fanout_child_nodes_on_2x2_exposes_flat_frontier_and_root_anchor() {
        // 2 x 2 tile, fanout 2:
        //   slab dim 0: select(0, 1, 2, 1) -> ranks [2, 3]   (Sibling{0,1})
        //   slab dim 1: anchor row 0, select(1, 1, 2, 1) -> [1]   (Sibling{1,1})
        //   root point: [0]   (Anchor{1,0})
        let view = Region::from(shape!(row = 2, col = 2));
        let tiling = bounded_fanout(2);
        let root = Tile::from_view(&view);
        let nodes = tiling.child_nodes(&root);

        assert_eq!(nodes.len(), 3);

        assert!(matches!(
            nodes[0].relation,
            TileRelation::Sibling(Split { dim: 0, index: 1 })
        ));
        assert!(matches!(
            nodes[1].relation,
            TileRelation::Sibling(Split { dim: 1, index: 1 })
        ));
        assert!(matches!(
            nodes[2].relation,
            TileRelation::Anchor(Split { dim: 1, index: 0 })
        ));

        assert_eq!(nodes[0].tile.ranks().collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!(nodes[1].tile.ranks().collect::<Vec<_>>(), vec![1]);
        assert_eq!(nodes[2].tile.ranks().collect::<Vec<_>>(), vec![0]);
    }

    // Tests that BoundedFanout splits a single-dimension frontier into
    // left-heavy communication children when the requested fanout is smaller
    // than the number of away-from-root ranks.
    #[test]
    fn test_bounded_fanout_children_on_1x8_split_into_left_heavy_intervals() {
        // 1 x 8, fanout 2 over a single active dim of extent 8:
        //   away-from-root indices [1..8) split into 2 left-heavy groups:
        //   [1..5) (size 4) and [5..8) (size 3).
        let view = Region::from(shape!(row = 1, col = 8));
        let tiling = bounded_fanout(2);
        let root = Tile::from_view(&view);

        let rank_sets: Vec<Vec<usize>> = tiling
            .children(&root)
            .iter()
            .map(|t| t.ranks().collect())
            .collect();
        assert_eq!(rank_sets, vec![vec![1, 2, 3, 4], vec![5, 6, 7]]);
    }

    // Tests that each tile's immediate BoundedFanout frontier uses at least
    // one child per active dimension, even when the requested cap is lower.
    #[test]
    fn test_bounded_fanout_below_minimum_uses_minimum() {
        // This assertion is about the immediate communication frontier at the
        // root tile. The shape has 4 active dimensions, so the root frontier
        // needs at least one sibling child per dimension. A requested cap of 3
        // is therefore raised to the active-dimension minimum of 4.
        let view = Region::from(shape!(a = 4, b = 4, c = 4, d = 3));
        let tiling = bounded_fanout(3);
        let root = Tile::from_view(&view);
        assert_eq!(tiling.children(&root).len(), 4);
    }

    // Tests that BoundedFanout builds children from the sliced tile's actual
    // covered ranks. The sliced tile covers [1, 2, 5, 6], so its children
    // should be subsets of those ranks, not subsets of a fresh 0..4 tile.
    #[test]
    fn test_bounded_fanout_sliced_tile_keeps_affine_ranks() {
        // Start with a 2 x 4 region and select columns 1..3. The resulting
        // 2 x 2 tile covers ranks [1, 2, 5, 6] and has root rank 1. Its
        // communication children must stay within those ranks: roots [5, 2],
        // rank sets [[5, 6], [2]].
        let view = Region::from(shape!(row = 2, col = 4))
            .range("col", ndslice::Range(1, Some(3), 1))
            .unwrap();
        let tiling = bounded_fanout(2);
        let root = Tile::from_view(&view);

        assert_eq!(root.root_rank(), 1);
        assert_eq!(root.ranks().collect::<Vec<_>>(), vec![1, 2, 5, 6]);

        let children = tiling.children(&root);
        let roots: Vec<usize> = children.iter().map(|c| c.root_rank()).collect();
        let rank_sets: Vec<Vec<usize>> = children.iter().map(|c| c.ranks().collect()).collect();
        assert_eq!(roots, vec![5, 2]);
        assert_eq!(rank_sets, vec![vec![5, 6], vec![2]]);
    }

    // Tests that the serializable policy selector preserves BoundedFanout's
    // geometry. The rank assertion keeps the example readable; the direct-call
    // assertion catches a wrong `TilingPolicy` dispatch arm.
    #[test]
    fn test_tiling_policy_bounded_fanout_dispatch_matches_direct_call() {
        let view = Region::from(shape!(row = 1, col = 8));
        let root = Tile::from_view(&view);
        let fanout = NonZeroUsize::new(2).unwrap();

        let via_policy = TilingPolicy::BoundedFanout { fanout }.children(&root);
        let via_direct = BoundedFanout { fanout }.children(&root);

        let policy_ranks: Vec<Vec<usize>> =
            via_policy.iter().map(|t| t.ranks().collect()).collect();
        let direct_ranks: Vec<Vec<usize>> =
            via_direct.iter().map(|t| t.ranks().collect()).collect();

        // Pin the example to the documented 1 x 8, fanout 2 intuition: two
        // away-from-root groups, left-heavy.
        assert_eq!(policy_ranks, vec![vec![1, 2, 3, 4], vec![5, 6, 7]]);
        // Dispatch equivalence: TilingPolicy::BoundedFanout must call the
        // same implementation as BoundedFanout directly.
        assert_eq!(policy_ranks, direct_ranks);
    }

    // BoundedFanout proptests. Root-shape generators use
    // `small_shape_sizes()`; sliced generators use
    // `gen_region_strided(1..=4, 4, 3, 0)` from `ndslice::strategy`.
    proptest! {
        // Theorem: BoundedFanout's immediate communication frontier never
        // exceeds the effective fanout for the tile. The effective fanout is
        // the requested cap raised to the tile's active-dimension minimum when
        // needed.
        #[test]
        fn prop_bounded_fanout_respects_effective_fanout(
            sizes in small_shape_sizes(),
            fanout in cap_strategy(),
        ) {
            let view = Region::from(shape_from_sizes(&sizes));
            let tile = Tile::from_view(&view);
            let tiling = BoundedFanout { fanout };
            let bound = effective_fanout(&tile, fanout.get());
            prop_assert!(tiling.children(&tile).len() <= bound);
        }

        // Theorem: the effective-fanout bound is independent of whether the
        // tile is rooted at a dense row-major Region or at a strided/offset
        // affine Region produced by slicing.
        #[test]
        fn prop_bounded_fanout_respects_effective_fanout_sliced(
            view in gen_region_strided(1..=4, 4, 3, 0),
            fanout in cap_strategy(),
        ) {
            let tile = Tile::from_view(&view);
            let tiling = BoundedFanout { fanout };
            let bound = effective_fanout(&tile, fanout.get());
            prop_assert!(tiling.children(&tile).len() <= bound);
        }

        // Theorem: BoundedFanout uses exactly as much immediate frontier as
        // the effective fanout allows. If the requested cap is below the
        // active-dimension minimum, the effective fanout raises it; if the
        // frontier has fewer available children than the cap, the frontier
        // capacity wins.
        #[test]
        fn prop_bounded_fanout_honors_achievable_cap(
            sizes in small_shape_sizes(),
            fanout in cap_strategy(),
        ) {
            let view = Region::from(shape_from_sizes(&sizes));
            let tile = Tile::from_view(&view);
            let tiling = BoundedFanout { fanout };
            // Expected child count is the smaller of the policy limit for
            // this tile and the frontier children the tile can actually
            // expose.
            let expected = effective_fanout(&tile, fanout.get())
                .min(immediate_frontier_capacity(&tile));
            prop_assert_eq!(tiling.children(&tile).len(), expected);
        }

        // Theorem: the exact immediate-frontier count theorem also holds for
        // strided/offset affine Regions produced by slicing.
        #[test]
        fn prop_bounded_fanout_honors_achievable_cap_sliced(
            view in gen_region_strided(1..=4, 4, 3, 0),
            fanout in cap_strategy(),
        ) {
            let tile = Tile::from_view(&view);
            let tiling = BoundedFanout { fanout };
            // Same exact-count rule: min(policy limit, available frontier).
            let expected = effective_fanout(&tile, fanout.get())
                .min(immediate_frontier_capacity(&tile));
            prop_assert_eq!(tiling.children(&tile).len(), expected);
        }

        // Theorem: recursively following BoundedFanout communication children
        // reaches every rank covered by the root tile exactly once.
        #[test]
        fn prop_bounded_fanout_recursive_tiles_cover_each_rank_once(
            sizes in small_shape_sizes(),
            fanout in cap_strategy(),
        ) {
            let view = Region::from(shape_from_sizes(&sizes));
            let tiling = BoundedFanout { fanout };
            let root = Tile::from_view(&view);

            // Each recursive tile contributes its root rank. If the
            // communication tree is a proper cover, those roots are exactly
            // the ranks covered by the original view.
            let mut roots = Vec::new();
            collect_roots(&tiling, &root, &mut roots);
            roots.sort();

            let mut expected = view.slice().iter().collect::<Vec<_>>();
            expected.sort();
            prop_assert_eq!(roots, expected);
        }

        // Theorem: at every recursive communication step, BoundedFanout
        // children are subsets of their parent tile and are disjoint from
        // their sibling communication children.
        #[test]
        fn prop_bounded_fanout_child_tiles_are_disjoint_parent_subsets(
            sizes in small_shape_sizes(),
            fanout in cap_strategy(),
        ) {
            let view = Region::from(shape_from_sizes(&sizes));
            let tiling = BoundedFanout { fanout };
            let root = Tile::from_view(&view);
            validate_child_tiles_are_parent_subsets(&tiling, &root);
        }

        // Theorem: the recursive communication-child subset/disjointness
        // property also holds when the root tile comes from a strided/offset
        // affine Region.
        #[test]
        fn prop_bounded_fanout_child_tiles_are_disjoint_parent_subsets_sliced(
            view in gen_region_strided(1..=4, 4, 3, 0),
            fanout in cap_strategy(),
        ) {
            let tiling = BoundedFanout { fanout };
            let root = Tile::from_view(&view);
            validate_child_tiles_are_parent_subsets(&tiling, &root);
        }

        // Theorem: BoundedFanout's immediate structural children, including
        // the terminal anchor, all cover subsets of the parent tile.
        #[test]
        fn prop_bounded_fanout_structural_children_are_parent_subsets(
            sizes in small_shape_sizes(),
            fanout in cap_strategy(),
        ) {
            let view = Region::from(shape_from_sizes(&sizes));
            let tiling = BoundedFanout { fanout };
            let root = Tile::from_view(&view);
            validate_structural_child_subsets(&tiling, &root);
        }

        // Theorem: structural-child subset containment also holds for
        // strided/offset affine Regions.
        #[test]
        fn prop_bounded_fanout_structural_children_are_parent_subsets_sliced(
            view in gen_region_strided(1..=4, 4, 3, 0),
            fanout in cap_strategy(),
        ) {
            let tiling = BoundedFanout { fanout };
            let root = Tile::from_view(&view);
            validate_structural_child_subsets(&tiling, &root);
        }
    }

    // Bisection unit tests.

    // Tests Bisection's raw structural split before anchor contraction. For a
    // 1 x 5 tile, the first splittable dimension is `col`; `mid = 5 / 2 = 2`.
    // The upper half, ranks [2, 3, 4], has a new root and is the sibling. The
    // lower half, ranks [0, 1], keeps the parent root and is the anchor.
    #[test]
    fn test_bisection_child_nodes_on_1x5_exposes_upper_sibling_and_lower_anchor() {
        let view = Region::from(shape!(row = 1, col = 5));
        let tiling = Bisection;
        let root = Tile::from_view(&view);

        let nodes = tiling.child_nodes(&root);

        assert_eq!(
            nodes.iter().map(|node| node.relation).collect::<Vec<_>>(),
            vec![
                TileRelation::Sibling(Split { dim: 1, index: 2 }),
                TileRelation::Anchor(Split { dim: 1, index: 0 }),
            ],
        );
        assert_eq!(
            nodes
                .iter()
                .map(|node| node.tile.ranks().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![2, 3, 4], vec![0, 1]],
        );
    }

    // Tests the communication projection of that same 1 x 5 split. The upper
    // half [2, 3, 4] is already a sibling, so it remains a direct child. The
    // lower half [0, 1] is an anchor, so it is not returned; its own sibling
    // [1] is promoted to a direct communication child of the root.
    #[test]
    fn test_bisection_children_on_1x5_contracts_lower_anchor() {
        let view = Region::from(shape!(row = 1, col = 5));
        let tiling = Bisection;
        let root = Tile::from_view(&view);

        assert_eq!(
            tiling
                .children(&root)
                .iter()
                .map(|tile| tile.ranks().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![2, 3, 4], vec![1]],
        );
    }

    // Tests the serializable policy selector for Bisection. The pinned rank
    // sets are the communication children of rank 0 after anchor contraction;
    // the direct-call assertion catches a wrong `TilingPolicy` dispatch arm.
    #[test]
    fn test_tiling_policy_bisection_dispatch_matches_direct_call() {
        let view = Region::from(shape!(row = 1, col = 8));
        let root = Tile::from_view(&view);

        let via_policy = TilingPolicy::Bisection.children(&root);
        let via_direct = Bisection.children(&root);

        let policy_ranks: Vec<Vec<usize>> =
            via_policy.iter().map(|t| t.ranks().collect()).collect();
        let direct_ranks: Vec<Vec<usize>> =
            via_direct.iter().map(|t| t.ranks().collect()).collect();

        // For a 1 x 8 tile, the root first exposes the upper half [4, 5, 6, 7].
        // The lower anchor half [0, 1, 2, 3] is contracted, which promotes its
        // sibling [2, 3]; contracting [0, 1] then promotes [1].
        assert_eq!(policy_ranks, vec![vec![4, 5, 6, 7], vec![2, 3], vec![1]],);
        // Dispatch equivalence: TilingPolicy::Bisection must call the concrete
        // Bisection tiler.
        assert_eq!(policy_ranks, direct_ranks);
    }

    // Bisection proptests. Root-shape generators use `small_shape_sizes()`;
    // sliced generators use `gen_region_strided(1..=4, 4, 3, 0)` from
    // `ndslice::strategy`.
    proptest! {
        // Theorem: the Bisection send tree covers exactly the ranks in the
        // root tile. Collecting communication roots recursively should produce
        // every covered rank once, no omissions and no duplicates.
        #[test]
        fn prop_bisection_recursive_tiles_cover_each_rank_once(sizes in small_shape_sizes()) {
            let view = Region::from(shape_from_sizes(&sizes));
            let tiling = Bisection;
            let root = Tile::from_view(&view);

            let mut roots = Vec::new();
            collect_roots(&tiling, &root, &mut roots);
            roots.sort();

            let mut expected = view.slice().iter().collect::<Vec<_>>();
            expected.sort();
            prop_assert_eq!(roots, expected);
        }

        // Theorem: each Bisection communication child stays inside its parent
        // tile, and sibling communication children at the same step do not
        // overlap.
        #[test]
        fn prop_bisection_child_tiles_are_disjoint_parent_subsets(sizes in small_shape_sizes()) {
            let view = Region::from(shape_from_sizes(&sizes));
            let tiling = Bisection;
            let root = Tile::from_view(&view);
            validate_child_tiles_are_parent_subsets(&tiling, &root);
        }

        // Theorem: the communication-child subset/disjointness property is
        // affine-frame independent. It must still hold when the root tile is a
        // strided or offset slice of a larger Region.
        #[test]
        fn prop_bisection_child_tiles_are_disjoint_parent_subsets_sliced(
            view in gen_region_strided(1..=4, 4, 3, 0),
        ) {
            let tiling = Bisection;
            let root = Tile::from_view(&view);
            validate_child_tiles_are_parent_subsets(&tiling, &root);
        }

        // Theorem: the raw structural split is contained in the parent tile.
        // This checks both halves before anchor contraction: the upper sibling
        // half and the lower anchor half.
        #[test]
        fn prop_bisection_structural_children_are_parent_subsets(sizes in small_shape_sizes()) {
            let view = Region::from(shape_from_sizes(&sizes));
            let tiling = Bisection;
            let root = Tile::from_view(&view);
            validate_structural_child_subsets(&tiling, &root);
        }

        // Theorem: structural-child containment is affine-frame independent.
        // Bisection should preserve base ranks correctly even when the root is
        // a strided or offset slice.
        #[test]
        fn prop_bisection_structural_children_are_parent_subsets_sliced(
            view in gen_region_strided(1..=4, 4, 3, 0),
        ) {
            let tiling = Bisection;
            let root = Tile::from_view(&view);
            validate_structural_child_subsets(&tiling, &root);
        }
    }
}
