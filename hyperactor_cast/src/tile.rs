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
//! [`TilingPolicy`]. Decomposition first produces structural [`TileNode`]s:
//! children that are labeled by their relationship to the parent split. Anchor
//! children preserve the parent representative and sibling children introduce a
//! distinct representative. Communication children are then derived by
//! contracting anchor edges and keeping sibling edges.
//!
//! This mirrors the routing model described in `~/tile/article/routing-foundations.md`:
//! tile geometry -> structural decomposition tree -> anchor contraction -> send
//! tree. This module only models that pure geometry. Cast-actor concerns such
//! as actor addresses, domains, and ordering state are layered outside it.
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

use anyhow::Result;
use ndslice::Point;
use ndslice::Region;
use ndslice::Shape;
use ndslice::Slice;
use ndslice::reshape::Limit;
use ndslice::reshape::ReshapeShapeExt;
use serde::Deserialize;
use serde::Serialize;

/// Decomposable affine tile.
///
/// The tile's [`Slice`] is an affine footprint in the current region's dense
/// local rank space. It says which local ranks the tile owns and how those
/// ranks are organized for recursive tiling. The tile root is the
/// representative of that footprint; child tiles are meaningful relative to the
/// parent tile/region that produced them.
///
/// A tile by itself is pure geometry. When a caller needs to compare a tile
/// against root/base-rank state, methods such as [`Tile::point_in_parent`],
/// [`Tile::base_rank_in_parent`], and [`Tile::base_slice`] translate the tile
/// through the parent region that owns the decomposition step.
///
/// Example decomposition, rendered by tile-root rank within each level:
///
/// ```text
/// T0 [ A B C D
///      E F G H ] Root
/// |-- T1 [ A B C D ] Anchor  { dim=0, index=0 }
/// |   |-- T3 [ A ] Anchor  { dim=1, index=0 }
/// |   |-- T4 [ B ] Sibling { dim=1, index=1 }
/// |   |-- T5 [ C ] Sibling { dim=1, index=2 }
/// |   `-- T6 [ D ] Sibling { dim=1, index=3 }
/// `-- T2 [ E F G H ] Sibling { dim=0, index=1 }
///     |-- T7 [ E ] Anchor  { dim=1, index=0 }
///     |-- T8 [ F ] Sibling { dim=1, index=1 }
///     |-- T9 [ G ] Sibling { dim=1, index=2 }
///     `-- T10 [ H ] Sibling { dim=1, index=3 }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
pub(crate) struct Tile(Slice);
wirevalue::register_type!(Tile);

impl Tile {
    pub(crate) fn new(space: Slice) -> Self {
        Self(space)
    }

    /// Affine tile footprint in the current region's dense local rank space.
    ///
    /// The returned slice's values are local ranks owned by this tile,
    /// not root/base ranks. Use [`Tile::base_slice`] when the same footprint
    /// needs to be expressed in the root/base-rank frame.
    ///
    /// ```text
    /// parent-region local ranks:
    /// 0 1 2 3
    /// 4 5 6 7
    ///
    /// space() for the bottom row:
    /// (0, 0) (0, 1) (0, 2) (0, 3)
    ///   4     5     6     7
    /// ```
    pub(crate) fn space(&self) -> &Slice {
        &self.0
    }

    /// Representative/root rank of this tile in the current region's dense
    /// local rank space.
    ///
    /// This is the first parent-region local rank in [`Tile::space`]. It is
    /// the rank/member that represents the tile when the decomposition is
    /// projected into a routing tree.
    ///
    /// ```text
    /// parent region, shape [row=2, col=4]:
    ///          col=0 col=1 col=2 col=3
    /// row=0      0     1     2     3
    /// row=1      4     5     6     7
    ///            ^
    ///            child tile representative
    ///
    /// child tile space() contains only parent ranks 4, 5, 6, 7:
    /// (0, 0) (0, 1) (0, 2) (0, 3)
    ///   4     5     6     7
    ///   ^
    ///   root() = space().offset() = 4
    /// ```
    ///
    /// In the routing tree, the same representative is the child edge root:
    ///
    /// ```text
    /// 0
    /// |-- 1
    /// |-- 2
    /// |-- 3
    /// `-- 4  child tile root()
    ///     |-- 5
    ///     |-- 6
    ///     `-- 7
    /// ```
    pub(crate) fn root(&self) -> usize {
        self.space().offset()
    }

    /// Local point of the tile representative within the parent region.
    ///
    /// `root()` is a dense rank in the parent region; `point_in_parent()` converts
    /// it into coordinates in the parent region's extent.
    ///
    /// ```text
    /// parent region, shape [row=2, col=4]:
    ///          col=0 col=1 col=2 col=3
    /// row=0      0     1     2     3
    /// row=1      4     5     6     7
    ///            ^
    ///            child tile root in parent coordinates
    ///
    /// child tile space() contains only parent ranks 4, 5, 6, 7:
    /// (0, 0) (0, 1) (0, 2) (0, 3)
    ///   4     5     6     7
    ///   ^
    ///   root() = 4
    ///
    /// point_in_parent(parent_region) = (row=1, col=0)
    /// ```
    pub(crate) fn point_in_parent(&self, parent_region: &Region) -> Result<Point> {
        Ok(parent_region.extent().point_of_rank(self.root())?)
    }

    /// Base rank of the tile representative in the parent region.
    ///
    /// `root()` is a dense local rank in the parent region, not directly a
    /// rank in the root/base frame. Whenever ordering metadata or
    /// root-relative membership checks need the representative in base-rank
    /// space, we convert `root()` to a point in `parent_region` and ask that
    /// region for the corresponding base rank.
    ///
    /// Example for a selected region containing parent rows `1..3`:
    ///
    /// ```text
    /// root/base ranks:
    /// row=0: 0 1
    /// row=1: 2 3
    /// row=2: 4 5
    /// row=3: 6 7
    ///
    /// selected-region local ranks -> root/base ranks:
    /// row=0: 0->2 1->3
    /// row=1: 2->4 3->5
    ///
    /// tile root() = 2
    /// point_in_parent() = (row=1, col=0)
    /// base_rank_in_parent() = 4
    /// ```
    pub(crate) fn base_rank_in_parent(&self, parent_region: &Region) -> Result<usize> {
        Ok(parent_region.base_rank_of_point(self.point_in_parent(parent_region)?)?)
    }

    /// Affine footprint of this tile expressed in the root/base-rank frame.
    ///
    /// The tile's `space` is laid out in the current region's dense local rank
    /// space. This composes that layout through `base_region` so callers can
    /// compare the tile footprint against state stored in a stable base-rank
    /// frame.
    ///
    /// ```text
    /// root/base ranks:
    /// row=0: 0 1
    /// row=1: 2 3
    /// row=2: 4 5
    /// row=3: 6 7
    ///
    /// selected-region local ranks -> root/base ranks:
    /// row=0: 0->2 1->3
    /// row=1: 2->4 3->5
    ///
    /// tile space() in selected-region local ranks:
    /// (0, 0) (0, 1)
    ///   2      3
    ///
    /// base_slice() in root/base ranks:
    /// (0, 0) (0, 1)
    ///   4      5
    /// ```
    pub(crate) fn base_slice(&self, base_region: &Region) -> Result<Slice> {
        let base_offset = base_region.slice().get(self.space().offset())?;
        let strides = self
            .space()
            .sizes()
            .iter()
            .zip(self.space().strides())
            .enumerate()
            .map(|(dim, (size, stride))| -> Result<usize> {
                if *size <= 1 {
                    let _ = stride;
                    return Ok(1);
                }

                let mut coords = vec![0; self.space().sizes().len()];
                coords[dim] = 1;
                let local_rank = self.space().location(&coords)?;
                let base_rank = base_region.slice().get(local_rank)?;
                base_rank
                    .checked_sub(base_offset)
                    .ok_or_else(|| anyhow::anyhow!("tile base-rank stride must be non-negative"))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Slice::new(
            base_offset,
            self.space().sizes().to_vec(),
            strides,
        )?)
    }
}

/// A pure [`Tile`] zipped with one concrete item for each tile cell.
///
/// [`Tile`] remains the geometry primitive: it owns the affine rank space and
/// decomposition behavior, but it does not know what occupies those ranks.
/// `MaterializedTile<T>` adds that application-level mapping. `items[i]` is
/// the item for the `i`th cell yielded by `tile.space().iter()`.
///
/// ```text
/// tile.space().iter():
/// 0 1 2 3
///
/// items:
/// A0 A1 A2 A3
///
/// zipped cells:
/// 0 -> A0
/// 1 -> A1
/// 2 -> A2
/// 3 -> A3
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct MaterializedTile<T: Clone> {
    tile: Tile,
    items: Vec<T>,
}

impl<T: Clone> MaterializedTile<T> {
    pub(crate) fn new(tile: Tile, items: Vec<T>) -> Self {
        Self { tile, items }
    }

    pub(crate) fn tile(&self) -> &Tile {
        &self.tile
    }

    /// Representative/root rank of this tile in the current domain's dense
    /// local rank space.
    pub(crate) fn root(&self) -> usize {
        self.tile.root()
    }

    /// Coordinates of the tile representative in the containing parent view.
    pub(crate) fn point_in_parent(&self, parent_view: &Region) -> Result<Point> {
        self.tile.point_in_parent(parent_view)
    }

    /// Base rank of the tile representative in the containing parent view.
    pub(crate) fn base_rank_in_parent(&self, parent_view: &Region) -> Result<usize> {
        self.tile.base_rank_in_parent(parent_view)
    }

    /// Items of the tile in dense tile-local rank order.
    pub(crate) fn items(&self) -> &[T] {
        &self.items
    }

    /// Number of dense local ranks owned by this tile.
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    /// Construct a child materialized tile represented as an affine subspace of
    /// this tile.
    ///
    /// The child tile must be a valid affine subspace of `self.tile`.
    /// The child tile's local rank order is the iteration order of its space.
    ///
    /// ```text
    /// T0 [ A0 A1 A2 A3
    ///      A4 A5 A6 A7 ]
    ///
    /// first-level child tiles rendered by tile-root rank:
    /// T1 [ A0 A1 A2 A3 ]
    /// T2 [ A4 A5 A6 A7 ]
    ///
    /// child tile:
    /// T2 [ A4 A5 A6 A7 ]
    ///
    /// subtile(child):
    /// T2 [ A4 A5 A6 A7 ]
    /// ```
    pub(crate) fn subtile(&self, tile: Tile) -> Self {
        let items = tile
            .space()
            .iter()
            .map(|root_rank| -> Result<T> {
                let parent_rank = self.tile.space().index(root_rank)?;
                self.items.get(parent_rank).cloned().ok_or_else(|| {
                    anyhow::anyhow!("missing item at parent local rank {parent_rank}")
                })
            })
            .collect::<Result<Vec<_>>>()
            .expect("child tiling must produce a valid tile subspace");
        Self { tile, items }
    }
}

/// Policy for recursively decomposing a region into tiles.
///
/// A policy defines the geometric decomposition rule. Consumers can read that
/// decomposition structurally with [`TilingPolicy::child_nodes`] or as routing
/// children with [`TilingPolicy::children`], which applies anchor contraction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
pub(crate) enum TilingPolicy {
    ReshapedHypercube { max_dimension_size: usize },
}
wirevalue::register_type!(TilingPolicy);

/// Coordinates one structural decomposition step.
///
/// A split fixes one dimension of the parent tile to one child index while
/// preserving the other dimensions. The selected index determines the child's
/// relationship to the parent:
///
/// - `index == 0`: the child is an anchor; fixing coordinate zero leaves the
///   parent root unchanged.
/// - `index > 0`: the child is a sibling; fixing a nonzero coordinate moves the
///   root to that child's offset and creates a possible communication edge.
///
/// ```text
/// parent tile, dim=0 has two rows:
/// 0 1 2 3
/// 4 5 6 7
///
/// Split { dim=0, index=1 } -> Sibling child:
/// 4 5 6 7
/// root changes 0 -> 4
///
/// Split { dim=0, index=0 } -> Anchor child:
/// 0 1 2 3
/// root stays 0
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Split {
    dim: usize,
    index: usize,
}

/// Relationship between a structural child tile and its parent.
///
/// This is the decomposition-tree label from the routing model. It is not yet
/// a communication edge: anchor children preserve the parent root and are
/// contracted away, while sibling children become send-tree edges.
///
/// ```text
/// structural children rendered by tile-root rank:
/// T0 [ A B
///      C D ] Root
/// |-- T1 [ A B ] Anchor  { dim=0, index=0 }
/// `-- T2 [ C D ] Sibling { dim=0, index=1 }
///
/// send tree after anchor contraction:
/// A
/// |-- B
/// `-- C
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TileRelation {
    /// Child at index 0 in a split dimension. Its root is the same as the
    /// parent root, so it is geometry-only and is contracted out of the
    /// communication tree.
    Anchor(Split),
    /// Child at index > 0 in a split dimension. Its root differs from the
    /// parent root, so it becomes an outgoing communication child.
    Sibling(Split),
}

/// Structural child produced by a tiling policy before anchor contraction.
///
/// A [`TileNode`] belongs to the decomposition tree. Calling
/// [`TilingPolicy::children`] projects these nodes into the send tree by
/// removing anchor self-edges.
///
/// ```text
/// decomposition tree, rendered by tile-root rank:
/// T0 [ A B
///      C D ] Root
/// |-- T1 [ A B ] Anchor  { dim=0, index=0 }
/// |   |-- T3 [ A ] Anchor  { dim=1, index=0 }
/// |   `-- T4 [ B ] Sibling { dim=1, index=1 }
/// `-- T2 [ C D ] Sibling { dim=0, index=1 }
///     |-- T5 [ C ] Anchor  { dim=1, index=0 }
///     `-- T6 [ D ] Sibling { dim=1, index=1 }
///
/// send tree:
/// A
/// |-- B
/// `-- C
///     `-- D
/// ```
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TileNode {
    tile: Tile,
    relation: TileRelation,
}

impl TilingPolicy {
    /// Construct the root tile's local affine space for this tiling policy.
    ///
    /// The input `view` says which base ranks participate. `root_space` chooses
    /// the dense local rank layout that tiling will decompose. The returned
    /// slice is always in routing-local rank space: its values are `0..n`, not
    /// base ranks from `view`.
    ///
    /// For reshaped hypercubes, the routing-local shape may differ from the
    /// input region's shape while preserving the same number of ranks. This
    /// lets routing split a long dimension into a more balanced decomposition
    /// without changing membership.
    ///
    /// ```text
    /// input view shape:
    /// 0 1 2 3 4 5 6 7
    ///
    /// with max_dimension_size = 4, routing shape can be reshaped to:
    /// 0 1 2 3
    /// 4 5 6 7
    ///
    /// root_space(view) = Slice(offset=0, sizes=[2, 4], strides=[4, 1])
    /// ```
    pub(crate) fn root_space(&self, view: &Region) -> Slice {
        match self {
            TilingPolicy::ReshapedHypercube { max_dimension_size } => {
                let reshaped =
                    reshape_routing_shape_with_limit(&Shape::from(view), *max_dimension_size);
                Slice::new_row_major(reshaped.slice().sizes().to_vec())
            }
        }
    }

    /// Materialize the immediate structural child nodes of `tile`.
    ///
    /// Structural children partition the parent tile's dense local ranks and
    /// retain their relation to the parent split. This is the decomposition
    /// tree layer from the routing model, before anchor edges are contracted.
    ///
    /// ```text
    /// structural children rendered by tile-root rank:
    /// T0 [ A B
    ///      C D ] Root
    /// |-- T1 [ A B ] Anchor  { dim=0, index=0 }
    /// `-- T2 [ C D ] Sibling { dim=0, index=1 }
    /// ```
    pub(crate) fn child_nodes(&self, tile: &Tile) -> Vec<TileNode> {
        match self {
            TilingPolicy::ReshapedHypercube { .. } => hypercube_child_nodes(tile.space()),
        }
    }

    /// Materialize the immediate communication child tiles of `tile`.
    ///
    /// This projects the decomposition tree into the send tree. A sibling
    /// child becomes a direct communication child; an anchor child is
    /// recursively decomposed and spliced out because it has the same root as
    /// its parent.
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
    /// children(tile) after contracting anchors, rendered by tile-root rank:
    /// |-- T4 [ B ]
    /// `-- T2 [ C D ]
    ///
    /// send tree:
    /// A
    /// |-- B
    /// `-- C
    ///     `-- D
    /// ```
    pub(crate) fn children(&self, tile: &Tile) -> Vec<Tile> {
        self.child_nodes(tile)
            .into_iter()
            .flat_map(|node| match node.relation {
                TileRelation::Sibling(split) => {
                    let _ = (split.dim, split.index);
                    vec![node.tile]
                }
                TileRelation::Anchor(split) => {
                    let _ = (split.dim, split.index);
                    self.children(&node.tile)
                }
            })
            .collect()
    }
}

/// Hypercube decomposition over a tile-local affine space.
/// Computes the immediate structural child nodes for this local execution
/// shape.
///
/// # Example
///
/// For shape `[2, 2]` over `[a, b, c, d]`, the structural children are:
///
/// - sibling row `[c, d]`
/// - anchor row `[a, b]`
///
/// Contracting the anchor row then exposes sibling column `[b]`, producing
/// the communication children `[c, d]` and `[b]`.
///
/// Resulting tree:
/// ```text
/// a
/// |-- c
/// |   `-- d
/// `-- b
/// ```
///
/// The process continues recursively on the anchor child for the next
/// dimension. [`Slice::select`] preserves dimensionality, so singleton
/// dimensions remain explicit in the child spaces.
fn hypercube_child_nodes(space: &Slice) -> Vec<TileNode> {
    let Some(dim) = first_non_singleton_dim(space) else {
        return vec![];
    };

    let size = space.sizes()[dim];

    let siblings = (1..size).map(|index| TileNode {
        tile: Tile::new(
            space
                .select(dim, index, index + 1, 1)
                .expect("fixing a valid dimension should produce a valid child space"),
        ),
        relation: TileRelation::Sibling(Split { dim, index }),
    });

    let anchor = {
        TileNode {
            tile: Tile::new(
                space
                    .select(dim, 0, 1, 1)
                    .expect("fixing the anchor dimension should produce a valid child space"),
            ),
            relation: TileRelation::Anchor(Split { dim, index: 0 }),
        }
    };

    siblings.chain(std::iter::once(anchor)).collect()
}

/// Return the first dimension that can still be decomposed.
///
/// Reshaped-hypercube tiling recursively fixes the first varying dimension.
/// Dimensions of size one remain in the slice shape, but they do not produce
/// children.
fn first_non_singleton_dim(space: &Slice) -> Option<usize> {
    space.sizes().iter().position(|size| *size > 1)
}

/// Apply the reshape limit used by reshaped-hypercube tiling.
fn reshape_routing_shape_with_limit(shape: &Shape, max_dim: usize) -> Shape {
    shape.reshape(Limit::from(max_dim)).shape
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ndslice::ViewExt;
    use ndslice::shape;
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

    fn collect_roots(tiling: &TilingPolicy, tile: &Tile, out: &mut Vec<usize>) {
        out.push(tile.root());
        for child in tiling.children(tile) {
            collect_roots(tiling, &child, out);
        }
    }

    fn collect_tiles(tiling: &TilingPolicy, tile: &Tile, out: &mut Vec<Tile>) {
        out.push(tile.clone());
        for child in tiling.children(tile) {
            collect_tiles(tiling, &child, out);
        }
    }

    fn validate_child_tiles_are_parent_subsets(tiling: &TilingPolicy, tile: &Tile) {
        let parent_ranks = tile.space().iter().collect::<BTreeSet<_>>();
        let mut sibling_ranks = BTreeSet::new();

        for child in tiling.children(tile) {
            let child_ranks = child.space().iter().collect::<BTreeSet<_>>();
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

    #[test]
    fn test_reshaped_hypercube_covers_each_rank_once() {
        // The recursive send tree should choose each rank as the root of
        // exactly one tile.
        //
        // ```text
        // region local ranks:
        //  0  1  2  3
        //  4  5  6  7
        //  8  9 10 11
        // 12 13 14 15
        //
        // collected tile roots, sorted:
        // 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
        // ```
        let view = Region::from(shape!(a = 4, b = 4));
        let tiling = TilingPolicy::ReshapedHypercube {
            max_dimension_size: 16,
        };
        let root = Tile::new(tiling.root_space(&view));

        let mut roots = Vec::new();
        collect_roots(&tiling, &root, &mut roots);
        roots.sort();

        assert_eq!(roots, (0..view.num_ranks()).collect::<Vec<_>>());
    }

    #[test]
    fn test_child_nodes_expose_anchor_and_sibling_relations() {
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
        let tiling = TilingPolicy::ReshapedHypercube {
            max_dimension_size: 16,
        };
        let root = Tile::new(tiling.root_space(&view));

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
                .map(|node| node.tile.space().iter().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![2, 3], vec![0, 1]],
        );
    }

    #[test]
    fn test_children_contract_anchor_nodes() {
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
        let tiling = TilingPolicy::ReshapedHypercube {
            max_dimension_size: 16,
        };
        let root = Tile::new(tiling.root_space(&view));

        assert_eq!(
            tiling
                .children(&root)
                .iter()
                .map(|tile| tile.space().iter().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![2, 3], vec![1]],
        );
    }

    #[test]
    fn test_tile_base_slice_composes_through_region() {
        // The tile is expressed in selected-region local ranks; `base_slice`
        // maps that footprint back through the selected region into root/base
        // ranks.
        //
        // ```text
        // root/base region R:
        // row=0: 0 1
        // row=1: 2 3
        // row=2: 4 5
        // row=3: 6 7
        //
        // selected region S = R[row=1..3]:
        // S row=0: 0->2 1->3
        // S row=1: 2->4 3->5
        //
        // tile space in S local ranks: [2 3]
        // base_slice in R ranks:       [4 5]
        // ```
        let root = Region::from(shape!(row = 4, col = 2));
        let view = root.range("row", ndslice::Range(1, Some(3), 1)).unwrap();
        let tile = Tile::new(Slice::new(2, vec![2], vec![1]).unwrap());

        assert_eq!(
            tile.base_slice(&view).unwrap(),
            Slice::new(4, vec![2], vec![1]).unwrap()
        );
    }

    proptest! {
        #[test]
        fn prop_recursive_tiles_cover_each_rank_once(sizes in small_shape_sizes()) {
            // Example generalized by this property:
            //
            // ```text
            // region local ranks:
            // 0 1 2
            // 3 4 5
            //
            // recursive tile roots, sorted:
            // 0 1 2 3 4 5
            // ```
            let view = Region::from(shape_from_sizes(&sizes));
            let tiling = TilingPolicy::ReshapedHypercube {
                max_dimension_size: 16,
            };
            let root = Tile::new(tiling.root_space(&view));

            let mut roots = Vec::new();
            collect_roots(&tiling, &root, &mut roots);
            roots.sort();

            prop_assert_eq!(roots, (0..view.num_ranks()).collect::<Vec<_>>());
        }

        #[test]
        fn prop_child_tiles_are_disjoint_parent_subsets(sizes in small_shape_sizes()) {
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
            let tiling = TilingPolicy::ReshapedHypercube {
                max_dimension_size: 16,
            };
            let root = Tile::new(tiling.root_space(&view));

            validate_child_tiles_are_parent_subsets(&tiling, &root);
        }

        #[test]
        fn prop_base_slice_matches_naive_region_composition(sizes in small_shape_sizes()) {
            // Example generalized by this property:
            //
            // ```text
            // root/base region R:
            // row=0: 0 1 2
            // row=1: 3 4 5
            // row=2: 6 7 8
            //
            // selected region S = R[row=1..3]:
            // S row=0: 0->3 1->4 2->5
            // S row=1: 3->6 4->7 5->8
            //
            // tile space in S local ranks: [3 4 5]
            // base_slice in R ranks:       [6 7 8]
            // ```
            let shape = shape_from_sizes(&sizes);
            let view = sliced_view(&shape);
            let tiling = TilingPolicy::ReshapedHypercube {
                max_dimension_size: 16,
            };
            let root = Tile::new(tiling.root_space(&view));

            let mut tiles = Vec::new();
            collect_tiles(&tiling, &root, &mut tiles);

            for tile in tiles {
                let base_slice = tile.base_slice(&view).unwrap();
                prop_assert_eq!(base_slice.offset(), tile.base_rank_in_parent(&view).unwrap());
                prop_assert_eq!(
                    base_slice.iter().collect::<Vec<_>>(),
                    tile.space()
                        .iter()
                        .map(|local_rank| view.slice().get(local_rank).unwrap())
                        .collect::<Vec<_>>(),
                );
            }
        }

        #[test]
        fn prop_tile_root_translates_through_region(sizes in small_shape_sizes()) {
            // Example generalized by this property:
            //
            // ```text
            // root/base region R:
            // row=0: 0 1 2
            // row=1: 3 4 5
            // row=2: 6 7 8
            //
            // selected region S = R[row=1..3]:
            // S row=0: 0->3 1->4 2->5
            // S row=1: 3->6 4->7 5->8
            //
            // tile.root() = 3
            // point_in_parent(S) = (row=1, col=0)
            // base_rank_in_parent(S) = 6
            // base_slice(S).offset() = 6
            // ```
            let shape = shape_from_sizes(&sizes);
            let view = sliced_view(&shape);
            let tiling = TilingPolicy::ReshapedHypercube {
                max_dimension_size: 16,
            };
            let root = Tile::new(tiling.root_space(&view));

            let mut tiles = Vec::new();
            collect_tiles(&tiling, &root, &mut tiles);

            for tile in tiles {
                let point_in_parent = tile.point_in_parent(&view).unwrap();
                let base_rank_in_parent = tile.base_rank_in_parent(&view).unwrap();
                let base_slice = tile.base_slice(&view).unwrap();

                prop_assert_eq!(point_in_parent.clone(), view.extent().point_of_rank(tile.root()).unwrap());
                prop_assert_eq!(base_rank_in_parent, view.base_rank_of_point(point_in_parent).unwrap());
                prop_assert_eq!(base_slice.offset(), base_rank_in_parent);
            }
        }
    }
}
