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
//! being routed; child tiles are affine subspaces produced from that same
//! frame. There is no separate route-local rank frame to translate out of.
//!
//! Decomposition first produces structural [`TileNode`]s: children labeled by
//! their relationship to the parent split. Anchor children preserve the parent
//! representative and sibling children introduce a distinct representative.
//! Communication children are then derived by contracting anchor edges and
//! keeping sibling edges.
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

#![allow(dead_code)]

use std::collections::HashMap;
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
/// The tile's [`Slice`] is an affine footprint in the parent view's rank
/// frame. Its values are the ranks being routed, not a re-baselined `0..n`
/// coordinate system. The tile root is the representative of that footprint.
///
/// Root tiles should be constructed with [`Tile::from_view`]. Descendant tiles
/// should be produced by a [`Tiling`] implementation.
///
/// ```text
/// parent view ranks:
/// 0 1 2 3
/// 4 5 6 7
///
/// bottom-row child tile in the parent view's coordinate/rank frame:
/// (1, 0) (1, 1) (1, 2) (1, 3)
///   4      5      6      7
///
/// child.root_rank() = 4
/// child.space().iter() = 4 5 6 7
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
pub(crate) struct Tile(Slice);
wirevalue::register_type!(Tile);

impl Tile {
    /// Construct the root tile for a view.
    ///
    /// The root tile is exactly the view's affine slice. This preserves the
    /// view's offset and strides so descendants remain in the same rank frame.
    pub(crate) fn from_view(view: &Region) -> Self {
        Self(view.slice().clone())
    }

    fn from_space(space: Slice) -> Self {
        Self(space)
    }

    /// Affine tile footprint in the parent view's rank frame.
    pub(crate) fn space(&self) -> &Slice {
        &self.0
    }

    /// Representative/root rank of this tile in the parent view's rank frame.
    pub(crate) fn root_rank(&self) -> usize {
        self.space().offset()
    }
}

/// A pure [`Tile`] zipped with one concrete item for each tile cell.
///
/// [`Tile`] remains the geometry primitive: it owns the affine rank space and
/// decomposition behavior, but it does not know what occupies those ranks.
/// `MaterializedTile<T>` adds a rank-addressed item map. Items are looked up by
/// the base rank carried by the tile's affine slice; there is no separate
/// tile-local indexing frame.
///
/// ```text
/// tile.space().iter():
/// 4 5 6 7
///
/// items_by_rank:
/// 4 -> A4
/// 5 -> A5
/// 6 -> A6
/// 7 -> A7
///
/// representative_item() = item_at(4) = A4
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
    pub(crate) fn new(tile: Tile, items: Vec<T>) -> Self {
        assert_eq!(tile.space().len(), items.len());
        let items_by_rank = tile.space().iter().zip(items).collect();
        Self {
            tile,
            items_by_rank: Arc::new(items_by_rank),
        }
    }

    pub(crate) fn tile(&self) -> &Tile {
        &self.tile
    }

    /// Representative/root rank of this tile in the parent view's rank frame.
    pub(crate) fn root_rank(&self) -> usize {
        self.tile.root_rank()
    }

    /// Item stored at `rank`, if this materialized tile knows that rank.
    pub(crate) fn item_at(&self, rank: usize) -> Option<&T> {
        if !self.tile.space().contains(rank) {
            return None;
        }
        self.items_by_rank.get(&rank)
    }

    /// Item stored at this tile's representative/root rank.
    pub(crate) fn representative_item(&self) -> Option<&T> {
        self.item_at(self.root_rank())
    }

    /// Items of the tile in affine tile iteration order.
    pub(crate) fn items(&self) -> impl Iterator<Item = &T> {
        self.tile.space().iter().map(|rank| {
            self.item_at(rank)
                .expect("materialized tile must contain every rank in its tile")
        })
    }

    /// Number of ranks owned by this tile.
    pub(crate) fn len(&self) -> usize {
        self.tile.space().len()
    }

    /// Construct a child materialized tile represented as an affine subspace of
    /// this tile.
    ///
    /// The child tile's space is expressed in the same rank frame as the
    /// parent tile. `subtile` keeps the same rank-addressed item map, so it is
    /// O(1) and avoids translating child ranks through tile-local indices.
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
    /// tile.space().iter() = 4 5 6 7
    /// representative_item() = item_at(4) = A4
    /// ```
    pub(crate) fn subtile(&self, tile: Tile) -> Self {
        debug_assert!(
            tile.space()
                .iter()
                .all(|rank| self.items_by_rank.contains_key(&rank))
        );
        Self {
            tile,
            items_by_rank: Arc::clone(&self.items_by_rank),
        }
    }
}

/// Open interface for recursively decomposing tiles.
pub(crate) trait Tiling {
    /// Materialize the immediate structural child nodes of `tile`.
    fn child_nodes(&self, tile: &Tile) -> Vec<TileNode>;

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

/// Block-partitioning tiler.
///
/// The tiler recursively fixes the first varying dimension. Child at index `0`
/// is an anchor because it preserves the parent root; children at index `> 0`
/// are siblings because they introduce new roots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockPartitioning;

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

/// Coordinates one structural decomposition step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Split {
    dim: usize,
    index: usize,
}

/// Relationship between a structural child tile and its parent.
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

/// Structural child produced by a tiler before anchor contraction.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TileNode {
    tile: Tile,
    relation: TileRelation,
}

/// Return the first dimension that can still be decomposed.
fn first_non_singleton_dim(space: &Slice) -> Option<usize> {
    space.sizes().iter().position(|size| *size > 1)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ndslice::Shape;
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

    fn collect_roots<T: Tiling>(tiling: &T, tile: &Tile, out: &mut Vec<usize>) {
        out.push(tile.root_rank());
        for child in tiling.children(tile) {
            collect_roots(tiling, &child, out);
        }
    }

    fn validate_child_tiles_are_parent_subsets<T: Tiling>(tiling: &T, tile: &Tile) {
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
        let tiling = BlockPartitioning;
        let root = Tile::from_view(&view);

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
    fn test_sliced_view_tiles_stay_in_affine_frame() {
        // Root tile construction preserves the sliced view's affine offset.
        // Children are produced in the same rank frame.
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
        assert_eq!(tile.space().iter().collect::<Vec<_>>(), vec![2, 3, 4, 5]);
        assert_eq!(
            view.point_of_base_rank(tile.root_rank()).unwrap(),
            view.extent().point(vec![0, 0]).unwrap(),
        );
        assert_eq!(
            tiling
                .children(&tile)
                .iter()
                .map(|child| child.space().iter().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![4, 5], vec![3]],
        );
    }

    #[test]
    fn test_materialized_tile_is_rank_addressed() {
        // `MaterializedTile` stores items by the base ranks in its tile rather
        // than by a rebased tile-local position. A subtile keeps the same map
        // and narrows only the affine tile footprint.
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
        // child.representative_item() = A4
        // child.items() = A4 A5 A6 A7
        // ```
        let view = Region::from(shape!(row = 2, col = 4));
        let root = Tile::from_view(&view);
        let materialized = MaterializedTile::new(
            root.clone(),
            root.space().iter().map(|rank| format!("A{rank}")).collect(),
        );
        let child =
            materialized.subtile(Tile::from_space(root.space().select(0, 1, 2, 1).unwrap()));

        assert_eq!(materialized.item_at(4).map(String::as_str), Some("A4"));
        assert_eq!(child.representative_item().map(String::as_str), Some("A4"),);
        assert_eq!(
            child.items().map(String::as_str).collect::<Vec<_>>(),
            vec!["A4", "A5", "A6", "A7"],
        );
    }

    proptest! {
        #[test]
        fn prop_recursive_tiles_cover_each_rank_once(sizes in small_shape_sizes()) {
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
            let tiling = BlockPartitioning;
            let root = Tile::from_view(&view);

            validate_child_tiles_are_parent_subsets(&tiling, &root);
        }

        #[test]
        fn prop_sliced_view_tiles_remain_in_view_frame(sizes in small_shape_sizes()) {
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
}
