/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Property-based generators for [`Selection`] and related types.
//!
//! These strategies are used in `proptest`-based tests to construct
//! randomized selection expressions for testing evaluation, routing,
//! and normalization logic.
//!
//! The main entry point is [`gen_selection(depth)`], which generates
//! a structurally diverse [`Selection`] of bounded depth, supporting
//! the `True`, `Range`, `All`, `Union`, and `Intersection`
//! constructors.
//!
//! Example usage:
//!
//! ```
//! use ndslice::strategy::gen_selection;
//! use proptest::prelude::*;
//!
//! proptest! {
//!     #[test]
//!     fn test_selection(s in gen_selection(3)) {
//!         // Use `s` as input to evaluation or routing tests
//!     }
//! }
//! ```

use proptest::prelude::*;

use crate::Slice;
use crate::selection::EvalOpts;
use crate::selection::Selection;
use crate::selection::dsl;
use crate::shape::Range;
use crate::view::Extent;
use crate::view::Region;

/// Generates a random [`Slice`] with up to `max_dims` dimensions,
/// where each dimension has a size between 1 and `max_len`
/// (inclusive).
///
/// The slice is constructed using standard row-major layout with no
/// offset, making it suitable for use in evaluation, routing, and
/// normalization tests.
///
/// This generator is used in property-based tests to provide diverse
/// input shapes for selection and routing logic.
///
/// # Parameters
///
/// - `max_dims`: Maximum number of dimensions in the generated slice.
/// - `max_len`: Maximum size per dimension.
///
/// # Example
///
/// ```
/// use ndslice::strategy::gen_slice;
/// use proptest::prelude::*;
///
/// proptest! {
///     fn test_slice_generation(slice in gen_slice(4, 8)) {
///         assert!(!slice.sizes().is_empty());
///     }
/// }
/// ```
pub fn gen_slice(max_dims: usize, max_len: usize) -> impl Strategy<Value = Slice> {
    prop::collection::vec(1..=max_len, 1..=max_dims).prop_map(Slice::new_row_major)
}

/// Generate a random [`Extent`] with `dims` dimensions, where each
/// size is in `1..=max_len`.
///
/// For example, `gen_extent(1..=4, 1..=8)` generates extents like:
/// - x=3
/// - x=2, y=4
/// - x=2, y=4, z=1, w=5
pub fn gen_extent(
    dims: std::ops::RangeInclusive<usize>,
    max_len: usize,
) -> impl Strategy<Value = Extent> {
    prop::collection::vec(1..=max_len, dims).prop_map(|sizes| {
        let labels = (0..sizes.len())
            .map(|i| format!("d/{}", i))
            .collect::<Vec<_>>();
        Extent::new(labels, sizes).unwrap()
    })
}

/// Generate a random [`Region`] strategy for property tests.
///
/// This builds on [`gen_extent`], producing a region with the same
/// randomly chosen dimensionality and sizes, but wrapped as a full
/// [`Region`] (with labels and strides).
///
/// - `dims`: inclusive range of allowed dimensionalities (e.g.
///   `1..=4`)
/// - `max_len`: maximum size of any dimension
pub fn gen_region(
    dims: std::ops::RangeInclusive<usize>,
    max_len: usize,
) -> impl proptest::strategy::Strategy<Value = Region> {
    gen_extent(dims, max_len).prop_map(Into::into)
}

/// Generate a random [`Region`] strategy with striding for property
/// tests.
///
/// Similar to [`gen_region`], but each dimension may additionally use
/// a non-unit step. This produces regions whose underlying slice has
/// non-contiguous strides, useful for testing strided layouts.
///
/// - `dims`: inclusive range of allowed dimensionalities (e.g.
///   `1..=4`)
/// - `max_len`: maximum size of any dimension
/// - `max_step`: maximum stride step size applied to each dimension
/// - `_max_offset`: reserved for future use (currently ignored)
pub fn gen_region_strided(
    dims: std::ops::RangeInclusive<usize>,
    max_len: usize,
    max_step: usize,
    _max_offset: usize,
) -> impl Strategy<Value = Region> {
    use crate::view::ViewExt;

    prop::collection::vec(1..=max_len, dims)
        .prop_flat_map(move |sizes| {
            let n = sizes.len();
            let labels: Vec<String> = (0..n).map(|i| format!("d/{}", i)).collect();

            let steps_raw = prop::collection::vec(1..=max_step.max(1), n);
            let begins_unclamped = prop::collection::vec(proptest::num::usize::ANY, n);

            (Just((labels, sizes)), steps_raw, begins_unclamped)
        })
        .prop_map(move |((labels, sizes), steps_raw, begins_unclamped)| {
            // 1) Make steps obey the divisibility chain: step[i] %
            // step[i+1] == 0
            let mut steps = steps_raw;
            if !steps.is_empty() {
                // innermost is free
                let last = steps.len() - 1;
                steps[last] = steps[last].max(1).min(max_step.max(1));
                // Each outer step is an integer multiple of the next
                // inner step.
                for i in (0..last).rev() {
                    let inner = steps[i + 1].max(1);
                    let max_mult = (max_step / inner).max(1);
                    // Clamp current to be a multiple of `inner`
                    // within [inner, max_step].
                    let m = ((steps[i].max(1) - 1) % max_mult) + 1;
                    steps[i] = inner.saturating_mul(m);
                }
            }

            // 2) Build from a row-major region and compose per-axis
            // ranges
            let mut region: Region = Extent::new(labels.clone(), sizes.clone()).unwrap().into();
            for i in 0..sizes.len() {
                let begin = begins_unclamped[i] % sizes[i]; // in [0, size-1]
                let step = steps[i].max(1);
                region = region
                    .range(&labels[i], Range(begin, None, step))
                    .expect("range stayed rectangular");
            }
            region
        })
}

/// Generates a pair `(base, subview)` where:
/// - `base` is a randomly shaped row-major `Slice`,
/// - `subview` is a valid rectangular region within `base`.
pub fn gen_slice_and_subview(
    max_dims: usize,
    max_len: usize,
) -> impl Strategy<Value = (Slice, Slice)> {
    assert!(max_dims <= 8, "Supports up to 4 dimensions explicitly");

    gen_slice(max_dims, max_len).prop_flat_map(|base| {
        let sizes = base.sizes().to_vec();

        // Strategy per dimension
        let dim_strat = |extent| {
            if extent == 0 {
                Just((0, 0)).boxed()
            } else {
                (0..extent)
                    .prop_flat_map(move |start| {
                        (1..=extent - start).prop_map(move |len| (start, len))
                    })
                    .boxed()
            }
        };

        // Explicit match based on dims
        match sizes.len() {
            1 => (dim_strat(sizes[0]),).prop_map(|(a,)| vec![a]).boxed(),
            2 => (dim_strat(sizes[0]), dim_strat(sizes[1]))
                .prop_map(|(a, b)| vec![a, b])
                .boxed(),
            3 => (
                dim_strat(sizes[0]),
                dim_strat(sizes[1]),
                dim_strat(sizes[2]),
            )
                .prop_map(|(a, b, c)| vec![a, b, c])
                .boxed(),
            4 => (
                dim_strat(sizes[0]),
                dim_strat(sizes[1]),
                dim_strat(sizes[2]),
                dim_strat(sizes[3]),
            )
                .prop_map(|(a, b, c, d)| vec![a, b, c, d])
                .boxed(),
            5 => (
                dim_strat(sizes[0]),
                dim_strat(sizes[1]),
                dim_strat(sizes[2]),
                dim_strat(sizes[3]),
                dim_strat(sizes[4]),
            )
                .prop_map(|(a, b, c, d, e)| vec![a, b, c, d, e])
                .boxed(),
            6 => (
                dim_strat(sizes[0]),
                dim_strat(sizes[1]),
                dim_strat(sizes[2]),
                dim_strat(sizes[3]),
                dim_strat(sizes[4]),
                dim_strat(sizes[5]),
            )
                .prop_map(|(a, b, c, d, e, f)| vec![a, b, c, d, e, f])
                .boxed(),
            7 => (
                dim_strat(sizes[0]),
                dim_strat(sizes[1]),
                dim_strat(sizes[2]),
                dim_strat(sizes[3]),
                dim_strat(sizes[4]),
                dim_strat(sizes[5]),
                dim_strat(sizes[6]),
            )
                .prop_map(|(a, b, c, d, e, f, g)| vec![a, b, c, d, e, f, g])
                .boxed(),
            8 => (
                dim_strat(sizes[0]),
                dim_strat(sizes[1]),
                dim_strat(sizes[2]),
                dim_strat(sizes[3]),
                dim_strat(sizes[4]),
                dim_strat(sizes[5]),
                dim_strat(sizes[6]),
                dim_strat(sizes[7]),
            )
                .prop_map(|(a, b, c, d, e, f, g, h)| vec![a, b, c, d, e, f, g, h])
                .boxed(),
            _ => unreachable!("max_dims constrained to 8"),
        }
        .prop_map(move |ranges| {
            let (starts, lens): (Vec<_>, Vec<_>) = ranges.into_iter().unzip();
            let subview = base.subview(&starts, &lens).expect("valid subview");
            (base.clone(), subview)
        })
    })
}

/// Recursively generates a random `Selection` expression of bounded
/// depth, aligned with the given slice `shape`.
///
/// Each recursive call corresponds to one dimension of the shape,
/// starting from `dim`, and constructs a selection operator (`range`,
/// `all`, `intersection`, etc.) that applies at that level.
///
/// The recursion proceeds until either:
/// - `depth == 0`, which limits structural complexity, or
/// - `dim >= shape.len()`, which prevents exceeding the
///   dimensionality.
///
/// In both cases, the recursion terminates with a `true_()` leaf
/// node, effectively selecting all remaining elements.
///
/// The resulting selections are guaranteed to be valid under a strict
/// validation regime: they contain no empty ranges, no out-of-bounds
/// accesses, and no dynamic constructs like `any` or `first`.
pub fn gen_selection(depth: u32, shape: Vec<usize>, dim: usize) -> BoxedStrategy<Selection> {
    let leaf = Just(dsl::true_()).boxed();

    if depth == 0 || dim >= shape.len() {
        return leaf;
    }

    let recur = {
        let shape = shape.clone();
        move || gen_selection(depth - 1, shape.clone(), dim + 1)
    };

    let range_strategy = {
        let dim_size = shape[dim];
        (0..dim_size)
            .prop_flat_map(move |start| {
                let max_len = dim_size - start;
                (1..=max_len).prop_flat_map(move |len| {
                    (1..=len).prop_map(move |step| {
                        let r = Range(start, Some(start + len), step);
                        dsl::range(r, dsl::true_())
                    })
                })
            })
            .boxed()
    };

    let all = recur().prop_map(dsl::all).boxed();

    let union = (recur(), recur())
        .prop_map(|(a, b)| dsl::union(a, b))
        .boxed();

    let inter = (recur(), recur())
        .prop_map(|(a, b)| dsl::intersection(a, b))
        .boxed();

    prop_oneof![
        2 => leaf,
        3 => range_strategy,
        3 => all,
        2 => union,
        2 => inter,
    ]
    .prop_filter("valid selection", move |s| {
        let slice = Slice::new_row_major(shape.clone());
        let eval_opts = EvalOpts {
            disallow_dynamic_selections: true,
            ..EvalOpts::strict()
        };
        s.validate(&eval_opts, &slice).is_ok()
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use proptest::strategy::ValueTree;
    use proptest::test_runner::Config;
    use proptest::test_runner::TestRunner;

    use super::*;
    use crate::selection::EvalOpts;
    use crate::selection::routing::RoutingFrame;
    use crate::selection::test_utils::collect_commactor_routing_tree;
    use crate::selection::test_utils::collect_routed_paths;

    #[test]
    fn print_some_slices() {
        let mut runner = TestRunner::new(Config::default());

        for _ in 0..256 {
            let strat = gen_slice(4, 8); // up to 4 dimensions, max size per dim = 8
            let value = strat.new_tree(&mut runner).unwrap().current();
            println!("{:?}", value);
        }
    }

    proptest! {
        #[test]
        fn test_slice_properties(slice in gen_slice(4, 8)) {
            let total_size: usize = slice.sizes().iter().product();
            prop_assert!(total_size > 0);
        }
    }

    #[test]
    fn print_some_selections() {
        let mut runner = TestRunner::new(Config::default());

        for _ in 0..256 {
            let strat = gen_selection(3, vec![2, 4, 8], 0);
            let value = strat.new_tree(&mut runner).unwrap().current();
            println!("{:?}", value);
        }
    }

    // Test `trace_route` exhibits path determinism.
    //
    // This test instantiates a general theorem about the selection
    // algebra and its routing semantics:
    //
    //   ∀ `S`, `T`, and `n`,
    //     `n ∈ eval(S, slice)` ∧ `n ∈ eval(T, slice)` ⇒
    //     `route(n, S) == route(n, T)`.
    //
    // This property enables us to enforce in-order delivery using
    // only per-sender, per-peer sequence numbers. Since every message
    // to a given destination follows the same deterministic path
    // through the mesh, intermediate nodes can forward messages in
    // order, and receivers can detect missing or out-of-order
    // messages using only local state.
    //
    // This test uses `trace_route` to observe the path to each
    // overlapping destination node under `S` and `T`, asserting that
    // the results agree.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 8, ..ProptestConfig::default()
        })]
        #[test]
        fn trace_route_path_determinism(
            slice in gen_slice(4, 8)
        ) {
            let shape = slice.sizes().to_vec();

            let mut runner = TestRunner::default();
            let s = gen_selection(4, shape.clone(), 0).new_tree(&mut runner).unwrap().current();
            let t = gen_selection(4, shape.clone(), 0).new_tree(&mut runner).unwrap().current();

            let eval_opts = EvalOpts::strict();
            let sel_s: HashSet<_> = s.eval(&eval_opts, &slice).unwrap().collect();
            let sel_t: HashSet<_> = t.eval(&eval_opts, &slice).unwrap().collect();
            let ranks: Vec<_> = sel_s.intersection(&sel_t).cloned().collect();

            if ranks.is_empty() {
                println!("skipping empty intersection");
            } else {
                println!("testing {} nodes", ranks.len());
                for rank in ranks {
                    let coords = slice.coordinates(rank).unwrap();
                    let start_s = RoutingFrame::root(s.clone(), slice.clone());
                    let start_t = RoutingFrame::root(t.clone(), slice.clone());
                    let path_s = start_s.trace_route(&coords).unwrap();
                    let path_t = start_t.trace_route(&coords).unwrap();
                    prop_assert_eq!(
                        path_s.clone(),
                        path_t.clone(),
                        "path to {:?} differs under S and T\nS path: {:?}\nT path: {:?}",
                        rank, path_s, path_t
                    );
                }
            }
        }
    }

    // Test `collect_routed_paths` exhibits path determinism.
    //
    // This test instantiates the same fundamental property as the
    // `trace_route` test, but does so using `collect_routed_paths`,
    // which performs a breadth-first traversal of the routing tree.
    //
    //   ∀ `S`, `T`, and `n`,
    //     `n ∈ eval(S, slice)` ∧ `n ∈ eval(T, slice)` ⇒
    //     `route(n, S) == route(n, T)`.
    //
    // The property guarantees that every destination node reachable
    // by both `S` and `T` is routed to via the same deterministic
    // path.
    //
    // This test avoids calls to `eval` by intersecting the routed
    // destinations directly. For each rank routed to by both
    // selections, it compares the path returned by
    // `collect_routed_paths`, ensuring the selection algebra routes
    // consistently regardless of expression structure or traversal
    // order.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128, ..ProptestConfig::default()
        })]
        #[test]
        fn collect_routed_path_determinism(
            slice in gen_slice(4, 8)
        ) {
            let shape = slice.sizes().to_vec();

            let mut runner = TestRunner::default();
            let s = gen_selection(4, shape.clone(), 0).new_tree(&mut runner).unwrap().current();
            let t = gen_selection(4, shape.clone(), 0).new_tree(&mut runner).unwrap().current();

            let paths_s = collect_routed_paths(&s, &slice);
            let paths_t = collect_routed_paths(&t, &slice);
            let ranks: Vec<_> = paths_s.delivered.keys()
                .filter(|r| paths_t.delivered.contains_key(*r))
                .cloned()
                .collect();

            if ranks.is_empty() {
                println!("skipping empty intersection");
            } else {
                println!("testing {} nodes", ranks.len());
                for rank in ranks {
                    let path_s = paths_s.delivered.get(&rank).unwrap();
                    let path_t = paths_t.delivered.get(&rank).unwrap();
                    prop_assert_eq!(
                        path_s.clone(),
                        path_t.clone(),
                        "path to {:?} differs under S and T\nS path: {:?}\nT path: {:?}",
                        rank, path_s, path_t
                    );
                }
            }
        }
    }

    // Test `collect_commactor_routing_tree` exhibits path
    // determinism.
    //
    // This test instantiates the same routing path determinism
    // property as in `collect_routed_path_determinism`, but uses the
    // full CommActor-style routing simulation instead.
    //
    //   ∀ `S`, `T`, and `n`,
    //     `n ∈ eval(S, slice)` ∧ `n ∈ eval(T, slice)` ⇒
    //     `route(n, S) == route(n, T)`.
    //
    // This ensures that every destination rank reachable by both `S`
    // and `T` receives its message along the same logical path, even
    // when selection expressions differ structurally.
    //
    // The test avoids explicit calls to eval by intersecting the
    // delivered ranks from both traversals. For each rank delivered
    // to by both selections, it compares the delivery path recorded
    // in `CommActorRoutingTree::delivered`. This validates that
    // CommActor message routing is structurally deterministic.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128, ..ProptestConfig::default()
        })]
        #[test]
        fn collect_commactor_routed_path_determinism(
            slice in gen_slice(4, 8)
        ) {
            let extents = slice.sizes().to_vec();

            let mut runner = TestRunner::default();
            let s = gen_selection(4, extents.clone(), 0).new_tree(&mut runner).unwrap().current();
            let t = gen_selection(4, extents.clone(), 0).new_tree(&mut runner).unwrap().current();

            let tree_s = collect_commactor_routing_tree(&s, &slice);
            let tree_t = collect_commactor_routing_tree(&t, &slice);

            let ranks: Vec<_> = tree_s
                .delivered
                .keys()
                .filter(|r| tree_t.delivered.contains_key(*r))
                .cloned()
                .collect();

            if ranks.is_empty() {
                println!("skipping empty intersection");
            } else {
                println!("testing {} nodes", ranks.len());
                for rank in ranks {
                    let path_s = &tree_s.delivered[&rank];
                    let path_t = &tree_t.delivered[&rank];
                    prop_assert_eq!(
                        path_s.clone(),
                        path_t.clone(),
                        "path to {:?} differs under S and T\nS path: {:?}\nT path: {:?}",
                        rank, path_s, path_t
                    );
                }
            }
        }
    }

    // Property test: Unique Predecessor Theorem
    //
    // This test verifies a structural invariant of the routing graph
    // produced by `collect_routed_paths`, which performs a
    // breadth-first traversal of a selection over a multidimensional
    // mesh.
    //
    // ───────────────────────────────────────────────────────────────
    // Unique Predecessor Theorem
    //
    // In a full routing traversal, each coordinate `x` is the target
    // of at most one `RoutingStep::Forward` from a distinct
    // coordinate `y ≠ x`.
    //
    // Any additional frames that reach `x` arise only from:
    //   - self-forwarding (i.e., `x → x`)
    //   - structural duplication from the same parent node (e.g., via
    //     unions)
    //
    // This ensures that routing paths form a tree-like structure
    // rooted at the origin, with no multiple distinct predecessors
    // except in the degenerate (self-loop) or duplicated-parent
    // cases.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256, ..ProptestConfig::default()
        })]
        #[test]
        fn collect_routed_paths_unique_predecessor(
            slice in gen_slice(4, 8)
        ) {
            let shape = slice.sizes().to_vec();

            let mut runner = TestRunner::default();
            let s = gen_selection(4, shape.clone(), 0).new_tree(&mut runner).unwrap().current();

            let tree = collect_routed_paths(&s, &slice);

            for (node, preds) in tree.predecessors {
                let non_self_preds: Vec<_> = preds.clone().into_iter()
                    .filter(|&p| p != node)
                    .collect();

                prop_assert!(
                    non_self_preds.len() <= 1,
                    "Node {} had multiple non-self predecessors: {:?} (selection: {})",
                    node,
                    non_self_preds,
                    s,
                );
            }
        }
    }

    // Property test: Unique Predecessor Theorem (CommActor Routing)
    //
    // This test verifies structural invariants of the routing graph
    // produced by `collect_commactor_routing_tree`, which simulates
    // CommActor-style peer-to-peer multicast forwarding.
    //
    // ───────────────────────────────────────────────────────────────
    // Unique Predecessor Theorem
    //
    // In a full routing traversal, each coordinate `x` is the target
    // of at most one `RoutingStep::Forward` from a distinct
    // coordinate `y ≠ x`.
    //
    // Any additional frames that reach `x` arise only from:
    //   - structural duplication from the same parent node (e.g., via
    //     unions)
    //
    // Unlike the general `collect_routed_paths`, CommActor routing
    // never performs self-forwarding (`x → x`). This test confirms
    // that as well.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256, ..ProptestConfig::default()
        })]
        #[test]
        fn commactor_routed_paths_unique_predecessor(
            slice in gen_slice(4, 8)
        ) {
            let shape = slice.sizes().to_vec();

            let mut runner = TestRunner::default();
            let s = gen_selection(4, shape.clone(), 0).new_tree(&mut runner).unwrap().current();

            let tree = collect_commactor_routing_tree(&s, &slice);

            let mut preds: HashMap<usize, HashSet<usize>> = HashMap::new();

            for (from, frames) in &tree.forwards {
                for frame in frames {
                    let to = slice.location(&frame.here).unwrap();

                    // We assert that a CommActor never forwards to
                    // itself.
                    prop_assert_ne!(
                        *from, to,
                        "CommActor forwarded to itself: {} → {} (selection: {})",
                        from, to, s
                    );

                    preds.entry(to).or_default().insert(*from);
                }
            }

            for (node, parents) in preds {
                let non_self_preds: Vec<_> = parents.into_iter()
                    .filter(|&p| p != node)
                    .collect();

                prop_assert!(
                    non_self_preds.len() <= 1,
                    "Node {} had multiple non-self predecessors: {:?} (selection: {})",
                    node,
                    non_self_preds,
                    s,
                );
            }
        }
    }

    // Theorem (Subview-Coordinate Inclusion):
    //
    // For any rectangular subview `V` of a base slice `B`, each
    // coordinate `v` in `V` maps to a coordinate `b = view_offset +
    // v` that must be in `B`.
    //
    // This test verifies that all coordinates of a generated subview
    // are valid when translated back into the coordinate system of
    // the base slice.
    proptest! {
        #[test]
        fn test_gen_slice_and_subview((base, subview) in gen_slice_and_subview(4, 8)) {
            for idx in subview.iter() {
                let v = subview.coordinates(idx).unwrap();
                let view_offset_in_base = base.coordinates(subview.offset()).unwrap();

                // b = view_offset + v
                let b: Vec<_> = v.iter()
                    .zip(&view_offset_in_base)
                    .map(|(sub_c, offset)| sub_c + offset)
                    .collect();

                assert!(base.location(&b).is_ok());
            }
        }
    }

    // Coordinate–Rank Isomorphism for Extents

    // Theorem 1: Rank is injective on valid points
    //
    // For a given Extent, every distinct coordinate (i.e. Point)
    // maps to a unique rank.
    //
    // ∀ p ≠ q ∈ extent.iter(),  p.rank() ≠ q.rank()
    proptest! {
      #[test]
      fn rank_is_injective(extent in gen_extent(1..=4, 8)) {
        let mut seen = HashSet::new();
        for point in extent.points() {
          let rank = point.rank();
          prop_assert!(
            seen.insert(rank),
            "Duplicate rank {} for point {}",
            rank,
            point
          );
        }
      }
    }

    // Theorem 2: Row-major monotonicity
    //
    // The rank function is monotonic in lexicographic (row-major)
    // coordinate order.
    //
    // ∀ p, q ∈ ℕᵈ, p ≺ q ⇒ rank(p) < rank(q)
    proptest! {
      #[test]
      fn rank_is_monotonic(extent in gen_extent(1..=4, 8)) {
        let mut last_rank = None;
        for point in extent.points() {
          let rank = point.rank();
          if let Some(prev) = last_rank {
            prop_assert!(prev < rank, "Rank not monotonic: {} >= {}", prev, rank);
          }
          last_rank = Some(rank);
        }
      }
    }

    // Theorem 3: Rank bounds
    //
    // For any point p in extent E, the rank of p is in the range:
    //     0 ≤ rank(p) < E.len()
    //
    // ∀ p ∈ E, 0 ≤ rank(p) < |E|
    proptest! {
      #[test]
      fn rank_bounds(extent in gen_extent(1..=4, 8)) {
        let len = extent.num_ranks();
        for point in extent.points() {
          let rank = point.rank();
          prop_assert!(rank < len, "Rank {} out of bounds for extent of size {}", rank, len);
        }
      }
    }

    // Theorem 4: Isomorphism (Rank-point round-trip is identity on
    // all ranks)
    //
    // For every valid rank r ∈ [0, extent.len()), converting it to a
    // point and back gives the same rank:
    //
    //     rank(point_of_rank(r)) = r
    //
    // In categorical terms: rank ∘ point_of_rank = 𝟙
    proptest! {
        #[test]
        fn rank_point_trip(extent in gen_extent(1..=4, 8)) {
            for r in 0..extent.num_ranks() {
                let point = extent.point_of_rank(r).unwrap();
                prop_assert_eq!(
                    point.rank(),
                    r,
                    "point_of_rank({}) returned {}, which maps to rank {}",
                    r,
                    point,
                    point.rank()
                );
            }
        }
    }

    // Theorem 5: Isomorphism (Point–Coords–Rank round-trip is
    // identity on all points)
    //
    // For every point p ∈ extent.points(), converting its coordinates
    // back to a rank yields the same rank:
    //
    //     rank_of_coords(coords(p)) = rank(p)
    //
    // In categorical terms: rank_of_coords ∘ coords = rank
    proptest! {
        #[test]
        fn coords_to_rank_roundtrip(extent in gen_extent(0..=4, 8)) {
            for p in extent.points() {
                let c = p.coords();
                let r = extent.rank_of_coords(&c).expect("coords from Point must be valid");
                prop_assert_eq!(r, p.rank(), "coords->rank mismatch for {}", p);
            }
        }
    }
}
