/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # Routing
//!
//! This module defines [`RoutingFrame`] and its [`next_steps`]
//! method, which model how messages propagate through a
//! multidimensional mesh based on a [`Selection`] expression.
//!
//! A [`RoutingFrame`] represents the state of routing at a particular
//! point in the mesh. It tracks the current coordinate (`here`), the
//! remaining selection to apply (`selection`), the mesh layout
//! (`slice`), and the current dimension of traversal (`dim`).
//!
//! [`next_steps`] defines a routing-specific evaluation
//! strategy for `Selection`. Unlike [`Selection::eval`], which
//! produces flat indices that match a selection, this method produces
//! intermediate routing states — new frames or deferred steps to
//! continue traversing.
//!
//! Rather than returning raw frames directly, [`next_steps`]
//! produces a stream of [`RoutingStep`]s via a callback — each
//! representing a distinct kind of routing progression:
//!
//! - [`RoutingStep::Forward`] indicates that routing proceeds
//!   deterministically to a new [`RoutingFrame`] — the next
//!   coordinate is fully determined by the current selection and
//!   frame state.
//!
//! - [`RoutingStep::Choice`] represents a deferred decision: it
//!   returns a set of admissible indices, and **the caller must
//!   select one** (e.g., for load balancing or policy-based routing)
//!   **before routing can proceed**.
//!
//! In this way, non-determinism is treated as a **first-class,
//! policy-driven** aspect of the routing system — enabling
//! inspection, customization, and future extensions without
//! complicating the core traversal logic.
//!
//! A frame is considered a delivery target if its selection is
//! [`Selection::True`] and all dimensions have been traversed, as
//! determined by [`RoutingFrame::deliver_here`]. All other frames are
//! forwarded further using [`RoutingFrame::should_route`].
//!
//! This design enables **compositional**, **local**, and **scalable**
//! routing:
//! - **Compositional**: complex selection expressions decompose into
//!   simpler, independently evaluated sub-selections.
//! - **Local**: each frame carries exactly the state needed for its
//!   next step — no global coordination or lookahead is required.
//! - **Scalable**: routing unfolds recursively, one hop at a time,
//!   allowing for efficient traversal even in high-dimensional spaces.
//!
//! This module provides the foundation for building structured,
//! recursive routing logic over multidimensional coordinate spaces.
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Write;
use std::hash::Hash;
use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::Result;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::SliceError;
use crate::selection::NormalizedSelectionKey;
use crate::selection::Selection;
use crate::selection::Slice;

/// Represents the outcome of evaluating a routing step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingAction {
    Deliver,
    Forward,
}

/// `RoutingFrame` captures the state of a selection being evaluated:
/// the current coordinate (`here`), the remaining selection to apply,
/// the shape and layout information (`slice`), and the current
/// dimension (`dim`).
///
/// Each frame represents an independent routing decision and produces
/// zero or more new frames via `next_steps`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingFrame {
    /// The current coordinate in the mesh where this frame is being
    /// evaluated.
    ///
    /// This is the source location for the next routing step.
    pub here: Vec<usize>,

    /// The residual selection expression describing where routing
    /// should continue.
    ///
    /// At each step, only the current dimension (tracked by `dim`) of
    /// this selection is considered.
    pub selection: Selection,

    /// The shape and layout of the full multidimensional space being
    /// routed.
    ///
    /// This determines the bounds and stride information used to
    /// compute coordinates and flat indices.
    pub slice: Arc<Slice>,

    /// The current axis of traversal within the selection and slice.
    ///
    /// Routing proceeds dimension-by-dimension; this value tracks how
    /// many dimensions have already been routed.
    pub dim: usize,
}

// Compile-time check: ensure `RoutingFrame` is thread-safe and fully
// owned.
fn _assert_routing_frame_traits()
where
    RoutingFrame: Send + Sync + Serialize + DeserializeOwned + 'static,
{
}

/// A `RoutingStep` represents a unit of progress in the routing
/// process.
///
/// Emitted by [`RoutingFrame::next_steps`], each step describes
/// how routing should proceed from a given frame:
///
/// - [`RoutingStep::Forward`] represents a deterministic hop to the
///   next coordinate in the mesh, with an updated [`RoutingFrame`].
///
/// - [`RoutingStep::Choice`] indicates that routing cannot proceed
///   until the caller selects one of several admissible indices. This
///   allows for policy-driven or non-deterministic routing behavior,
///   such as load balancing.
#[derive(Debug, Clone, EnumAsInner)]
pub enum RoutingStep {
    /// A deterministic routing hop to the next coordinate. Carries an
    /// updated [`RoutingFrame`] describing the new position and
    /// residual selection.
    Forward(RoutingFrame),

    /// A deferred routing decision at the current dimension. Contains
    /// a set of admissible indices and a residual [`RoutingFrame`] to
    /// continue routing once a choice is made.
    Choice(Choice),
}

/// A deferred routing decision as contained in a
/// [`RoutingStep::Choice`].
///
/// A `Choice` contains:
/// - `candidates`: the admissible indices at the current dimension
/// - `frame`: the residual [`RoutingFrame`] describing how routing
///   continues once a choice is made
///
/// To continue routing, the caller must select one of the
/// `candidates` and call [`Choice::choose`] to produce the
/// corresponding [`RoutingStep::Forward`].
#[derive(Debug, Clone)]
pub struct Choice {
    pub(crate) candidates: Vec<usize>,
    pub(crate) frame: RoutingFrame,
}

impl Choice {
    /// Returns the list of admissible indices at the current
    /// dimension.
    ///
    /// These represent the valid choices that the caller can select
    /// from when resolving this deferred routing step.
    pub fn candidates(&self) -> &[usize] {
        &self.candidates
    }

    /// Returns a reference to the residual [`RoutingFrame`]
    /// associated with this choice.
    ///
    /// This frame encodes the selection and mesh context to be used
    /// once a choice is made, and routing continues at the next
    /// dimension.
    pub fn frame(&self) -> &RoutingFrame {
        &self.frame
    }

    /// Resolves the choice by selecting a specific index.
    ///
    /// Constrains the residual selection to the chosen index at the
    /// current dimension and returns a [`RoutingStep::Forward`] for
    /// continued routing.
    pub fn choose(self, index: usize) -> RoutingStep {
        // The only thing `next()` has to do is constrain the
        // selection to a concrete choice at the current dimension.
        // `self.frame.selection` is the residual (inner) selection to
        // be applied *past* the current dimension.
        RoutingStep::Forward(RoutingFrame {
            selection: crate::dsl::range(index..=index, self.frame.selection),
            ..self.frame
        })
    }
}

/// Key used to deduplicate routing frames.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct RoutingFrameKey {
    here: Vec<usize>,
    dim: usize,
    selection: NormalizedSelectionKey,
}

impl RoutingFrameKey {
    /// Constructs a `RoutingFrameKey` from a `RoutingFrame`.
    ///
    /// This key uniquely identifies a routing frame by its coordinate
    /// (`here`), current dimension, and normalized selection. It is
    /// used during traversal for purposes such as deduplication and
    /// memoization.
    pub fn new(frame: &RoutingFrame) -> Self {
        Self {
            here: frame.here.clone(),
            dim: frame.dim,
            selection: NormalizedSelectionKey::new(&frame.selection),
        }
    }
}

impl RoutingFrame {
    /// Constructs the initial frame at the root coordinate (all
    /// zeros). Selections are expanded as necessary to ensure they
    /// have depth equal to the slice dimensionality. See the docs for
    /// `canonicalize_to_dimensions` for the rules.
    ///
    /// ### Canonical Handling of Zero-Dimensional Slices
    ///
    /// A `Slice` with zero dimensions represents the empty product
    /// `∏_{i=1}^{0} Xᵢ`, which has exactly one element: the empty
    /// tuple. To maintain uniform routing semantics, we canonically
    /// embed such 0D slices as 1D slices of extent 1:
    ///
    /// ```text
    /// Slice::new(offset, [1], [1])
    /// ```
    ///
    /// This embedding preserves the correct number of addressable
    /// points and allows the routing machinery to proceed through the
    /// usual recursive strategy without introducing special cases. The
    /// selected coordinate is `vec![0]`, and `dim = 0` proceeds as
    /// usual. This makes the routing logic consistent with evaluation
    /// and avoids edge case handling throughout the codebase.
    pub fn root(selection: Selection, slice: Slice) -> Self {
        // Canonically embed 0D as 1D (extent 1).
        let slice = if slice.num_dim() > 0 {
            Arc::new(slice)
        } else {
            Arc::new(Slice::new(slice.offset(), vec![1], vec![1]).unwrap())
        };
        let n = slice.num_dim();
        RoutingFrame {
            here: vec![0; n],
            selection: selection.canonicalize_to_dimensions(n),
            slice,
            dim: 0,
        }
    }

    /// Produces a new frame advanced to the next dimension with
    /// updated position and selection.
    pub fn advance(&self, here: Vec<usize>, selection: Selection) -> Self {
        RoutingFrame {
            here,
            selection,
            slice: Arc::clone(&self.slice),
            dim: self.dim + 1,
        }
    }

    /// Returns a new frame with the same position and dimension but a
    /// different selection.
    pub fn with_selection(&self, selection: Selection) -> Self {
        RoutingFrame {
            here: self.here.clone(),
            selection,
            slice: Arc::clone(&self.slice),
            dim: self.dim,
        }
    }

    /// Determines the appropriate routing action for this frame.
    ///
    /// Returns [`RoutingAction::Deliver`] if the message should be
    /// delivered at this coordinate, or [`RoutingAction::Forward`] if
    /// it should be routed further.
    pub fn action(&self) -> RoutingAction {
        if self.deliver_here() {
            RoutingAction::Deliver
        } else {
            RoutingAction::Forward
        }
    }

    /// Returns the location of this frame in the underlying slice.
    pub fn location(&self) -> Result<usize, SliceError> {
        self.slice.location(&self.here)
    }
}

impl RoutingFrame {
    /// Visits the next routing steps from this frame using a
    /// callback-based traversal.
    /// This method structurally recurses on the [`Selection`]
    /// expression, yielding [`RoutingStep`]s via the `f` callback.
    /// Early termination is supported via [`ControlFlow::Break`].
    ///
    /// Compared to the (old, now removed) [`next_steps`] method, this
    /// avoids intermediate allocation, supports interruptibility, and
    /// allows policy-driven handling of [`RoutingStep::Choice`]s via
    /// the `chooser` callback.
    ///
    /// ---
    ///
    /// ### Traversal Strategy
    ///
    /// The traversal proceeds **dimension-by-dimension**,
    /// structurally mirroring the shape of the selection expression:
    ///
    /// - [`Selection::All`] and [`Selection::Range`] iterate over a
    ///   range of coordinates, emitting one [`RoutingStep::Forward`]
    ///   per valid index.
    /// - [`Selection::Union`] and [`Selection::Intersection`] recurse
    ///   into both branches. Intersection steps are joined at matching
    ///   coordinates and residual selections are reduced.
    /// - [`Selection::Any`] randomly selects one index along the
    ///   current dimension and emits a single step.
    /// - [`Selection::True`] and [`Selection::False`] emit no steps.
    ///
    /// At each step, only the current dimension (tracked via `self.dim`)
    /// is evaluated. Future dimensions remain untouched until deeper
    /// recursion.
    ///
    /// ---
    ///
    /// ### Evaluation Semantics
    ///
    /// - **Selection::True**
    ///   No further routing is performed — if this frame is at the
    ///   final dimension, delivery occurs.
    /// - **Selection::False**
    ///   No match — routing halts.
    ///
    /// - **Selection::All / Selection::Range**
    ///   Emits one [`RoutingStep::Forward`] per matching index, each
    ///   advancing to the next dimension with the inner selection.
    ///
    /// - **Selection::Union**
    ///   Evaluates both branches independently and emits all
    ///   resulting steps.
    ///
    /// - **Selection::Intersection**
    ///   Emits only those steps where both branches produce the same
    ///   coordinate, combining the residual selections at that point.
    ///
    /// - **Selection::Any**
    ///   Randomly selects one index and emits a single
    ///   [`RoutingStep::Forward`].
    ///
    /// - **Selection::Choice**
    ///   Defers decision to the caller by invoking the `chooser`
    ///   function, which resolves the candidate index.
    ///
    /// ---
    ///
    /// ### Delivery Semantics
    ///
    /// Message delivery is determined by
    /// [`RoutingFrame::deliver_here`], which returns true when:
    ///
    /// - The frame’s selection is [`Selection::True`], and
    /// - All dimensions have been traversed (`dim ==
    ///   slice.num_dim()`).
    ///
    /// ---
    ///
    /// ### Panics
    ///
    /// Panics if `slice.num_dim() == 0`. Use a canonical embedding
    /// (e.g., 0D → 1D) before calling this (see e.g.
    /// `RoutingFrame::root`).
    ///
    /// ---
    ///
    /// ### Summary
    ///
    /// - **Structure-driven**: Mirrors the shape of the selection
    ///   expression.
    /// - **Compositional**: Each variant defines its own traversal
    ///   behavior.
    /// - **Interruptible**: Early termination is supported via
    ///   [`ControlFlow`].
    /// - **Minimally allocating**: Avoids intermediate buffers in
    ///   most cases; only [`Selection::Intersection`] allocates
    ///   temporary state for pairwise matching.
    /// - **Policy-ready**: Integrates with runtime routing policies
    ///   via the `chooser`.
    pub fn next_steps(
        &self,
        _chooser: &mut dyn FnMut(&Choice) -> usize,
        f: &mut dyn FnMut(RoutingStep) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        assert!(self.slice.num_dim() > 0, "next_steps requires num_dims > 0");

        match &self.selection {
            Selection::True => ControlFlow::Continue(()),
            Selection::False => ControlFlow::Continue(()),
            Selection::All(inner) => {
                let size = self.slice.sizes()[self.dim];
                for i in 0..size {
                    let mut coord = self.here.clone();
                    coord[self.dim] = i;
                    let frame = self.advance(coord, (**inner).clone());
                    if let ControlFlow::Break(_) = f(RoutingStep::Forward(frame)) {
                        return ControlFlow::Break(());
                    }
                }
                ControlFlow::Continue(())
            }

            Selection::Range(range, inner) => {
                let size = self.slice.sizes()[self.dim];
                let (min, max, step) = range.resolve(size);

                for i in (min..max).step_by(step) {
                    let mut coord = self.here.clone();
                    coord[self.dim] = i;
                    let frame = self.advance(coord, (**inner).clone());
                    if let ControlFlow::Break(_) = f(RoutingStep::Forward(frame)) {
                        return ControlFlow::Break(());
                    }
                }

                ControlFlow::Continue(())
            }

            Selection::Any(inner) => {
                let size = self.slice.sizes()[self.dim];
                if size == 0 {
                    return ControlFlow::Continue(());
                }

                use rand::Rng;
                let mut rng: rand::prelude::ThreadRng = rand::rng();
                let i = rng.random_range(0..size);
                let mut coord = self.here.clone();
                coord[self.dim] = i;
                let frame = self.advance(coord, (**inner).clone());
                f(RoutingStep::Forward(frame))
            }

            Selection::Union(a, b) => {
                if let ControlFlow::Break(_) =
                    self.with_selection((**a).clone()).next_steps(_chooser, f)
                {
                    return ControlFlow::Break(());
                }
                self.with_selection((**b).clone()).next_steps(_chooser, f)
            }

            Selection::Intersection(a, b) => {
                let mut left = vec![];
                let mut right = vec![];

                let mut collect_left = |step: RoutingStep| {
                    if let RoutingStep::Forward(frame) = step {
                        left.push(frame);
                    }
                    ControlFlow::Continue(())
                };
                let mut collect_right = |step: RoutingStep| {
                    if let RoutingStep::Forward(frame) = step {
                        right.push(frame);
                    }
                    ControlFlow::Continue(())
                };

                self.with_selection((**a).clone())
                    .next_steps(_chooser, &mut collect_left)?;
                self.with_selection((**b).clone())
                    .next_steps(_chooser, &mut collect_right)?;

                for fa in &left {
                    for fb in &right {
                        if fa.here == fb.here {
                            let residual = fa
                                .selection
                                .clone()
                                .reduce_intersection(fb.selection.clone());
                            let frame = self.advance(fa.here.clone(), residual);
                            if let ControlFlow::Break(_) = f(RoutingStep::Forward(frame)) {
                                return ControlFlow::Break(());
                            }
                        }
                    }
                }

                ControlFlow::Continue(())
            }

            // TODO(SF, 2025-04-30): This term is not in the algebra
            // yet.
            // Selection::LoadBalanced(inner) => {
            //     let size = self.slice.sizes()[self.dim];
            //     if size == 0 {
            //         ControlFlow::Continue(())
            //     } else {
            //         let candidates = (0..size).collect();
            //         let choice = Choice {
            //             candidates,
            //             frame: self.with_selection((*inner).clone()),
            //         };
            //         let index = chooser(&choice);
            //         f(choice.choose(index))
            //     }
            // }

            // Catch-all for future combinators (e.g., Label).
            _ => unimplemented!(),
        }
    }

    /// Returns true if this frame represents a terminal delivery
    /// point — i.e., the selection is `True` and all dimensions have
    /// been traversed.
    pub fn deliver_here(&self) -> bool {
        matches!(self.selection, Selection::True) && self.dim == self.slice.num_dim()
    }

    /// Returns true if the message has not yet reached its final
    /// destination and should be forwarded to the next routing step.
    pub fn should_route(&self) -> bool {
        !self.deliver_here()
    }
}

impl RoutingFrame {
    /// Traces the unique routing path to the given destination
    /// coordinate.
    ///
    /// Returns `Some(vec![root, ..., dest])` if `dest` is selected,
    /// or `None` if not.
    pub fn trace_route(&self, dest: &[usize]) -> Option<Vec<Vec<usize>>> {
        use std::collections::HashSet;
        use std::ops::ControlFlow;

        use crate::selection::routing::RoutingFrameKey;

        fn go(
            frame: RoutingFrame,
            dest: &[usize],
            mut path: Vec<Vec<usize>>,
            seen: &mut HashSet<RoutingFrameKey>,
        ) -> Option<Vec<Vec<usize>>> {
            let key = RoutingFrameKey::new(&frame);
            if !seen.insert(key) {
                return None;
            }

            path.push(frame.here.clone());

            if frame.deliver_here() && frame.here == dest {
                return Some(path);
            }

            let mut found = None;
            let _ = frame.next_steps(
                &mut |_| panic!("Choice encountered in trace_route"),
                &mut |step: RoutingStep| {
                    let next = step.into_forward().unwrap();
                    if let Some(result) = go(next, dest, path.clone(), seen) {
                        found = Some(result);
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                },
            );

            found
        }

        let mut seen = HashSet::new();
        go(self.clone(), dest, Vec::new(), &mut seen)
    }
}

/// Formats a routing path as a string, showing each hop in order.
///
/// Each line shows the hop index, an arrow (`→` for intermediate
/// steps, `⇨` for the final destination), and the coordinate as a
/// tuple (e.g., `(0, 1)`).
/// # Example
///
/// ```text
///  0 → (0, 0)
///  1 → (0, 1)
///  2 ⇨ (1, 1)
/// ```
#[track_caller]
#[allow(dead_code)]
pub fn format_route(route: &[Vec<usize>]) -> String {
    let mut out = String::new();
    for (i, hop) in route.iter().enumerate() {
        let arrow = if i == route.len() - 1 { "⇨" } else { "→" };
        let coord = format!(
            "({})",
            hop.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
        let _ = writeln!(&mut out, "{:>2} {} {}", i, arrow, coord);
    }
    out
}

/// Formats a routing tree as an indented string.
///
/// Traverses the tree of `RoutingFrame`s starting from the root,
/// displaying each step with indentation by dimension. Delivery
/// targets are marked `✅`.
///
/// # Example
/// ```text
/// (0, 0)
///   (0, 1) ✅
/// (1, 0)
///   (1, 1) ✅
/// ```
#[track_caller]
#[allow(dead_code)]
pub fn format_routing_tree(selection: Selection, slice: &Slice) -> String {
    let root = RoutingFrame::root(selection, slice.clone());
    let mut out = String::new();
    let mut seen = HashSet::new();
    format_routing_tree_rec(&root, 0, &mut out, &mut seen).unwrap();
    out
}

fn format_routing_tree_rec(
    frame: &RoutingFrame,
    indent: usize,
    out: &mut String,
    seen: &mut HashSet<RoutingFrameKey>,
) -> std::fmt::Result {
    use crate::selection::routing::RoutingFrameKey;

    let key = RoutingFrameKey::new(frame);
    if !seen.insert(key) {
        return Ok(()); // already visited
    }

    let indent_str = "  ".repeat(indent);
    let coord_str = format!(
        "({})",
        frame
            .here
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );

    match frame.action() {
        RoutingAction::Deliver => {
            writeln!(out, "{}{} ✅", indent_str, coord_str)?;
        }
        RoutingAction::Forward => {
            writeln!(out, "{}{}", indent_str, coord_str)?;
            let _ = frame.next_steps(
                &mut |_| panic!("Choice encountered in format_routing_tree_rec"),
                &mut |step| {
                    let next = step.into_forward().unwrap();
                    format_routing_tree_rec(&next, indent + 1, out, seen).unwrap();
                    ControlFlow::Continue(())
                },
            );
        }
    }

    Ok(())
}

// Pretty-prints a routing path from source to destination.
//
// Each hop is shown as a numbered step with directional arrows.
#[track_caller]
#[allow(dead_code)]
pub fn print_route(route: &[Vec<usize>]) {
    println!("{}", format_route(route));
}

/// Prints the routing tree for a selection over a slice.
///
/// Traverses the routing structure from the root, printing each step
/// with indentation by dimension. Delivery points are marked with
/// `✅`.
#[track_caller]
#[allow(dead_code)]
pub fn print_routing_tree(selection: Selection, slice: &Slice) {
    println!("{}", format_routing_tree(selection, slice));
}

// == "CommActor multicast" routing ==

/// Resolves the current set of routing frames (`dests`) to determine
/// whether the message should be delivered at this rank, and which
/// routing frames should be forwarded to peer ranks.
///
/// This is the continuation of a multicast operation: each forwarded
/// message contains one or more `RoutingFrame`s that represent
/// partial routing state. This call determines how those frames
/// propagate next.
///
/// `deliver_here` is true if any frame targets this rank and
/// indicates delivery. `next_steps` contains the peer ranks and frames
/// to forward.
///
/// This is also the top-level entry point for CommActor's routing
/// logic.
pub fn resolve_routing(
    rank: usize,
    frames: impl IntoIterator<Item = RoutingFrame>,
    chooser: &mut dyn FnMut(&Choice) -> usize,
) -> Result<(bool, HashMap<usize, Vec<RoutingFrame>>)> {
    let mut deliver_here = false;
    let mut next_steps = HashMap::new();
    for frame in frames {
        resolve_routing_one(rank, frame, chooser, &mut deliver_here, &mut next_steps)?;
    }
    Ok((deliver_here, next_steps))
}

/// Recursively resolves routing for a single `RoutingFrame` at the
/// given rank, determining whether the message should be delivered
/// locally and which frames should be forwarded to peer ranks.
///
/// - If the frame targets the local `rank` and is a delivery point,
///   `deliver_here` is set to `true`.
/// - If the frame targets the local `rank` but is not a delivery
///   point, the function recurses on its forward steps.
/// - If the frame targets a different rank, it is added to
///   `next_steps`.
///
/// Deduplication is handled by `get_next_steps`. Dynamic constructs
/// such as `Any` or `First` must be resolved by the provided
/// `chooser`, which selects an index from a `Choice`.
///
/// Traversal is depth-first within a rank and breadth-first across
/// ranks. This defines the exact routing behavior used by
/// `CommActor`: it exhaustively evaluates all local routing structure
/// before forwarding to peers.
///
/// The resulting `next_steps` map contains all non-local ranks that
/// should receive forwarded frames, where each entry maps a peer rank
/// to a list of routing continuations to evaluate at that peer.
/// `deliver_here` is set to `true` if the current rank is a final
/// delivery point.
pub(crate) fn resolve_routing_one(
    rank: usize,
    frame: RoutingFrame,
    chooser: &mut dyn FnMut(&Choice) -> usize,
    deliver_here: &mut bool,
    next_steps: &mut HashMap<usize, Vec<RoutingFrame>>,
) -> Result<()> {
    let frame_rank = frame.slice.location(&frame.here)?;
    if frame_rank == rank {
        if frame.deliver_here() {
            *deliver_here = true;
        } else {
            for next in get_next_steps(frame, chooser)? {
                resolve_routing_one(rank, next, chooser, deliver_here, next_steps)?;
            }
        }
    } else {
        next_steps.entry(frame_rank).or_default().push(frame);
    }
    Ok(())
}

/// Computes the set of `Forward` routing frames reachable from the
/// given `RoutingFrame`.
///
/// This function traverses the result of `frame.next_steps(...)`,
/// collecting only `RoutingStep::Forward(_)` steps. The caller
/// provides a `chooser` function to resolve dynamic constructs such
/// as `Any` or `First`.
///
/// Some obviously redundant steps may be filtered, but no strict
/// guarantee is made about structural uniqueness.
fn get_next_steps(
    dest: RoutingFrame,
    chooser: &mut dyn FnMut(&Choice) -> usize,
) -> Result<Vec<RoutingFrame>> {
    let mut seen = HashSet::new();
    let mut unique_steps = vec![];
    let _ = dest.next_steps(chooser, &mut |step| {
        if let RoutingStep::Forward(frame) = step {
            let key = RoutingFrameKey::new(&frame);
            if seen.insert(key) {
                unique_steps.push(frame);
            }
        }
        ControlFlow::Continue(())
    });
    Ok(unique_steps)
}

// == Testing (`collect_commactor_routing_tree` mesh simulation) ===

/// Captures the logical structure of a CommActor multicast operation.
///
/// This type models how a message is delivered and forwarded through
/// a mesh under CommActor routing semantics. It is used in tests to
/// verify path determinism and understand message propagation
/// behavior.
///
/// - `delivered`: ranks where the message was delivered (`post`
///   called)
/// - `visited`: all ranks that participated, including forwarding
///   only
/// - `forwards`: maps each rank to the routing frames it forwarded
#[cfg(test)]
#[allow(dead_code)]
#[derive(Default)]
pub(crate) struct CommActorRoutingTree {
    // Ranks that were delivered the message (i.e. called `post`). Map
    // from rank → delivery path (flat rank indices) from root to that
    // rank.
    pub delivered: HashMap<usize, Vec<usize>>,

    // Ranks that participated in the multicast - either by delivering
    // the message or forwarding it to peers.
    pub visited: HashSet<usize>,

    /// Map from rank → routing frames this rank forwarded to other
    /// ranks.
    pub forwards: HashMap<usize, Vec<RoutingFrame>>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::collections::VecDeque;

    use super::RoutingAction;
    use super::RoutingFrame;
    use super::print_route;
    use super::print_routing_tree;
    use crate::Slice;
    use crate::selection::EvalOpts;
    use crate::selection::Selection;
    use crate::selection::dsl::*;
    use crate::selection::test_utils::RoutedMessage;
    use crate::selection::test_utils::collect_commactor_routing_tree;
    use crate::selection::test_utils::collect_routed_nodes;
    use crate::selection::test_utils::collect_routed_paths;
    use crate::shape;

    // A test slice: (zones = 2, hosts = 4, gpus = 8).
    fn test_slice() -> Slice {
        Slice::new(0usize, vec![2, 4, 8], vec![32, 8, 1]).unwrap()
    }

    /// Asserts that a routing strategy produces the same set of nodes
    /// as `Selection::eval`.
    ///
    /// This macro compares the result of evaluating a `Selection`
    /// using the given `collector` against the reference
    /// implementation `Selection::eval` (with lenient options).
    ///
    /// The `collector` should be a function or closure of type
    /// `Fn(&Selection, &Slice) -> Vec<usize>`, such as
    /// `collect_routed_nodes` or a CommActor-based simulation.
    ///
    /// Panics if the two sets of routed nodes differ.
    ///
    /// # Example
    /// ```
    /// assert_routing_eq_with!(slice, selection, collect_routed_nodes);
    /// ```
    macro_rules! assert_routing_eq_with {
        ($slice:expr, $sel:expr, $collector:expr) => {{
            let sel = $sel;
            let slice = $slice.clone();
            let mut expected: Vec<_> = sel.eval(&EvalOpts::lenient(), &slice).unwrap().collect();
            expected.sort();
            let mut actual: Vec<_> = ($collector)(&sel, &slice);
            actual.sort();
            assert_eq!(actual, expected, "Mismatch for selection: {}", sel);
        }};
    }

    /// Asserts that `collect_routed_nodes` matches `Selection::eval`
    /// on the given slice.
    macro_rules! assert_collect_routed_nodes_eq {
        ($slice:expr, $sel:expr) => {
            assert_routing_eq_with!($slice, $sel, collect_routed_nodes)
        };
    }

    /// Asserts that CommActor routing delivers to the same nodes as
    /// `Selection::eval`.
    macro_rules! assert_commactor_routing_eq {
        ($slice:expr, $sel:expr) => {
            assert_routing_eq_with!($slice, $sel, |s, sl| {
                collect_commactor_routing_tree(s, sl)
                    .delivered
                    .into_keys()
                    .collect()
            });
        };
    }

    /// Asserts that all routing strategies produce the same set of
    /// routed nodes as `Selection::eval`.
    ///
    /// Compares both the direct strategy (`collect_routed_nodes`) and
    /// the CommActor routing simulation
    /// (`collect_commactor_routing_tree`) against the expected output
    /// from `Selection::eval`.
    macro_rules! assert_all_routing_strategies_eq {
        ($slice:expr, $sel:expr) => {
            assert_collect_routed_nodes_eq!($slice, $sel);
            assert_commactor_routing_eq!($slice, $sel);
        };
    }

    #[test]
    fn test_routing_04() {
        use crate::selection::dsl::*;

        let slice = test_slice(); // [2, 4, 8], strides [32, 8, 1]

        // Destination: GPU 2 on host 2 in zone 1.
        let dest = vec![1, 2, 2];
        let selection = range(1, range(2, range(2, true_())));
        let root = RoutingFrame::root(selection.clone(), slice.clone());
        let path = root.trace_route(&dest).expect("no route found");
        println!(
            "\ndest: {:?}, (singleton-)selection: ({})\n",
            &dest, &selection
        );
        print_route(&path);
        println!("\n");
        assert_eq!(path.last(), Some(&dest));

        // Destination: "Right back where we started from 🙂".
        let dest = vec![0, 0, 0];
        let selection = range(0, range(0, range(0, true_())));
        let root = RoutingFrame::root(selection.clone(), slice.clone());
        let path = root.trace_route(&dest).expect("no route found");
        println!(
            "\ndest: {:?}, (singleton-)selection: ({})\n",
            &dest, &selection
        );
        print_route(&path);
        println!("\n");
        assert_eq!(path.last(), Some(&dest));
    }

    #[test]
    fn test_routing_05() {
        use crate::selection::dsl::*;

        // "Jun's example" -- a 2 x 2 row major mesh.
        let slice = Slice::new(0usize, vec![2, 2], vec![2, 1]).unwrap();
        // Thats is,
        //  (0, 0)    (0, 1)
        //  (0, 1)    (1, 0)
        //
        // and we want to cast to {(0, 1), (1, 0) and (1, 1)}:
        //
        //  (0, 0)❌    (0, 1)✅
        //  (0, 1)✅    (1, 0)✅
        //
        // One reasonable selection expression describing the
        // destination set.
        let selection = union(range(0, range(1, true_())), range(1, all(true_())));

        // Now print the routing tree.
        print_routing_tree(selection, &slice);

        // Prints:
        // (0, 0)
        //   (0, 0)
        //     (0, 1) ✅
        //   (1, 0)
        //     (1, 0) ✅
        //     (1, 1) ✅

        // Another example: (zones = 2, hosts = 4, gpus = 8).
        let slice = Slice::new(0usize, vec![2, 4, 8], vec![32, 8, 1]).unwrap();
        // Let's have all the odd GPUs on hosts 1, 2 and 3 in zone 0.
        let selection = range(
            0,
            range(1..4, range(shape::Range(1, None, /*step*/ 2), true_())),
        );

        // Now print the routing tree.
        print_routing_tree(selection, &slice);

        // Prints:
        // (0, 0, 0)
        //   (0, 0, 0)
        //     (0, 1, 0)
        //       (0, 1, 1) ✅
        //       (0, 1, 3) ✅
        //       (0, 1, 5) ✅
        //       (0, 1, 7) ✅
        //     (0, 2, 0)
        //       (0, 2, 1) ✅
        //       (0, 2, 3) ✅
        //       (0, 2, 5) ✅
        //       (0, 2, 7) ✅
        //     (0, 3, 0)
        //       (0, 3, 1) ✅
        //       (0, 3, 3) ✅
        //       (0, 3, 5) ✅
        //       (0, 3, 7) ✅
    }

    #[test]
    fn test_routing_00() {
        let slice = test_slice();

        assert_all_routing_strategies_eq!(slice, false_());
        assert_all_routing_strategies_eq!(slice, true_());
        assert_all_routing_strategies_eq!(slice, all(true_()));
        assert_all_routing_strategies_eq!(slice, all(all(true_())));
        assert_all_routing_strategies_eq!(slice, all(all(false_())));
        assert_all_routing_strategies_eq!(slice, all(all(all(true_()))));
        assert_all_routing_strategies_eq!(slice, all(range(0..=0, all(true_()))));
        assert_all_routing_strategies_eq!(slice, all(all(range(0..4, true_()))));
        assert_all_routing_strategies_eq!(slice, all(range(1..=2, all(true_()))));
        assert_all_routing_strategies_eq!(slice, all(all(range(2..6, true_()))));
        assert_all_routing_strategies_eq!(slice, all(all(range(3..=3, true_()))));
        assert_all_routing_strategies_eq!(slice, all(range(1..3, all(true_()))));
        assert_all_routing_strategies_eq!(slice, all(all(range(0..=0, true_()))));
        assert_all_routing_strategies_eq!(slice, range(1..=1, range(3..=3, range(0..=2, true_()))));
        assert_all_routing_strategies_eq!(
            slice,
            all(all(range(shape::Range(0, Some(8), 2), true_())))
        );
        assert_all_routing_strategies_eq!(
            slice,
            all(range(shape::Range(1, Some(4), 2), all(true_())))
        );
    }

    #[test]
    fn test_routing_03() {
        let slice = test_slice();

        assert_all_routing_strategies_eq!(
            slice,
            // sel!(0 & (0,(1|3), *))
            intersection(
                range(0, true_()),
                range(0, union(range(1, all(true_())), range(3, all(true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!(0 & (0, (3|1), *)),
            intersection(
                range(0, true_()),
                range(0, union(range(3, all(true_())), range(1, all(true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!((*, *, *) & (*, *, (2 | 4)))
            intersection(
                all(all(all(true_()))),
                all(all(union(range(2, true_()), range(4, true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!((*, *, *) & (*, *, (4 | 2)))
            intersection(
                all(all(all(true_()))),
                all(all(union(range(4, true_()), range(2, true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!((*, (1 | 2)) & (*, (2 | 1)))
            intersection(
                all(union(range(1, true_()), range(2, true_()))),
                all(union(range(2, true_()), range(1, true_())))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            intersection(all(all(all(true_()))), all(true_()))
        );
        assert_all_routing_strategies_eq!(slice, intersection(true_(), all(all(all(true_())))));
        assert_all_routing_strategies_eq!(slice, intersection(all(all(all(true_()))), false_()));
        assert_all_routing_strategies_eq!(slice, intersection(false_(), all(all(all(true_())))));
        assert_all_routing_strategies_eq!(
            slice,
            intersection(
                all(all(range(0..4, true_()))),
                all(all(range(0..4, true_())))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            intersection(all(all(range(1, true_()))), all(all(range(2, true_()))))
        );
        assert_all_routing_strategies_eq!(
            slice,
            intersection(all(all(range(2, true_()))), all(all(range(1, true_()))))
        );
        assert_all_routing_strategies_eq!(
            slice,
            intersection(
                all(all(range(1, true_()))),
                intersection(all(all(true_())), all(all(range(1, true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            intersection(
                range(0, true_()),
                range(0, all(union(range(1, true_()), range(3, true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            range(
                0,
                intersection(true_(), all(union(range(1, true_()), range(3, true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            intersection(all(range(1..=2, true_())), all(range(2..=3, true_())))
        );
        assert_all_routing_strategies_eq!(
            slice,
            intersection(
                range(0, true_()),
                intersection(range(0, all(true_())), range(0, range(1, all(true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            intersection(
                range(0, range(1, all(true_()))),
                intersection(range(0, all(true_())), range(0, true_()))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!( (*, *, *) & ((*, *, *) & (*, *, *)) ),
            intersection(
                all(all(all(true_()))),
                intersection(all(all(all(true_()))), all(all(all(true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            union(
                intersection(range(0, true_()), range(0, range(1, all(true_())))),
                range(1, all(all(true_())))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!((1, *, *) | (0 & (0, 3, *)))
            union(
                range(1, all(all(true_()))),
                intersection(range(0, true_()), range(0, range(3, all(true_()))))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            intersection(
                union(range(0, true_()), range(1, true_())),
                union(range(1, true_()), range(0, true_()))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            union(
                intersection(range(0, range(1, true_())), range(0, range(1, true_()))),
                intersection(range(1, range(3, true_())), range(1, range(3, true_())))
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!(*, 8 : 8)
            all(range(8..8, true_()))
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!((*, 1) & (*, 8 : 8))
            intersection(all(range(1..2, true_())), all(range(8..8, true_())))
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!((*, 8 : 8) | (*, 1))
            union(all(range(8..8, true_())), all(range(1..2, true_())))
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!((*, 1) | (*, 2:8))
            union(all(range(1..2, true_())), all(range(2..8, true_())))
        );
        assert_all_routing_strategies_eq!(
            slice,
            // sel!((*, *, *) & (*, *, 2:8))
            intersection(all(all(all(true_()))), all(all(range(2..8, true_()))))
        );
    }

    #[test]
    fn test_routing_02() {
        let slice = test_slice();

        // zone 0 or 1: sel!(0 | 1, *, *)
        assert_all_routing_strategies_eq!(slice, union(range(0, true_()), range(1, true_())));
        assert_all_routing_strategies_eq!(
            slice,
            union(range(0, all(true_())), range(1, all(true_())))
        );
        // hosts 1 and 3 in zone 0: sel!(0, (1 | 3), *)
        assert_all_routing_strategies_eq!(
            slice,
            range(0, union(range(1, all(true_())), range(3, all(true_()))))
        );
        // sel!(0, 1:3 | 5:7, *)
        assert_all_routing_strategies_eq!(
            slice,
            range(
                0,
                union(
                    range(shape::Range(1, Some(3), 1), all(true_())),
                    range(shape::Range(5, Some(7), 1), all(true_()))
                )
            )
        );

        // sel!(* | *): We start with `union(true_(), true_())`.
        //
        // Evaluating the left branch generates routing frames
        // recursively. Evaluating the right branch generates the same
        // frames again.
        //
        // As a result, we produce duplicate `RoutingFrame`s that
        // have:
        // - the same `here` coordinate,
        // - the same dimension (`dim`), and
        // - the same residual selection (`True`).
        //
        // When both frames reach the delivery condition, the second
        // call to `delivered.insert()` returns `false`. If we put an
        // `assert!` on that line this would trigger assertion failure
        // in the routing simulation.
        //
        // TODO: We need memoization to avoid redundant work.
        //
        // This can be achieved without transforming the algebra itself.
        // However, adding normalization will make memoization more
        // effective, so we should plan to implement both.
        //
        // Once that's done, we can safely restore the `assert!`.
        assert_all_routing_strategies_eq!(slice, union(true_(), true_()));
        // sel!(*, *, * | *, *, *)
        assert_all_routing_strategies_eq!(
            slice,
            union(all(all(all(true_()))), all(all(all(true_()))))
        );
        // no 'false' support in sel!
        assert_all_routing_strategies_eq!(slice, union(false_(), all(all(all(true_())))));
        assert_all_routing_strategies_eq!(slice, union(all(all(all(true_()))), false_()));
        // sel!(0, 0:4, 0 | 1 | 2)
        assert_all_routing_strategies_eq!(
            slice,
            range(
                0,
                range(
                    shape::Range(0, Some(4), 1),
                    union(
                        range(0, true_()),
                        union(range(1, true_()), range(2, true_()))
                    )
                )
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            range(
                0,
                union(range(2, range(4, true_())), range(3, range(5, true_())),),
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            range(0, range(2, union(range(4, true_()), range(5, true_()),),),)
        );
        assert_all_routing_strategies_eq!(
            slice,
            range(
                0,
                union(range(2, range(4, true_())), range(3, range(5, true_())),),
            )
        );
        assert_all_routing_strategies_eq!(
            slice,
            union(
                range(
                    0,
                    union(range(2, range(4, true_())), range(3, range(5, true_())))
                ),
                range(
                    1,
                    union(range(2, range(4, true_())), range(3, range(5, true_())))
                )
            )
        );
    }

    #[test]
    fn test_routing_01() {
        use std::ops::ControlFlow;

        let slice = test_slice();
        let sel = range(0..=0, all(true_()));

        let expected_fanouts: &[&[&[usize]]] = &[
            &[&[0, 0, 0]],
            &[&[0, 0, 0], &[0, 1, 0], &[0, 2, 0], &[0, 3, 0]],
            &[
                &[0, 0, 0],
                &[0, 0, 1],
                &[0, 0, 2],
                &[0, 0, 3],
                &[0, 0, 4],
                &[0, 0, 5],
                &[0, 0, 6],
                &[0, 0, 7],
            ],
            &[
                &[0, 1, 0],
                &[0, 1, 1],
                &[0, 1, 2],
                &[0, 1, 3],
                &[0, 1, 4],
                &[0, 1, 5],
                &[0, 1, 6],
                &[0, 1, 7],
            ],
            &[
                &[0, 2, 0],
                &[0, 2, 1],
                &[0, 2, 2],
                &[0, 2, 3],
                &[0, 2, 4],
                &[0, 2, 5],
                &[0, 2, 6],
                &[0, 2, 7],
            ],
            &[
                &[0, 3, 0],
                &[0, 3, 1],
                &[0, 3, 2],
                &[0, 3, 3],
                &[0, 3, 4],
                &[0, 3, 5],
                &[0, 3, 6],
                &[0, 3, 7],
            ],
        ];

        let expected_deliveries: &[bool] = &[
            false, false, false, false, false, false, // Steps 0–5
            true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, // Steps 6–38
        ];

        let mut step = 0;
        let mut pending = VecDeque::new();

        pending.push_back(RoutingFrame::root(sel.clone(), slice.clone()));

        println!("Fan-out trace for selection: {}", sel);

        while let Some(frame) = pending.pop_front() {
            let mut next_coords = vec![];

            let deliver_here = frame.deliver_here();

            let _ = frame.next_steps(
                &mut |_| panic!("Choice encountered in test_routing_01"),
                &mut |step| {
                    let next = step.into_forward().unwrap();
                    next_coords.push(next.here.clone());
                    pending.push_back(next);
                    ControlFlow::Continue(())
                },
            );

            println!(
                "Step {:>2}: from {:?} (flat = {:>2}) | deliver = {} | fan-out count = {} | selection = {:?}",
                step,
                frame.here,
                frame.slice.location(&frame.here).unwrap(),
                deliver_here,
                next_coords.len(),
                format!("{}", frame.selection),
            );

            for next in &next_coords {
                println!("         → {:?}", next);
            }

            if step < expected_fanouts.len() {
                let expected = expected_fanouts[step]
                    .iter()
                    .map(|v| v.to_vec())
                    .collect::<Vec<_>>();
                assert_eq!(
                    next_coords, expected,
                    "Mismatch in next_coords at step {}",
                    step
                );
            }

            if step < expected_deliveries.len() {
                assert_eq!(
                    deliver_here, expected_deliveries[step],
                    "Mismatch in deliver_here at step {} (coord = {:?})",
                    step, frame.here
                );
            }

            step += 1;
        }
    }

    #[test]
    fn test_routing_06() {
        use std::ops::ControlFlow;

        use crate::selection::dsl::*;
        use crate::selection::routing::RoutingFrameKey;
        use crate::selection::routing::RoutingStep;

        let slice = test_slice();
        let selection = union(all(true_()), all(true_()));

        let mut pending = VecDeque::new();
        let mut dedup_delivered = Vec::new();
        let mut nodup_delivered = Vec::new();
        let mut seen = HashSet::new();

        let root = RoutingFrame::root(selection.clone(), slice.clone());
        pending.push_back(RoutedMessage::<()>::new(root.here.clone(), root));

        while let Some(RoutedMessage { frame, .. }) = pending.pop_front() {
            let mut visitor = |step: RoutingStep| {
                let next = step.into_forward().unwrap();

                if next.action() == RoutingAction::Deliver {
                    nodup_delivered.push(next.slice.location(&next.here).unwrap());
                }

                let key = RoutingFrameKey::new(&next);
                if seen.insert(key) && next.action() == RoutingAction::Deliver {
                    dedup_delivered.push(next.slice.location(&next.here).unwrap());
                }

                if next.action() == RoutingAction::Forward {
                    pending.push_back(RoutedMessage::new(frame.here.clone(), next));
                }

                ControlFlow::Continue(())
            };

            let _ = frame.next_steps(
                &mut |_| panic!("Choice encountered in test_routing_06"),
                &mut visitor,
            );
        }

        assert_eq!(dedup_delivered.len(), 64);
        assert_eq!(nodup_delivered.len(), 128);
    }

    #[test]
    fn test_routing_07() {
        use std::ops::ControlFlow;

        use crate::selection::dsl::*;
        use crate::selection::routing::RoutingFrame;
        use crate::selection::routing::RoutingStep;

        let slice = test_slice(); // shape: [2, 4, 8]

        // Selection: any zone, all hosts, all gpus.
        let selection = any(all(all(true_())));
        let frame = RoutingFrame::root(selection, slice.clone());

        let mut steps = vec![];
        let _ = frame.next_steps(
            &mut |_| panic!("Choice encountered in test_routing_07"),
            &mut |step: RoutingStep| {
                steps.push(step);
                ControlFlow::Continue(())
            },
        );

        // Only one hop should be produced at the `any` dimension.
        assert_eq!(steps.len(), 1);

        // Reject choices.
        let hop = &steps[0].as_forward().unwrap();

        // There should be 3 components to the frame's coordinate.
        assert_eq!(hop.here.len(), 3);

        // The selected zone (dim 0) should be in bounds.
        let zone = hop.here[0];
        assert!(zone < 2, "zone out of bounds: {}", zone);

        // Inner selection should still be All(All(True))
        assert!(matches!(hop.selection, Selection::All(_)));
    }

    // This test relies on a deep structural property of the routing
    // semantics:
    //
    //   Overdelivery is prevented not by ad hoc guards, but by the
    //   structure of the traversal itself — particularly in the
    //   presence of routing frame deduplication.
    //
    // When a frame reaches the final dimension with `selection ==
    // True`, it becomes a delivery frame. If multiple such frames
    // target the same coordinate, then:
    //
    //   - They must share the same coordinate `here`
    //   - They must have reached it via the same routing path (by the
    //     Unique Path Theorem)
    //   - Their `RoutingFrame` state is thus structurally identical:
    //       - Same `here`
    //       - Same `dim` (equal to `slice.num_dim()`)
    //       - Same residual `selection == True`
    //
    // The deduplication logic (via `RoutingFrameKey`) collapses such
    // structurally equivalent frames. As a result, only one frame
    // delivers to the target coordinate, and overdelivery is
    // structurally ruled out.
    //
    // This test verifies that behavior holds as expected — and, when
    // deduplication is disabled, confirms that overdelivery becomes
    // observable.
    #[test]
    fn test_routing_deduplication_precludes_overdelivery() {
        // Ensure the environment is clean — this test depends on a
        // known configuration of deduplication behavior.
        let var = "HYPERACTOR_SELECTION_DISABLE_ROUTING_FRAME_DEDUPLICATION";
        assert!(
            std::env::var_os(var).is_none(),
            "env var `{}` should not be set prior to test",
            var
        );
        let slice = test_slice();

        // Construct a structurally duplicated selection.
        //
        // The union duplicates a singleton selection expression.
        // Without deduplication, this would result in two logically
        // identical frames targeting the same node — which should
        // trigger an over-delivery panic in the simulation.
        let a = range(0, range(0, range(0, true_())));
        let sel = union(a.clone(), a.clone());

        // Sanity check: with deduplication enabled (default), this
        // selection does not cause overdelivery.
        let result = std::panic::catch_unwind(|| {
            let _ = collect_routed_paths(&sel, &slice);
        });
        assert!(result.is_ok(), "Unexpected panic due to overdelivery");

        // Now explicitly disable deduplication.
        // SAFETY: TODO: Audit that the environment access only
        // happens in single-threaded code.
        unsafe { std::env::set_var(var, "1") };

        // Expect overdelivery: the duplicated union arms will each
        // produce a delivery to the same coordinate.
        let result = std::panic::catch_unwind(|| {
            let _ = collect_routed_paths(&sel, &slice);
        });

        // Clean up: restore environment to avoid affecting other
        // tests.
        // SAFETY: TODO: Audit that the environment access only
        // happens in single-threaded code.
        unsafe { std::env::remove_var(var) };

        assert!(
            result.is_err(),
            "Expected panic due to overdelivery, but no panic occurred"
        );
    }

    #[test]
    fn test_next_steps_zero_dim_slice() {
        use std::ops::ControlFlow;

        use crate::selection::dsl::*;

        let slice = Slice::new(42, vec![], vec![]).unwrap();

        let selection = true_();
        let frame = RoutingFrame::root(selection, slice.clone());
        let mut steps = vec![];
        let _ = frame.next_steps(
            &mut |_| panic!("Unexpected Choice in 0D test"),
            &mut |step| {
                steps.push(step);
                ControlFlow::Continue(())
            },
        );

        assert_eq!(steps.len(), 1);
        let step = steps[0].as_forward().unwrap();
        assert_eq!(step.here, vec![0]);
        assert!(step.deliver_here());
        assert_eq!(step.slice.location(&step.here).unwrap(), 42);

        let selection = all(true_());
        let frame = RoutingFrame::root(selection, slice.clone());
        let mut steps = vec![];
        let _ = frame.next_steps(
            &mut |_| panic!("Unexpected Choice in 0D test"),
            &mut |step| {
                steps.push(step);
                ControlFlow::Continue(())
            },
        );

        assert_eq!(steps.len(), 1);
        let step = steps[0].as_forward().unwrap();
        assert_eq!(step.here, vec![0]);
        assert!(step.deliver_here());
        assert_eq!(step.slice.location(&step.here).unwrap(), 42);

        let selection = false_();
        let frame = RoutingFrame::root(selection, slice.clone());
        let mut steps = vec![];
        let _ = frame.next_steps(
            &mut |_| panic!("Unexpected Choice in 0D test"),
            &mut |step| {
                steps.push(step);
                ControlFlow::Continue(())
            },
        );

        assert_eq!(steps.len(), 1);
        let step = steps[0].as_forward().unwrap();
        assert_eq!(step.here, vec![0]);
        assert!(!step.deliver_here());
        assert_eq!(step.slice.location(&step.here).unwrap(), 42);

        let selection = all(false_());
        let frame = RoutingFrame::root(selection, slice.clone());
        let mut steps = vec![];
        let _ = frame.next_steps(
            &mut |_| panic!("Unexpected Choice in 0D test"),
            &mut |step| {
                steps.push(step);
                ControlFlow::Continue(())
            },
        );
        assert_eq!(steps.len(), 1);
        let step = steps[0].as_forward().unwrap();
        assert_eq!(step.here, vec![0]);
        assert!(!step.deliver_here());
        assert_eq!(step.slice.location(&step.here).unwrap(), 42);
    }
}
