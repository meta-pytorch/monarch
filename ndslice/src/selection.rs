/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines a recursive algebra for selecting coordinates
//! in a multidimensional space.
//!
//! A `Selection` describes constraints across dimensions of an
//! `ndslice::Slice`. Variants like [`All`], [`First`], and [`Range`]
//! operate dimensionally, while [`Intersection`] and [`Union`] allow
//! for logical composition of selections.
//!
//! ## Example
//!
//! Suppose a 3-dimensional mesh system of:
//! - 2 zones
//! - 4 hosts per zone
//! - 8 GPUs per host
//!
//! The corresponding `Slice` will have shape `[2, 4, 8]`. An
//! expression to denote the first 4 GPUs of host 0 together with the
//! last 4 GPUs on host 1 across all regions can be written as:
//! ```rust
//! use ndslice::selection::dsl::all;
//! use ndslice::selection::dsl::range;
//! use ndslice::selection::dsl::true_;
//! use ndslice::selection::dsl::union;
//!
//! let s = all(range(0, range(0..4, true_())));
//! let t = all(range(1, range(4.., true_())));
//! let selection = union(s, t);
//! ```
//! Assuming a row-major layout, that is the set of 4 x 2 x 2 = 16
//! coordinates *{(0, 0, 0), ... (0, 0, 3), (0, 1, 5), ..., (0, 1, 7),
//! (1, 0, 0), ..., (1, 0, 3), (1, 1, 4), ..., (1, 1, 7)}* where code
//! to print that set might read as follows.
//! ```rust
//! use ndslice::Slice;
//! use ndslice::selection::EvalOpts;
//! use ndslice::selection::dsl::all;
//! use ndslice::selection::dsl::range;
//! use ndslice::selection::dsl::true_;
//! use ndslice::selection::dsl::union;
//!
//! let slice = Slice::new(0usize, vec![2, 4, 8], vec![32, 8, 1]).unwrap();
//! let s = all(range(0, range(0..4, true_())));
//! let t = all(range(1, range(4.., true_())));
//!
//! for r in union(s, t).eval(&EvalOpts::lenient(), &slice).unwrap() {
//!     println!("{:?}", slice.coordinates(r).unwrap());
//! }
//! ```
//! which is using the `eval` function described next.
//!
//! ## Evaluation
//!
//! Selections are evaluated against an `ndslice::Slice` using the
//! [`Selection::eval`] method, which returns a lazy iterator over the
//! flat (linearized) indices of elements that match.
//!
//! Evaluation proceeds recursively, dimension by dimension, with each
//! variant of `Selection` contributing logic at a particular level of
//! the slice.
//!
//! If a `Selection` is shorter than the number of dimensions, it is
//! implicitly extended with `true_()` at the remaining levels. This
//! means `Selection::True` acts as the identity element, matching all
//! remaining indices by default.

/// A parser for selection expressions in a compact textual syntax.
///
/// See [`selection::parse`] for syntax details and examples.
pub mod parse;

/// Formatting utilities for `Selection` expressions.
///
/// This module defines pretty-printers and compact syntax renderers
/// for selections, based on implementations of the `SelectionSYM`
/// trait.
///
/// The `Display` implementation for [`Selection`] uses this module.
pub mod pretty;

/// A `TokenStream` to [`Selection`] parser. Used at compile time in
/// [`sel!]`. See [`selection::parse`] for syntax details and
/// examples.
pub mod token_parser;

/// Shape navigation guided by [`Selection`] expressions.
pub mod routing;

/// Normalization logic for `Selection`.
pub mod normal;

pub mod test_utils;

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

use rand::Rng;
use serde::Deserialize;
use serde::Serialize;

use crate::Slice;
use crate::selection::normal::NormalizedSelection;
use crate::selection::normal::RewriteRuleExt;
use crate::shape;
use crate::shape::ShapeError;
use crate::slice::SliceError;

/// This trait defines an abstract syntax without committing to a
/// specific representation. It follow the
/// [tagless-final](https://okmij.org/ftp/tagless-final/index.html)
/// style where [`Selection`] is a default AST representation.
pub trait SelectionSYM {
    /// The identity selection (matches no nodes).
    fn false_() -> Self;

    /// The universal selection (matches all nodes).
    fn true_() -> Self;

    /// Selects all values along the current dimension, then applies
    /// the inner selection.
    fn all(selection: Self) -> Self;

    /// Selects the first index along the current dimension for which
    /// the inner selection is non-empty.
    fn first(selection: Self) -> Self;

    /// Selects values within the given range along the current
    /// dimension, then applies the inner selection.
    fn range<R: Into<shape::Range>>(range: R, selection: Self) -> Self;

    /// Selects values along the current dimension that match the
    /// given labels, then applies the inner selection.
    fn label<L: Into<LabelKey>>(labels: Vec<L>, selection: Self) -> Self;

    /// Selects a random index along the current dimension, then applies
    /// the inner selection.
    fn any(selection: Self) -> Self;

    /// The intersection (logical AND) of two selection expressions.
    fn intersection(lhs: Self, selection: Self) -> Self;

    /// The union (logical OR) of two selection expressions.
    fn union(lhs: Self, selection: Self) -> Self;
}

/// `SelectionSYM`-based constructors specialized to the [`Selection`]
/// AST.
pub mod dsl {

    use super::LabelKey;
    use super::Selection;
    use super::SelectionSYM;
    use crate::shape;

    pub fn false_() -> Selection {
        SelectionSYM::false_()
    }
    pub fn true_() -> Selection {
        SelectionSYM::true_()
    }
    pub fn all(inner: Selection) -> Selection {
        SelectionSYM::all(inner)
    }
    pub fn first(inner: Selection) -> Selection {
        SelectionSYM::first(inner)
    }
    pub fn range<R: Into<shape::Range>>(r: R, inner: Selection) -> Selection {
        SelectionSYM::range(r, inner)
    }
    pub fn label<L: Into<LabelKey>>(labels: Vec<L>, inner: Selection) -> Selection {
        SelectionSYM::label(labels, inner)
    }
    pub fn any(inner: Selection) -> Selection {
        SelectionSYM::any(inner)
    }
    pub fn intersection(lhs: Selection, rhs: Selection) -> Selection {
        SelectionSYM::intersection(lhs, rhs)
    }
    pub fn union(lhs: Selection, rhs: Selection) -> Selection {
        SelectionSYM::union(lhs, rhs)
    }
}

impl SelectionSYM for Selection {
    fn false_() -> Self {
        ast::false_()
    }
    fn true_() -> Self {
        ast::true_()
    }
    fn all(selection: Self) -> Self {
        ast::all(selection)
    }
    fn first(selection: Self) -> Self {
        ast::first(selection)
    }
    fn range<R: Into<shape::Range>>(range: R, selection: Self) -> Self {
        ast::range(range, selection)
    }
    fn label<L: Into<LabelKey>>(labels: Vec<L>, selection: Selection) -> Selection {
        let labels = labels.into_iter().map(|l| l.into()).collect();
        Selection::Label(labels, Box::new(selection))
    }
    fn any(selection: Self) -> Self {
        ast::any(selection)
    }
    fn intersection(lhs: Self, rhs: Self) -> Self {
        ast::intersection(lhs, rhs)
    }
    fn union(lhs: Self, rhs: Self) -> Self {
        ast::union(lhs, rhs)
    }
}

impl fmt::Display for Selection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", pretty::pretty(self))
    }
}

/// A metadata label used to constrain values at a given coordinate
/// dimension.
///
/// `LabelKey` represents attribute values associated with indices —
/// for example, GPU model names like `"A100"` or capabilities like
/// "AVX-512".
///
/// Labels are not dimension names (like `"zone"` or `"rack"`); they
/// are **values** assigned to elements at a given dimension, and are
/// used by `Selection::Label` to restrict which values are eligible
/// during selection or routing.
/// For example, a selection like `sel!(["A100"]*)` matches only
/// indices at the current dimension whose associated label value is
/// `"A100"`.
///
/// `Ord` is derived to allow deterministic sorting and set membership,
/// based on lexicographic ordering of label strings.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord
)]
pub enum LabelKey {
    /// A plain string label value.
    Value(String),
}

impl From<String> for LabelKey {
    fn from(s: String) -> Self {
        LabelKey::Value(s)
    }
}

impl From<&str> for LabelKey {
    fn from(s: &str) -> Self {
        LabelKey::Value(s.to_string())
    }
}

impl std::fmt::Display for LabelKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LabelKey::Value(s) => write!(f, "\"{}\"", s),
        }
    }
}

/// An algebra for expressing node selection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Selection {
    /// A selection that never matches any node.
    False,

    /// A selection that always matches any node.
    True,

    /// Selects all values along the current dimension, continuing
    /// with the given selection.
    All(Box<Selection>),

    /// Selects the first value along the current dimension for which
    /// applying the inner selection yields any results.
    First(Box<Selection>),

    /// Selects values within a given range along the current
    /// dimension, continuing with the given selection.
    Range(shape::Range, Box<Selection>),

    /// Selects values based on metadata (i.e., labels) along the
    /// current dimension. This provides attribute-based selection.
    Label(Vec<LabelKey>, Box<Selection>),

    /// Selects a random index along the current dimension, continuing
    /// with the given selection.
    Any(Box<Selection>),

    /// The intersection (logical AND) of two selections.
    Intersection(Box<Selection>, Box<Selection>),

    /// The union (logical OR) of two selections.
    Union(Box<Selection>, Box<Selection>),
}

// Compile-time check: ensure Selection is thread-safe and fully
// owned.
fn _assert_selection_traits()
where
    Selection: Send + Sync + 'static,
{
}

/// Compares two `Selection` values for structural equality.
///
/// Two selections are structurally equal if they have the same shape
/// and recursively equivalent substructure, but not necessarily the
/// same pointer identity or formatting.
pub fn structurally_equal(a: &Selection, b: &Selection) -> bool {
    use Selection::*;
    match (a, b) {
        (False, False) => true,
        (True, True) => true,
        (All(x), All(y)) => structurally_equal(x, y),
        (Any(x), Any(y)) => structurally_equal(x, y),
        (First(x), First(y)) => structurally_equal(x, y),
        (Range(r1, x), Range(r2, y)) => r1 == r2 && structurally_equal(x, y),
        (Intersection(x1, y1), Intersection(x2, y2)) => {
            structurally_equal(x1, x2) && structurally_equal(y1, y2)
        }
        (Union(x1, y1), Union(x2, y2)) => structurally_equal(x1, x2) && structurally_equal(y1, y2),
        _ => false,
    }
}

/// Normalizes a [`Selection`] toward a canonical form for structural
/// comparison.
///
/// This rewrites the selection to eliminate redundant subtrees and
/// bring structurally similar selections into a common
/// representation. The result is suitable for comparison, hashing,
/// and deduplication (e.g., in [`RoutingFrameKey`]).
///
/// Normalization preserves semantics but may alter syntactic
/// structure. It is designed to improve over time as additional
/// rewrites (e.g., flattening, simplification) are introduced.
pub fn normalize(sel: &Selection) -> NormalizedSelection {
    let rule = normal::FlatteningRules
        .then(normal::IdentityRules)
        .then(normal::AbsorbtionRules);
    sel.fold::<normal::NormalizedSelection>()
        .rewrite_bottom_up(&rule)
}

/// Wraps a normalized selection and derives `Eq` and `Hash`, relying
/// on the canonical structure of the normalized form.
///
/// This ensures that logically equivalent selections (e.g., unions
/// with reordered elements) compare equal and hash identically.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NormalizedSelectionKey(NormalizedSelection);

impl NormalizedSelectionKey {
    /// Constructs a `NormalizedSelectionKey`, normalizing the input
    /// selection.
    pub fn new(sel: &Selection) -> Self {
        Self(crate::selection::normalize(sel))
    }

    /// Access the normalized selection.
    pub fn inner(&self) -> &NormalizedSelection {
        &self.0
    }

    /// Consumes the key and returns the owned normalized selection.
    pub fn into_inner(self) -> NormalizedSelection {
        self.0
    }
}

mod ast {
    use super::LabelKey;
    use super::Selection;
    use crate::shape;

    pub(crate) fn false_() -> Selection {
        Selection::False
    }
    pub(crate) fn true_() -> Selection {
        Selection::True
    }
    pub(crate) fn all(selection: Selection) -> Selection {
        Selection::All(Box::new(selection))
    }
    pub(crate) fn first(selection: Selection) -> Selection {
        Selection::First(Box::new(selection))
    }
    pub(crate) fn range<R: Into<shape::Range>>(range: R, selection: Selection) -> Selection {
        Selection::Range(range.into(), Box::new(selection))
    }
    #[allow(dead_code)] // Harmless.
    pub(crate) fn label<L: Into<LabelKey>>(labels: Vec<L>, selection: Selection) -> Selection {
        let labels = labels.into_iter().map(Into::into).collect();
        Selection::Label(labels, Box::new(selection))
    }
    pub(crate) fn any(selection: Selection) -> Selection {
        Selection::Any(Box::new(selection))
    }
    pub(crate) fn intersection(lhs: Selection, rhs: Selection) -> Selection {
        Selection::Intersection(Box::new(lhs), Box::new(rhs))
    }
    pub(crate) fn union(lhs: Selection, rhs: Selection) -> Selection {
        Selection::Union(Box::new(lhs), Box::new(rhs))
    }
}

/// `EvalOpts` controls runtime behavior of [`Selection::eval`] by
/// enforcing stricter validation rules.
pub struct EvalOpts {
    /// Fail `eval` on empty range expressions.
    pub disallow_empty_ranges: bool,

    /// Fail `eval` on a range beginning after the slice's extent in
    /// the evaluation context's dimension.
    pub disallow_out_of_range: bool,

    /// Fail `eval` if a selection can be shown to be not "static".
    pub disallow_dynamic_selections: bool,
}

impl EvalOpts {
    // Produce empty iterators but don't panic.
    pub fn lenient() -> Self {
        Self {
            disallow_empty_ranges: false,
            disallow_out_of_range: false,
            disallow_dynamic_selections: false,
        }
    }

    // `eval()` should fail with all the same [`shape::ShapeError`]s
    // as [`Shape::select()`].
    #[allow(dead_code)]
    pub fn strict() -> Self {
        Self {
            disallow_empty_ranges: true,
            disallow_out_of_range: true,
            ..Self::lenient()
        }
    }
}

impl Selection {
    pub(crate) fn validate(&self, opts: &EvalOpts, slice: &Slice) -> Result<&Self, ShapeError> {
        let depth = 0;
        self.validate_rec(opts, slice, self, depth).map(|_| self)
    }

    fn validate_rec(
        &self,
        opts: &EvalOpts,
        slice: &Slice,
        top: &Selection,
        dim: usize,
    ) -> Result<(), ShapeError> {
        if dim == slice.num_dim() {
            // This enables us to maintain identities like 'all(true)
            // <=> true' and 'all(false) <=> false' in leaf positions.
            match self {
                Selection::True | Selection::False => return Ok(()),
                _ => {
                    return Err(ShapeError::SelectionTooDeep {
                        expr: top.clone(),
                        num_dim: slice.num_dim(),
                    });
                }
            }
        }

        match self {
            Selection::False | Selection::True => Ok(()),
            Selection::Range(range, s) => {
                if range.is_empty() && opts.disallow_empty_ranges {
                    return Err(ShapeError::EmptyRange {
                        range: range.clone(),
                    });
                } else {
                    if opts.disallow_out_of_range {
                        let size = slice.sizes()[dim];
                        let (min, _, _) = range.resolve(size);
                        if min >= size {
                            // Use EmptyRange here for now (evaluation would result in an empty range),
                            // until we figure out how to differentiate between slices and shapes
                            return Err(ShapeError::EmptyRange {
                                range: range.clone(),
                            });
                        }
                    }

                    s.validate_rec(opts, slice, top, dim + 1)?;
                }

                Ok(())
            }
            Selection::Any(s) => {
                if opts.disallow_dynamic_selections {
                    return Err(ShapeError::SelectionDynamic { expr: top.clone() });
                }
                s.validate_rec(opts, slice, top, dim + 1)?;
                Ok(())
            }
            Selection::All(s) | Selection::Label(_, s) | Selection::First(s) => {
                s.validate_rec(opts, slice, top, dim + 1)?;
                Ok(())
            }
            Selection::Intersection(a, b) | Selection::Union(a, b) => {
                a.validate_rec(opts, slice, top, dim)?;
                b.validate_rec(opts, slice, top, dim)?;
                Ok(())
            }
        }
    }

    /// Lazily evaluates this selection against the given `slice`
    /// yielding flat indices.
    ///
    /// Returns a boxed iterator that produces indices of elements
    /// matching the selection expression when evaluated over `slice`.
    ///
    /// # Lifetimes
    ///
    /// The returned iterator borrows `slice` because the internal
    /// iterators are implemented as closures that **capture**
    /// `&slice` in their environment. Evaluation is lazy, so these
    /// closures dereference `slice` each time a coordinate is
    /// visited. The `'a` lifetime ensures that the iterator cannot
    /// outlive the `slice` it reads from.
    ///
    /// # Why `Box<dyn Iterator>`
    ///
    /// The selection algebra supports multiple recursive strategies
    /// (`All`, `Range`, `Intersection`, etc.) that return different
    /// iterator types (e.g. `Selection::True` =>
    /// `std::iter::once(...)`, `Selection::False` =>
    /// `std::iter::empty()`). Returning `impl Iterator` is not
    /// feasible because the precise type depends on dynamic selection
    /// structure. Boxing erases this variability and allows a uniform
    /// return type.
    ///
    /// # Canonical handling of 0-dimensional slices
    ///
    /// A `Slice` with zero dimensions represents the empty product
    /// `∏_{i=1}^{0} Xᵢ`, which has exactly one element: the empty
    /// tuple. To ensure that evaluation behaves uniformly across
    /// dimensions, we canonically embed the 0-dimensional case into a
    /// 1-dimensional slice of extent 1. That is, we reinterpret the
    /// 0D slice as `Slice::new(offset, [1], [1])`, which is
    /// semantically equivalent and enables evaluation to proceed
    /// through the normal recursive machinery without special-casing.
    /// The result is that selection expressions are always evaluated
    /// over a slice with at least one dimension, and uniform logic
    /// applies.
    pub fn eval<'a>(
        &self,
        opts: &EvalOpts,
        slice: &'a Slice,
    ) -> Result<Box<dyn Iterator<Item = usize> + 'a>, ShapeError> {
        // Canonically embed 0D as 1D (extent 1).
        if slice.num_dim() == 0 {
            let slice = Slice::new(slice.offset(), vec![1], vec![1]).unwrap();
            return Ok(Box::new(
                self.validate(opts, &slice)?
                    .eval_rec(&slice, vec![0; 1], 0)
                    .collect::<Vec<_>>()
                    .into_iter(),
            ));
        }

        Ok(self
            .validate(opts, slice)?
            .eval_rec(slice, vec![0; slice.num_dim()], 0))
    }

    fn eval_rec<'a>(
        &self,
        slice: &'a Slice,
        env: Vec<usize>,
        dim: usize,
    ) -> Box<dyn Iterator<Item = usize> + 'a> {
        if dim == slice.num_dim() {
            match self {
                Selection::True => return Box::new(std::iter::once(slice.location(&env).unwrap())),
                Selection::False => return Box::new(std::iter::empty()),
                _ => {
                    panic!("structural combinator {self:?} at leaf level (dim = {dim}))",);
                }
            }
        }

        use itertools;
        use itertools::EitherOrBoth;

        match self {
            Selection::False => Box::new(std::iter::empty()),
            Selection::True => Box::new((0..slice.sizes()[dim]).flat_map(move |i| {
                let mut env = env.clone();
                env[dim] = i;
                Selection::True.eval_rec(slice, env, dim + 1)
            })),
            Selection::All(select) => {
                let select = Box::clone(select);
                Box::new((0..slice.sizes()[dim]).flat_map(move |i| {
                    let mut env = env.clone();
                    env[dim] = i;
                    select.eval_rec(slice, env, dim + 1)
                }))
            }
            Selection::First(select) => {
                let select = Box::clone(select);
                Box::new(iterutils::first(slice.sizes()[dim], move |i| {
                    let mut env = env.clone();
                    env[dim] = i;
                    select.eval_rec(slice, env, dim + 1)
                }))
            }
            Selection::Range(range, select) => {
                let select = Box::clone(select);
                let (min, max, step) = range.resolve(slice.sizes()[dim]);
                Box::new((min..max).step_by(step).flat_map(move |i| {
                    let mut env = env.clone();
                    env[dim] = i;
                    select.eval_rec(slice, env, dim + 1)
                }))
            }

            // Label-based selection: filters candidates at this
            // dimension, then either selects one (Any) or recurses.
            //
            // When the inner selection is `Any`, we choose one match
            // at random (eager). Otherwise, we recurse normally and
            // filter the results lazily.
            //
            // This separation reflects that `Label(...)` does *not*
            // consume a dimension — it restricts access to it while
            // preserving dimensional structure.
            //
            // See `eval_label` for more on the distinction between
            // filtering and traversal, and the underlying
            // projection-based interpretation.
            //
            // For example:
            //
            //   sel!(*, ["foo"]?, *)  // select one host with label "foo", then all GPUs
            //   = all(label(["foo"], any(all(true_()))))
            //
            //   sel!(*, ["foo"]*, *)  // select all hosts with label "foo", then all GPUs
            //   = all(label(["foo"], all(all(true_()))))
            //
            // **Note:** Label filtering is not yet implemented — all coordinates
            // are currently accepted.
            Selection::Label(labels, inner) => {
                Self::eval_label(labels, inner, slice, env, dim /*, provider */)
            }
            Selection::Any(select) => {
                let select = Box::clone(select);
                let r = {
                    let upper = slice.sizes()[dim];
                    let mut rng = rand::thread_rng();
                    rng.gen_range(0..upper)
                };
                Box::new((r..r + 1).flat_map(move |i| {
                    let mut env = env.clone();
                    env[dim] = i;
                    select.eval_rec(slice, env, dim + 1)
                }))
            }
            Selection::Intersection(a, b) => Box::new(
                itertools::merge_join_by(
                    a.eval_rec(slice, env.clone(), dim),
                    b.eval_rec(slice, env.clone(), dim),
                    |x, y| x.cmp(y),
                )
                .filter_map(|either| match either {
                    EitherOrBoth::Both(x, _) => Some(x),
                    _ => None,
                }),
            ),
            Selection::Union(a, b) => Box::new(
                itertools::merge_join_by(
                    a.eval_rec(slice, env.clone(), dim),
                    b.eval_rec(slice, env.clone(), dim),
                    |x, y| x.cmp(y),
                )
                .map(|either| match either {
                    EitherOrBoth::Left(x) => x,
                    EitherOrBoth::Right(y) => y,
                    EitherOrBoth::Both(x, _) => x,
                }),
            ),
        }
    }

    /// Evaluates a `Label(labels, inner)` selection.
    ///
    /// This operator filters coordinates along the current dimension
    /// based on associated metadata (labels). It then evaluates the inner
    /// selection at matching positions.
    ///
    /// Conceptually, this corresponds to computing a pullback along a
    /// projection `p : E → B`, where:
    ///
    /// - `B` is the base space of coordinates (e.g. zones × hosts × gpus)
    /// - `E` is the space of labeled coordinates
    /// - `p⁻¹(S)` lifts a geometric selection `S ⊆ B` into the labeled
    ///   space
    ///
    /// At runtime, we simulate `p⁻¹(S)` by traversing `B` and querying a
    /// `LabelProvider` at each coordinate. Under the identity provider,
    /// label filtering has no effect and `eval_label` reduces to the
    /// geometric case.
    ///
    /// - If `inner` is `Any`, we select one matching index at random
    /// - Otherwise, we recurse and filter lazily
    ///
    /// **Note:** Label filtering is not yet implemented — all coordinates
    /// are currently accepted.
    fn eval_label<'a>(
        _labels: &[LabelKey],
        inner: &Selection,
        slice: &'a Slice,
        env: Vec<usize>,
        dim: usize,
        // provider: &dyn LabelProvider  // TODO: add when ready
    ) -> Box<dyn Iterator<Item = usize> + 'a> {
        match inner {
            // Case 1: label(..., any(...))
            // - We evaluate all indices at this dimension that match
            //   the label predicate.
            // - From those, choose one at random and continue
            //   evaluating the inner selection.
            // - Semantically: filter → choose one → recurse
            Selection::Any(sub_inner) => {
                let matching: Vec<usize> = (0..slice.sizes()[dim])
                    .filter(|&i| {
                        let mut prefix = env.clone();
                        prefix[dim] = i;
                        true // TODO: provider.matches(dim, &prefix[0..=dim], labels)
                    })
                    .collect();

                if matching.is_empty() {
                    return Box::new(std::iter::empty());
                }

                let mut rng = rand::thread_rng();
                let chosen = matching[rng.gen_range(0..matching.len())];

                let mut coord = env;
                coord[dim] = chosen;
                sub_inner.eval_rec(slice, coord, dim + 1 /*, provider */)
            }
            // Case 2: label(..., inner)
            //
            // Applies label filtering after evaluating `inner`. We
            // first recurse into `inner`, then lazily filter the
            // resulting flat indices based on whether the coordinate
            // at `dim` matches the given labels.
            //
            // This preserves laziness for all cases except `Any`,
            // which requires eager collection and is handled
            // separately.
            _ => {
                // evaluate the inner selection — recurse as usual
                let iter = inner.eval_rec(slice, env.clone(), dim /* , provider */);
                Box::new(iter.filter(move |&flat| {
                    let _coord = slice.coordinates(flat);
                    true // TODO: provider.matches(dim, &coord[0..=dim], labels)
                }))
            }
        }
    }

    /// Returns `true` if this selection is equivalent to `True` under
    /// the algebra.
    ///
    /// In the selection algebra, `All(True)` is considered equivalent
    /// to `True`, and this identity extends recursively. For example:
    ///
    ///   - `All(True)`      ≡ `True`
    ///   - `All(All(True))` ≡ `True`
    ///   - `All(All(All(True)))` ≡ `True`
    ///
    /// This method checks whether the selection is structurally
    /// identical to True, possibly wrapped in one or more All(...)
    /// layers. It does **not** perform full normalization—only
    /// structural matching sufficient to recognize this identity.
    ///
    /// Used to detect when a selection trivially selects all elements
    /// at all levels.
    ///
    /// ## Limitations
    ///
    /// This is a **syntactic check** only. It does *not* recognize
    /// semantically equivalent expressions such as:
    ///
    ///   - `Union(True, True)`
    ///   - `All(Union(True, False))`
    ///   - A union of all singleton ranges covering the full space
    ///
    /// For a semantic check, use evaluation against a known slice.
    pub fn is_equivalent_to_true(mut sel: &Selection) -> bool {
        while let Selection::All(inner) = sel {
            sel = inner;
        }
        matches!(sel, Selection::True)
    }

    /// Evaluates whether the specified coordinates are part of the selection.
    /// Returns true if they are, false otherwise.
    ///
    /// Example:
    /// let selection = union(
    ///     range(0..2, range(0..1, range(0..2, true_()))),
    ///     range(0..2, range(1..2, range(0..2, true_()))),
    /// );
    ///
    /// assert!(selection.contains(&[0, 0, 1]));
    /// assert!(!selection.contains(&[2, 0, 1]));
    pub fn contains(&self, coords: &[usize]) -> bool {
        self.contains_rec(coords, 0)
    }

    fn contains_rec(&self, coords: &[usize], dim: usize) -> bool {
        if dim >= coords.len() {
            return matches!(self, Selection::True);
        }

        match self {
            Selection::False => false,
            Selection::True => true,
            Selection::All(inner) => inner.contains_rec(coords, dim + 1),
            Selection::Range(range, inner) => {
                let (min, max, step) = range.resolve(coords.len());
                let index = coords[dim];
                index >= min
                    && index < max
                    && (index - min).is_multiple_of(step)
                    && inner.contains_rec(coords, dim + 1)
            }
            Selection::Intersection(a, b) => {
                a.contains_rec(coords, dim) && b.contains_rec(coords, dim)
            }
            Selection::Union(a, b) => a.contains_rec(coords, dim) || b.contains_rec(coords, dim),
            Selection::Label(_, _) | Selection::First(_) | Selection::Any(_) => {
                unimplemented!()
            }
        }
    }

    /// Simplifies the intersection of two `Selection` expressions.
    ///
    /// Applies short-circuit logic to avoid constructing redundant or
    /// degenerate intersections:
    ///
    /// - If either side is `False`, the result is `False`.
    /// - If either side is `True`, the result is the other side.
    /// - Otherwise, constructs an explicit `Intersection`.
    ///
    /// This is required during routing to make progress when
    /// evaluating intersections. Without this reduction, routing may
    /// stall — for example, in intersections like `Intersection(True,
    /// X)`, which should simplify to `X`.
    pub fn reduce_intersection(self: Selection, b: Selection) -> Selection {
        match (&self, &b) {
            (Selection::False, _) | (_, Selection::False) => Selection::False,
            (Selection::True, other) | (other, Selection::True) => other.clone(),
            _ => Selection::Intersection(Box::new(self), Box::new(b)),
        }
    }

    /// Canonicalizes this selection to the specified number of
    /// dimensions.
    ///
    /// Ensures that the selection has exactly `num_dims` dimensions
    /// by recursively wrapping it in combinators (`All`, `Any`, etc.)
    /// where needed. This transformation enforces a canonical
    /// structural form suitable for dimensional evaluation (e.g.,
    /// routing via `next_steps`).
    ///
    /// Examples:
    /// - `True` becomes `All(All(...(True)))`
    /// - `Any(True)` becomes `Any(Any(...(True)))`
    /// - Fully specified selections are left unchanged.
    ///
    /// ---
    ///
    /// # Panics
    /// Panics if `num_dims == 0`. Use a canonical embedding (e.g., 0D
    /// → 1D) before calling this (see e.g. `RoutingFrame::root`).
    pub(crate) fn canonicalize_to_dimensions(self, num_dims: usize) -> Selection {
        assert!(
            num_dims > 0,
            "canonicalize_to_dimensions requires num_dims > 0"
        );
        self.canonicalize_to_dimensions_rec(0, num_dims)
    }

    fn canonicalize_to_dimensions_rec(self, dim: usize, num_dims: usize) -> Selection {
        use crate::selection::dsl::*;

        match self {
            Selection::True if dim < num_dims => {
                let mut out = true_();
                for _ in (dim..num_dims).rev() {
                    out = all(out);
                }
                out
            }
            Selection::False if dim < num_dims => {
                let mut out = false_();
                for _ in (dim..num_dims).rev() {
                    out = all(out);
                }
                out
            }
            Selection::Any(inner) if dim < num_dims && matches!(*inner, Selection::True) => {
                let mut out = true_();
                for _ in (dim..num_dims).rev() {
                    out = any(out);
                }
                out
            }
            Selection::All(inner) => all(inner.canonicalize_to_dimensions_rec(dim + 1, num_dims)),
            Selection::Any(inner) => any(inner.canonicalize_to_dimensions_rec(dim + 1, num_dims)),
            Selection::First(inner) => {
                first(inner.canonicalize_to_dimensions_rec(dim + 1, num_dims))
            }
            Selection::Range(r, inner) => {
                range(r, inner.canonicalize_to_dimensions_rec(dim + 1, num_dims))
            }
            Selection::Intersection(a, b) => intersection(
                a.canonicalize_to_dimensions_rec(dim, num_dims),
                b.canonicalize_to_dimensions_rec(dim, num_dims),
            ),
            Selection::Union(a, b) => union(
                a.canonicalize_to_dimensions_rec(dim, num_dims),
                b.canonicalize_to_dimensions_rec(dim, num_dims),
            ),

            other => other,
        }
    }

    /// Recursively folds the `Selection` into an abstract syntax via
    /// the `SelectionSYM` interface.
    ///
    /// This method structurally traverses the `Selection` tree and
    /// reconstructs it using the operations provided by the
    /// `SelectionSYM` trait. It is typically used to reify a
    /// `Selection` into alternate forms, such as pretty-printers.
    ///
    /// # Type Parameters
    ///
    /// - `S`: An implementation of the `SelectionSYM` trait,
    ///   providing the constructors for the target representation.
    pub fn fold<S: SelectionSYM>(&self) -> S {
        match self {
            Selection::False => S::false_(),
            Selection::True => S::true_(),
            Selection::All(inner) => S::all(inner.fold::<S>()),
            Selection::First(inner) => S::first(inner.fold::<S>()),
            Selection::Range(r, inner) => S::range(r.clone(), inner.fold::<S>()),
            Selection::Label(labels, inner) => S::label(labels.clone(), inner.fold::<S>()),
            Selection::Any(inner) => S::any(inner.fold::<S>()),
            Selection::Intersection(a, b) => S::intersection(a.fold::<S>(), b.fold::<S>()),
            Selection::Union(a, b) => S::union(a.fold::<S>(), b.fold::<S>()),
        }
    }

    /// Iterator over indices selected by `self` and not in
    /// `exclusions`.
    ///
    /// Evaluates the selection against `slice` using `opts`, then
    /// filters out any indices present in the exclusion set.
    /// Evaluation is lazy and streaming; the exclusion set is used
    /// directly for fast membership checks.
    pub fn difference<'a>(
        &self,
        opts: &EvalOpts,
        slice: &'a Slice,
        exclusions: &'a HashSet<usize>,
    ) -> Result<impl Iterator<Item = usize> + use<'a>, ShapeError> {
        Ok(self
            .eval(opts, slice)?
            .filter(move |idx| !exclusions.contains(idx)))
    }

    /// Calculate a new `Selection` that excludes the specified flat
    /// ranks.
    ///
    /// This computes `self \ exclusions` by evaluating `self`,
    /// removing the given ranks, and reconstructing a `Selection`
    /// that selects exactly the remaining elements.
    ///
    /// The result is a concrete, structurally uniform expression with
    /// predictable construction order and exact correspondence to the
    /// surviving ranks.
    pub fn without(
        &self,
        slice: &Slice,
        exclusions: &HashSet<usize>,
    ) -> Result<Selection, ShapeError> {
        let remaining = self
            .difference(&EvalOpts::strict(), slice, exclusions)?
            .collect::<BTreeSet<_>>();
        Ok(Selection::of_ranks(slice, &remaining)?)
    }

    /// Converts a set of flat indices into a symbolic `Selection`
    /// expression over the given `slice`. Returns an error if any index
    /// is invalid.
    ///
    /// Each flat index is converted into coordinates using
    /// `slice.coordinates`, then folded into a nested chain of singleton
    /// ranges. The resulting selection evaluates exactly to the input
    /// indices.
    ///
    /// The selections are combined left-associatively using `union`, but
    /// since `union` is associative, the grouping does not affect
    /// correctness.
    ///
    /// The input `BTreeSet` ensures:
    /// - all indices are unique (no redundant singleton ranges),
    /// - the resulting selection has a stable, deterministic structure,
    /// - and iteration proceeds in ascending order, which helps produce
    ///   predictable routing trees and consistent test results.
    ///
    /// This choice avoids an explicit sort and makes downstream behavior
    /// more reproducible and auditable.
    pub fn of_ranks(slice: &Slice, ranks: &BTreeSet<usize>) -> Result<Selection, SliceError> {
        let selections = ranks
            .iter()
            .map(|&i| {
                Ok(slice
                    .coordinates(i)?
                    .into_iter()
                    .rev()
                    .fold(dsl::true_(), |acc, i| dsl::range(i..=i, acc)))
            })
            .collect::<Result<Vec<_>, SliceError>>()?;

        Ok(selections
            .into_iter()
            .reduce(dsl::union)
            .unwrap_or_else(dsl::false_))
    }
} // impl Selection

mod sealed {
    pub trait Sealed {}
    impl Sealed for crate::slice::Slice {}
}

/// Connects the `select!` API to the `Selection` algebra by enabling
/// `base.reify_slice(slice)` syntax, where `base: Slice`.
///
/// The base slice defines the coordinate system in which the slice is
/// interpreted. Slices are themselves `Slice` values, typically
/// produced by `select!`, and are reified as `Selection` expressions
/// over the base.
pub trait ReifySlice: sealed::Sealed {
    /// Reify a slice as a `Selection` in the coordinate system of
    /// `self`.
    fn reify_slice(&self, slice: &Slice) -> Result<Selection, SliceError>;

    /// Reify multiple slices as a union of selections in the
    /// coordinate system of `self`.
    fn reify_slices<V: AsRef<[Slice]>>(&self, slices: V) -> Result<Selection, SliceError>;
}

impl ReifySlice for Slice {
    /// Constructs a [`Selection`] expression that symbolically
    /// matches all coordinates in the given `slice`, expressed in the
    /// coordinate system of the provided `base` slice (`self`).
    ///
    /// The result is a nested sequence of `range(start..end, step)`
    /// combinators that match the rectangular region covered by `slice`
    /// in base coordinates. This preserves geometry and layout when
    /// `slice` is *layout-aligned* — that is, each of its strides is
    /// a multiple of the corresponding base stride.
    ///
    /// If any dimension is not layout-aligned, the slice is reified
    /// by explicitly enumerating its coordinates.
    ///
    /// Returns [`dsl::false_()`] if the slice is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The base is not contiguous and row-major
    /// - The slice lies outside the bounds of the base
    ///
    /// # Example
    ///
    /// ```rust
    /// use ndslice::selection::ReifySlice;
    /// let shape = ndslice::shape!(x = 4, y = 4);
    /// let base = shape.slice();
    /// let selected = ndslice::select!(shape, x = 1..3, y = 2..4).unwrap();
    /// let slice = selected.slice();
    /// let selection = base.reify_slice(slice).unwrap();
    /// ```
    fn reify_slice(&self, slice: &Slice) -> Result<Selection, SliceError> {
        // Precondition: the base is contiguous and row major.
        if !self.is_contiguous() {
            return Err(SliceError::NonContiguous);
        }

        if slice.is_empty() {
            return Ok(dsl::false_());
        }

        if slice.num_dim() != self.num_dim()
            || slice.sizes().iter().zip(self.sizes()).any(|(&v, &s)| v > s)
        {
            return Selection::of_ranks(self, &slice.iter().collect::<BTreeSet<usize>>());
        }

        let origin = self.coordinates(slice.offset())?;
        let mut acc = dsl::true_();
        for dim in (0..self.num_dim()).rev() {
            let start = origin[dim];
            let len = slice.sizes()[dim];
            let slice_stride = slice.strides()[dim];
            let base_stride = self.strides()[dim];

            if slice_stride.is_multiple_of(base_stride) {
                // Layout-aligned with base.
                let step = slice_stride / base_stride;
                let end = start + step * len;
                // Check that `end` is within bounds.
                if end - 1 > self.sizes()[dim] {
                    let bad = origin
                        .iter()
                        .enumerate()
                        .map(|(d, &x)| if d == dim { end } else { x })
                        .collect::<Vec<_>>();
                    return Err(SliceError::ValueNotInSlice {
                        value: self.location(&bad).unwrap(),
                    });
                }
                acc = dsl::range(crate::shape::Range(start, Some(end), step), acc);
            } else {
                // Irregular layout; fallback to explicit enumeration.
                return Selection::of_ranks(self, &slice.iter().collect::<BTreeSet<_>>());
            }
        }

        Ok(acc)
    }

    /// Converts a list of `slices` into a symbolic [`Selection`]
    /// expression over a common `base` [`Slice`].
    ///
    /// Each slice describes a rectangular subregion of the base. This
    /// function reifies each slice into a nested `range(.., ..)`
    /// expression in the base coordinate system and returns the union
    /// of all such selections.
    ///
    /// Empty slices are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if any slice:
    /// - Refers to coordinates not contained within the base
    ///
    /// # Example
    ///
    /// ```rust
    /// use ndslice::selection::ReifySlice;
    ///
    /// let shape = ndslice::shape!(x = 4, y = 4);
    /// let base = shape.slice();
    ///
    /// let a = ndslice::select!(shape, x = 0..2, y = 0..2)
    ///     .unwrap()
    ///     .slice()
    ///     .clone();
    /// let b = ndslice::select!(shape, x = 2..4, y = 2..4)
    ///     .unwrap()
    ///     .slice()
    ///     .clone();
    ///
    /// let sel = base.reify_slices(&[a, b]).unwrap();
    /// ```
    fn reify_slices<V: AsRef<[Slice]>>(&self, slices: V) -> Result<Selection, SliceError> {
        let slices = slices.as_ref();
        let mut selections = Vec::with_capacity(slices.len());

        for slice in slices {
            if slice.is_empty() {
                continue;
            }
            selections.push(self.reify_slice(slice)?);
        }

        let mut iter = selections.into_iter();
        let first = iter.next().unwrap_or_else(dsl::false_);
        Ok(iter.fold(first, dsl::union))
    }
}

/// Trivial all(true) equivalence.
pub fn is_equivalent_true(sel: impl std::borrow::Borrow<Selection>) -> bool {
    Selection::is_equivalent_to_true(sel.borrow())
}

mod iterutils {
    // An iterator over the first non-empty result 1 applying
    // `mk_iter` to indices in the range `0..size`.
    pub(crate) fn first<'a, F>(size: usize, mut mk_iter: F) -> impl Iterator<Item = usize> + 'a
    where
        F: FnMut(usize) -> Box<dyn Iterator<Item = usize> + 'a>,
    {
        (0..size)
            .find_map(move |i| {
                let mut iter = mk_iter(i).peekable();
                if iter.peek().is_some() {
                    Some(iter)
                } else {
                    None
                }
            })
            .into_iter()
            .flatten()
    }
}

/// Construct a [`Selection`] from a [`Shape`] and a single labeled
/// constraint.
///
/// This function produces a multidimensional selection expression
/// that is structurally aligned with the shape. It applies the given
/// range to the named dimension, and fills all preceding dimensions
/// with [`all`] to maintain alignment. Trailing dimensions are left
/// unconstrained.
///
/// # Arguments
///
/// - `shape`: The labeled shape describing the coordinate space.
/// - `label`: The name of the dimension to constrain.
/// - `rng`: The range to apply in the selected dimension.
///
/// # Errors
///
/// Returns [`ShapeError::InvalidLabels`] if the label is not present
/// in the shape.
///
/// # Example
///
/// ```
/// use ndslice::shape;
/// use ndslice::selection::selection_from_one;
///
/// let shape = shape!(zone = 2, host = 4, gpu = 8);
/// let sel = selection_from_one(&shape, "host", 1..3).unwrap();
/// assert_eq!(format!("{sel:?}"), "All(Range(Range(1, Some(3), 1), True))"); // corresponds to (*, 1..3, *)
/// ```
///
/// [`all`]: crate::selection::dsl::all
/// [`Shape`]: crate::shape::Shape
/// [`Selection`]: crate::selection::Selection
pub fn selection_from_one<R>(
    shape: &shape::Shape,
    label: &str,
    rng: R,
) -> Result<Selection, ShapeError>
where
    R: Into<shape::Range>,
{
    use crate::selection::dsl;

    let Some(pos) = shape.labels().iter().position(|l| l == label) else {
        return Err(ShapeError::InvalidLabels {
            labels: vec![label.to_string()],
        });
    };

    let mut selection = dsl::range(rng.into(), dsl::true_());
    for _ in 0..pos {
        selection = dsl::all(selection)
    }

    Ok(selection)
}

/// Construct a [`Selection`] from a [`Shape`] and multiple labeled
/// range constraints.
///
/// This function produces a multidimensional selection aligned with
/// the shape, applying the specified constraints to their
/// corresponding dimensions. All unconstrained dimensions are filled
/// with [`all`] to preserve structural alignment.
///
/// # Arguments
///
/// - `shape`: The labeled shape defining the full coordinate space.
/// - `constraints`: A slice of `(label, range)` pairs specifying
///   dimension constraints.
///
/// # Errors
///
/// Returns [`ShapeError::InvalidLabels`] if any label in
/// `constraints` is not present in the shape.
///
/// # Example
///
/// ```
/// use ndslice::selection::selection_from;
/// use ndslice::shape;
///
/// let shape = shape!(zone = 2, host = 4, gpu = 8);
/// let sel = selection_from(&shape, &[("host", 1..3), ("gpu", 0..4)]).unwrap();
/// assert_eq!(
///     format!("{sel:?}"),
///     "All(Range(Range(1, Some(3), 1), Range(Range(0, Some(4), 1), True)))"
/// );
/// ```
///
/// [`Shape`]: crate::shape::Shape
/// [`Selection`]: crate::selection::Selection
/// [`all`]: crate::selection::dsl::all
pub fn selection_from<'a, R>(
    shape: &shape::Shape,
    constraints: &[(&'a str, R)],
) -> Result<Selection, ShapeError>
where
    R: Clone + Into<shape::Range> + 'a,
{
    use crate::selection::dsl::*;

    let mut label_to_constraint = HashMap::new();
    for (label, r) in constraints {
        if !shape.labels().iter().any(|l| l == label) {
            return Err(ShapeError::InvalidLabels {
                labels: vec![label.to_string()],
            });
        }
        label_to_constraint.insert(*label, r.clone().into());
    }

    let selection = shape.labels().iter().rev().fold(true_(), |acc, label| {
        if let Some(rng) = label_to_constraint.get(label.as_str()) {
            range(rng.clone(), acc)
        } else {
            all(acc)
        }
    });

    Ok(selection)
}

/// Construct a [`Selection`] from a [`Shape`] using labeled dimension
/// constraints.
///
/// This macro provides a convenient syntax for specifying
/// sub-selections on a shape by labeling dimensions and applying
/// either exact indices or ranges. Internally, it wraps
/// [`selection_from_one`] and [`selection_from`] to produce a
/// fully-aligned [`Selection`] expression.
///
/// # Forms
///
/// - Single labeled range:
///   ```
///   let shape = ndslice::shape!(zone = 2, host = 4, gpu = 8);
///   let sel = ndslice::sel_from_shape!(&shape, host = 1..3);
///   ```
///
/// - Multiple exact indices (converted to `n..n+1`):
///   ```
///   let shape = ndslice::shape!(zone = 2, host = 4, gpu = 8);
///   let sel = ndslice::sel_from_shape!(&shape, zone = 1, gpu = 4);
///   ```
///
/// - Multiple labeled ranges:
///   ```
///   let shape = ndslice::shape!(zone = 2, host = 4, gpu = 8);
///   let sel = ndslice::sel_from_shape!(&shape, zone = 0..1, host = 1..3, gpu = 4..8);
///   ```
///
/// # Panics
///
/// This macro calls `.unwrap()` on the result of the underlying
/// functions. It will panic if any label is not found in the shape.
///
/// # See Also
///
/// - [`selection_from_one`]
/// - [`selection_from`]
/// - [`Selection`]
/// - [`Shape`]
///
/// [`Selection`]: crate::selection::Selection
/// [`Shape`]: crate::shape::Shape
/// [`selection_from_one`]: crate::selection::selection_from_one
/// [`selection_from`]: crate::selection::selection_from
#[macro_export]
macro_rules! sel_from_shape {
    ($shape:expr, $label:ident = $range:expr) => {
        $crate::selection::selection_from_one($shape, stringify!($label), $range).unwrap()
    };

    ($shape:expr, $($label:ident = $val:literal),* $(,)?) => {
        $crate::selection::selection_from($shape,
                                          &[
                                              $((stringify!($label), $val..$val+1)),*
                                          ]).unwrap()
    };

    ($shape:expr, $($label:ident = $range:expr),* $(,)?) => {
        $crate::selection::selection_from($shape, &[
            $((stringify!($label), $range)),*
        ]).unwrap()
    };
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::BTreeSet;

    use super::EvalOpts;
    use super::ReifySlice;
    use super::Selection;
    use super::dsl::*;
    use super::is_equivalent_true;
    use crate::Range;
    use crate::Slice;
    use crate::assert_structurally_eq;
    use crate::select;
    use crate::shape;
    use crate::shape::ShapeError;

    // A test slice: (zones = 2, hosts = 4, gpus = 8).
    fn test_slice() -> Slice {
        Slice::new(0usize, vec![2, 4, 8], vec![32, 8, 1]).unwrap()
    }

    // Given expression `expr`, options `opts` and slice `slice`,
    // canonical usage is:
    // ```rust
    // let nodes = expr.eval(&opts, slice.clone())?.collect::<Vec<usize>>();
    // ```
    // This utility cuts down on the syntactic repetition that results
    // from the above in the tests that follow.
    fn eval(expr: Selection, slice: &Slice) -> Vec<usize> {
        expr.eval(&EvalOpts::lenient(), slice).unwrap().collect()
    }

    #[test]
    fn test_selection_00() {
        let slice = &test_slice();

        // No GPUs on any host in any region.
        assert!(eval(false_(), slice).is_empty());
        assert!(eval(all(false_()), slice).is_empty());
        assert!(eval(all(all(false_())), slice).is_empty());

        // All GPUs on all hosts in all regions.
        assert_eq!((0..=63).collect::<Vec<_>>(), eval(true_(), slice));
        assert_eq!(eval(true_(), slice), eval(all(true_()), slice));
        assert_eq!(eval(all(true_()), slice), eval(all(all(true_())), slice));

        // Terminal `true_()` and `false_()` selections are allowed at
        // the leaf.
        assert_eq!(eval(true_(), slice), eval(all(all(all(true_()))), slice));
        assert!(eval(all(all(all(false_()))), slice).is_empty());
    }

    #[test]
    fn test_selection_01() {
        let slice = &test_slice();

        // Structural combinators beyond the slice's dimensionality
        // are invalid.
        let expr = all(all(all(all(true_()))));
        let result = expr.validate(&EvalOpts::lenient(), slice);
        assert!(
            matches!(result, Err(ShapeError::SelectionTooDeep { .. })),
            "Unexpected: {:?}",
            result
        );
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "selection `all(all(all(all(true_()))))` exceeds dimensionality 3"
        );
    }

    #[test]
    fn test_selection_02() {
        let slice = &test_slice();

        // GPU 0 on host 0 in region 0.
        let select = range(0..=0, range(0..=0, range(0..=0, true_())));
        assert_eq!((0..=0).collect::<Vec<_>>(), eval(select, slice));

        // GPU 1 on host 1 in region 1.
        let select = range(1..=1, range(1..=1, range(1..=1, true_())));
        assert_eq!((41..=41).collect::<Vec<_>>(), eval(select, slice));

        // All GPUs on host 0 in all regions:
        let select = all(range(0..=0, all(true_())));
        assert_eq!(
            (0..=7).chain(32..=39).collect::<Vec<_>>(),
            eval(select, slice)
        );

        // All GPUs on host 1 in all regions:
        let select = all(range(1..=1, all(true_())));
        assert_eq!(
            (8..=15).chain(40..=47).collect::<Vec<_>>(),
            eval(select, slice)
        );

        // The first 4 GPUs on all hosts in all regions:
        let select = all(all(range(0..4, true_())));
        assert_eq!(
            (0..=3)
                .chain(8..=11)
                .chain(16..=19)
                .chain(24..=27)
                .chain(32..=35)
                .chain(40..=43)
                .chain(48..=51)
                .chain(56..=59)
                .collect::<Vec<_>>(),
            eval(select, slice)
        );

        // The last 4 GPUs on all hosts in all regions:
        let select = all(all(range(4..8, true_())));
        assert_eq!(
            (4..=7)
                .chain(12..=15)
                .chain(20..=23)
                .chain(28..=31)
                .chain(36..=39)
                .chain(44..=47)
                .chain(52..=55)
                .chain(60..=63)
                .collect::<Vec<_>>(),
            eval(select, slice)
        );

        // All regions, all hosts, odd GPUs:
        let select = all(all(range(shape::Range(1, None, 2), true_())));
        assert_eq!(
            (1..8)
                .step_by(2)
                .chain((9..16).step_by(2))
                .chain((17..24).step_by(2))
                .chain((25..32).step_by(2))
                .chain((33..40).step_by(2))
                .chain((41..48).step_by(2))
                .chain((49..56).step_by(2))
                .chain((57..64).step_by(2))
                .collect::<Vec<_>>(),
            eval(select, slice)
        );
    }

    #[test]
    fn test_selection_03() {
        let slice = &test_slice();

        assert_eq!(
            eval(intersection(true_(), true_()), slice),
            eval(true_(), slice)
        );
        assert_eq!(
            eval(intersection(true_(), false_()), slice),
            eval(false_(), slice)
        );
        assert_eq!(
            eval(intersection(false_(), true_()), slice),
            eval(false_(), slice)
        );
        assert_eq!(
            eval(intersection(false_(), false_()), slice),
            eval(false_(), slice)
        );
        assert_eq!(
            eval(
                intersection(
                    all(all(range(0..=3, true_()))),
                    all(all(range(4..=7, true_())))
                ),
                slice
            ),
            eval(false_(), slice)
        );
        assert_eq!(
            eval(intersection(true_(), all(all(range(4..8, true_())))), slice),
            eval(all(all(range(4..8, true_()))), slice)
        );
        assert_eq!(
            eval(
                intersection(
                    all(all(range(0..=4, true_()))),
                    all(all(range(4..=7, true_())))
                ),
                slice
            ),
            eval(all(all(range(4..=4, true_()))), slice)
        );
    }

    #[test]
    fn test_selection_04() {
        let slice = &test_slice();

        assert_eq!(eval(union(true_(), true_()), slice), eval(true_(), slice));
        assert_eq!(eval(union(false_(), true_()), slice), eval(true_(), slice));
        assert_eq!(eval(union(true_(), false_()), slice), eval(true_(), slice));
        assert_eq!(
            eval(union(false_(), false_()), slice),
            eval(false_(), slice)
        );
        assert_eq!(
            eval(
                union(
                    all(all(range(0..4, true_()))),
                    all(all(range(4.., true_())))
                ),
                slice
            ),
            eval(true_(), slice)
        );

        // Across all regions, get the first 4 GPUs on host 0 and the
        // last 4 GPUs on host 1.
        let s = all(range(0..=0, range(0..4, true_())));
        let t = all(range(1..=1, range(4.., true_())));
        assert_eq!(
            (0..=3)
                .chain(12..=15)
                .chain(32..=35)
                .chain(44..=47)
                .collect::<Vec<_>>(),
            eval(union(s, t), slice)
        );

        // All regions, all hosts, skip GPUs 2, 3, 4 and 5.
        assert_eq!(
            // z=0, h=0
            (0..=1)
                .chain(6..=7)
                // z=0, h=1
                .chain(8..=9)
                .chain(14..=15)
                // z=0, h=2
                .chain(16..=17)
                .chain(22..=23)
                // z=0, h=3
                .chain(24..=25)
                .chain(30..=31)
                // z=1, h=0
                .chain(32..=33)
                .chain(38..=39)
                // z=1, h=1
                .chain(40..=41)
                .chain(46..=47)
                // z=1, h=2
                .chain(48..=49)
                .chain(54..=55)
                // z=1, h=3
                .chain(56..=57)
                .chain(62..=63)
                .collect::<Vec<_>>(),
            eval(
                all(all(union(range(0..2, true_()), range(6..8, true_())))),
                slice
            )
        );

        // All regions, all hosts, odd GPUs.
        assert_eq!(
            (1..8)
                .step_by(2)
                .chain((9..16).step_by(2))
                .chain((17..24).step_by(2))
                .chain((25..32).step_by(2))
                .chain((33..40).step_by(2))
                .chain((41..48).step_by(2))
                .chain((49..56).step_by(2))
                .chain((57..64).step_by(2))
                .collect::<Vec<_>>(),
            eval(
                all(all(union(
                    range(shape::Range(1, Some(4), 2), true_()),
                    range(shape::Range(5, Some(8), 2), true_())
                ))),
                slice
            )
        );
        assert_eq!(
            eval(
                all(all(union(
                    range(shape::Range(1, Some(4), 2), true_()),
                    range(shape::Range(5, Some(8), 2), true_())
                ))),
                slice
            ),
            eval(
                all(all(union(
                    union(range(1..=1, true_()), range(3..=3, true_()),),
                    union(range(5..=5, true_()), range(7..=7, true_()),),
                ))),
                slice
            ),
        );
    }

    #[test]
    fn test_selection_05() {
        let slice = &test_slice();

        // First region, first host, no GPU.
        assert!(eval(first(first(false_())), slice).is_empty());
        // First region, first host, first GPU.
        assert_eq!(vec![0], eval(first(first(range(0..1, true_()))), slice));
        // First region, first host, all GPUs.
        assert_eq!(
            (0..8).collect::<Vec<_>>(),
            eval(first(first(true_())), slice)
        );

        // Terminal `true_()` and `false_()` selections are allowed at
        // the leaf.
        // First region, first host, no GPU.
        assert!(eval(first(first(first(false_()))), slice).is_empty());
        // First region, first host, first GPU.
        assert_eq!(vec![0], eval(first(first(first(true_()))), slice));

        // All regions, first host, all GPUs.
        assert_eq!(
            (0..8).chain(32..40).collect::<Vec<_>>(),
            eval(all(first(true_())), slice)
        );

        // First region, first host, GPUs 0, 1 and 2.
        assert_eq!(
            (0..3).collect::<Vec<_>>(),
            eval(first(first(range(0..=2, true_()))), slice)
        );
    }

    #[test]
    fn test_selection_06() {
        let slice = &test_slice();

        // Structural combinators beyond the slice's dimensionality
        // are invalid.
        let expr = first(first(first(first(true_()))));
        let result = expr.validate(&EvalOpts::lenient(), slice);
        assert!(
            matches!(result, Err(ShapeError::SelectionTooDeep { .. })),
            "Unexpected: {:?}",
            result
        );
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "selection `first(first(first(first(true_()))))` exceeds dimensionality 3"
        );
    }

    #[test]
    fn test_selection_07() {
        use crate::select;
        use crate::shape;

        // 2 x 8 row-major.
        let s = shape!(host = 2, gpu = 8);

        // All GPUs on host 1.
        assert_eq!(
            select!(s, host = 1)
                .unwrap()
                .slice()
                .iter()
                .collect::<Vec<_>>(),
            eval(range(1..2, true_()), s.slice())
        );

        // All hosts, GPUs 2 through 7.
        assert_eq!(
            select!(s, gpu = 2..)
                .unwrap()
                .slice()
                .iter()
                .collect::<Vec<_>>(),
            eval(all(range(2.., true_())), s.slice())
        );

        // All hosts, GPUs 3 and 4.
        assert_eq!(
            select!(s, gpu = 3..5)
                .unwrap()
                .slice()
                .iter()
                .collect::<Vec<_>>(),
            eval(all(range(3..5, true_())), s.slice())
        );

        // GPUS 3 and 4 on host 1.
        assert_eq!(
            select!(s, gpu = 3..5, host = 1)
                .unwrap()
                .slice()
                .iter()
                .collect::<Vec<_>>(),
            eval(range(1..=1, range(3..5, true_())), s.slice())
        );

        // All hosts, no GPUs.
        assert_matches!(
            select!(s, gpu = 1..1).unwrap_err(),
            ShapeError::EmptyRange {
                range: shape::Range(1, Some(1), 1)
            },
        );
        assert!(eval(all(range(1..1, true_())), s.slice()).is_empty());

        // All hosts, GPU 8.
        assert_matches!(
            select!(s, gpu = 8).unwrap_err(),
            ShapeError::OutOfRange {
                range: shape::Range(8, Some(9), 1),
                dim,
                size: 8,
            } if dim == "gpu",
        );
        assert!(eval(all(range(8..8, true_())), s.slice()).is_empty());
    }

    #[test]
    fn test_selection_08() {
        let s = &shape!(host = 2, gpu = 8);

        assert_eq!(
            eval(range(1..2, true_()), s.slice()),
            eval(sel_from_shape!(s, host = 1), s.slice())
        );

        assert_eq!(
            eval(all(range(2.., true_())), s.slice()),
            eval(sel_from_shape!(s, gpu = 2..), s.slice())
        );

        assert_eq!(
            eval(all(range(3..5, true_())), s.slice()),
            eval(sel_from_shape!(s, gpu = 3..5), s.slice())
        );

        assert_eq!(
            eval(range(1..2, range(3..5, true_())), s.slice()),
            eval(sel_from_shape!(s, host = 1..2, gpu = 3..5), s.slice())
        );

        assert_eq!(
            eval(
                union(
                    sel_from_shape!(s, host = 0..1, gpu = 0..4),
                    sel_from_shape!(s, host = 1..2, gpu = 4..5)
                ),
                s.slice()
            ),
            eval(
                union(
                    range(0..1, range(0..4, true_())),
                    range(1..2, range(4..5, true_()))
                ),
                s.slice()
            )
        );
    }

    #[test]
    fn test_selection_09() {
        let slice = &test_slice(); // 2 x 4 x 8

        // Identity.
        assert_eq!(eval(any(false_()), slice), eval(false_(), slice));

        // An arbitrary GPU.
        let res = eval(any(any(any(true_()))), slice);
        assert_eq!(res.len(), 1);
        assert!(res[0] < 64);

        // The first 4 GPUs of any host in region-0.
        let res = eval(range(0, any(range(0..4, true_()))), slice);
        assert!((0..4).any(|host| res == eval(range(0, range(host, range(0..4, true_()))), slice)));

        // Any GPU on host-0 in region-0.
        let res = eval(range(0, range(0, any(true_()))), slice);
        assert_eq!(res.len(), 1);
        assert!(res[0] < 8);

        // All GPUs on any host in region-0.
        let res = eval(range(0, any(true_())), slice);
        assert!((0..4).any(|host| res == eval(range(0, range(host, true_())), slice)));

        // All GPUs on any host in region-1.
        let res = eval(range(1, any(true_())), slice);
        assert!((0..4).any(|host| res == eval(range(1, range(host, true_())), slice)));

        // Any two GPUs on host-0 in region-0.
        let mut res = vec![];
        while res.len() < 2 {
            res = eval(
                union(
                    range(0, range(0, any(true_()))),
                    range(0, range(0, any(true_()))),
                ),
                slice,
            );
        }
        assert_matches!(res.as_slice(), [i, j] if *i < *j && *i < 8 && *j < 8);
    }

    #[test]
    fn test_eval_zero_dim_slice() {
        let slice_0d = Slice::new(1, vec![], vec![]).unwrap();
        // Let s be a slice with dim(s) = 0. Then: ∃! x ∈ s :
        // coordsₛ(x) = ().
        assert_eq!(slice_0d.coordinates(1).unwrap(), vec![]);

        assert_eq!(eval(true_(), &slice_0d), vec![1]);
        assert_eq!(eval(false_(), &slice_0d), vec![]);
        assert_eq!(eval(all(true_()), &slice_0d), vec![1]);
        assert_eq!(eval(all(false_()), &slice_0d), vec![]);
        assert_eq!(eval(union(true_(), true_()), &slice_0d), vec![1]);
        assert_eq!(eval(intersection(true_(), false_()), &slice_0d), vec![]);
    }

    #[test]
    fn test_selection_10() {
        let slice = &test_slice();
        let opts = EvalOpts {
            disallow_dynamic_selections: true,
            ..EvalOpts::lenient()
        };
        let expr = any(any(any(true_())));
        let res = expr.validate(&opts, slice);
        assert_matches!(res, Err(ShapeError::SelectionDynamic { .. }));
    }

    #[test]
    fn test_13() {
        // Structural identity: `all(true)` <=> `true`.
        assert!(is_equivalent_true(true_()));
        assert!(is_equivalent_true(all(true_())));
        assert!(is_equivalent_true(all(all(true_()))));
        assert!(is_equivalent_true(all(all(all(true_())))));
        assert!(is_equivalent_true(all(all(all(all(true_()))))));
        assert!(is_equivalent_true(all(all(all(all(all(true_())))))));
        // ...

        assert!(!is_equivalent_true(false_()));
        assert!(!is_equivalent_true(union(true_(), true_())));
        assert!(!is_equivalent_true(range(0..=0, true_())));
        assert!(!is_equivalent_true(all(false_())));
    }

    #[test]
    fn test_14() {
        use std::collections::HashSet;

        use crate::selection::NormalizedSelectionKey;
        use crate::selection::dsl::*;

        let a = all(all(true_()));
        let b = all(all(true_()));

        let key_a = NormalizedSelectionKey::new(&a);
        let key_b = NormalizedSelectionKey::new(&b);

        // They should be structurally equal.
        assert_eq!(key_a, key_b);

        // Their hashes should agree, and they deduplicate in a set.
        let mut set = HashSet::new();
        set.insert(key_a);
        assert!(set.contains(&key_b));
    }

    #[test]
    fn test_contains_true() {
        let selection = true_();
        assert!(selection.contains(&[0, 0, 0]));
        assert!(selection.contains(&[1, 2, 3]));
    }

    #[test]
    fn test_contains_false() {
        let selection = false_();
        assert!(!selection.contains(&[0, 0, 0]));
        assert!(!selection.contains(&[1, 2, 3]));
    }

    #[test]
    fn test_contains_all() {
        let selection = all(true_());
        assert!(selection.contains(&[0, 0, 0]));
        assert!(selection.contains(&[1, 2, 3]));
    }

    #[test]
    fn test_contains_range() {
        let selection = range(1..3, true_());
        assert!(selection.contains(&[1, 0, 0]));
        assert!(!selection.contains(&[3, 0, 0]));
    }

    #[test]
    fn test_contains_intersection() {
        let selection = intersection(range(1..3, true_()), range(2..4, true_()));
        assert!(selection.contains(&[2, 0, 0]));
        assert!(!selection.contains(&[1, 0, 0]));
    }

    #[test]
    fn test_contains_union() {
        let selection = union(range(1..2, true_()), range(3..4, true_()));
        assert!(selection.contains(&[1, 0, 0]));
        assert!(!selection.contains(&[2, 0, 0]));
    }

    #[test]
    #[should_panic(expected = "not implemented")]
    fn test_contains_any() {
        let selection = any(true_());
        selection.contains(&[0, 0, 0]);
    }

    #[test]
    #[should_panic(expected = "not implemented")]
    fn test_contains_label() {
        let selection = label(vec!["zone".to_string()], true_());
        selection.contains(&[1, 2, 3]);
    }

    #[test]
    #[should_panic(expected = "not implemented")]
    fn test_contains_first() {
        let selection = first(true_());
        selection.contains(&[0, 0, 0]);
    }

    #[test]
    fn test_difference_1d() {
        assert_eq!(
            true_()
                .difference(
                    &EvalOpts::strict(),
                    &Slice::new_row_major([5]),
                    &[2usize, 4].into(),
                )
                .unwrap()
                .collect::<Vec<_>>(),
            vec![0, 1, 3]
        );
    }

    #[test]
    fn test_difference_empty_selection() {
        assert_eq!(
            false_()
                .difference(
                    &EvalOpts::strict(),
                    &Slice::new_row_major([3]),
                    &[0usize, 1].into(),
                )
                .unwrap()
                .collect::<Vec<_>>(),
            vec![]
        );
    }

    #[test]
    fn test_difference_2d() {
        // [[0, 1, 2],
        //  [3, 4, 5]]
        // Select everything, exclude the second row.
        assert_eq!(
            all(all(true_()))
                .difference(
                    &EvalOpts::strict(),
                    &Slice::new_row_major([2, 3]),
                    &[3usize, 4, 5].into(),
                )
                .unwrap()
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn test_of_ranks_1d() {
        let slice = Slice::new_row_major([5]);
        let ranks = BTreeSet::from([1, 3]);
        let selection = Selection::of_ranks(&slice, &ranks).unwrap();
        assert_eq!(
            selection
                .eval(&EvalOpts::strict(), &slice)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![1, 3]
        )
    }

    #[test]
    fn test_of_ranks_empty_set() {
        let slice = Slice::new_row_major([4]);
        let ranks = BTreeSet::new();
        let selection = Selection::of_ranks(&slice, &ranks).unwrap();
        assert_eq!(
            selection
                .eval(&EvalOpts::strict(), &slice)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![]
        )
    }

    #[test]
    fn test_of_ranks_singleton_structural() {
        let slice = Slice::new_row_major([5]);
        let ranks = BTreeSet::from([2]);
        let actual = Selection::of_ranks(&slice, &ranks).unwrap();
        let expected = range(2..=2, true_());
        assert_structurally_eq!(&actual, &expected);
    }

    #[test]
    fn test_of_ranks_union_2d_structural() {
        let slice = Slice::new_row_major([2, 3]);
        // [ [0, 1, 2],
        //   [3, 4, 5] ]
        // We'll select (0, 2), (1, 0) and (1, 1).
        let ranks = BTreeSet::from([2, 3, 4]);
        let actual = Selection::of_ranks(&slice, &ranks).unwrap();
        // Each rank becomes a nested selection:
        // 2 -> (0, 2) -> range(0, range(2, true_()))
        // 3 -> (1, 0) -> range(1, range(0, true_()))
        // 4 -> (1, 1) -> range(1, range(1, true_()))
        //
        // Their union is:
        let expected = union(
            union(range(0, range(2, true_())), range(1, range(0, true_()))),
            range(1, range(1, true_())),
        );
        assert_structurally_eq!(&actual, &expected);
    }

    #[test]
    fn test_of_ranks_3d_structural() {
        let slice = Slice::new_row_major([2, 2, 2]);
        // [ [ [0, 1],
        //     [2, 3] ],
        //   [ [4, 5],
        //     [6, 7] ] ]
        let ranks = BTreeSet::from([1, 6]);
        let actual = Selection::of_ranks(&slice, &ranks).unwrap();
        let expected = union(
            range(0, range(0, range(1, true_()))), // (0, 0, 1)
            range(1, range(1, range(0, true_()))), // (1, 1, 0)
        );
        assert_structurally_eq!(&actual, &expected);
    }

    #[test]
    fn test_of_ranks_invalid_index() {
        let slice = Slice::new_row_major([4]);
        let ranks = BTreeSet::from([0, 4]); // 4 is out of bounds
        assert!(
            Selection::of_ranks(&slice, &ranks).is_err(),
            "expected out-of-bounds error"
        );
    }

    #[test]
    fn test_reify_slice_empty() {
        let slice = Slice::new_row_major([0]);
        let selection = slice.reify_slice(&slice).unwrap();
        let expected = false_();
        assert_structurally_eq!(&selection, expected);
        assert_eq!(
            selection
                .eval(&EvalOpts::lenient(), &slice)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![]
        );
    }

    #[test]
    fn test_reify_slice_1d() {
        let shape = shape!(x = 6); // 1D shape with 6 elements
        let base = shape.slice();

        let selected = select!(shape, x = 2..5).unwrap();
        let view = selected.slice();

        let selection = base.reify_slice(view).unwrap();
        let expected = range(2..5, true_());
        assert_structurally_eq!(&selection, expected);

        let flat: Vec<_> = selection.eval(&EvalOpts::strict(), base).unwrap().collect();
        assert_eq!(flat, vec![2, 3, 4]);
    }

    #[test]
    fn test_reify_slice_2d() {
        let shape = shape!(x = 4, y = 5); // 2D shape: 4 rows, 5 columns
        let base = shape.slice();

        // Select the middle 2x3 block: rows 1..3 and columns 2..5
        let selected = select!(shape, x = 1..3, y = 2..5).unwrap();
        let view = selected.slice();
        let selection = base.reify_slice(view).unwrap();
        let expected = range(1..3, range(2..5, true_()));
        assert_structurally_eq!(&selection, expected);

        let flat: Vec<_> = selection.eval(&EvalOpts::strict(), base).unwrap().collect();
        assert_eq!(
            flat,
            vec![
                base.location(&[1, 2]).unwrap(),
                base.location(&[1, 3]).unwrap(),
                base.location(&[1, 4]).unwrap(),
                base.location(&[2, 2]).unwrap(),
                base.location(&[2, 3]).unwrap(),
                base.location(&[2, 4]).unwrap(),
            ]
        );
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_reify_slice_1d_with_stride() {
        let shape = shape!(x = 7); // 1D shape with 7 elements
        let selected = shape.select("x", Range(0, None, 2)).unwrap();
        let view = selected.slice();
        assert_eq!(view, &Slice::new(0, vec![4], vec![1 * 2]).unwrap());

        let base = shape.slice();
        let selection = base.reify_slice(view).unwrap();
        // Note: ceil(7 / 2) = 4, hence end = 0 + 2 × 4 = 8. See the
        // more detailed explanation in
        // `test_reify_slice_2d_with_stride`.
        let expected = range(Range(0, Some(8), 2), true_());
        assert_structurally_eq!(&selection, expected);

        let flat: Vec<_> = selection.eval(&EvalOpts::strict(), base).unwrap().collect();
        assert_eq!(flat, vec![0, 2, 4, 6]);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_reify_slice_2d_with_stride() {
        // 4 x 4: x = 4, y = 4.
        let base = shape!(x = 4, y = 4);
        // Step 1: select odd rows (x = 1..4 step 2)
        let shape = base.select("x", Range(1, Some(4), 2)).unwrap();
        // Step 2: then select odd columns (y = 1..4 step 2)
        let shape = shape.select("y", Range(1, Some(4), 2)).unwrap();
        let view = shape.slice();
        assert_eq!(
            view,
            &Slice::new(5, vec![2, 2], vec![4 * 2, 1 * 2]).unwrap()
        );

        let base = base.slice();
        let selection = base.reify_slice(view).unwrap();
        // We use `end = start + step * len` to reify the selection.
        // Note: This may yield `end > original_end` (e.g., 5 instead of 4)
        // when the selection length was computed via ceiling division.
        // This is safe: the resulting range will still select the correct
        // indices (e.g., 1 and 3 for Range(1, Some(5), 2)).
        let expected = range(Range(1, Some(5), 2), range(Range(1, Some(5), 2), true_()));
        assert_structurally_eq!(&selection, expected);

        let flat: Vec<_> = selection.eval(&EvalOpts::strict(), base).unwrap().collect();
        assert_eq!(flat, vec![5, 7, 13, 15]);
    }

    #[test]
    fn test_reify_slice_selects_column_across_rows() {
        let shape = shape!(host = 2, gpu = 4); // shape [2, 4]
        let base = shape.slice();

        // Select the 3rd GPU (index 2) across both hosts
        let selected = select!(shape, gpu = 2).unwrap(); // (0, 2) and (1, 2)
        let view = selected.slice();
        let coordinates: Vec<_> = view.iter().map(|i| view.coordinates(i).unwrap()).collect();
        assert_eq!(coordinates, [[0, 0], [1, 0]]);

        let selection = base.reify_slice(view).unwrap();
        let expected = range(0..2, range(2..3, true_()));
        assert_structurally_eq!(&selection, expected);

        let actual = selection
            .eval(&EvalOpts::strict(), base)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                base.location(&[0, 2]).unwrap(),
                base.location(&[1, 2]).unwrap()
            ]
        );
    }

    #[test]
    fn test_reify_slice_dimension_mismatch() {
        let shape = shape!(host = 2, gpu = 4);
        let base = shape.slice();

        // Select the 3rd GPU (index 2) across both hosts i.e. flat
        // indices [2, 6]
        let indices = vec![
            base.location(&[0, 2]).unwrap(),
            base.location(&[1, 2]).unwrap(),
        ];

        let view = Slice::new(indices[0], vec![indices.len()], vec![4]).unwrap();
        let selection = base.reify_slice(&view).unwrap();

        let expected = Selection::of_ranks(base, &indices.iter().cloned().collect()).unwrap();
        assert_structurally_eq!(&selection, expected);

        let actual: Vec<_> = selection.eval(&EvalOpts::strict(), base).unwrap().collect();
        assert_eq!(actual, indices);
    }

    #[test]
    fn test_union_of_slices_empty() {
        let base = Slice::new_row_major([2]);
        let sel = base.reify_slices(&[]).unwrap();
        assert_structurally_eq!(&sel, &false_());
        assert_eq!(
            sel.eval(&EvalOpts::strict(), &base)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![]
        );
    }

    #[test]
    fn test_union_of_slices_singleton() {
        let shape = shape!(x = 3);
        let base = shape.slice();
        let selected = select!(shape, x = 1).unwrap();
        let view = selected.slice().clone();

        let selection = base.reify_slices(&[view]).unwrap();
        let expected = range(1..=1, true_());
        assert_structurally_eq!(&selection, &expected);

        assert_eq!(
            selection
                .eval(&EvalOpts::strict(), base)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![1],
        );
    }

    #[test]
    fn test_union_of_slices_disjoint() {
        let shape = shape!(x = 2, y = 2); // 2x2 grid
        let base = shape.slice();

        // View A: (0, *)
        let a = select!(shape, x = 0).unwrap();
        let view_a = a.slice().clone();

        // View B: (1, *)
        let b = select!(shape, x = 1).unwrap();
        let view_b = b.slice().clone();

        let selection = base.reify_slices(&[view_a, view_b]).unwrap();
        let expected = union(
            range(0..1, range(0..2, true_())),
            range(1..2, range(0..2, true_())),
        );
        assert_structurally_eq!(&selection, &expected);
        assert_eq!(
            selection
                .eval(&EvalOpts::strict(), base)
                .unwrap()
                .collect::<Vec<_>>(),
            base.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_union_of_slices_overlapping() {
        let shape = shape!(x = 1, y = 4); // 1x4 grid
        let base = shape.slice();

        let selected1 = select!(shape, y = 0..2).unwrap();
        let view1 = selected1.slice().clone();

        let selected2 = select!(shape, y = 1..4).unwrap();
        let view2 = selected2.slice().clone();

        let selection = base.reify_slices(&[view1, view2]).unwrap();
        let expected = union(
            range(0..1, range(0..2, true_())),
            range(0..1, range(1..4, true_())),
        );
        assert_structurally_eq!(&selection, &expected);

        assert_eq!(
            selection
                .eval(&EvalOpts::strict(), base)
                .unwrap()
                .collect::<Vec<_>>(),
            base.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_canonicalize_to_dimensions() {
        assert_structurally_eq!(
            true_().canonicalize_to_dimensions(3),
            &all(all(all(true_())))
        );
        assert_structurally_eq!(
            all(true_()).canonicalize_to_dimensions(3),
            &all(all(all(true_())))
        );
        assert_structurally_eq!(
            all(all(true_())).canonicalize_to_dimensions(3),
            &all(all(all(true_())))
        );
        assert_structurally_eq!(
            all(all(all(true_()))).canonicalize_to_dimensions(3),
            &all(all(all(true_())))
        );

        assert_structurally_eq!(
            false_().canonicalize_to_dimensions(3),
            &all(all(all(false_())))
        );
        assert_structurally_eq!(
            all(false_()).canonicalize_to_dimensions(3),
            &all(all(all(false_())))
        );
        assert_structurally_eq!(
            all(all(false_())).canonicalize_to_dimensions(3),
            &all(all(all(false_())))
        );
        assert_structurally_eq!(
            all(all(all(false_()))).canonicalize_to_dimensions(3),
            &all(all(all(false_())))
        );

        assert_structurally_eq!(
            any(true_()).canonicalize_to_dimensions(3),
            &any(any(any(true_())))
        );
        assert_structurally_eq!(
            any(any(true_())).canonicalize_to_dimensions(3),
            &any(any(any(true_())))
        );
        assert_structurally_eq!(
            any(any(any(true_()))).canonicalize_to_dimensions(3),
            &any(any(any(true_())))
        );

        // 0:1 -> 0:1, *, * <=> range(0..1, all(all(true_())))
        assert_structurally_eq!(
            range(0..1, true_()).canonicalize_to_dimensions(3),
            &range(0..1, all(all(true_())))
        );
        // *, 0:1 -> *, 0:1, * <=> all(range(0..1, all(true_())))
        assert_structurally_eq!(
            all(range(0..1, true_())).canonicalize_to_dimensions(3),
            &all(range(0..1, all(true_())))
        );
        // 0:1, ? -> 0:1, ?, ? <=> range(0..1, any(any(true_())))
        assert_structurally_eq!(
            range(0..1, any(true_())).canonicalize_to_dimensions(3),
            &range(0..1, any(any(true_())))
        );
        // 0:1, ?, * -> 0:1, ?, * <=> range(0..1, any(all(true_())))
        assert_structurally_eq!(
            range(0..1, any(all(true_()))).canonicalize_to_dimensions(3),
            &range(0..1, any(all(true_())))
        );
    }
}
