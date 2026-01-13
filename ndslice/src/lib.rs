/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Core mesh components for the hyperactor framework.
//!
//! Provides [`Slice`], a compact representation of a subset of a
//! multidimensional array. See [`Slice`] for more details.
//!
//! This crate defines the foundational abstractions used in
//! hyperactor's mesh layer, including multidimensional shapes and
//! selection algebra. The crate avoids dependencies on procedural
//! macros and other higher-level constructs, enabling reuse in both
//! runtime and macro contexts.

#![feature(assert_matches)]
#![recursion_limit = "512"]

mod slice;
pub use slice::DimSliceIterator;
pub use slice::Slice;
pub use slice::SliceError;
pub use slice::SliceIterator;

/// Selection algebra for describing multidimensional mesh regions.
pub mod selection;

/// Core types for representing multidimensional shapes and strides.
pub mod shape;

/// Reshaping transformations for multidimensional slices and shapes.
pub mod reshape;

/// The selection expression type used to define routing constraints.
pub use selection::Selection;
/// DSL-style constructors for building `Selection` expressions.
pub use selection::dsl;
/// Represents an interval with an optional end and step, used to
/// define extents in `Shape` and coordinate filters in `Selection`.
pub use shape::Range;
/// Describes the size and layout of a multidimensional mesh.
pub use shape::Shape;
/// Errors that can occur during shape construction or validation.
pub use shape::ShapeError;

/// Property-based generators for randomized test input. TODO: Move
/// into dedicated crate and access via
// `test_deps`.
pub mod strategy;

/// Utilities.
pub mod utils;

/// A small parsing library for identifiers.
pub mod parse;

/// Types to describe extents, points and views.
pub mod view;
/// Describes the shape of a coordinate space.
pub use view::Extent;
/// Errors that can occur during extent construction or validation.
pub use view::ExtentError;
/// Extension trait for creating points from coordinate vectors.
pub use view::InExtent;
/// Representation of a point in a coordinate space.
pub use view::Point;
/// Errors that can occur during point construction or validation.
pub use view::PointError;
/// Represents a logical view or projection.
pub use view::Region;
/// Trait for data structures from which views can be created.
pub use view::View;
/// Extension methods for view construction.
pub use view::ViewExt;
/// The iterator over views.
pub use view::ViewIterator;
