/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Semigroup: associative binary operation.

/// A type with an associative binary operation.
///
/// Laws:
/// - Associativity: `a.combine(b).combine(c) == a.combine(b.combine(c))`
pub trait Semigroup {
    /// Combine two values associatively.
    fn combine(&self, other: &Self) -> Self;

    /// In-place variant of combine (optional optimization).
    fn combine_assign(&mut self, other: &Self)
    where
        Self: Sized,
    {
        *self = self.combine(other);
    }
}
