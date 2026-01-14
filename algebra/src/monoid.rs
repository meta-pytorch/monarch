/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Monoid: semigroup with identity element.

use super::Semigroup;

/// A semigroup with an identity element.
///
/// Laws:
/// - Identity: `empty().combine(a) == a` and `a.combine(empty()) == a`
/// - Associativity: inherited from Semigroup
pub trait Monoid: Semigroup {
    /// The identity element.
    fn empty() -> Self;

    /// Combine all elements from an iterator.
    fn concat<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Self>,
        Self: Sized,
    {
        iter.into_iter()
            .fold(Self::empty(), |acc, x| acc.combine(&x))
    }
}

/// A monoid where the operation is commutative.
///
/// Additional law:
/// - Commutativity: `a.combine(b) == b.combine(a)`
pub trait CommutativeMonoid: Monoid {}
