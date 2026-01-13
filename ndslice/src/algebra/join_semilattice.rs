/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Join-semilattice: commutative, associative, idempotent join.

use super::CommutativeMonoid;
use super::Monoid;
use super::Semigroup;

/// A join-semilattice with an associative, commutative, idempotent
/// join operation.
///
/// Laws:
/// - Associativity: `a.join(b).join(c) == a.join(b.join(c))`
/// - Commutativity: `a.join(b) == b.join(a)`
/// - Idempotence: `a.join(a) == a`
///
/// Idempotence is the key property distinguishing lattices from
/// general semigroups. It ensures that merging the same value
/// multiple times has no additional effect, which is critical for
/// distributed systems with at-least-once delivery semantics.
pub trait JoinSemilattice: Clone {
    /// Join two values to produce their least upper bound.
    fn join(&self, other: &Self) -> Self;

    /// In-place variant of join (optional optimization).
    fn join_assign(&mut self, other: &Self) {
        *self = self.join(other);
    }
}

/// A bounded join-semilattice with an explicit bottom element (⊥).
///
/// Additional law:
/// - Identity: `bottom().join(a) == a` and `a.join(bottom()) == a`
pub trait BoundedJoinSemilattice: JoinSemilattice {
    /// The bottom element (⊥), which is the identity for join.
    fn bottom() -> Self;

    /// Join all elements from an iterator, starting from bottom.
    fn join_all_from_bottom<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Self>,
        Self: Sized,
    {
        iter.into_iter().fold(Self::bottom(), |acc, x| acc.join(&x))
    }
}

// Blanket implementations: JoinSemilattice → Semigroup

impl<T: JoinSemilattice> Semigroup for T {
    fn combine(&self, other: &Self) -> Self {
        self.join(other)
    }

    fn combine_assign(&mut self, other: &Self) {
        self.join_assign(other);
    }
}

impl<T: BoundedJoinSemilattice> Monoid for T {
    fn empty() -> Self {
        Self::bottom()
    }

    fn concat<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        Self::join_all_from_bottom(iter)
    }
}

impl<T: BoundedJoinSemilattice> CommutativeMonoid for T {}
