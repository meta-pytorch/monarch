/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![deny(missing_docs)]

//! Algebraic structures for monotonic accumulation and lattice-based
//! reduction.
//!
//! This module provides foundational traits and types for expressing
//! commutative, associative, and idempotent operations, enabling
//! principled accumulation in distributed systems.
//!
//! # Quick Start
//!
//! ```rust
//! use algebra::BoundedJoinSemilattice;
//! use algebra::JoinSemilattice;
//! use algebra::Max;
//!
//! // Max<T> wraps an ordered type where join = max
//! let a = Max(5);
//! let b = Max(3);
//! let c = a.join(&b);
//! assert_eq!(c, Max(5));
//!
//! // Bottom element is the identity for join
//! assert_eq!(Max::<i32>::bottom().join(&a), a);
//! ```
//!
//! # Core Concepts
//!
//! - **Semigroup**: A type with an associative binary operation
//!   (`combine`). Examples: addition, max, string concatenation.
//!
//! - **Monoid**: A semigroup with an identity element (`empty`).
//!   Examples: 0 for addition, empty string for concatenation.
//!
//! - **CommutativeMonoid**: A monoid where combine is commutative.
//!
//! - **Group**: A monoid where every element has an inverse.
//!
//! - **AbelianGroup**: A commutative group.
//!
//! - **JoinSemilattice**: A commutative, associative, and
//!   **idempotent** merge operation (`join`). You can think of `join`
//!   as computing a **least upper bound**: it produces the smallest
//!   value that is "at least as large as" both inputs under the order
//!   induced by join (`a ≤ b` iff `a.join(&b) == b`). Idempotence (`a
//!   ⊔ a = a`) is what makes this safe under at-least-once delivery:
//!   re-merging the same update is a no-op. Examples: max, min, set
//!   union.
//!
//! - **BoundedJoinSemilattice**: A join-semilattice with an explicit
//!   bottom element (⊥) that serves as the identity for join.
//!
//! # Homomorphisms
//!
//! - **SemigroupHom**: A structure-preserving map between semigroups.
//! - **MonoidHom**: A structure-preserving map between monoids.
//!
//! # Provided Types
//!
//! - [`Max<T>`]: Ordered type where `join = max`, bottom = minimum.
//! - [`Min<T>`]: Ordered type where `join = min`, bottom = maximum.
//! - [`Any`]: Boolean where `join = ||` (OR), bottom = false.
//! - [`All`]: Boolean where `join = &&` (AND), bottom = true.
//! - [`LatticeMap<K, V>`]: Pointwise map lattice over `HashMap`.
//! - [`LWW<T>`]: Last-Writer-Wins register CRDT.
//!
//! # Why Idempotence Matters for Distributed Systems
//!
//! In distributed systems with at-least-once delivery, messages may
//! be delivered multiple times due to retries, network partitions,
//! or failover. Non-idempotent operations (like addition) produce
//! incorrect results when applied multiple times:
//!
//! ```text
//! // Problem: sum accumulator with duplicates
//! sum(1, 2, 2, 3)  // Intended: 1+2+3=6, Actual: 1+2+2+3=8 ❌
//!
//! // Solution: max accumulator is idempotent
//! max(1, 2, 2, 3)  // Always 3, regardless of duplicates ✓
//! ```
//!
//! By using lattice types like [`Max`] and [`Min`], we make the
//! idempotence guarantee explicit in the type system.
//!
//! # Examples
//!
//! ```
//! use algebra::BoundedJoinSemilattice;
//! use algebra::JoinSemilattice;
//! use algebra::Max;
//!
//! let a = Max(5);
//! let b = Max(10);
//! let c = a.join(&b);
//! assert_eq!(c, Max(10));
//!
//! // Idempotence: joining with self has no effect
//! assert_eq!(c.join(&c), c);
//!
//! // Identity: joining with bottom has no effect
//! assert_eq!(c.join(&Max::bottom()), c);
//! ```

mod crdt;
mod join_semilattice;

// Re-export CRDTs
pub use crdt::LWW;
// Re-export concrete lattice types
pub use join_semilattice::All;
pub use join_semilattice::Any;
pub use join_semilattice::LatticeMap;
pub use join_semilattice::Max;
pub use join_semilattice::Min;

// Semigroup

/// A **semigroup**: a type with an associative binary operation.
///
/// Laws (not enforced by type system):
///
/// - **Associative**:
///   `a.combine(b).combine(c) == a.combine(b.combine(c))`
///
/// # Example
///
/// ```rust
/// use algebra::Semigroup;
///
/// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// struct Max(i32);
///
/// impl Semigroup for Max {
///     fn combine(&self, other: &Self) -> Self {
///         Max(self.0.max(other.0))
///     }
/// }
///
/// let x = Max(3);
/// let y = Max(5);
/// let z = Max(2);
/// assert_eq!(x.combine(&y).combine(&z), x.combine(&y.combine(&z)));
/// ```
pub trait Semigroup: Sized {
    /// Combine two elements associatively.
    fn combine(&self, other: &Self) -> Self;

    /// In-place combine.
    fn combine_assign(&mut self, other: &Self) {
        *self = self.combine(other);
    }
}

// Monoid

/// A **monoid**: a semigroup with an identity element.
///
/// Laws (not enforced by type system):
///
/// - **Associative**:
///   `a.combine(b).combine(c) == a.combine(b.combine(c))`
/// - **Left identity**: `empty().combine(a) == a`
/// - **Right identity**: `a.combine(empty()) == a`
///
/// # Example
///
/// ```rust
/// use algebra::Monoid;
/// use algebra::Semigroup;
///
/// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// struct Product(i32);
///
/// impl Semigroup for Product {
///     fn combine(&self, other: &Self) -> Self {
///         Product(self.0 * other.0)
///     }
/// }
///
/// impl Monoid for Product {
///     fn empty() -> Self {
///         Product(1)
///     }
/// }
///
/// let x = Product(3);
/// let y = Product(5);
/// assert_eq!(x.combine(&y), Product(15));
/// assert_eq!(Product::empty().combine(&x), x);
/// assert_eq!(x.combine(&Product::empty()), x);
/// ```
pub trait Monoid: Semigroup {
    /// The identity element.
    fn empty() -> Self;

    /// Fold an iterator using combine, starting from empty.
    fn concat<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        iter.into_iter()
            .fold(Self::empty(), |acc, x| acc.combine(&x))
    }
}

// CommutativeMonoid

/// A **commutative monoid**: a monoid where combine is commutative.
///
/// Laws (not enforced by type system):
///
/// - **Associative**:
///   `a.combine(b).combine(c) == a.combine(b.combine(c))`
/// - **Commutative**: `a.combine(b) == b.combine(a)`
/// - **Identity**: `a.combine(empty()) == a == empty().combine(a)`
pub trait CommutativeMonoid: Monoid {}

// Group

/// A **group**: a monoid where every element has an inverse.
///
/// Laws (not enforced by type system):
///
/// - **Associative**: `a.combine(b).combine(c) == a.combine(b.combine(c))`
/// - **Identity**: `a.combine(empty()) == a == empty().combine(a)`
/// - **Inverse**: `a.combine(a.inverse()) == empty() ==
///   a.inverse().combine(a)`
///
/// # Example
///
/// ```rust
/// use algebra::Group;
/// use algebra::Monoid;
/// use algebra::Semigroup;
///
/// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// struct AddInt(i32);
///
/// impl Semigroup for AddInt {
///     fn combine(&self, other: &Self) -> Self {
///         AddInt(self.0 + other.0)
///     }
/// }
///
/// impl Monoid for AddInt {
///     fn empty() -> Self {
///         AddInt(0)
///     }
/// }
///
/// impl Group for AddInt {
///     fn inverse(&self) -> Self {
///         AddInt(-self.0)
///     }
/// }
///
/// let x = AddInt(5);
/// let inv = x.inverse();
/// assert_eq!(x.combine(&inv), AddInt::empty());
/// ```
pub trait Group: Monoid {
    /// Return the inverse of this element.
    fn inverse(&self) -> Self;
}

// AbelianGroup

/// An **abelian group** (commutative group): a group where combine
/// is commutative.
///
/// Laws (not enforced by type system):
///
/// - **Associative**: `a.combine(b).combine(c) == a.combine(b.combine(c))`
/// - **Commutative**: `a.combine(b) == b.combine(a)`
/// - **Identity**: `a.combine(empty()) == a == empty().combine(a)`
/// - **Inverse**: `a.combine(a.inverse()) == empty() ==
///   a.inverse().combine(a)`
///
/// Named after mathematician Niels Henrik Abel.
pub trait AbelianGroup: Group + CommutativeMonoid {}

// SemigroupHom

/// A **semigroup homomorphism**: a structure-preserving map between
/// semigroups that preserves combine.
///
/// A homomorphism `f: S → T` preserves the semigroup operation:
///
/// Laws (not enforced by type system):
///
/// - **Preserve combine**: `f(x.combine(y)) == f(x).combine(f(y))`
///
/// # Example
///
/// ```rust
/// use algebra::Max;
/// use algebra::Semigroup;
/// use algebra::SemigroupHom;
///
/// // A homomorphism from Max<i32> to Max<i32> that doubles the value
/// struct Double;
///
/// impl SemigroupHom for Double {
///     type Source = Max<i32>;
///     type Target = Max<i32>;
///
///     fn apply(&self, x: &Max<i32>) -> Max<i32> {
///         Max(x.0 * 2)
///     }
/// }
///
/// // Verify: f(x ⊔ y) = f(x) ⊔ f(y)
/// // Since join = max: f(max(a, b)) = max(f(a), f(b))
/// let f = Double;
/// let x = Max(3);
/// let y = Max(5);
/// assert_eq!(f.apply(&x.combine(&y)), f.apply(&x).combine(&f.apply(&y)));
/// ```
pub trait SemigroupHom {
    /// The source semigroup
    type Source: Semigroup;

    /// The target semigroup
    type Target: Semigroup;

    /// Apply the homomorphism
    fn apply(&self, x: &Self::Source) -> Self::Target;
}

/// Helper trait for explicitly specifying source and target types.
///
/// This is a blanket-implemented alias that allows writing
/// `T: SemigroupHomFromTo<S, T>` instead of
/// `T: SemigroupHom<Source = S, Target = T>`.
pub trait SemigroupHomFromTo<S: Semigroup, T: Semigroup>:
    SemigroupHom<Source = S, Target = T>
{
}

impl<H, S, T> SemigroupHomFromTo<S, T> for H
where
    H: SemigroupHom<Source = S, Target = T>,
    S: Semigroup,
    T: Semigroup,
{
}

// MonoidHom

/// A **monoid homomorphism**: a structure-preserving map between
/// monoids that preserves both combine and identity.
///
/// A homomorphism `f: M → N` preserves both the monoid operation and
/// identity:
///
/// Laws (not enforced by type system):
///
/// - **Preserve combine**: `f(x.combine(y)) == f(x).combine(f(y))`
/// - **Preserve identity**: `f(M::empty()) == N::empty()`
///
/// # Example
///
/// ```rust
/// use algebra::Max;
/// use algebra::Monoid;
/// use algebra::MonoidHom;
/// use algebra::Semigroup;
/// use algebra::SemigroupHom;
///
/// // A monoid homomorphism that widens Max<u32> to Max<u64>.
/// // This preserves identity because bottom for both is 0.
/// struct Widen;
///
/// impl SemigroupHom for Widen {
///     type Source = Max<u32>;
///     type Target = Max<u64>;
///
///     fn apply(&self, x: &Max<u32>) -> Max<u64> {
///         Max(x.0 as u64)
///     }
/// }
///
/// impl MonoidHom for Widen {}
///
/// // Verify identity preservation: f(⊥) = ⊥
/// let widen = Widen;
/// assert_eq!(widen.apply(&Max::<u32>::empty()), Max::<u64>::empty());
///
/// // Verify combine preservation
/// let a = Max(10u32);
/// let b = Max(20u32);
/// assert_eq!(
///     widen.apply(&a.combine(&b)),
///     widen.apply(&a).combine(&widen.apply(&b))
/// );
/// ```
pub trait MonoidHom: SemigroupHom {}

/// Helper trait for explicitly specifying source and target monoids.
///
/// This is a blanket-implemented alias that allows writing
/// `T: MonoidHomFromTo<M, N>` instead of
/// `T: MonoidHom<Source = M, Target = N>`.
pub trait MonoidHomFromTo<M: Monoid, N: Monoid>: MonoidHom<Source = M, Target = N> {}

impl<H, M, N> MonoidHomFromTo<M, N> for H
where
    H: MonoidHom<Source = M, Target = N>,
    M: Monoid,
    N: Monoid,
{
}

// JoinSemilattice

/// A **join-semilattice**: a type with an associative, commutative,
/// and idempotent binary operation (the join).
///
/// Laws (not enforced by type system):
///
/// - **Associative**: `a.join(b).join(c) == a.join(b.join(c))`
/// - **Commutative**: `a.join(b) == b.join(a)`
/// - **Idempotent**: `a.join(a) == a`
///
/// The `join` operation computes the least upper bound (supremum) in
/// the induced partial order: `x ≤ y` iff `x.join(y) == y`.
///
/// # Example
///
/// ```rust
/// use algebra::JoinSemilattice;
/// use algebra::Max;
///
/// let a = Max(3);
/// let b = Max(5);
///
/// // join = max
/// let c = a.join(&b);
/// assert_eq!(c, Max(5));
///
/// // Idempotent
/// assert_eq!(a.join(&a), a);
/// ```
pub trait JoinSemilattice: Sized {
    /// The join (least upper bound).
    fn join(&self, other: &Self) -> Self;

    /// In-place variant.
    fn join_assign(&mut self, other: &Self) {
        *self = self.join(other);
    }

    /// Derived partial order: x ≤ y iff x ⊔ y = y.
    fn leq(&self, other: &Self) -> bool
    where
        Self: PartialEq,
    {
        self.join(other) == *other
    }

    /// Join a finite iterator of values. Returns `None` for empty
    /// iterators.
    fn join_all<I>(it: I) -> Option<Self>
    where
        I: IntoIterator<Item = Self>,
    {
        it.into_iter().reduce(|acc, x| acc.join(&x))
    }
}

// BoundedJoinSemilattice

/// A **bounded join-semilattice**: a join-semilattice with a bottom
/// element that serves as the identity for join.
///
/// Laws (not enforced by type system):
///
/// - **Associative**: `a.join(b).join(c) == a.join(b.join(c))`
/// - **Commutative**: `a.join(b) == b.join(a)`
/// - **Idempotent**: `a.join(a) == a`
/// - **Identity**: `bottom().join(a) == a == a.join(bottom())`
///
/// The bottom element (⊥) is the least element in the partial order.
///
/// # Example
///
/// ```rust
/// use algebra::BoundedJoinSemilattice;
/// use algebra::JoinSemilattice;
/// use algebra::Max;
///
/// let a = Max(10);
///
/// // bottom = minimum value
/// let bottom = Max::<i32>::bottom();
///
/// // Identity law
/// assert_eq!(bottom.join(&a), a);
/// assert_eq!(a.join(&bottom), a);
/// ```
pub trait BoundedJoinSemilattice: JoinSemilattice {
    /// The bottom element of the lattice (⊥).
    ///
    /// This is the least element w.r.t. the induced partial order: for
    /// all `x`, `bottom().join(x) == x`.
    fn bottom() -> Self;

    /// Join a finite iterator of values, starting from ⊥.
    ///
    /// Never returns `None`: an empty iterator produces `bottom()`.
    fn join_all_from_bottom<I>(it: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        it.into_iter().fold(Self::bottom(), |acc, x| acc.join(&x))
    }
}

// Blanket implementations: JoinSemilattice provides Semigroup/Monoid

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

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Sum(i32);

    impl Semigroup for Sum {
        fn combine(&self, other: &Self) -> Self {
            Sum(self.0 + other.0)
        }
    }

    impl Monoid for Sum {
        fn empty() -> Self {
            Sum(0)
        }
    }

    impl CommutativeMonoid for Sum {}

    #[test]
    fn semigroup_combine_works() {
        let x = Sum(3);
        let y = Sum(5);
        assert_eq!(x.combine(&y), Sum(8));
    }

    #[test]
    fn semigroup_is_associative() {
        let x = Sum(1);
        let y = Sum(2);
        let z = Sum(3);
        assert_eq!(x.combine(&y).combine(&z), x.combine(&y.combine(&z)));
    }

    #[test]
    fn monoid_has_identity() {
        let x = Sum(5);
        assert_eq!(Sum::empty().combine(&x), x);
        assert_eq!(x.combine(&Sum::empty()), x);
    }

    #[test]
    fn monoid_concat_works() {
        let values = vec![Sum(1), Sum(2), Sum(3)];
        assert_eq!(Sum::concat(values), Sum(6));
    }

    #[test]
    fn monoid_concat_empty_is_identity() {
        let empty: Vec<Sum> = vec![];
        assert_eq!(Sum::concat(empty), Sum::empty());
    }

    #[test]
    fn commutative_monoid_is_commutative() {
        let x = Sum(3);
        let y = Sum(5);
        assert_eq!(x.combine(&y), y.combine(&x));
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct AddInt(i32);

    impl Semigroup for AddInt {
        fn combine(&self, other: &Self) -> Self {
            AddInt(self.0 + other.0)
        }
    }

    impl Monoid for AddInt {
        fn empty() -> Self {
            AddInt(0)
        }
    }

    impl CommutativeMonoid for AddInt {}

    impl Group for AddInt {
        fn inverse(&self) -> Self {
            AddInt(-self.0)
        }
    }

    impl AbelianGroup for AddInt {}

    #[test]
    fn group_has_inverse() {
        let x = AddInt(5);
        assert_eq!(x.combine(&x.inverse()), AddInt::empty());
        assert_eq!(x.inverse().combine(&x), AddInt::empty());
    }

    #[test]
    fn abelian_group_is_commutative() {
        let x = AddInt(3);
        let y = AddInt(-7);
        assert_eq!(x.combine(&y), y.combine(&x));
    }

    #[test]
    fn join_semilattice_leq() {
        let a = Max(3);
        let b = Max(5);
        assert!(a.leq(&b));
        assert!(!b.leq(&a));
        assert!(a.leq(&a));
    }

    #[test]
    fn join_all_works() {
        let values = vec![Max(1), Max(5), Max(3)];
        assert_eq!(Max::join_all(values), Some(Max(5)));
    }

    #[test]
    fn join_all_empty_is_none() {
        let empty: Vec<Max<i32>> = vec![];
        assert_eq!(Max::join_all(empty), None);
    }

    #[test]
    fn join_all_from_bottom_works() {
        let values = vec![Max(1), Max(5), Max(3)];
        assert_eq!(Max::join_all_from_bottom(values), Max(5));
    }

    #[test]
    fn join_all_from_bottom_empty_is_bottom() {
        let empty: Vec<Max<i32>> = vec![];
        assert_eq!(Max::join_all_from_bottom(empty), Max::bottom());
    }
}
