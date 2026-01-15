/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Primitive lattice types: Max, Min, Any, All.

use serde::Deserialize;
use serde::Serialize;

use super::BoundedJoinSemilattice;
use super::JoinSemilattice;

// Max<T>: join = max

/// Newtype wrapper for an `Ord` type where `join` is `max`.
///
/// - `join = max(a, b)`
/// - `bottom = T::MIN` (when T: Bounded)
///
/// # Example
/// ```
/// use algebra::JoinSemilattice;
/// use algebra::Max;
///
/// let a = Max(5);
/// let b = Max(10);
/// assert_eq!(a.join(&b), Max(10));
/// ```
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    typeuri::Named
)]
pub struct Max<T>(pub T);

impl<T: Ord + Clone> JoinSemilattice for Max<T> {
    fn join(&self, other: &Self) -> Self {
        if self.0 >= other.0 {
            self.clone()
        } else {
            other.clone()
        }
    }
}

impl<T: Ord + Clone + num_traits::Bounded> BoundedJoinSemilattice for Max<T> {
    fn bottom() -> Self {
        Max(num_traits::Bounded::min_value())
    }
}

impl<T> From<T> for Max<T> {
    fn from(value: T) -> Self {
        Max(value)
    }
}

impl<T: Ord + Clone + num_traits::Bounded> Default for Max<T> {
    fn default() -> Self {
        Self::bottom()
    }
}

impl<T> Max<T> {
    /// Get the inner value.
    pub fn get(&self) -> &T {
        &self.0
    }
}

// Min<T>: join = min

/// Newtype wrapper for an `Ord` type where `join` is `min`.
///
/// - `join = min(a, b)`
/// - `bottom = T::MAX` (when T: Bounded)
///
/// # Example
/// ```
/// use algebra::JoinSemilattice;
/// use algebra::Min;
///
/// let a = Min(5);
/// let b = Min(10);
/// assert_eq!(a.join(&b), Min(5));
/// ```
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    typeuri::Named
)]
pub struct Min<T>(pub T);

impl<T: Ord + Clone> JoinSemilattice for Min<T> {
    fn join(&self, other: &Self) -> Self {
        if self.0 <= other.0 {
            self.clone()
        } else {
            other.clone()
        }
    }
}

impl<T: Ord + Clone + num_traits::Bounded> BoundedJoinSemilattice for Min<T> {
    fn bottom() -> Self {
        Min(num_traits::Bounded::max_value())
    }
}

impl<T> From<T> for Min<T> {
    fn from(value: T) -> Self {
        Min(value)
    }
}

impl<T: Ord + Clone + num_traits::Bounded> Default for Min<T> {
    fn default() -> Self {
        Self::bottom()
    }
}

impl<T> Min<T> {
    /// Get the inner value.
    pub fn get(&self) -> &T {
        &self.0
    }
}

// Any: join = OR (disjunction)

/// Newtype wrapper for `bool` where `join` is logical OR.
///
/// - `join = a || b`
/// - `bottom = false`
///
/// # Example
/// ```
/// use algebra::Any;
/// use algebra::JoinSemilattice;
///
/// let a = Any(false);
/// let b = Any(true);
/// assert_eq!(a.join(&b), Any(true));
/// ```
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    typeuri::Named
)]
pub struct Any(pub bool);

impl JoinSemilattice for Any {
    fn join(&self, other: &Self) -> Self {
        Any(self.0 || other.0)
    }
}

impl BoundedJoinSemilattice for Any {
    fn bottom() -> Self {
        Any(false)
    }
}

impl From<bool> for Any {
    fn from(value: bool) -> Self {
        Any(value)
    }
}

// All: join = AND (conjunction, dual order)

/// Newtype wrapper for `bool` where `join` is logical AND.
///
/// - `join = a && b`
/// - `bottom = true` (dual order)
///
/// # Example
/// ```
/// use algebra::All;
/// use algebra::JoinSemilattice;
///
/// let a = All(true);
/// let b = All(false);
/// assert_eq!(a.join(&b), All(false));
/// ```
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    typeuri::Named
)]
pub struct All(pub bool);

impl JoinSemilattice for All {
    fn join(&self, other: &Self) -> Self {
        All(self.0 && other.0)
    }
}

impl BoundedJoinSemilattice for All {
    fn bottom() -> Self {
        All(true)
    }
}

impl From<bool> for All {
    fn from(value: bool) -> Self {
        All(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_join_is_maximum() {
        let a = Max(5);
        let b = Max(10);
        assert_eq!(a.join(&b), Max(10));
        assert_eq!(b.join(&a), Max(10));
    }

    #[test]
    fn max_bottom_is_min_value() {
        assert_eq!(Max::<i32>::bottom(), Max(i32::MIN));
        assert_eq!(Max::<u64>::bottom(), Max(u64::MIN));
    }

    #[test]
    fn max_is_idempotent() {
        let x = Max(42);
        assert_eq!(x.join(&x), x);
    }

    #[test]
    fn max_is_commutative() {
        let a = Max(3);
        let b = Max(7);
        assert_eq!(a.join(&b), b.join(&a));
    }

    #[test]
    fn max_is_associative() {
        let a = Max(1);
        let b = Max(5);
        let c = Max(3);
        assert_eq!(a.join(&b).join(&c), a.join(&b.join(&c)));
    }

    #[test]
    fn max_bottom_is_identity() {
        let x = Max(100);
        assert_eq!(Max::bottom().join(&x), x);
        assert_eq!(x.join(&Max::bottom()), x);
    }

    #[test]
    fn min_join_is_minimum() {
        let a = Min(5);
        let b = Min(10);
        assert_eq!(a.join(&b), Min(5));
        assert_eq!(b.join(&a), Min(5));
    }

    #[test]
    fn min_bottom_is_max_value() {
        assert_eq!(Min::<i32>::bottom(), Min(i32::MAX));
        assert_eq!(Min::<u64>::bottom(), Min(u64::MAX));
    }

    #[test]
    fn min_is_idempotent() {
        let x = Min(42);
        assert_eq!(x.join(&x), x);
    }

    #[test]
    fn min_is_commutative() {
        let a = Min(3);
        let b = Min(7);
        assert_eq!(a.join(&b), b.join(&a));
    }

    #[test]
    fn min_is_associative() {
        let a = Min(1);
        let b = Min(5);
        let c = Min(3);
        assert_eq!(a.join(&b).join(&c), a.join(&b.join(&c)));
    }

    #[test]
    fn min_bottom_is_identity() {
        let x = Min(100);
        assert_eq!(Min::bottom().join(&x), x);
        assert_eq!(x.join(&Min::bottom()), x);
    }

    #[test]
    fn any_join_is_or() {
        assert_eq!(Any(false).join(&Any(false)), Any(false));
        assert_eq!(Any(false).join(&Any(true)), Any(true));
        assert_eq!(Any(true).join(&Any(false)), Any(true));
        assert_eq!(Any(true).join(&Any(true)), Any(true));
    }

    #[test]
    fn any_bottom_is_false() {
        assert_eq!(Any::bottom(), Any(false));
        assert_eq!(Any(false).join(&Any(true)), Any(true));
        assert_eq!(Any(true).join(&Any(false)), Any(true));
    }

    #[test]
    fn any_is_idempotent() {
        assert_eq!(Any(false).join(&Any(false)), Any(false));
        assert_eq!(Any(true).join(&Any(true)), Any(true));
    }

    #[test]
    fn all_join_is_and() {
        assert_eq!(All(false).join(&All(false)), All(false));
        assert_eq!(All(false).join(&All(true)), All(false));
        assert_eq!(All(true).join(&All(false)), All(false));
        assert_eq!(All(true).join(&All(true)), All(true));
    }

    #[test]
    fn all_bottom_is_true() {
        assert_eq!(All::bottom(), All(true));
        assert_eq!(All(true).join(&All(false)), All(false));
        assert_eq!(All(false).join(&All(true)), All(false));
    }

    #[test]
    fn all_is_idempotent() {
        assert_eq!(All(false).join(&All(false)), All(false));
        assert_eq!(All(true).join(&All(true)), All(true));
    }

    // From<T> tests

    #[test]
    fn max_from_value() {
        let x: Max<i32> = 42.into();
        assert_eq!(x, Max(42));
        assert_eq!(Max::from(100u64), Max(100u64));
    }

    #[test]
    fn min_from_value() {
        let x: Min<i32> = 42.into();
        assert_eq!(x, Min(42));
        assert_eq!(Min::from(100u64), Min(100u64));
    }

    #[test]
    fn any_from_bool() {
        let t: Any = true.into();
        let f: Any = false.into();
        assert_eq!(t, Any(true));
        assert_eq!(f, Any(false));
    }

    #[test]
    fn all_from_bool() {
        let t: All = true.into();
        let f: All = false.into();
        assert_eq!(t, All(true));
        assert_eq!(f, All(false));
    }

    // Default tests

    #[test]
    fn max_default_is_bottom() {
        assert_eq!(Max::<i32>::default(), Max::<i32>::bottom());
        assert_eq!(Max::<u64>::default(), Max(u64::MIN));
    }

    #[test]
    fn min_default_is_bottom() {
        assert_eq!(Min::<i32>::default(), Min::<i32>::bottom());
        assert_eq!(Min::<u64>::default(), Min(u64::MAX));
    }

    // Serialization round-trip tests

    #[test]
    fn max_serde_roundtrip() {
        let original = Max(42i64);
        let encoded = bincode::serialize(&original).unwrap();
        let decoded: Max<i64> = bincode::deserialize(&encoded).unwrap();
        assert_eq!(original, decoded);

        // Also test with bottom value
        let bottom = Max::<i64>::bottom();
        let encoded = bincode::serialize(&bottom).unwrap();
        let decoded: Max<i64> = bincode::deserialize(&encoded).unwrap();
        assert_eq!(bottom, decoded);
    }

    #[test]
    fn min_serde_roundtrip() {
        let original = Min(42i64);
        let encoded = bincode::serialize(&original).unwrap();
        let decoded: Min<i64> = bincode::deserialize(&encoded).unwrap();
        assert_eq!(original, decoded);

        // Also test with bottom value
        let bottom = Min::<i64>::bottom();
        let encoded = bincode::serialize(&bottom).unwrap();
        let decoded: Min<i64> = bincode::deserialize(&encoded).unwrap();
        assert_eq!(bottom, decoded);
    }

    #[test]
    fn any_serde_roundtrip() {
        for value in [true, false] {
            let original = Any(value);
            let encoded = bincode::serialize(&original).unwrap();
            let decoded: Any = bincode::deserialize(&encoded).unwrap();
            assert_eq!(original, decoded);
        }

        // Also test bottom
        let bottom = Any::bottom();
        let encoded = bincode::serialize(&bottom).unwrap();
        let decoded: Any = bincode::deserialize(&encoded).unwrap();
        assert_eq!(bottom, decoded);
    }

    #[test]
    fn all_serde_roundtrip() {
        for value in [true, false] {
            let original = All(value);
            let encoded = bincode::serialize(&original).unwrap();
            let decoded: All = bincode::deserialize(&encoded).unwrap();
            assert_eq!(original, decoded);
        }

        // Also test bottom
        let bottom = All::bottom();
        let encoded = bincode::serialize(&bottom).unwrap();
        let decoded: All = bincode::deserialize(&encoded).unwrap();
        assert_eq!(bottom, decoded);
    }
}
