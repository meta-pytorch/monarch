/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Concrete **join-semilattice** types and building blocks.
//!
//! This module provides a small toolkit of lattice wrappers
//! convenient for CRDTs and other monotone data structures.
//!
//! # Overview
//!
//! - [`Max<T>`] / [`Min<T>`]: wrapper types where `join` is `max`
//!   or `min` on the inner value. Useful for timestamps, counters,
//!   or high/low-water marks.
//!
//! - [`Any`] / [`All`]: boolean lattices where `join` is `||` (OR)
//!   or `&&` (AND). Useful for error flags, presence checks,
//!   or invariants.
//!
//! - [`LatticeMap<K, V>`]: pointwise map lattice over `HashMap<K, V>`
//!   where `V` is a lattice; `join` unions keys and joins overlapping
//!   values. Ideal for per-replica state maps.
//!
//! # Example
//!
//! ```rust
//! use algebra::JoinSemilattice;
//! use algebra::Max;
//!
//! let x: Max<_> = 1.into();
//! let y: Max<_> = 3.into();
//!
//! // join = max
//! let z = x.join(&y);
//! assert_eq!(z.0, 3);
//! ```
//!
//! These primitives are intentionally small and generic. Higher-level
//! CRDTs build on them to express their replica states as lattices with
//! well-defined, convergent merge operations.

use std::collections::HashMap;
use std::hash::Hash;

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
/// `All(bool)` forms a join-semilattice under the **dual** boolean order
/// (true < false):
///
/// - `All(a).join(&All(b)) == All(a && b)`
/// - Bottom element is `All(true)`
///
/// This is useful for combining boolean conditions where "all must
/// be true", such as validation checks, preconditions, or invariants.
///
/// Note: The dual order may seem counterintuitive, but it makes `AND`
/// the join operation (least upper bound). This mirrors how [`Min`] uses
/// the dual order to make `min` the join.
///
/// # Example
///
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

// LatticeMap<K, V>

/// Pointwise map lattice over `HashMap`.
///
/// This is a reusable building block for CRDT states that look like
/// "map from IDs to lattice values".
///
/// Keys are optional; values form a join-semilattice. The induced
/// lattice order is:
///
///   m1 ≤ m2  iff  for all k, m1[k] ≤ m2[k]
///
/// Operationally, `join` is:
/// - keys: union of the key sets
/// - values: pointwise `join` on overlapping keys
///
/// Bottom is the empty map.
///
/// # Key growth and deletion
///
/// Under this lattice, the **set of keys grows monotonically** under
/// `join` (it is the union of key sets). As a result, `LatticeMap`
/// does **not** directly support deletion semantics (there is no
/// "remove" operation that is monotone w.r.t. this order).
///
/// Different applications want different deletion semantics and may
/// require causal context, so `LatticeMap` intentionally leaves
/// deletion policy to composition.
///
/// To model deletions, encode them in the *value* lattice `V`
/// (tombstones), or use a dedicated set/map CRDT depending on the
/// desired semantics. For example, one simple tombstone pattern is:
///
/// ```rust,ignore
/// use algebra::{LatticeMap, LWW};
///
/// // Treat `None` as "deleted" at the application layer.
/// // Conflicts resolve via LWW; a delete can be represented by
/// // writing `None`.
/// type Tombstoned<V> = LWW<Option<V>>;
/// type Map<K, V> = LatticeMap<K, Tombstoned<V>>;
/// ```
///
/// (If this pattern becomes common, we can add a small helper wrapper
/// type later, but the policy is intentionally left to composition.)
///
/// # Example: Watermark tracking
///
/// Track the low watermark across multiple ranks where each rank
/// reports its progress monotonically:
///
/// ```
/// use algebra::JoinSemilattice;
/// use algebra::LatticeMap;
/// use algebra::Min;
///
/// // Rank 0 reports progress: 100
/// let mut state1: LatticeMap<u32, Min<u64>> = LatticeMap::new();
/// state1.insert(0, Min(100));
///
/// // Rank 1 reports progress: 200
/// let mut state2: LatticeMap<u32, Min<u64>> = LatticeMap::new();
/// state2.insert(1, Min(200));
///
/// // Merge: union keys, pointwise min on values
/// let merged = state1.join(&state2);
///
/// // Low watermark is the min across all ranks
/// let watermark = merged.iter().map(|(_, v)| v.0).min().unwrap();
/// assert_eq!(watermark, 100); // min(rank0=100, rank1=200)
/// ```
///
/// # Commutativity and Idempotence
///
/// Unlike a simple HashMap with last-write-wins merge, `LatticeMap`
/// provides true commutativity:
///
/// ```
/// use algebra::JoinSemilattice;
/// use algebra::LatticeMap;
/// use algebra::Min;
///
/// let mut a: LatticeMap<u32, Min<i64>> = LatticeMap::new();
/// a.insert(0, Min(10));
///
/// let mut b: LatticeMap<u32, Min<i64>> = LatticeMap::new();
/// b.insert(0, Min(20));
///
/// // Join is commutative: a ⊔ b = b ⊔ a
/// assert_eq!(a.join(&b), b.join(&a));
///
/// // Join is idempotent: a ⊔ a = a
/// assert_eq!(a.join(&a), a);
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Eq + Hash + Serialize, V: Serialize",
    deserialize = "K: Eq + Hash + Deserialize<'de>, V: Deserialize<'de>"
))]
pub struct LatticeMap<K, V> {
    inner: HashMap<K, V>,
}

// Manual impl to keep bounds minimal and aligned with HashMap
// equality: HashMap<K, V>: PartialEq requires K: Eq + Hash and V:
// PartialEq.
impl<K, V> PartialEq for LatticeMap<K, V>
where
    K: Eq + Hash,
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<K, V> Eq for LatticeMap<K, V>
where
    K: Eq + Hash,
    V: Eq,
{
}

impl<K, V> LatticeMap<K, V>
where
    K: Eq + Hash,
{
    /// Create an empty map lattice.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Insert or replace a value for a key.
    pub fn insert(&mut self, k: K, v: V) {
        self.inner.insert(k, v);
    }

    /// Get a reference to the value for this key, if present.
    pub fn get(&self, k: &K) -> Option<&V> {
        self.inner.get(k)
    }

    /// Iterate over `(key, value)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter()
    }

    /// Access the underlying `HashMap`.
    pub fn as_inner(&self) -> &HashMap<K, V> {
        &self.inner
    }

    /// Consume the wrapper and return the underlying `HashMap`.
    pub fn into_inner(self) -> HashMap<K, V> {
        self.inner
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Is the map empty?
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<K, V> Default for LatticeMap<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> JoinSemilattice for LatticeMap<K, V>
where
    K: Eq + Hash + Clone,
    V: JoinSemilattice + Clone,
{
    fn join(&self, other: &Self) -> Self {
        let mut out = self.inner.clone();

        for (k, v_other) in &other.inner {
            out.entry(k.clone())
                .and_modify(|v_here| {
                    *v_here = v_here.join(v_other);
                })
                .or_insert_with(|| v_other.clone());
        }

        LatticeMap { inner: out }
    }
}

impl<K, V> BoundedJoinSemilattice for LatticeMap<K, V>
where
    K: Eq + Hash + Clone,
    V: BoundedJoinSemilattice + Clone,
{
    fn bottom() -> Self {
        LatticeMap {
            inner: HashMap::new(),
        }
    }
}

use std::collections::HashSet;

/// `HashSet<T>`: join = union
///
/// A `HashSet<T>` forms a join-semilattice under set union:
/// - join = union (∪)
/// - bottom = empty set (∅)
///
/// This makes `HashSet` ideal for tracking "sets of things" in
/// distributed systems where we want to accumulate all observed values
/// across replicas.
impl<T: Eq + Hash + Clone> JoinSemilattice for HashSet<T> {
    fn join(&self, other: &Self) -> Self {
        self.union(other).cloned().collect()
    }
}

impl<T: Eq + Hash + Clone> BoundedJoinSemilattice for HashSet<T> {
    fn bottom() -> Self {
        HashSet::new()
    }
}

use std::collections::BTreeSet;

/// `BTreeSet<T>`: join = union
///
/// Same semantics as `HashSet`, but uses `Ord` instead of `Hash`.
impl<T: Ord + Clone> JoinSemilattice for BTreeSet<T> {
    fn join(&self, other: &Self) -> Self {
        self.union(other).cloned().collect()
    }
}

impl<T: Ord + Clone> BoundedJoinSemilattice for BTreeSet<T> {
    fn bottom() -> Self {
        BTreeSet::new()
    }
}

/// `Option<L>`: lifted lattice
///
/// `Option<L>` lifts a lattice `L` by adding a new bottom element (`None`).
/// - `None` is the bottom element
/// - `Some(a).join(Some(b))` = `Some(a.join(b))`
/// - `None.join(x)` = `x.join(None)` = `x`
///
/// This is useful for representing "optional" lattice values where
/// absence is distinct from any present value.
impl<L: JoinSemilattice + Clone> JoinSemilattice for Option<L> {
    fn join(&self, other: &Self) -> Self {
        match (self, other) {
            (None, x) | (x, None) => x.clone(),
            (Some(a), Some(b)) => Some(a.join(b)),
        }
    }
}

impl<L: JoinSemilattice + Clone> BoundedJoinSemilattice for Option<L> {
    fn bottom() -> Self {
        None
    }
}

/// `()`: trivial unit lattice
///
/// The unit type forms a trivial lattice with a single element.
impl JoinSemilattice for () {
    fn join(&self, _other: &Self) -> Self {}
}

impl BoundedJoinSemilattice for () {
    fn bottom() -> Self {}
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    // Max tests

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

    // Min tests

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

    // Any tests

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

    // All tests

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

    // LatticeMap tests

    #[test]
    fn lattice_map_new_is_empty() {
        let m: LatticeMap<i32, Max<i32>> = LatticeMap::new();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn lattice_map_bottom_is_empty() {
        let m: LatticeMap<i32, Max<i32>> = BoundedJoinSemilattice::bottom();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn lattice_map_insert_and_get() {
        let mut m: LatticeMap<&str, Max<i32>> = LatticeMap::new();
        m.insert("foo", Max(42));
        assert_eq!(m.get(&"foo"), Some(&Max(42)));
        assert_eq!(m.get(&"bar"), None);
        assert_eq!(m.len(), 1);
        assert!(!m.is_empty());
    }

    #[test]
    fn lattice_map_join_is_pointwise() {
        // Values are Max<i32>, so join = max on each entry.
        let mut m1: LatticeMap<&str, Max<i32>> = LatticeMap::new();
        m1.insert("a", Max(1));
        m1.insert("b", Max(10));

        let mut m2: LatticeMap<&str, Max<i32>> = LatticeMap::new();
        m2.insert("b", Max(7));
        m2.insert("c", Max(3));

        let j = m1.join(&m2);

        // Keys: union of {a,b} and {b,c} = {a,b,c}. Values: pointwise max.
        assert_eq!(j.get(&"a"), Some(&Max(1)));
        assert_eq!(j.get(&"b"), Some(&Max(10))); // max(10, 7)
        assert_eq!(j.get(&"c"), Some(&Max(3)));
    }

    #[test]
    fn lattice_map_join_is_commutative() {
        let mut m1: LatticeMap<u32, Min<i64>> = LatticeMap::new();
        m1.insert(0, Min(10));

        let mut m2: LatticeMap<u32, Min<i64>> = LatticeMap::new();
        m2.insert(0, Min(20));

        assert_eq!(m1.join(&m2), m2.join(&m1));
    }

    #[test]
    fn lattice_map_join_is_idempotent() {
        let mut m: LatticeMap<u32, Max<i64>> = LatticeMap::new();
        m.insert(1, Max(100));
        m.insert(2, Max(200));

        assert_eq!(m.join(&m), m);
    }

    #[test]
    fn lattice_map_watermark_example() {
        // Simulate distributed watermark tracking
        let mut state1: LatticeMap<u32, Min<u64>> = LatticeMap::new();
        state1.insert(0, Min(100)); // Rank 0 at 100
        state1.insert(1, Min(200)); // Rank 1 at 200

        let mut state2: LatticeMap<u32, Min<u64>> = LatticeMap::new();
        state2.insert(0, Min(150)); // Rank 0 updated to 150
        state2.insert(2, Min(50)); // Rank 2 at 50

        // Merge states
        let merged = state1.join(&state2);

        // Check pointwise min
        assert_eq!(merged.get(&0), Some(&Min(100))); // min(100, 150)
        assert_eq!(merged.get(&1), Some(&Min(200))); // only in state1
        assert_eq!(merged.get(&2), Some(&Min(50))); // only in state2

        // Overall low watermark
        let watermark = merged.iter().map(|(_, v)| v.0).min().unwrap();
        assert_eq!(watermark, 50);
    }

    #[test]
    fn lattice_map_iter_works() {
        let mut m: LatticeMap<&str, Max<i32>> = LatticeMap::new();
        m.insert("a", Max(1));
        m.insert("b", Max(2));

        let items: HashMap<_, _> = m.iter().map(|(k, v)| (*k, v.0)).collect();
        assert_eq!(items.get(&"a"), Some(&1));
        assert_eq!(items.get(&"b"), Some(&2));
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn lattice_map_serde_roundtrip() {
        let mut original: LatticeMap<u32, Min<i64>> = LatticeMap::new();
        original.insert(1, Min(10));
        original.insert(2, Min(20));

        let encoded = bincode::serialize(&original).unwrap();
        let decoded: LatticeMap<u32, Min<i64>> = bincode::deserialize(&encoded).unwrap();

        assert_eq!(original, decoded);
    }

    // HashSet tests

    #[test]
    fn hashset_join_is_union() {
        let mut a = HashSet::new();
        a.insert(1);
        a.insert(2);

        let mut b = HashSet::new();
        b.insert(2);
        b.insert(3);

        let c = a.join(&b);
        assert_eq!(c.len(), 3);
        assert!(c.contains(&1));
        assert!(c.contains(&2));
        assert!(c.contains(&3));
    }

    #[test]
    fn hashset_bottom_is_empty() {
        let bottom: HashSet<i32> = HashSet::bottom();
        assert!(bottom.is_empty());

        let mut a = HashSet::new();
        a.insert(42);

        assert_eq!(bottom.join(&a), a);
        assert_eq!(a.join(&bottom), a);
    }

    #[test]
    fn hashset_is_idempotent() {
        let mut a = HashSet::new();
        a.insert(1);
        a.insert(2);

        assert_eq!(a.join(&a), a);
    }

    #[test]
    fn hashset_is_commutative() {
        let mut a = HashSet::new();
        a.insert(1);
        a.insert(2);

        let mut b = HashSet::new();
        b.insert(2);
        b.insert(3);

        assert_eq!(a.join(&b), b.join(&a));
    }

    #[test]
    fn hashset_is_associative() {
        let mut a = HashSet::new();
        a.insert(1);

        let mut b = HashSet::new();
        b.insert(2);

        let mut c = HashSet::new();
        c.insert(3);

        assert_eq!(a.join(&b).join(&c), a.join(&b.join(&c)));
    }

    #[test]
    fn hashset_semigroup_blanket_impl() {
        // Semigroup::combine is blanket-impl'd from JoinSemilattice::join
        // so combine(a, b) == join(a, b) == union
        let mut a: HashSet<&str> = HashSet::new();
        a.insert("foo");

        let mut b: HashSet<&str> = HashSet::new();
        b.insert("bar");

        // Test via join (Semigroup::combine delegates to join)
        let c = a.join(&b);
        assert_eq!(c.len(), 2);
        assert!(c.contains(&"foo"));
        assert!(c.contains(&"bar"));
    }

    #[test]
    fn hashset_monoid_blanket_impl() {
        // Monoid::empty is blanket-impl'd from BoundedJoinSemilattice::bottom
        // so empty() == bottom() == empty set
        let empty: HashSet<i32> = HashSet::bottom();
        assert!(empty.is_empty());

        let mut a = HashSet::new();
        a.insert(42);

        // Test identity: bottom.join(a) == a == a.join(bottom)
        assert_eq!(empty.join(&a), a);
        assert_eq!(a.join(&empty), a);
    }

    // BTreeSet tests

    #[test]
    fn btreeset_join_is_union() {
        let a: BTreeSet<i32> = [1, 2].into_iter().collect();
        let b: BTreeSet<i32> = [2, 3].into_iter().collect();

        let c = a.join(&b);
        assert_eq!(c.len(), 3);
        assert!(c.contains(&1));
        assert!(c.contains(&2));
        assert!(c.contains(&3));
    }

    #[test]
    fn btreeset_bottom_is_empty() {
        let bottom: BTreeSet<i32> = BTreeSet::bottom();
        assert!(bottom.is_empty());

        let a: BTreeSet<i32> = [42].into_iter().collect();
        assert_eq!(bottom.join(&a), a);
        assert_eq!(a.join(&bottom), a);
    }

    #[test]
    fn btreeset_is_idempotent() {
        let a: BTreeSet<i32> = [1, 2].into_iter().collect();
        assert_eq!(a.join(&a), a);
    }

    #[test]
    fn btreeset_is_commutative() {
        let a: BTreeSet<i32> = [1, 2].into_iter().collect();
        let b: BTreeSet<i32> = [2, 3].into_iter().collect();
        assert_eq!(a.join(&b), b.join(&a));
    }

    // Option tests

    #[test]
    fn option_join_lifts_inner() {
        let a: Option<Max<i32>> = Some(Max(5));
        let b: Option<Max<i32>> = Some(Max(10));

        assert_eq!(a.join(&b), Some(Max(10)));
    }

    #[test]
    fn option_join_with_none() {
        let a: Option<Max<i32>> = Some(Max(5));
        let none: Option<Max<i32>> = None;

        // None acts as identity for join
        assert_eq!(a.join(&none), a);
        assert_eq!(none.join(&a), a);
        assert_eq!(none.join(&none), None);
    }

    #[test]
    fn option_bottom_is_none() {
        let bottom: Option<Max<i32>> = Option::bottom();
        assert_eq!(bottom, None);
    }

    #[test]
    fn option_with_hashset() {
        let a: Option<HashSet<i32>> = Some([1, 2].into_iter().collect());
        let b: Option<HashSet<i32>> = Some([2, 3].into_iter().collect());

        let c = a.join(&b);
        let expected: HashSet<i32> = [1, 2, 3].into_iter().collect();
        assert_eq!(c, Some(expected));
    }

    #[test]
    fn option_is_idempotent() {
        let a: Option<Max<i32>> = Some(Max(5));
        assert_eq!(a.join(&a), a);

        let none: Option<Max<i32>> = None;
        assert_eq!(none.join(&none), none);
    }

    #[test]
    fn option_is_commutative() {
        let a: Option<Max<i32>> = Some(Max(5));
        let b: Option<Max<i32>> = Some(Max(10));
        assert_eq!(a.join(&b), b.join(&a));
    }

    // Unit tests

    #[test]
    fn unit_join_is_trivial() {
        assert_eq!(().join(&()), ());
    }

    #[test]
    fn unit_bottom_is_unit() {
        assert_eq!(<()>::bottom(), ());
    }

    #[test]
    fn unit_is_idempotent() {
        assert_eq!(().join(&()), ());
    }
}
