/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Map lattice with pointwise join on values.

use std::collections::HashMap;
use std::hash::Hash;

use serde::Deserialize;
use serde::Serialize;

use super::BoundedJoinSemilattice;
use super::JoinSemilattice;

/// Pointwise map lattice over `HashMap`.
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
/// This is a reusable building block for CRDT states that look like
/// "map from IDs to lattice values".
///
/// # Example: Watermark tracking
///
/// Track the low watermark across multiple ranks where each rank
/// reports its progress monotonically:
///
/// ```
/// use ndslice::algebra::JoinSemilattice;
/// use ndslice::algebra::LatticeMap;
/// use ndslice::algebra::Min;
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
/// use ndslice::algebra::JoinSemilattice;
/// use ndslice::algebra::LatticeMap;
/// use ndslice::algebra::Min;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::Max;
    use crate::algebra::Min;

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
}
