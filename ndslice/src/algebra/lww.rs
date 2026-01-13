/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Last-Writer-Wins (LWW) register CRDT.

use std::cmp::Ordering;

use serde::Deserialize;
use serde::Serialize;

use super::BoundedJoinSemilattice;
use super::JoinSemilattice;

/// A **Last-Writer-Wins register** lattice.
///
/// The state is a triple `(value, ts, replica)` where `ts` is a logical
/// timestamp (e.g. Lamport clock, HLC, or monotone counter) and `replica`
/// is a unique identifier for the writer. Ordering uses `(ts, replica)`
/// lexicographically, yielding a total order on register versions; `join`
/// returns the greater version and is commutative, associative, and
/// idempotent.
///
/// This makes `LWW<T>` a simple register-style lattice that can be
/// used as the payload in higher-level CRDTs or accumulators where
/// "latest value" semantics are needed.
///
/// # Properties
///
/// - **Commutative**: `a.join(b) == b.join(a)`
/// - **Associative**: `a.join(b).join(c) == a.join(b.join(c))`
/// - **Idempotent**: `a.join(a) == a`
///
/// # Use Cases
///
/// - Distributed caches (last-write-wins per key)
/// - Configuration management (latest config wins)
/// - Watermark tracking where ranks can report decreasing values
///   (failure recovery, reprocessing)
///
/// # Example
///
/// ```
/// use ndslice::algebra::JoinSemilattice;
/// use ndslice::algebra::LWW;
///
/// // Two writers (replicas 1 and 2) with different timestamps
/// let v1 = LWW::new(100, 1, 1);
/// let v2 = LWW::new(50, 2, 2);
///
/// // Higher timestamp wins, even if value is smaller
/// assert_eq!(v1.join(&v2), LWW::new(50, 2, 2));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LWW<T> {
    /// The current value of the register.
    pub value: T,
    /// The logical timestamp associated with this value.
    pub ts: u64,
    /// The replica ID of the writer (for deterministic tie-breaking).
    pub replica: u64,
}

impl<T: Clone + PartialEq> JoinSemilattice for LWW<T> {
    fn join(&self, other: &Self) -> Self {
        match self.ts.cmp(&other.ts) {
            Ordering::Greater => self.clone(),
            Ordering::Less => other.clone(),
            Ordering::Equal => {
                // Tie-break by replica: higher replica wins
                if self.replica > other.replica {
                    self.clone()
                } else if other.replica > self.replica {
                    other.clone()
                } else {
                    // Same (ts, replica) should mean same write (duplicate delivery)
                    debug_assert!(
                        self.value == other.value,
                        "LWW collision: same (ts, replica) but different values"
                    );
                    self.clone()
                }
            }
        }
    }
}

impl<T: Clone + PartialEq + Default> BoundedJoinSemilattice for LWW<T> {
    fn bottom() -> Self {
        LWW {
            value: T::default(),
            ts: 0,
            replica: 0,
        }
    }
}

impl<T> LWW<T> {
    /// Create a new LWW register with the given value, timestamp, and replica ID.
    pub fn new(value: T, ts: u64, replica: u64) -> Self {
        LWW { value, ts, replica }
    }

    /// Get the current value.
    pub fn get(&self) -> &T {
        &self.value
    }

    /// Get the timestamp.
    pub fn timestamp(&self) -> u64 {
        self.ts
    }

    /// Get the replica ID.
    pub fn replica(&self) -> u64 {
        self.replica
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lww_join_takes_higher_timestamp() {
        let v1 = LWW::new(100, 1, 0);
        let v2 = LWW::new(50, 2, 0);

        // Higher timestamp wins even with smaller value
        assert_eq!(v1.join(&v2), LWW::new(50, 2, 0));
        assert_eq!(v2.join(&v1), LWW::new(50, 2, 0));
    }

    #[test]
    fn lww_join_on_tie_higher_replica_wins() {
        let v1 = LWW::new(100, 1, 1);
        let v2 = LWW::new(200, 1, 2);

        // Same timestamp: higher replica wins (commutative)
        assert_eq!(v1.join(&v2), v2);
        assert_eq!(v2.join(&v1), v2);
    }

    #[test]
    fn lww_is_idempotent() {
        let v = LWW::new(42, 5, 1);
        assert_eq!(v.join(&v), v);
    }

    #[test]
    fn lww_is_commutative() {
        let v1 = LWW::new(10, 1, 1);
        let v2 = LWW::new(20, 2, 2);
        let v3 = LWW::new(30, 3, 3);

        assert_eq!(v1.join(&v2), v2.join(&v1));
        assert_eq!(v1.join(&v2).join(&v3), v3.join(&v2).join(&v1));

        // Also test commutativity with same timestamp, different replica
        let a = LWW::new(100, 5, 1);
        let b = LWW::new(200, 5, 2);
        assert_eq!(a.join(&b), b.join(&a));
    }

    #[test]
    fn lww_is_associative() {
        let v1 = LWW::new(10, 1, 1);
        let v2 = LWW::new(20, 2, 2);
        let v3 = LWW::new(30, 3, 3);

        assert_eq!(v1.join(&v2).join(&v3), v1.join(&v2.join(&v3)));

        // Also test with same timestamp
        let a = LWW::new(10, 5, 1);
        let b = LWW::new(20, 5, 2);
        let c = LWW::new(30, 5, 3);
        assert_eq!(a.join(&b).join(&c), a.join(&b.join(&c)));
    }

    #[test]
    fn lww_bottom_is_identity() {
        let v = LWW::new(42, 5, 1);
        let bottom = LWW::<i32>::bottom();

        assert_eq!(bottom.join(&v), v);
        assert_eq!(v.join(&bottom), v);
    }

    #[test]
    fn lww_serde_roundtrip() {
        let v = LWW::new(42, 12345, 99);
        let encoded = bincode::serialize(&v).unwrap();
        let decoded: LWW<i32> = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded, v);
    }
}
