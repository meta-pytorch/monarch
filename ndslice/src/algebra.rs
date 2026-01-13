/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Algebraic structures for monotonic accumulation and lattice-based
//! reduction.
//!
//! This module provides the foundational traits and types for
//! expressing commutative, associative, and idempotent operations,
//! enabling principled accumulation in distributed systems.
//!
//! # Core Concepts
//!
//! - **Semigroup**: A type with an associative binary operation
//!   (`combine`). Examples: addition, max, string concatenation.
//!
//! - **Monoid**: A semigroup with an identity element (`empty`).
//!   Examples: 0 for addition, empty string for concatenation.
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
//! # Why Idempotence Matters for Distributed Systems
//!
//! In distributed systems with at-least-once delivery, messages may be
//! delivered multiple times due to retries, network partitions, or
//! failover. Non-idempotent operations (like addition) produce incorrect
//! results when applied multiple times:
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
//! use ndslice::algebra::BoundedJoinSemilattice;
//! use ndslice::algebra::JoinSemilattice;
//! use ndslice::algebra::Max;
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

mod join_semilattice;
mod monoid;
mod primitives;
mod semigroup;

// Re-export core traits
pub use join_semilattice::BoundedJoinSemilattice;
pub use join_semilattice::JoinSemilattice;
pub use monoid::CommutativeMonoid;
pub use monoid::Monoid;
// Re-export primitive lattice types
pub use primitives::{All, Any, Max, Min};
pub use semigroup::Semigroup;
