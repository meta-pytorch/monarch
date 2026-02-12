/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::ops::Range;

use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Builder error for overlays (structure only; region bounds are
/// checked at merge time).
///
/// Note: serialization assumes identical pointer width between sender
/// and receiver, as `Range<usize>` is not portable across
/// architectures. TODO: introduce a wire‐stable run type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildError {
    /// A run with an empty range (`start == end`) was provided.
    EmptyRange,

    /// Two runs overlap or are unsorted: `prev.end > next.start`. The
    /// offending ranges are returned for debugging.
    OverlappingRanges {
        prev: Range<usize>,
        next: Range<usize>,
    },

    /// A run exceeds the region bounds when applying an overlay
    /// merge.
    OutOfBounds {
        range: Range<usize>,
        region_len: usize,
    },
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::EmptyRange => {
                write!(f, "a run with an empty range (start == end) was provided")
            }
            BuildError::OverlappingRanges { prev, next } => write!(
                f,
                "overlapping or unsorted runs: prev={:?}, next={:?}",
                prev, next
            ),
            BuildError::OutOfBounds { range, region_len } => write!(
                f,
                "range {:?} exceeds region bounds (len={})",
                range, region_len
            ),
        }
    }
}

impl std::error::Error for BuildError {}

/// A sparse overlay of rank ranges and values, used to assemble or
/// patch a [`ValueMesh`] without materializing per-rank data.
///
/// Unlike `ValueMesh`, which always represents a complete, gap-free
/// mapping over a [`Region`], a `ValueOverlay` is intentionally
/// partial: it may describe only the ranks that have changed. This
/// allows callers to build and merge small, incremental updates
/// efficiently, while preserving the `ValueMesh` invariants after
/// merge.
///
/// Invariants:
/// - Runs are sorted by `(start, end)`.
/// - Runs are non-empty and non-overlapping.
/// - Adjacent runs with equal values are coalesced.
/// - Region bounds are validated when the overlay is merged, not on
///   insert.
///
/// Note: serialization assumes identical pointer width between sender
/// and receiver, as `Range<usize>` is not portable across
/// architectures. TODO: introduce a wire‐stable run type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Named, Default)]
pub struct ValueOverlay<T> {
    runs: Vec<(Range<usize>, T)>,
}

impl<T> ValueOverlay<T> {
    /// Creates an empty overlay.
    pub fn new() -> Self {
        Self { runs: Vec::new() }
    }

    /// Returns an iterator over the internal runs.
    pub fn runs(&self) -> impl Iterator<Item = &(Range<usize>, T)> {
        self.runs.iter()
    }

    /// Current number of runs.
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    /// Returns `true` if the overlay contains no runs. This indicates
    /// that no ranges have been added — i.e., the overlay represents
    /// an empty or no-op patch.
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }
}

impl<T: PartialEq> ValueOverlay<T> {
    /// Adds a `(range, value)` run while maintaining the overlay
    /// invariants.
    ///
    /// Fast path:
    /// - If `range` is **after** the last run (`last.end <=
    ///   range.start`), this is an O(1) append. If it **touches** the
    ///   last run and `value` is equal, the two runs are **coalesced**
    ///   by extending `end`.
    ///
    /// Slow path:
    /// - If the input is **unsorted** or **overlaps** the last run,
    ///   the method falls back to a full **normalize** (sort +
    ///   coalesce + overlap check), making this call **O(n log n)**
    ///   in the number of runs.
    ///
    /// Errors:
    /// - Returns `BuildError::EmptyRange` if `range.is_empty()`.
    ///
    /// Notes & guidance:
    /// - Use this for **already-sorted, non-overlapping** appends to
    ///   get the cheap fast path.
    /// - For **bulk or unsorted** inserts, prefer
    ///   `ValueOverlay::try_from_runs` (collect → sort → coalesce) to
    ///   avoid repeated re-normalization.
    /// - Adjacent equal-value runs are coalesced automatically.
    ///
    /// # Examples
    /// ```ignore
    /// let mut ov = ValueOverlay::new();
    /// ov.push_run(0..3, 1).unwrap();      // append
    /// ov.push_run(3..5, 1).unwrap();      // coalesces to (0..5, 1)
    ///
    /// // Unsorted input triggers normalize (O(n log n)):
    /// ov.push_run(1..2, 2).unwrap();      // re-sorts, checks overlaps, coalesces
    /// ```
    pub fn push_run(&mut self, range: Range<usize>, value: T) -> Result<(), BuildError> {
        // Reject empty ranges.
        if range.is_empty() {
            return Err(BuildError::EmptyRange);
        }

        // Look at the last run.
        match self.runs.last_mut() {
            // The common case is appending in sorted order. Fast-path
            // append if new run is after the last and
            // non-overlapping.
            Some((last_r, last_v)) if last_r.end <= range.start => {
                if last_r.end == range.start && *last_v == value {
                    // Coalesce equal-adjacent.
                    last_r.end = range.end;
                    return Ok(());
                }
                self.runs.push((range, value));
                Ok(())
            }
            // The overlay was previously empty or, the caller
            // inserted out of order (unsorted input).
            _ => {
                // Slow path. Re-sort, merge and validate the full
                // runs vector.
                self.runs.push((range, value));
                Self::normalize(&mut self.runs)
            }
        }
    }

    /// Sorts, checks for overlaps, and coalesces equal-adjacent runs
    /// in-place.
    fn normalize(v: &mut Vec<(Range<usize>, T)>) -> Result<(), BuildError> {
        // Early exit for empty overlays.
        if v.is_empty() {
            return Ok(());
        }

        // After this, ever later range has start >= prev.start. If
        // any later start < prev.end it's an overlap.
        v.sort_by_key(|(r, _)| (r.start, r.end));

        // Build a fresh vector to collect cleaned using drain(..) on
        // the input avoiding clone().
        let mut out: Vec<(Range<usize>, T)> = Vec::with_capacity(v.len());
        for (r, val) in v.drain(..) {
            if let Some((prev_r, prev_v)) = out.last_mut() {
                // If the next run's start is before the previous run's
                // end we have an overlapping interval.
                if r.start < prev_r.end {
                    return Err(BuildError::OverlappingRanges {
                        prev: prev_r.clone(),
                        next: r,
                    });
                }
                // If the previous run touches the new run and has the
                // same value, merge them by extending the end
                // boundary.
                if prev_r.end == r.start && *prev_v == val {
                    // Coalesce equal-adjacent.
                    prev_r.end = r.end;
                    continue;
                }
            }
            // Otherwise, push as a new independent run.
            out.push((r, val));
        }

        // Replace the old vector.
        *v = out;

        // Invariant: Runs is sorted, non-overlapping and coalesced.
        Ok(())
    }

    /// Builds an overlay from arbitrary runs, validating structure
    /// and coalescing equal-adjacent.
    pub fn try_from_runs<I>(runs: I) -> Result<Self, BuildError>
    where
        I: IntoIterator<Item = (Range<usize>, T)>,
    {
        // We need a modifiable buffer to sort and normalize so we
        // eagerly collect the iterator.
        let mut v: Vec<(Range<usize>, T)> = runs.into_iter().collect();

        // Reject empties up-front. Empty intervals are structurally
        // invalid for an overlay. Fail fast.
        for (r, _) in &v {
            if r.is_empty() {
                return Err(BuildError::EmptyRange);
            }
        }

        // Sort by (start, end).
        v.sort_by_key(|(r, _)| (r.start, r.end));

        // Normalize (validate + coalesce).
        Self::normalize(&mut v)?;

        // Invariant: Runs is sorted, non-overlapping and coalesced.
        Ok(Self { runs: v })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn push_run_appends_and_coalesces() {
        let mut ov = ValueOverlay::new();

        // First insert.
        ov.push_run(0..3, 1).unwrap();
        assert_eq!(ov.runs, vec![(0..3, 1)]);

        // Non-overlapping append.
        ov.push_run(5..7, 2).unwrap();
        assert_eq!(ov.runs, vec![(0..3, 1), (5..7, 2)]);

        // Coalesce equal-adjacent (touching with same value).
        ov.push_run(7..10, 2).unwrap();
        assert_eq!(ov.runs, vec![(0..3, 1), (5..10, 2)]);
    }

    #[test]
    fn push_run_detects_overlap() {
        let mut ov = ValueOverlay::new();
        ov.push_run(0..3, 1).unwrap();

        // Overlaps 2..4 with existing 0..3.
        let err = ov.push_run(2..4, 9).unwrap_err();
        assert!(matches!(err, BuildError::OverlappingRanges { .. }));
    }

    #[test]
    fn push_run_handles_unsorted_inserts() {
        let mut ov = ValueOverlay::new();
        // Insert out of order; normalize should sort and coalesce.
        ov.push_run(10..12, 3).unwrap();
        ov.push_run(5..8, 2).unwrap(); // Unsorted relative to last.
        ov.push_run(8..10, 2).unwrap(); // Coalesce with previous.

        assert_eq!(ov.runs, vec![(5..10, 2), (10..12, 3)]);
    }

    #[test]
    fn try_from_runs_builds_and_coalesces() {
        use super::ValueOverlay;

        // Unsorted, with adjacent equal-value ranges that should
        // coalesce.
        let ov = ValueOverlay::try_from_runs(vec![(8..10, 2), (5..8, 2), (12..14, 3)]).unwrap();

        assert_eq!(ov.runs, vec![(5..10, 2), (12..14, 3)]);
    }

    #[test]
    fn try_from_runs_rejects_overlap_and_empty() {
        // Overlap should error.
        let err = ValueOverlay::try_from_runs(vec![(0..3, 1), (2..5, 2)]).unwrap_err();
        assert!(matches!(err, BuildError::OverlappingRanges { .. }));

        // Empty range should error.
        let err = ValueOverlay::try_from_runs(vec![(0..0, 1)]).unwrap_err();
        assert!(matches!(err, BuildError::EmptyRange));
    }

    #[test]
    fn is_empty_reflects_state() {
        let mut ov = ValueOverlay::<i32>::new();
        assert!(ov.is_empty());

        ov.push_run(0..1, 7).unwrap();
        assert!(!ov.is_empty());
    }

    #[test]
    fn normalize_sorts_coalesces_and_detects_overlap() {
        // 1) Sort + coalesce equal-adjacent.
        let mut v = vec![(5..7, 2), (3..5, 2), (7..9, 2)]; // unsorted, all value=2
        ValueOverlay::<i32>::normalize(&mut v).unwrap();
        assert_eq!(v, vec![(3..9, 2)]);

        // 2) Overlap triggers error.
        let mut v = vec![(3..6, 1), (5..8, 2)];
        let err = ValueOverlay::<i32>::normalize(&mut v).unwrap_err();
        assert!(matches!(err, BuildError::OverlappingRanges { .. }));
    }
}
