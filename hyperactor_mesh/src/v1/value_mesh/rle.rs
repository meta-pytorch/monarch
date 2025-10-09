/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ops::Range;
use std::mem::replace;

/// Run-length encodes a dense sequence into a table of unique values
/// and a run list, using a caller-provided equivalence predicate.
///
/// Consumes the dense `values` vector and produces:
/// - `table`: the deduplicated values in **first-occurrence order**
/// - `runs`: a list of `(start..end, id)` where `id` indexes into
///   `table`
///
/// Two adjacent elements `a`, `b` belong to the same run iff `same(a,
/// b)` returns `true`. In practice `same` should behave like an
/// equivalence relation over adjacent elements (reflexive and
/// symmetric; transitive is not required globally because only
/// **adjacency** is consulted).
///
/// # Parameters
/// - `values`: dense sequence; exactly one value per rank (consumed)
/// - `same`: predicate deciding adjacency-equality (`same(&a, &b)` =>
///   merge)
///
/// # Returns
/// `(table, runs)` where:
/// - `table.len() >= 0`
/// - `runs` is empty iff `values` is empty
///
/// # Invariants on the output
/// - `runs` is sorted by `start` and **non-overlapping**
/// - `runs` **exactly covers** `0..values.len()` with contiguous,
///   non-empty half-open intervals
/// - Adjacent runs always refer to **different** `id`s under `same`
/// - For every `(r, id)` in `runs`, `id < table.len()`
/// - For every index `i` in `r`, `values[i]` is `same`-equal to
///   `table[id]`
/// - `table` preserves first-appearance order of distinct (under
///   `same`) values encountered while scanning left-to-right
///
/// # Examples
/// ```
/// // values: [A, A, B, B, B, A]
/// // table:  [A, B, A]
/// // runs:   [(0..2, 0), (2..5, 1), (5..6, 2)]
/// ```
///
/// Note: `table` preserves **first-appearance order of each distinct
/// contiguous run**, not global uniqueness - the same logical value
/// may appear multiple times in `table` if it occurs in disjoint
/// runs.
pub fn rle_from_dense<T: Clone, F>(
    values: Vec<T>,
    mut same: F,
) -> (Vec<T>, Vec<(Range<usize>, u32)>)
where
    F: FnMut(&T, &T) -> bool,
{
    if values.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let mut table: Vec<T> = Vec::new();
    let mut runs: Vec<(Range<usize>, u32)> = Vec::new();

    let mut start = 0usize;
    table.push(values[0].clone());
    let mut cur_id: u32 = 0;

    for i in 1..values.len() {
        if !same(&values[i], &table[cur_id as usize]) {
            runs.push((start..i, cur_id));
            start = i;
            table.push(values[i].clone());
            cur_id = (table.len() - 1) as u32;
        }
    }
    runs.push((start..values.len(), cur_id));
    (table, runs)
}

/// Converts **normalized** value-bearing runs into a deduplicated
/// table and a run list, coalescing adjacent runs that carry the same
/// value.
///
/// # Input
///
/// - `runs`: a vector of `(start..end, value)` pairs that is already
///   **normalized**:
///   - sorted by `(start, end)` in ascending order,
///   - non-empty (`start < end`),
///   - **non-overlapping** (touching is allowed).
///
/// # Behavior
///
/// - Builds `table` by **global** deduplication of values in
///   first-appearance order. The same logical value appearing in
///   disjoint places will share a single entry in `table`.
/// - Produces `out` as a list of `(start..end, id)`, where `id`
///   indexes into `table`.
/// - If two consecutive input runs are **touching** (`prev.end ==
///   next.start`) and their values are `==`, they are **coalesced**
///   into a single output run referencing the same `id`.
///
/// # Output invariants
/// - `out` is sorted by `start` and **non-overlapping**; touching
///   runs have different `id`s.
/// - Each `(r, id)` in `out` satisfies `id < table.len()`.
/// - `table` preserves **first-appearance order** of distinct values
///   across the entire input.
/// - The union of ranges in `out` equals the union of ranges in the
///   input `runs`.
///
/// # Preconditions
/// This function assumes the input `runs` is normalized as described
/// above; it does **not** revalidate or resort the input. Supplying
/// unsorted or overlapping ranges results in unspecified behavior.
///
/// # Example
/// ```
/// // Input runs (already sorted & non-overlapping):
/// // [(0..2, A), (2..5, A), (5..6, B), (8..10, A)]
/// // After coalescing equal-adjacent:
/// // [(0..5, A), (5..6, B), (8..10, A)]
/// // table  = [A, B]
/// // out    = [(0..5, 0), (5..6, 1), (8..10, 0)]
/// ```
pub fn rle_from_value_runs<T: Clone + PartialEq>(
    mut runs: Vec<(Range<usize>, T)>,
) -> (Vec<T>, Vec<(Range<usize>, u32)>) {
    if runs.is_empty() {
        return (Vec::new(), Vec::new());
    }
    // Runs are already normalized by caller (sorted,
    // non-overlapping).
    let mut table: Vec<T> = Vec::new();
    let mut out: Vec<(Range<usize>, u32)> = Vec::new();

    let mut push_run = |range: Range<usize>, v: &T| {
        // De-dup table.
        let id = if let Some(idx) = table.iter().position(|x| x == v) {
            idx as u32
        } else {
            table.push(v.clone());
            (table.len() - 1) as u32
        };
        // Coalesce equal-adjacent ids.
        if let Some((last_r, last_id)) = out.last_mut() {
            if last_r.end == range.start && *last_id == id {
                last_r.end = range.end;
                return;
            }
        }
        out.push((range, id));
    };

    for (r, v) in runs.drain(..) {
        push_run(r, &v);
    }
    (table, out)
}

/// True iff the two half-open ranges overlap.
#[inline]
pub(crate) fn ranges_overlap(a: &Range<usize>, b: &Range<usize>) -> bool {
    a.start < b.end && b.start < a.end
}

/// Merge two normalized `(Range, T)` run lists with "right wins"
/// overwrite semantics.
///
/// - Inputs must each be sorted, non-overlapping, coalesced
///   (equal-adjacent merged).
/// - Output is sorted, non-overlapping, coalesced.
/// - On overlaps, `right_in` overwrites `left_in` (last-writer-wins).
pub(crate) fn merge_value_runs<T: Eq + Clone>(
    left_in: Vec<(Range<usize>, T)>,
    right_in: Vec<(Range<usize>, T)>,
) -> Vec<(Range<usize>, T)> {
    // `out` will hold the merged, coalesced result.
    let mut out: Vec<(Range<usize>, T)> = Vec::new();

    // Local helper that appends a run butg coalesces equal-adjacent
    // (like RankedValues::append)
    let mut append = |range: Range<usize>, value: T| {
        if let Some((last_r, last_v)) = out.last_mut() {
            if last_r.end == range.start && *last_v == value {
                last_r.end = range.end;
                return;
            }
        }
        out.push((range, value));
    };

    // Turn each input into forward iterators.
    let mut left_iter  = left_in.into_iter();
    let mut right_iter = right_in.into_iter();

    // `left` and `right` are the current cursor items
    // (`Option<(Range, T)>`).
    let mut left  = left_iter.next();
    let mut right = right_iter.next();

    // Main merge loop: runs as long as both sides have a current
    // item.
    //
    // Invariant: all runs emitted so far are sorted, non-overlapping,
    // and coalesced; `left` and `right` point to the next unprocessed
    // run on each side.
    while left.is_some() && right.is_some() {
        // Mutable refs to the current (range, val) pairs so we can
        // adjust their start as we carve off emitted pieces.
        let (left_ranks,  left_value)  = left.as_mut().unwrap();
        let (right_ranks, right_value) = right.as_mut().unwrap();

        if ranges_overlap(left_ranks, right_ranks) {
            // Overlap case (half-open ranges): a.start < b.end &&
            // b.start < a.end.
            if *left_value == *right_value {
                // Equal-value overlap: merge into one unified run.
                //
                // We extend the emitted range up to right.end — not
                // max(left.end, right.end). This choice ensures:
                //   - The entire right run is consumed and the right
                //     iterator advances (guaranteeing progress).
                //   - If the left run extends further, we just shrink
                //     its start and handle its tail next iteration.
                // This avoids overlap or lookahead while keeping
                // output normalized.
                let ranks = (left_ranks.start.min(right_ranks.start))..right_ranks.end;
                // `replace` consumes the whole right run and advances
                // to the next.

                // Consume the right run entirely and move right
                // forward.
                let (_, value) = replace(&mut right, right_iter.next()).unwrap();
                // Advance `left_ranks.start` to the end of what we
                // just emitted; if left run is now empty, advance the
                // left iterator.
                left_ranks.start = ranks.end;
                if left_ranks.is_empty() {
                    left = left_iter.next();
                }
                // Append (coalescing if it touches the previous
                // output with the same value).
                append(ranks, value);
            } else if left_ranks.start < right_ranks.start {
                // Different values; left starts first. Emit the
                // prefix of left up to right.start — this portion
                // cannot be overwritten and can be finalized.
                let ranks = left_ranks.start..right_ranks.start;
                left_ranks.start = ranks.end;
                append(ranks, left_value.clone());
            } else {
                // Different values; right starts earlier or equal.
                // "Right wins" — emit the right chunk as-is, consuming it
                // fully and moving left forward as needed. Then make sure
                // no leftover left segments still overlap that right range.
                let (ranks, value) = replace(&mut right, right_iter.next()).unwrap();

                // Clamp the current left start so it never extends into
                // the just-emitted right region.
                left_ranks.start = left_ranks.start.max(ranks.end);
                if left_ranks.is_empty() {
                    left = left_iter.next();
                }

                // Trim or skip any following left runs that still overlap
                // this right run's extent.
                while let Some((next_r, _)) = left.as_mut() {
                    if next_r.start < ranks.end {
                        next_r.start = next_r.start.max(ranks.end);
                        if next_r.is_empty() {
                            left = left_iter.next();
                            continue;
                        }
                    }
                    break;
                }
                append(ranks, value);
            }
        } else if left_ranks.start < right_ranks.start {
            // No overlap, left starts earlier.
            let (ranks, value) = replace(&mut left, left_iter.next()).unwrap();
            append(ranks, value);
        } else {
            // Nov overlap, right starts earlier.
            let (ranks, value) = replace(&mut right, right_iter.next()).unwrap();
            append(ranks, value);
        }
    }

    // Drain whichever side still has runs. They are guaranteed to
    // follow all previously emitted ranges.
    while let Some((r, v)) = left {
        append(r, v);
        left = left_iter.next();
    }
    while let Some((r, v)) = right {
        append(r, v);
        right = right_iter.next();
    }

    // Postcondition: `out` is sorted, non-overlapping, coalesced. All
    // overlaps resolved by "right-wins" semantics.
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_disjoint_right_after_left() {
        let left  = vec![(0..5, 1)];
        let right = vec![(7..9, 2)];
        let out = merge_value_runs(left, right);
        assert_eq!(out, vec![(0..5, 1), (7..9, 2)]);
    }

    #[test]
    fn merge_disjoint_right_before_left() {
        let left  = vec![(7..9, 1)];
        let right = vec![(0..5, 2)];
        let out = merge_value_runs(left, right);
        assert_eq!(out, vec![(0..5, 2), (7..9, 1)]);
    }

    #[test]
    fn overlap_right_wins_simple() {
        let left  = vec![(0..10, 1)];
        let right = vec![(3..6, 2)];
        let out = merge_value_runs(left, right);
        // left prefix, right overwrite, left suffix
        assert_eq!(out, vec![(0..3, 1), (3..6, 2), (6..10, 1)]);
    }

    #[test]
    fn overlap_equal_values_union_to_right_end() {
        let left  = vec![(0..4, 5)];
        let right = vec![(2..6, 5)];
        let out = merge_value_runs(left, right);
        // same value: union emits [0..6) as two pieces depending on
        // algorithm's "extend to right.end" rule, but coalescing
        // should produce a single run:
        assert_eq!(out, vec![(0..6, 5)]);
    }

    #[test]
    fn overlap_equal_values_with_left_longer() {
        let left  = vec![(0..8, 5)];
        let right = vec![(2..6, 5)];
        let out = merge_value_runs(left, right);
        // equal case extends to right.end first, then left tail
        // remains and should coalesce to one; because they’re
        // touching & equal:
        assert_eq!(out, vec![(0..8, 5)]);
    }

    #[test]
    fn overlap_right_starts_earlier_right_wins() {
        let left  = vec![(4..10, 1)];
        let right = vec![(2..6, 2)];
        let out = merge_value_runs(left, right);
        // Right chunk first, then left remainder:
        assert_eq!(out, vec![(2..6, 2), (6..10, 1)]);
    }

    #[test]
    fn touching_coalesces_equal() {
        let left  = vec![(0..3, 1), (3..6, 1)];
        let right = vec![];
        let out = merge_value_runs(left, right);
        assert_eq!(out, vec![(0..6, 1)]);
    }

    #[test]
    fn multi_overlap_mixed_values() {
        let left  = vec![(0..5, 1), (5..10, 1), (10..15, 3)];
        let right = vec![(3..7, 2), (12..20, 4)];
        let out = merge_value_runs(left, right);
        assert_eq!(out, vec![
            (0..3, 1),
            (3..7, 2),
            (7..10, 1),
            (10..12, 3),
            (12..20, 4)
        ]);
    }

    #[test]
    fn overlap_mixed_values_right_inside_left() {
        // Right run sits strictly within a left run of a different
        // value.
        let left  = vec![(0..10, 1)];
        let right = vec![(3..7, 2)];
        let out = merge_value_runs(left, right);
        assert_eq!(
            out,
            vec![(0..3, 1), (3..7, 2), (7..10, 1)],
            "right overwrites interior portion of left run"
        );
    }

    #[test]
    fn overlap_mixed_values_right_spans_multiple_left_runs() {
        // Right spans two left runs, overwriting parts of both.
        let left  = vec![(0..5, 1), (5..10, 2)];
        let right = vec![(3..7, 9)];
        let out = merge_value_runs(left, right);
        assert_eq!(
            out,
            vec![(0..3, 1), (3..7, 9), (7..10, 2)],
            "right spans two left runs; left prefix/tail preserved"
        );
    }

    #[test]
    fn overlap_mixed_values_right_starts_before_left() {
        // Right begins before left and overlaps into it.
        let left  = vec![(5..10, 1)];
        let right = vec![(0..7, 2)];
        let out = merge_value_runs(left, right);
        assert_eq!(
            out,
            vec![(0..7, 2), (7..10, 1)],
            "right overwrites head and extends beyond left start"
        );
    }

    #[test]
    fn overlap_mixed_values_right_ends_after_left() {
        // Right starts inside left but extends beyond its end.
        let left  = vec![(0..5, 1)];
        let right = vec![(3..8, 2)];
        let out = merge_value_runs(left, right);
        assert_eq!(
            out,
            vec![(0..3, 1), (3..8, 2)],
            "right overwrites tail and extends past left"
        );
    }

    #[test]
    fn overlap_mixed_values_multiple_cascading_overlaps() {
        // Stress: multiple right runs each cutting through several
        // lefts.
        let left  = vec![(0..4, 1), (4..8, 1), (8..12, 2), (12..16, 3)];
        let right = vec![(2..6, 9), (10..14, 9)];
        let out = merge_value_runs(left, right);
        assert_eq!(
            out,
            vec![(0..2, 1), (2..6, 9), (6..8, 1), (8..10, 2), (10..14, 9), (14..16, 3)],
            "multiple right runs spanning multiple left runs, non-overlapping output"
        );
    }

    #[test]
    fn empty_inputs() {
        let out = merge_value_runs::<i32>(vec![], vec![]);
        assert!(out.is_empty());

        let out = merge_value_runs(vec![(0..2, 9)], vec![]);
        assert_eq!(out, vec![(0..2, 9)]);

        let out = merge_value_runs(vec![], vec![(5..7, 4)]);
        assert_eq!(out, vec![(5..7, 4)]);
    }
}
