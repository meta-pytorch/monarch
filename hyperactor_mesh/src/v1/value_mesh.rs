/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cmp::Ordering;
use std::mem;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::ptr;
use std::ptr::NonNull;

use futures::Future;
use ndslice::view;
use ndslice::view::Ranked;
use ndslice::view::Region;
use serde::Deserialize;
use serde::Serialize;

/// A mesh of values, one per rank in `region`.
///
/// The internal representation (`rep`) may be dense or compressed,
/// but externally the mesh always behaves as a complete mapping from
/// rank index → value.
///
/// # Invariants
/// - Complete: every rank in `region` has exactly one value.
/// - Order: iteration and indexing follow the region's linearization.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)] // only if T implements
pub struct ValueMesh<T> {
    /// The logical multidimensional domain of the mesh.
    ///
    /// Determines the number of ranks (`region.num_ranks()`) and the
    /// order in which they are traversed.
    region: Region,

    /// The underlying storage representation.
    ///
    /// - `Rep::Dense` stores a `Vec<T>` with one value per rank.
    /// - `Rep::Compressed` stores a run-length encoded table of
    ///   unique values plus `(Range<usize>, u32)` pairs describing
    ///   contiguous runs of identical values.
    ///
    /// The representation is an internal optimization detail; all
    /// public APIs (e.g. `values()`, `get()`, slicing) behave as if
    /// the mesh were dense.
    rep: Rep<T>,
}

/// A single run-length–encoded (RLE) segment within a [`ValueMesh`].
///
/// Each `Run` represents a contiguous range of ranks `[start, end)`
/// that all share the same value, referenced indirectly via a table
/// index `id`. This allows compact storage of large regions with
/// repeated values.
///
/// Runs are serialized in a stable, portable format using `u64` for
/// range bounds (`start`, `end`) to avoid platform‐dependent `usize`
/// encoding differences.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct Run {
    /// Inclusive start of the contiguous range of ranks (0-based).
    start: u64,
    /// Exclusive end of the contiguous range of ranks (0-based).
    end: u64,
    /// Index into the value table for this run's shared value.
    id: u32,
}

impl Run {
    /// Creates a new `Run` covering ranks `[start, end)` that all
    /// share the same table entry `id`.
    ///
    /// Converts `usize` bounds to `u64` for stable serialization.
    fn new(start: usize, end: usize, id: u32) -> Self {
        Self {
            start: start as u64,
            end: end as u64,
            id,
        }
    }
}

impl TryFrom<Run> for (Range<usize>, u32) {
    type Error = &'static str;

    /// Converts a serialized [`Run`] back into its in-memory form
    /// `(Range<usize>, u32)`.
    ///
    /// Performs checked conversion of the 64-bit wire fields back
    /// into `usize` indices, returning an error if either bound
    /// exceeds the platform’s addressable range. This ensures safe
    /// round-tripping between the serialized wire format and native
    /// representation.
    fn try_from(r: Run) -> Result<Self, Self::Error> {
        let start = usize::try_from(r.start).map_err(|_| "run.start too large")?;
        let end = usize::try_from(r.end).map_err(|_| "run.end too large")?;
        Ok((start..end, r.id))
    }
}

/// Internal storage representation for a [`ValueMesh`].
///
/// This enum abstracts how the per-rank values are stored.
/// Externally, both variants behave identically — the difference is
/// purely in memory layout and access strategy.
///
/// - [`Rep::Dense`] stores one value per rank, directly.
/// - [`Rep::Compressed`] stores a compact run-length-encoded form,
///   reusing identical values across contiguous ranks.
///
/// Users of [`ValueMesh`] normally never interact with `Rep`
/// directly; all iteration and slicing APIs present a dense logical
/// view.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)] // only if T implements
#[serde(tag = "rep", rename_all = "snake_case")]
enum Rep<T> {
    /// Fully expanded representation: one element per rank.
    ///
    /// The length of `values` is always equal to
    /// `region.num_ranks()`. This form is simple and fast for
    /// iteration and mutation but uses more memory when large runs of
    /// repeated values are present.
    Dense {
        /// Flat list of values, one per rank in the region's
        /// linearization order.
        values: Vec<T>,
    },

    /// Run-length-encoded representation.
    ///
    /// Each run `(Range<usize>, id)` indicates that the ranks within
    /// `Range` (half-open `[start, end)`) share the same value at
    /// `table[id]`. The `table` stores each distinct value once.
    ///
    /// # Invariants
    /// - Runs are non-empty and contiguous (`r.start < r.end`).
    /// - Runs collectively cover `0..region.num_ranks()` with no gaps
    ///   or overlaps.
    /// - `id` indexes into `table` (`id < table.len()`).
    Compressed {
        /// The deduplicated set of unique values referenced by runs.
        table: Vec<T>,

        /// List of `(range, table_id)` pairs describing contiguous
        /// runs of identical values in region order.
        runs: Vec<Run>,
    },
}

impl<T> ValueMesh<T> {
    /// Creates a new `ValueMesh` for `region` with exactly one value
    /// per rank.
    ///
    /// # Invariants
    /// This constructor validates that the number of provided values
    /// (`ranks.len()`) matches the region's cardinality
    /// (`region.num_ranks()`). A value mesh must be complete: every
    /// rank in `region` has a corresponding `T`.
    ///
    /// # Errors
    /// Returns [`Error::InvalidRankCardinality`] if `ranks.len() !=
    /// region.num_ranks()`.
    /// ```
    pub(crate) fn new(region: Region, ranks: Vec<T>) -> crate::v1::Result<Self> {
        let (actual, expected) = (ranks.len(), region.num_ranks());
        if actual != expected {
            return Err(crate::v1::Error::InvalidRankCardinality { expected, actual });
        }
        Ok(Self {
            region,
            rep: Rep::Dense { values: ranks },
        })
    }

    /// Constructs a `ValueMesh` without checking cardinality. Caller
    /// must ensure `ranks.len() == region.num_ranks()`.
    #[inline]
    pub(crate) fn new_unchecked(region: Region, ranks: Vec<T>) -> Self {
        debug_assert_eq!(region.num_ranks(), ranks.len());
        Self {
            region,
            rep: Rep::Dense { values: ranks },
        }
    }
}

impl<F: Future> ValueMesh<F> {
    /// Await all futures in the mesh, yielding a `ValueMesh` of their
    /// outputs.
    pub async fn join(self) -> ValueMesh<F::Output> {
        let ValueMesh { region, rep } = self;

        match rep {
            Rep::Dense { values } => {
                let results = futures::future::join_all(values).await;
                ValueMesh::new_unchecked(region, results)
            }
            Rep::Compressed { .. } => {
                unreachable!("join() not implemented for compressed meshes")
            }
        }
    }
}

impl<T, E> ValueMesh<Result<T, E>> {
    /// Transposes a `ValueMesh<Result<T, E>>` into a
    /// `Result<ValueMesh<T>, E>`.
    pub fn transpose(self) -> Result<ValueMesh<T>, E> {
        let ValueMesh { region, rep } = self;

        match rep {
            Rep::Dense { values } => {
                let values = values.into_iter().collect::<Result<Vec<T>, E>>()?;
                Ok(ValueMesh::new_unchecked(region, values))
            }
            Rep::Compressed { table, runs } => {
                let table: Vec<T> = table.into_iter().collect::<Result<Vec<T>, E>>()?;
                Ok(ValueMesh {
                    region,
                    rep: Rep::Compressed { table, runs },
                })
            }
        }
    }
}

impl<T: 'static> view::Ranked for ValueMesh<T> {
    type Item = T;

    /// Returns the region that defines this mesh's shape and rank
    /// order.
    fn region(&self) -> &Region {
        &self.region
    }

    /// Looks up the value at the given linearized rank.
    ///
    /// Works transparently for both dense and compressed
    /// representations:
    /// - In the dense case, it simply indexes into the `values`
    ///   vector.
    /// - In the compressed case, it performs a binary search over run
    ///   boundaries to find which run contains the given rank, then
    ///   returns the corresponding entry from `table`.
    ///
    /// Returns `None` if the rank is out of bounds.
    fn get(&self, rank: usize) -> Option<&Self::Item> {
        if rank >= self.region.num_ranks() {
            return None;
        }

        match &self.rep {
            Rep::Dense { values } => values.get(rank),

            Rep::Compressed { table, runs } => {
                let rank = rank as u64;

                // Binary search over runs: find the one whose range
                // contains `rank`.
                let idx = runs
                    .binary_search_by(|run| {
                        if run.end <= rank {
                            Ordering::Less
                        } else if run.start > rank {
                            Ordering::Greater
                        } else {
                            Ordering::Equal
                        }
                    })
                    .ok()?;

                // Map the run's table ID to its actual value.
                let id = runs[idx].id as usize;
                table.get(id)
            }
        }
    }
}

impl<T: Clone + 'static> view::RankedSliceable for ValueMesh<T> {
    fn sliced(&self, region: Region) -> Self {
        debug_assert!(region.is_subset(self.region()), "sliced: not a subset");
        let ranks: Vec<T> = self
            .region()
            .remap(&region)
            .unwrap()
            .map(|index| self.get(index).unwrap().clone())
            .collect();
        debug_assert_eq!(
            region.num_ranks(),
            ranks.len(),
            "sliced: cardinality mismatch"
        );
        Self::new_unchecked(region, ranks)
    }
}

impl<T> view::BuildFromRegion<T> for ValueMesh<T> {
    type Error = crate::v1::Error;

    fn build_dense(region: Region, values: Vec<T>) -> Result<Self, Self::Error> {
        Self::new(region, values)
    }

    fn build_dense_unchecked(region: Region, values: Vec<T>) -> Self {
        Self::new_unchecked(region, values)
    }
}

impl<T> view::BuildFromRegionIndexed<T> for ValueMesh<T> {
    type Error = crate::v1::Error;

    fn build_indexed(
        region: Region,
        pairs: impl IntoIterator<Item = (usize, T)>,
    ) -> Result<Self, Self::Error> {
        let n = region.num_ranks();

        // Allocate uninitialized buffer for T.
        // Note: Vec<MaybeUninit<T>>'s Drop will only free the
        // allocation; it never runs T's destructor. We must
        // explicitly drop any initialized elements (DropGuard) or
        // convert into Vec<T>.
        let mut buf: Vec<MaybeUninit<T>> = Vec::with_capacity(n);
        // SAFETY: set `len = n` to treat the buffer as n uninit slots
        // of `MaybeUninit<T>`. We never read before `ptr::write`,
        // drop only slots marked initialized (bitset), and convert to
        // `Vec<T>` only once all 0..n are initialized (guard enforces
        // this).
        unsafe {
            buf.set_len(n);
        }

        // Compact bitset for occupancy.
        let words = n.div_ceil(64);
        let mut bits = vec![0u64; words];
        let mut filled = 0usize;

        // Drop guard: cleans up initialized elements on early exit.
        // Stores raw, non-borrowed pointers (`NonNull`), so we don't
        // hold Rust references for the whole scope. This allows
        // mutating `buf`/`bits` inside the loop while still letting
        // the guard access them if dropped early.
        struct DropGuard<T> {
            buf: NonNull<MaybeUninit<T>>,
            bits: NonNull<u64>,
            n_elems: usize,
            n_words: usize,
            disarm: bool,
        }

        impl<T> DropGuard<T> {
            /// # Safety
            /// - `buf` points to `buf.len()` contiguous
            ///   `MaybeUninit<T>` and outlives the guard.
            /// - `bits` points to `bits.len()` contiguous `u64` words
            ///   and outlives the guard.
            /// - For every set bit `i` in `bits`, `buf[i]` has been
            ///   initialized with a valid `T` (and on duplicates, the
            ///   previous `T` at `i` was dropped before overwrite).
            /// - While the guard is alive, caller may mutate
            ///   `buf`/`bits` (the guard holds only raw pointers; it
            ///   does not create Rust borrows).
            /// - On drop (when not disarmed), the guard will read
            ///   `bits`, mask tail bits in the final word, and
            ///   `drop_in_place` each initialized `buf[i]`—never
            ///   accessing beyond `buf.len()` / `bits.len()`.
            /// - If a slice is empty, the stored pointer may be
            ///   `dangling()`; it is never dereferenced when the
            ///   corresponding length is zero.
            unsafe fn new(buf: &mut [MaybeUninit<T>], bits: &mut [u64]) -> Self {
                let n_elems = buf.len();
                let n_words = bits.len();
                // Invariant typically: n_words == (n_elems + 63) / 64
                // but we don't *require* it; tail is masked in Drop.
                Self {
                    buf: NonNull::new(buf.as_mut_ptr()).unwrap_or_else(NonNull::dangling),
                    bits: NonNull::new(bits.as_mut_ptr()).unwrap_or_else(NonNull::dangling),
                    n_elems,
                    n_words,
                    disarm: false,
                }
            }

            #[inline]
            fn disarm(&mut self) {
                self.disarm = true;
            }
        }

        impl<T> Drop for DropGuard<T> {
            fn drop(&mut self) {
                if self.disarm {
                    return;
                }

                // SAFETY:
                // - `self.buf` points to `n_elems` contiguous
                //   `MaybeUninit<T>` slots (or may be dangling if
                //   `n_elems == 0`); `self.bits` points to `n_words`
                //   contiguous `u64` words (or may be dangling if
                //   `n_words == 0`).
                // - Loop bounds ensure `w < n_words` when reading
                //   `*bits_base.add(w)`.
                // - For the final word we mask unused tail bits so
                //   any computed index `i = w * 64 + tz` always
                //   satisfies `i < n_elems` before we dereference
                //   `buf_base.add(i)`.
                // - Only slots whose bits are set are dropped, so no
                //   double-drops.
                // - No aliasing with active Rust borrows: the guard
                //   holds raw pointers and runs in `Drop` after the
                //   fill loop.
                unsafe {
                    let buf_base = self.buf.as_ptr();
                    let bits_base = self.bits.as_ptr();

                    for w in 0..self.n_words {
                        // Load word.
                        let mut word = *bits_base.add(w);

                        // Mask off bits beyond `n_elems` in the final
                        // word (if any).
                        if w == self.n_words.saturating_sub(1) {
                            let used_bits = self.n_elems.saturating_sub(w * 64);
                            if used_bits < 64 {
                                let mask = if used_bits == 0 {
                                    0
                                } else {
                                    (1u64 << used_bits) - 1
                                };
                                word &= mask;
                            }
                        }

                        // Fast scan set bits.
                        while word != 0 {
                            let tz = word.trailing_zeros() as usize;
                            let i = w * 64 + tz;
                            debug_assert!(i < self.n_elems);

                            let slot = buf_base.add(i);
                            // Drop the initialized element.
                            ptr::drop_in_place((*slot).as_mut_ptr());

                            // clear the bit we just handled
                            word &= word - 1;
                        }
                    }
                }
            }
        }

        // SAFETY:
        // - `buf` and `bits` are freshly allocated Vecs with
        //   capacity/len set to cover exactly `n_elems` and `n_words`,
        //   so their `.as_mut_ptr()` is valid for that many elements.
        // - Both slices live at least as long as the guard, and are
        //   not moved until after the guard is disarmed.
        // - No aliasing occurs: the guard holds only raw pointers and
        //   the fill loop mutates through those same allocations.
        let mut guard = unsafe { DropGuard::new(&mut buf, &mut bits) };

        for (rank, value) in pairs {
            // Single bounds check up front.
            if rank >= guard.n_elems {
                return Err(crate::v1::Error::InvalidRankCardinality {
                    expected: guard.n_elems,
                    actual: rank + 1,
                });
            }

            // Compute word index and bit mask once.
            let w = rank / 64;
            let b = rank % 64;
            let mask = 1u64 << b;

            // SAFETY:
            // - `rank < guard.n_elems` was checked above, so
            //   `buf_slot = buf.add(rank)` is within the
            //   `Vec<MaybeUninit<T>>` allocation.
            // - `w = rank / 64` and `bits.len() == (n + 63) / 64`
            //   ensure `bits_ptr = bits.add(w)` is in-bounds.
            // - If `(word & mask) != 0`, then this slot was
            //   previously initialized;
            //   `drop_in_place((*buf_slot).as_mut_ptr())` is valid and
            //   leaves the slot uninitialized.
            // - If the bit was clear, we set it and count `filled +=
            //   1`.
            // - `(*buf_slot).write(value)` is valid in both cases:
            //   either writing into an uninitialized slot or
            //   immediately after dropping the prior `T`.
            // - No aliasing with Rust references: the guard holds raw
            //   pointers and we have exclusive ownership of
            //   `buf`/`bits` within this function.
            unsafe {
                // Pointers from the guard (no long-lived & borrows).
                let bits_ptr = guard.bits.as_ptr().add(w);
                let buf_slot = guard.buf.as_ptr().add(rank);

                // Read the current word.
                let word = *bits_ptr;

                if (word & mask) != 0 {
                    // Duplicate: drop old value before overwriting.
                    core::ptr::drop_in_place((*buf_slot).as_mut_ptr());
                    // (Bit already set; no need to set again; don't
                    // bump `filled`.)
                } else {
                    // First time we see this rank.
                    *bits_ptr = word | mask;
                    filled += 1;
                }

                // Write new value into the slot.
                (*buf_slot).write(value);
            }
        }

        if filled != n {
            // Missing ranks: actual = number of distinct ranks seen.
            return Err(crate::v1::Error::InvalidRankCardinality {
                expected: n,
                actual: filled,
            });
        }

        // Success: prevent cleanup.
        guard.disarm();

        // SAFETY: all n slots are initialized
        let ranks = unsafe {
            let ptr = buf.as_mut_ptr() as *mut T;
            let len = buf.len();
            let cap = buf.capacity();
            // Prevent `buf` (Vec<MaybeUninit<T>>) from freeing the
            // allocation. Ownership of the buffer is about to be
            // transferred to `Vec<T>` via `from_raw_parts`.
            // Forgetting avoids a double free.
            mem::forget(buf);
            Vec::from_raw_parts(ptr, len, cap)
        };

        Ok(Self::new_unchecked(region, ranks))
    }
}

impl<T: PartialEq + Clone> ValueMesh<T> {
    /// Compresses the mesh in place using run-length encoding (RLE).
    ///
    /// This method scans the mesh's dense values, coalescing adjacent
    /// runs of identical elements into a compact [`Rep::Compressed`]
    /// representation. It replaces the internal storage (`rep`) with
    /// the compressed form.
    ///
    /// # Behavior
    /// - If the mesh is already compressed, this is a **no-op**.
    /// - If the mesh is dense, it consumes the current `Vec<T>` and
    ///   rebuilds the representation as a run table plus value table.
    /// - Only *adjacent* equal values are merged; non-contiguous
    ///   duplicates remain distinct.
    ///
    /// # Requirements
    /// - `T` must implement [`PartialEq`] (to detect equal values).
    /// - `T` must implement [`Clone`] (to populate the table of
    ///   unique values).
    ///
    /// This operation is lossless: expanding the compressed mesh back
    /// into a dense vector yields the same sequence of values.
    pub fn compress_adjacent_in_place(&mut self) {
        self.compress_adjacent_in_place_by(|a, b| a == b)
    }
}

impl<T: Clone> ValueMesh<T> {
    /// Compresses the mesh in place using a custom equivalence
    /// predicate.
    ///
    /// This is a generalized form of [`compress_adjacent_in_place`]
    /// that merges adjacent values according to an arbitrary
    /// predicate `same(a, b)`, rather than relying on `PartialEq`.
    ///
    /// # Behavior
    /// - If the mesh is already compressed, this is a **no-op**.
    /// - Otherwise, consumes the dense `Vec<T>` and replaces it with
    ///   a run-length encoded (`Rep::Compressed`) representation,
    ///   where consecutive elements satisfying `same(a, b)` are
    ///   coalesced into a single run.
    ///
    /// # Requirements
    /// - `T` must implement [`Clone`] (to populate the table of
    ///   unique values).
    /// - The predicate must be reflexive and symmetric for
    ///   correctness.
    ///
    /// This operation is lossless: expanding the compressed mesh
    /// reproduces the original sequence exactly under the same
    /// equivalence.
    pub fn compress_adjacent_in_place_by<F>(&mut self, same: F)
    where
        F: FnMut(&T, &T) -> bool,
    {
        let values = match &mut self.rep {
            Rep::Dense { values } => std::mem::take(values),
            Rep::Compressed { .. } => return,
        };
        let (table, runs) = compress_adjacent_with(values, same);
        self.rep = Rep::Compressed { table, runs };
    }
}

/// Performs simple run-length encoding (RLE) compression over a dense
/// sequence of values.
///
/// Adjacent "equal" elements are coalesced into contiguous runs,
/// producing:
///
/// - a **table** of unique values (in first-occurrence order)
/// - a **run list** of `(range, id)` pairs, where `range` is the
///   half-open index range `[start, end)` in the original dense
///   array, and `id` indexes into `table`.
///
/// # Example
/// ```
/// // Input: [A, A, B, B, B, A]
/// // Output:
/// // table = [A, B, A]
/// // runs  = [(0..2, 0), (2..5, 1), (5..6, 2)]
/// ```
///
/// # Requirements
/// - `T: Clone` is required to copy elements into the table.
///
/// # Returns
/// A tuple `(table, runs)` that together form the compressed
/// representation. Expanding the runs reproduces the original data.
fn compress_adjacent_with<T: Clone, F>(values: Vec<T>, mut same: F) -> (Vec<T>, Vec<Run>)
where
    F: FnMut(&T, &T) -> bool,
{
    // Empty input; trivial empty compression.
    if values.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let mut table = Vec::new(); // unique values
    let mut runs = Vec::new(); // (range, table_id) pairs

    let mut start = 0usize;
    table.push(values[0].clone());
    let mut cur_id: u32 = 0;

    // Walk through all subsequent elements, closing and opening runs
    // whenever the value changes.
    for (i, _value) in values.iter().enumerate().skip(1) {
        if !same(&values[i], &table[cur_id as usize]) {
            // Close current run [start, i)
            runs.push(Run::new(start, i, cur_id));

            // Start a new run
            start = i;
            table.push(values[i].clone());
            cur_id = (table.len() - 1) as u32;
        }
    }

    // Close the final run
    runs.push(Run::new(start, values.len(), cur_id));

    (table, runs)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Waker;

    use futures::executor::block_on;
    use futures::future;
    use ndslice::extent;
    use ndslice::strategy::gen_region;
    use ndslice::view::CollectExactMeshExt;
    use ndslice::view::CollectIndexedMeshExt;
    use ndslice::view::CollectMeshExt;
    use ndslice::view::MapIntoExt;
    use ndslice::view::Ranked;
    use ndslice::view::RankedSliceable;
    use ndslice::view::ViewExt;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use serde_json;

    use super::*;

    #[test]
    fn value_mesh_new_ok() {
        let region: Region = extent!(replica = 2, gpu = 3).into();
        let mesh = ValueMesh::new(region.clone(), (0..6).collect()).expect("new should succeed");
        assert_eq!(mesh.region().num_ranks(), 6);
        assert_eq!(mesh.values().count(), 6);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn value_mesh_new_len_mismatch_is_error() {
        let region: Region = extent!(replica = 2, gpu = 3).into();
        let err = ValueMesh::new(region, vec![0_i32; 5]).unwrap_err();
        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 5);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn value_mesh_transpose_ok_and_err() {
        let region: Region = extent!(x = 2).into();

        // ok case
        let ok_mesh = ValueMesh::new(region.clone(), vec![Ok::<_, Infallible>(1), Ok(2)]).unwrap();
        let ok = ok_mesh.transpose().unwrap();
        assert_eq!(ok.values().collect::<Vec<_>>(), vec![1, 2]);

        // err case: propagate user E
        #[derive(Debug, PartialEq)]
        enum E {
            Boom,
        }
        let err_mesh = ValueMesh::new(region, vec![Ok(1), Err(E::Boom)]).unwrap();
        let err = err_mesh.transpose().unwrap_err();
        assert_eq!(err, E::Boom);
    }

    #[test]
    fn value_mesh_join_preserves_region_and_values() {
        let region: Region = extent!(x = 2, y = 2).into();
        let futs = vec![
            future::ready(10),
            future::ready(11),
            future::ready(12),
            future::ready(13),
        ];
        let mesh = ValueMesh::new(region.clone(), futs).unwrap();

        let joined = block_on(mesh.join());
        assert_eq!(joined.region().num_ranks(), 4);
        assert_eq!(joined.values().collect::<Vec<_>>(), vec![10, 11, 12, 13]);
    }

    #[test]
    fn collect_mesh_ok() {
        let region: Region = extent!(x = 2, y = 3).into();
        let mesh = (0..6)
            .collect_mesh::<ValueMesh<_>>(region.clone())
            .expect("collect_mesh should succeed");

        assert_eq!(mesh.region().num_ranks(), 6);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn collect_mesh_len_too_short_is_error() {
        let region: Region = extent!(x = 2, y = 3).into();
        let err = (0..5).collect_mesh::<ValueMesh<_>>(region).unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 5);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_mesh_len_too_long_is_error() {
        let region: Region = extent!(x = 2, y = 3).into();
        let err = (0..7).collect_mesh::<ValueMesh<_>>(region).unwrap_err();
        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 7);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_mesh_from_map_pipeline() {
        let region: Region = extent!(x = 2, y = 2).into();
        let mesh = (0..4)
            .map(|i| i * 10)
            .collect_mesh::<ValueMesh<_>>(region.clone())
            .unwrap();

        assert_eq!(mesh.region().num_ranks(), 4);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 10, 20, 30]);
    }

    #[test]
    fn collect_exact_mesh_ok() {
        let region: Region = extent!(x = 2, y = 3).into();
        let mesh = (0..6)
            .collect_exact_mesh::<ValueMesh<_>>(region.clone())
            .expect("collect_exact_mesh should succeed");

        assert_eq!(mesh.region().num_ranks(), 6);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn collect_exact_mesh_len_too_short_is_error() {
        let region: Region = extent!(x = 2, y = 3).into();
        let err = (0..5)
            .collect_exact_mesh::<ValueMesh<_>>(region)
            .unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 5);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_exact_mesh_len_too_long_is_error() {
        let region: Region = extent!(x = 2, y = 3).into();
        let err = (0..7)
            .collect_exact_mesh::<ValueMesh<_>>(region)
            .unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 6);
                assert_eq!(actual, 7);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_exact_mesh_from_map_pipeline() {
        let region: Region = extent!(x = 2, y = 2).into();
        let mesh = (0..4)
            .map(|i| i * 10)
            .collect_exact_mesh::<ValueMesh<_>>(region.clone())
            .unwrap();

        assert_eq!(mesh.region().num_ranks(), 4);
        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![0, 10, 20, 30]);
    }

    #[test]
    fn collect_indexed_ok_shuffled() {
        let region: Region = extent!(x = 2, y = 3).into();
        // (rank, value) in shuffled order; values = rank * 10
        let pairs = vec![(3, 30), (0, 0), (5, 50), (2, 20), (1, 10), (4, 40)];
        let mesh = pairs
            .into_iter()
            .collect_indexed::<ValueMesh<_>>(region.clone())
            .unwrap();

        assert_eq!(mesh.region().num_ranks(), 6);
        assert_eq!(
            mesh.values().collect::<Vec<_>>(),
            vec![0, 10, 20, 30, 40, 50]
        );
    }

    #[test]
    fn collect_indexed_missing_rank_is_error() {
        let region: Region = extent!(x = 2, y = 2).into(); // 4
        // Missing rank 3
        let pairs = vec![(0, 100), (1, 101), (2, 102)];
        let err = pairs
            .into_iter()
            .collect_indexed::<ValueMesh<_>>(region)
            .unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 4);
                assert_eq!(actual, 3); // Distinct ranks seen.
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_indexed_out_of_bounds_is_error() {
        let region: Region = extent!(x = 2, y = 2).into(); // 4 (valid ranks 0..=3)
        let pairs = vec![(0, 1), (4, 9)]; // 4 is out-of-bounds
        let err = pairs
            .into_iter()
            .collect_indexed::<ValueMesh<_>>(region)
            .unwrap_err();

        match err {
            crate::v1::Error::InvalidRankCardinality { expected, actual } => {
                assert_eq!(expected, 4);
                assert_eq!(actual, 5); // offending index + 1
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn collect_indexed_duplicate_last_write_wins() {
        let region: Region = extent!(x = 1, y = 3).into(); // 3
        // rank 1 appears twice; last value should stick
        let pairs = vec![(0, 7), (1, 8), (1, 88), (2, 9)];
        let mesh = pairs
            .into_iter()
            .collect_indexed::<ValueMesh<_>>(region.clone())
            .unwrap();

        assert_eq!(mesh.values().collect::<Vec<_>>(), vec![7, 88, 9]);
    }

    // Indexed collector naïve implementation (for reference).
    fn build_value_mesh_indexed<T>(
        region: Region,
        pairs: impl IntoIterator<Item = (usize, T)>,
    ) -> crate::v1::Result<ValueMesh<T>> {
        let n = region.num_ranks();

        // Buffer for exactly n slots; fill by rank.
        let mut buf: Vec<Option<T>> = std::iter::repeat_with(|| None).take(n).collect();
        let mut filled = 0usize;

        for (rank, value) in pairs {
            if rank >= n {
                // Out-of-bounds: report `expected` = n, `actual` =
                // offending index + 1; i.e. number of ranks implied
                // so far.
                return Err(crate::v1::Error::InvalidRankCardinality {
                    expected: n,
                    actual: rank + 1,
                });
            }
            if buf[rank].is_none() {
                filled += 1;
            }
            buf[rank] = Some(value); // Last write wins.
        }

        if filled != n {
            // Missing ranks: actual = number of distinct ranks seen.
            return Err(crate::v1::Error::InvalidRankCardinality {
                expected: n,
                actual: filled,
            });
        }

        // All present and in-bounds: unwrap and build unchecked.
        let ranks: Vec<T> = buf.into_iter().map(Option::unwrap).collect();
        Ok(ValueMesh::new_unchecked(region, ranks))
    }

    /// This uses the bit-mixing portion of Sebastiano Vigna's
    /// [SplitMix64 algorithm](https://prng.di.unimi.it/splitmix64.c)
    /// to generate a high-quality 64-bit hash from a usize index.
    /// Unlike the full SplitMix64 generator, this is stateless - we
    /// accept an arbitrary x as input and apply the mix function to
    /// turn `x` deterministically into a "randomized" u64. input
    /// always produces the same output.
    fn hash_key(x: usize) -> u64 {
        let mut z = x as u64 ^ 0x9E3779B97F4A7C15;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    /// Shuffle a slice deterministically, using a hash of indices as
    /// the key.
    ///
    /// Each position `i` is assigned a pseudo-random 64-bit key (from
    /// `key(i)`), the slice is sorted by those keys, and the
    /// resulting permutation is applied in place.
    ///
    /// The permutation is fully determined by the sequence of indices
    /// `0..n` and the chosen `key` function. Running it twice on the
    /// same input yields the same "random-looking" arrangement.
    ///
    /// This is going to be used (below) for property tests: it gives
    /// the effect of a shuffle without introducing global RNG state,
    /// and ensures that duplicate elements are still ordered
    /// consistently (so we can test "last write wins" semantics in
    /// collectors).
    fn pseudo_shuffle<'a, T: 'a>(v: &'a mut [T], key: impl Fn(usize) -> u64 + Copy) {
        // Build perm.
        let mut with_keys: Vec<(u64, usize)> = (0..v.len()).map(|i| (key(i), i)).collect();
        with_keys.sort_by_key(|&(k, _)| k);
        let perm: Vec<usize> = with_keys.into_iter().map(|(_, i)| i).collect();

        // In-place permutation using a cycle based approach (e.g.
        // https://www.geeksforgeeks.org/dsa/permute-the-elements-of-an-array-following-given-order/).
        let mut seen = vec![false; v.len()];
        for i in 0..v.len() {
            if seen[i] {
                continue;
            }
            let mut a = i;
            while !seen[a] {
                seen[a] = true;
                let b = perm[a];
                // Short circuit on the cycle's start index.
                if b == i {
                    break;
                }
                v.swap(a, b);
                a = b;
            }
        }
    }

    // Property: Optimized and reference collectors yield the same
    // `ValueMesh` on complete inputs, even with duplicates.
    //
    // - Begin with a complete set of `(rank, value)` pairs covering
    //   all ranks of the region.
    // - Add extra pairs at arbitrary ranks (up to `extra_len`), which
    //   necessarily duplicate existing entries when `extra_len > 0`.
    // - Shuffle the combined pairs deterministically.
    // - Collect using both the reference (`try_collect_indexed`) and
    //   optimized (`try_collect_indexed_opt`) implementations.
    //
    // Both collectors must succeed and produce identical results.
    // This demonstrates that the optimized version preserves
    // last-write-wins semantics and agrees exactly with the reference
    // behavior.
    proptest! {
        #[test]
        fn try_collect_opt_equivalence(region in gen_region(1..=4, 6), extra_len in 0usize..=12) {
            let n = region.num_ranks();

            // Start with one pair per rank (coverage guaranteed).
            let mut pairs: Vec<(usize, i64)> = (0..n).map(|r| (r, r as i64)).collect();

            // Add some extra duplicates of random in-bounds ranks.
            // Their values differ so last-write-wins is observable.
            let extras = proptest::collection::vec(0..n, extra_len)
                .new_tree(&mut proptest::test_runner::TestRunner::default())
                .unwrap()
                .current();
            for (k, r) in extras.into_iter().enumerate() {
                pairs.push((r, (n as i64) + (k as i64)));
            }

            // Deterministic "shuffle" to fix iteration order across
            // both collectors.
            pseudo_shuffle(&mut pairs, hash_key);

            // Reference vs optimized.
            let mesh_ref = build_value_mesh_indexed(region.clone(), pairs.clone()).unwrap();
            let mesh_opt = pairs.into_iter().collect_indexed::<ValueMesh<_>>(region.clone()).unwrap();

            prop_assert_eq!(mesh_ref.region(), mesh_opt.region());
            prop_assert_eq!(mesh_ref.values().collect::<Vec<_>>(), mesh_opt.values().collect::<Vec<_>>());
        }
    }

    // Property: Optimized and reference collectors report identical
    // errors when ranks are missing.
    //
    // - Begin with a complete set of `(rank, value)` pairs.
    // - Remove one rank so coverage is incomplete.
    // - Shuffle deterministically.
    // - Collect with both implementations.
    //
    // Both must fail with `InvalidRankCardinality` describing the
    // same expected vs. actual counts.
    proptest! {
        #[test]
        fn try_collect_opt_missing_rank_errors_match(region in gen_region(1..=4, 6)) {
            let n = region.num_ranks();
            // Base complete.
            let mut pairs: Vec<(usize, i64)> = (0..n).map(|r| (r, r as i64)).collect();
            // Drop one distinct rank.
            if n > 0 {
                let drop_idx = 0usize; // Deterministic, fine for the property.
                pairs.remove(drop_idx);
            }
            // Shuffle deterministically.
            pseudo_shuffle(&mut pairs, hash_key);

            let ref_err  = build_value_mesh_indexed(region.clone(), pairs.clone()).unwrap_err();
            let opt_err  = pairs.into_iter().collect_indexed::<ValueMesh<_>>(region).unwrap_err();
            assert_eq!(format!("{ref_err:?}"), format!("{opt_err:?}"));
        }
    }

    // Property: Optimized and reference collectors report identical
    // errors when given out-of-bounds ranks.
    //
    // - Construct a set of `(rank, value)` pairs.
    // - Include at least one pair whose rank is ≥
    //   `region.num_ranks()`.
    // - Shuffle deterministically.
    // - Collect with both implementations.
    //
    // Both must fail with `InvalidRankCardinality`, and the reported
    // error values must match exactly.
    proptest! {
        #[test]
        fn try_collect_opt_out_of_bound_errors_match(region in gen_region(1..=4, 6)) {
            let n = region.num_ranks();
            // One valid, then one out-of-bound.
            let mut pairs = vec![(0usize, 0i64), (n, 123i64)];
            pseudo_shuffle(&mut pairs, hash_key);

            let ref_err = build_value_mesh_indexed(region.clone(), pairs.clone()).unwrap_err();
            let opt_err = pairs.into_iter().collect_indexed::<ValueMesh<_>>(region).unwrap_err();
            assert_eq!(format!("{ref_err:?}"), format!("{opt_err:?}"));
        }
    }

    #[test]
    fn map_into_preserves_region_and_order() {
        let region: Region = extent!(rows = 2, cols = 3).into();
        let vm = ValueMesh::new_unchecked(region.clone(), vec![0, 1, 2, 3, 4, 5]);

        let doubled: ValueMesh<_> = vm.map_into(|x| x * 2);
        assert_eq!(doubled.region, region);
        assert_eq!(
            doubled.values().collect::<Vec<_>>(),
            vec![0, 2, 4, 6, 8, 10]
        );
    }

    #[test]
    fn map_into_ref_borrows_and_preserves() {
        let region: Region = extent!(n = 4).into();
        let vm = ValueMesh::new_unchecked(
            region.clone(),
            vec!["a".to_string(), "b".into(), "c".into(), "d".into()],
        );

        let lens: ValueMesh<_> = vm.map_into(|s| s.len());
        assert_eq!(lens.region, region);
        assert_eq!(lens.values().collect::<Vec<_>>(), vec![1, 1, 1, 1]);
    }

    #[test]
    fn try_map_into_short_circuits_on_error() {
        let region = extent!(n = 4).into();
        let vm = ValueMesh::new_unchecked(region, vec![1, 2, 3, 4]);

        let res: Result<ValueMesh<i32>, &'static str> =
            vm.try_map_into(|x| if x == &3 { Err("boom") } else { Ok(x + 10) });

        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), "boom");
    }

    #[test]
    fn try_map_into_ref_short_circuits_on_error() {
        let region = extent!(n = 4).into();
        let vm = ValueMesh::new_unchecked(region, vec![1, 2, 3, 4]);

        let res: Result<ValueMesh<i32>, &'static str> =
            vm.try_map_into(|x| if x == &3 { Err("boom") } else { Ok(x + 10) });

        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), "boom");
    }

    // -- Helper to poll `core::future::Ready` without a runtime
    fn noop_waker() -> Waker {
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
        // SAFETY: The raw waker never dereferences its data pointer
        // (`null`), and all vtable fns are no-ops. It's only used to
        // satisfy `Context` for polling already-ready futures in
        // tests.
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    fn poll_now<F: Future>(mut fut: F) -> F::Output {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: `fut` is a local stack variable that we never move
        // after pinning, and we only use it to poll immediately
        // within this scope. This satisfies the invariants of
        // `Pin::new_unchecked`.
        let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => v,
            Poll::Pending => unreachable!("Ready futures must complete immediately"),
        }
    }
    // --

    #[test]
    fn map_into_ready_futures() {
        let region: Region = extent!(r = 2, c = 2).into();
        let vm = ValueMesh::new_unchecked(region.clone(), vec![10, 20, 30, 40]);

        // Map to `core::future::Ready` futures.
        let pending: ValueMesh<core::future::Ready<_>> =
            vm.map_into(|x| core::future::ready(x + 1));
        assert_eq!(pending.region, region);

        // Drive the ready futures without a runtime and collect results.
        let results: Vec<_> = pending.values().map(|f| poll_now(f.clone())).collect();
        assert_eq!(results, vec![11, 21, 31, 41]);
    }

    #[test]
    fn map_into_single_element_mesh() {
        let region: Region = extent!(n = 1).into();
        let vm = ValueMesh::new_unchecked(region.clone(), vec![7]);

        let out: ValueMesh<_> = vm.map_into(|x| x * x);
        assert_eq!(out.region, region);
        assert_eq!(out.values().collect::<Vec<_>>(), vec![49]);
    }

    #[test]
    fn map_into_ref_with_non_clone_field() {
        // A type that intentionally does NOT implement Clone.
        #[derive(Debug, PartialEq, Eq)]
        struct NotClone(i32);

        let region: Region = extent!(x = 3).into();
        let values = vec![(10, NotClone(1)), (20, NotClone(2)), (30, NotClone(3))];
        let mesh: ValueMesh<(i32, NotClone)> =
            values.into_iter().collect_mesh(region.clone()).unwrap();

        let projected: ValueMesh<i32> = mesh.map_into(|t| t.0);
        assert_eq!(projected.values().collect::<Vec<_>>(), vec![10, 20, 30]);
        assert_eq!(projected.region(), &region);
    }

    #[test]
    fn rle_roundtrip_all_equal() {
        let region: Region = extent!(n = 6).into();
        let mut vm = ValueMesh::new_unchecked(region.clone(), vec![42; 6]);

        // Compress and ensure logical equality preserved.
        vm.compress_adjacent_in_place();
        let collected: Vec<_> = vm.values().collect();
        assert_eq!(collected, vec![42, 42, 42, 42, 42, 42]);

        // Random access still works.
        for i in 0..region.num_ranks() {
            assert_eq!(vm.get(i), Some(&42));
        }
        assert_eq!(vm.get(region.num_ranks()), None); // out-of-bounds
    }

    #[test]
    fn rle_roundtrip_alternating() {
        let region: Region = extent!(n = 6).into();
        let original = vec![1, 2, 1, 2, 1, 2];
        let mut vm = ValueMesh::new_unchecked(region.clone(), original.clone());

        vm.compress_adjacent_in_place();
        let collected: Vec<_> = vm.values().collect();
        assert_eq!(collected, original);

        // Spot-check random access after compression.
        assert_eq!(vm.get(0), Some(&1));
        assert_eq!(vm.get(1), Some(&2));
        assert_eq!(vm.get(3), Some(&2));
        assert_eq!(vm.get(5), Some(&2));
    }

    #[test]
    fn rle_roundtrip_blocky_and_slice() {
        // Blocks: 0,0,0 | 1,1 | 2,2,2,2 | 3
        let region: Region = extent!(n = 10).into();
        let original = vec![0, 0, 0, 1, 1, 2, 2, 2, 2, 3];
        let mut vm = ValueMesh::new_unchecked(region.clone(), original.clone());

        vm.compress_adjacent_in_place();
        let collected: Vec<_> = vm.values().collect();
        assert_eq!(collected, original);

        // Slice a middle subregion [3..8) → [1,1,2,2,2]
        let sub_region = region.range("n", 3..8).unwrap();
        let sliced = vm.sliced(sub_region);
        let sliced_vec: Vec<_> = sliced.values().collect();
        assert_eq!(sliced_vec, vec![1, 1, 2, 2, 2]);
    }

    #[test]
    fn rle_idempotent_noop_on_second_call() {
        let region: Region = extent!(n = 7).into();
        let original = vec![9, 9, 9, 8, 8, 9, 9];
        let mut vm = ValueMesh::new_unchecked(region.clone(), original.clone());

        vm.compress_adjacent_in_place();
        let once: Vec<_> = vm.values().collect();
        assert_eq!(once, original);

        // Calling again should be a no-op and still yield identical
        // values.
        vm.compress_adjacent_in_place();
        let twice: Vec<_> = vm.values().collect();
        assert_eq!(twice, original);
    }

    #[test]
    fn rle_works_after_build_indexed() {
        // Build with shuffled pairs, then compress and verify
        // semantics.
        let region: Region = extent!(x = 2, y = 3).into(); // 6
        let pairs = vec![(3, 30), (0, 0), (5, 50), (2, 20), (1, 10), (4, 40)];
        let mut vm = pairs
            .into_iter()
            .collect_indexed::<ValueMesh<_>>(region.clone())
            .unwrap();

        // Should compress to 6 runs of length 1; still must
        // round-trip.
        vm.compress_adjacent_in_place();
        let collected: Vec<_> = vm.values().collect();
        assert_eq!(collected, vec![0, 10, 20, 30, 40, 50]);
        // Spot-check get()
        assert_eq!(vm.get(4), Some(&40));
    }

    #[test]
    fn rle_handles_singleton_mesh() {
        let region: Region = extent!(n = 1).into();
        let mut vm = ValueMesh::new_unchecked(region.clone(), vec![123]);

        vm.compress_adjacent_in_place();
        let collected: Vec<_> = vm.values().collect();
        assert_eq!(collected, vec![123]);
        assert_eq!(vm.get(0), Some(&123));
        assert_eq!(vm.get(1), None);
    }

    #[test]
    fn test_dense_round_trip() {
        // Build a simple dense mesh of 5 integers.
        let region: Region = extent!(x = 5).into();
        let dense = ValueMesh::new(region.clone(), vec![1, 2, 3, 4, 5]).unwrap();

        let json = serde_json::to_string_pretty(&dense).unwrap();
        let restored: ValueMesh<i32> = serde_json::from_str(&json).unwrap();

        assert_eq!(dense, restored);

        // Dense meshes should stay dense on the wire: check the
        // tagged variant.
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        // enum tag is nested: {"rep": {"rep":"dense", ...}}
        let tag = v
            .get("rep")
            .and_then(|o| o.get("rep"))
            .and_then(|s| s.as_str());
        assert_eq!(tag, Some("dense"));
    }

    #[test]
    fn test_compressed_round_trip() {
        // Build a dense mesh, compress it, and verify it stays
        // compressed on the wire.
        let region: Region = extent!(x = 10).into();
        let mut mesh = ValueMesh::new(region.clone(), vec![1, 1, 1, 2, 2, 3, 3, 3, 3, 3]).unwrap();
        mesh.compress_adjacent_in_place();

        let json = serde_json::to_string_pretty(&mesh).unwrap();
        let restored: ValueMesh<i32> = serde_json::from_str(&json).unwrap();

        // Logical equality preserved.
        assert_eq!(mesh, restored);

        // Compressed meshes should stay compressed on the wire.
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        // enum tag is nested: {"rep": {"rep":"compressed", ...}}
        let tag = v
            .get("rep")
            .and_then(|o| o.get("rep"))
            .and_then(|s| s.as_str());
        assert_eq!(tag, Some("compressed"));
    }

    #[test]
    fn test_stable_run_encoding() {
        let run = Run::new(0, 10, 42);
        let json = serde_json::to_string(&run).unwrap();
        let decoded: Run = serde_json::from_str(&json).unwrap();

        assert_eq!(run, decoded);
        assert_eq!(run.start, 0);
        assert_eq!(run.end, 10);
        assert_eq!(run.id, 42);

        // Ensure conversion back to Range<usize> works.
        let (range, id): (Range<usize>, u32) = run.try_into().unwrap();
        assert_eq!(range, 0..10);
        assert_eq!(id, 42);
    }
}
