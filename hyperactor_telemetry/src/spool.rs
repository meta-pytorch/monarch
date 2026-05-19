/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::SyncUnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// A spool is a write-optimized ring buffer that may also be tailed
/// without dequeueing its elements. Thus, it is a good fit for
/// "flight recorder" style applications where we rarely expect
/// to read, but write intensely.
///
/// Spools are allocation free in their write path (`push`),
/// and provides fully concurrent writes.
#[derive(Debug)]
pub(crate) struct Spool<T> {
    state: RwLock<State<T>>,
}

#[derive(Debug)]
struct State<T> {
    ring: Vec<Entry<T>>,
    seq: AtomicUsize,
}

impl<T> Drop for State<T> {
    fn drop(&mut self) {
        while let Some(entry) = self.ring.pop() {
            if !entry.initialized.load(Ordering::Acquire) {
                continue;
            }

            // SAFETY: We are here only if we have written up until `seq`,
            // thus we can safely assume that entries up to `seq`
            // are written.
            unsafe {
                entry.value.assume_init();
            }
        }
    }
}

struct Entry<T> {
    writer: AtomicUsize,
    written: AtomicUsize,
    value: MaybeUninit<SyncUnsafeCell<T>>,
    initialized: AtomicBool,
}

impl<T> fmt::Debug for Entry<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("writer", &self.writer)
            .field("written", &self.written)
            // We cannot safely dereference the value, so we omit it.
            .field("value", &"...")
            .finish()
    }
}

impl<T> Default for Entry<T> {
    fn default() -> Self {
        Self {
            writer: AtomicUsize::new(0),
            written: AtomicUsize::new(0),
            value: MaybeUninit::uninit(),
            initialized: AtomicBool::new(false),
        }
    }
}

impl<T> Spool<T> {
    /// Create a new spool with the provided capacity.
    pub(crate) fn new(capacity: usize) -> Self {
        // We can't do vec![Entry{}; capacity] since this requires Entry: Clone.
        let mut ring = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            ring.push(Default::default());
        }

        Self {
            state: RwLock::new(State {
                ring,
                seq: AtomicUsize::new(0),
            }),
        }
    }

    /// Push a new entry onto the spool. Pushes are concurrent and also ordered
    /// according to acquire/release semantics.
    pub(crate) fn push(&self, value: T) {
        let state = self.state.read().unwrap();
        let seq = state.seq.fetch_add(1, Ordering::Release);
        let entry = &state.ring[seq % state.ring.len()];

        loop {
            let prev = entry.writer.load(Ordering::Acquire);
            if prev > seq {
                // We lost the race; a new writer has come around.
                return;
            }

            // Try to claim the slot.
            if entry
                .writer
                .compare_exchange(prev, seq, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }

            // We claimed the slot; first wait for the previous writer to finish.
            // We are comfortable spinning here because this is always just a memmove.
            loop {
                if entry.written.load(Ordering::Acquire) == prev {
                    break;
                }
            }

            let initialized = entry.initialized.load(Ordering::Acquire);

            // SAFETY: We are the only writer here, as we have: 1) claimed the slot, and
            // 2) waited for the previous writer to finish.
            unsafe {
                let ptr = SyncUnsafeCell::raw_get(entry.value.as_ptr());
                // Entries are uninitialized in the first iteration.
                if initialized {
                    std::ptr::drop_in_place(ptr);
                }
                ptr.write(value);
            }

            // And finally, we mark the slot as written. On architectures with a relaxed
            // memory model, this also serves as the fence to make the previous write
            // visible.
            if !initialized {
                entry.initialized.store(true, Ordering::Release);
            }
            entry.written.store(seq, Ordering::Release);
            break;
        }
    }
}

impl<T: Clone> Spool<T> {
    pub(crate) fn tail(&self) -> Vec<T> {
        // By the time we have a write lock, all of the writers have flushed,
        // and they are waiting to get in. This means that entries up to 'seq'
        // are all written successfully. We can guarantee integrity, as every
        // writer allocated a seq will have written, and the last writer would
        // have won for every slot.

        // We are using the r/w lock here as an "exclusive" vs. "shared" lock,
        // uncorrelated with reading or writing the protected state.
        #[allow(clippy::readonly_write_lock)]
        let state = self.state.write().unwrap();

        // The "settled" sequence number, and the total spool size.
        let seq = state.seq.load(Ordering::Acquire);
        let n = std::cmp::min(state.ring.len(), seq);

        let mut tail = Vec::with_capacity(n);
        for i in 0..n {
            let m = seq - n + i;
            let entry = &state.ring[m % state.ring.len()];
            assert_eq!(
                m,
                entry.written.load(Ordering::Acquire),
                "writer integrity error"
            );
            tail.push(
                // SAFETY: by the spool invariants, we know that this entry
                // has been written in `push`. We can therefore safely get a
                // reference to it.
                unsafe {
                    // Pointer to the initialized underlying value.
                    (*entry.value.assume_init_ref().get())
                        // Then clone it
                        .clone()
                },
            );
        }
        tail
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let spool = Spool::new(10);

        spool.push(123);
        spool.push(321);
        assert_eq!(spool.tail(), vec![123, 321]);
        assert_eq!(spool.tail(), vec![123, 321]);

        for i in 0..1000usize {
            spool.push(i);
        }

        let tail = spool.tail();
        assert_eq!(tail.len(), 10);
        for (i, value) in tail.into_iter().enumerate() {
            assert_eq!(value, 990 + i);
        }
    }
}
