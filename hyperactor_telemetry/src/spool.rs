/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::SyncUnsafeCell;
use std::fmt;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// A spool is a write-optimized ring buffer that may also be tailed
/// without dequeueing its elements. Thus, it is a good fit for
/// "flight recorder" style applications where we rarely expect
/// to read, but write intensely.
///
/// A spool's ring grows as it is written, so a spool costs memory only for
/// the entries it holds. Until the ring reaches capacity, each `push` takes
/// an exclusive lock and appends. After that, the write path is allocation
/// free and fully concurrent.
#[derive(Debug)]
pub(crate) struct Spool<T> {
    state: RwLock<State<T>>,
}

#[derive(Debug)]
struct State<T> {
    capacity: usize,
    ring: Vec<Entry<T>>,
    seq: AtomicUsize,
}

struct Entry<T> {
    writer: AtomicUsize,
    written: AtomicUsize,
    value: SyncUnsafeCell<T>,
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

impl<T> State<T> {
    fn is_full(&self) -> bool {
        self.ring.len() == self.capacity
    }

    /// Append an entry to a ring that is not yet full. Taking `&mut self`
    /// means no concurrent writer exists, and since the ring has never
    /// wrapped, entry `i` holds sequence number `i`.
    fn append(&mut self, value: T) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        debug_assert_eq!(seq, self.ring.len(), "a filling ring holds every seq");

        // Grow geometrically, but never past `capacity`.
        let len = self.ring.len();
        if len == self.ring.capacity() {
            self.ring.reserve_exact(len.max(4).min(self.capacity - len));
        }

        self.ring.push(Entry {
            writer: AtomicUsize::new(seq),
            written: AtomicUsize::new(seq),
            value: SyncUnsafeCell::new(value),
        });
    }

    /// Overwrite the oldest entry of a full ring. Concurrent callers are
    /// ordered by their sequence numbers.
    fn overwrite(&self, value: T) {
        let seq = self.seq.fetch_add(1, Ordering::Release);
        let entry = &self.ring[seq % self.ring.len()];

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

            // SAFETY: We are the only writer here, as we have: 1) claimed the slot, and
            // 2) waited for the previous writer to finish. Readers take the exclusive
            // lock, so none is active.
            unsafe {
                *entry.value.get() = value;
            }

            // And finally, we mark the slot as written. On architectures with a relaxed
            // memory model, this also serves as the fence to make the previous write
            // visible.
            entry.written.store(seq, Ordering::Release);
            break;
        }
    }
}

impl<T> Spool<T> {
    /// Create a new spool with the provided capacity.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            state: RwLock::new(State {
                capacity,
                ring: Vec::new(),
                seq: AtomicUsize::new(0),
            }),
        }
    }

    /// Push a new entry onto the spool. Pushes are concurrent and also ordered
    /// according to acquire/release semantics.
    pub(crate) fn push(&self, value: T) {
        {
            let state = self.state.read().unwrap();
            if state.is_full() {
                state.overwrite(value);
                return;
            }
        }

        // The ring is still filling. Another writer may fill it before we
        // get the exclusive lock, so check again.
        let mut state = self.state.write().unwrap();
        if state.is_full() {
            state.overwrite(value);
        } else {
            state.append(value);
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

        // The "settled" sequence number, and the total spool size. A filling
        // ring holds exactly `seq` entries; a full one holds the last
        // `capacity`.
        let seq = state.seq.load(Ordering::Acquire);
        let n = state.ring.len();

        (seq - n..seq)
            .map(|m| {
                let entry = &state.ring[m % n];
                assert_eq!(
                    m,
                    entry.written.load(Ordering::Acquire),
                    "writer integrity error"
                );
                // SAFETY: we hold the exclusive lock, so no writer is active,
                // and every entry in the ring holds a value.
                unsafe { (*entry.value.get()).clone() }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn test_unwritten_spool_is_empty() {
        let spool = Spool::<usize>::new(10);
        assert!(spool.tail().is_empty(), "an unwritten spool has no entries");
        assert_eq!(
            spool.state.read().unwrap().ring.capacity(),
            0,
            "an unwritten spool allocates no ring"
        );
    }

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

    #[test]
    fn test_ring_grows_to_exactly_capacity() {
        let spool = Spool::new(10);
        for i in 0..5usize {
            spool.push(i);
        }
        assert!(
            spool.state.read().unwrap().ring.capacity() < 10,
            "a half-written ring stops short of capacity"
        );
        assert_eq!(spool.tail(), vec![0, 1, 2, 3, 4]);

        for i in 5..100usize {
            spool.push(i);
        }
        assert_eq!(
            spool.state.read().unwrap().ring.capacity(),
            10,
            "a full ring holds exactly `capacity` entries"
        );
        assert_eq!(spool.tail(), (90..100).collect::<Vec<_>>());
    }

    #[test]
    fn test_drops_each_value_once() {
        let value = Arc::new(());
        let spool = Spool::new(4);
        for _ in 0..10 {
            spool.push(value.clone());
        }
        assert_eq!(
            Arc::strong_count(&value),
            5,
            "the spool holds its 4 newest values; overwritten ones were dropped"
        );

        drop(spool);
        assert_eq!(
            Arc::strong_count(&value),
            1,
            "dropping the spool drops what it held"
        );
    }

    #[test]
    fn test_concurrent_pushes() {
        let spool = Spool::new(64);
        thread::scope(|s| {
            for t in 0..8usize {
                let spool = &spool;
                s.spawn(move || {
                    for i in 0..1000 {
                        spool.push(t * 1000 + i);
                    }
                });
            }
        });

        let mut tail = spool.tail();
        assert_eq!(tail.len(), 64, "a full spool tails `capacity` entries");
        tail.sort_unstable();
        tail.dedup();
        assert_eq!(tail.len(), 64, "each tailed entry is a distinct push");
    }
}
