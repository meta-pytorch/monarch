/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Per-actor local storage, analogous to `thread_local!` but scoped to actor instances.
//!
//! Use [`ActorLocal`] to declare static variables whose values are isolated per actor.
//!
//! # Example
//!
//! ```ignore
//! use hyperactor::ActorLocal;
//! use hyperactor::actor_local::Entry;
//!
//! static REQUEST_COUNT: ActorLocal<u64> = ActorLocal::new();
//!
//! impl Handler<MyMessage> for MyActor {
//!     async fn handle(&mut self, cx: &Context<Self>, msg: MyMessage) -> Result<(), Error> {
//!         // Fluent style (most common)
//!         *REQUEST_COUNT.entry(cx).or_default().get_mut() += 1;
//!
//!         // Or with and_modify
//!         REQUEST_COUNT.entry(cx)
//!             .and_modify(|v| *v += 1)
//!             .or_insert(0);
//!
//!         // Or pattern matching style
//!         match REQUEST_COUNT.entry(cx) {
//!             Entry::Occupied(mut o) => {
//!                 *o.get_mut() += 1;
//!             }
//!             Entry::Vacant(v) => {
//!                 v.insert(0);
//!             }
//!         }
//!
//!         Ok(())
//!     }
//! }
//! ```
//!
//! # Deadlock Warning
//!
//! All [`ActorLocal`] statics for a given actor instance share the same underlying
//! lock. The [`Entry`] returned by [`ActorLocal::entry`] holds this lock until dropped.
//!
//! **Do not hold multiple entries simultaneously** — this will deadlock:
//!
//! ```ignore
//! static LOCAL_A: ActorLocal<u64> = ActorLocal::new();
//! static LOCAL_B: ActorLocal<String> = ActorLocal::new();
//!
//! // DEADLOCK: second entry() blocks waiting for the lock held by first
//! let a = LOCAL_A.entry(cx);
//! let b = LOCAL_B.entry(cx);  // blocks forever!
//! ```
//!
//! Instead, access entries one at a time:
//!
//! ```ignore
//! // Correct: each entry is dropped before the next is acquired
//! *LOCAL_A.entry(cx).or_default().get_mut() += 1;
//! LOCAL_B.entry(cx).or_insert_with(|| "hello".to_string());
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::context;

/// A unique key identifying an [`ActorLocal`] static.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ActorLocalKey(usize);

/// Global counter for assigning unique keys to [`ActorLocal`] statics.
static NEXT_KEY: AtomicUsize = AtomicUsize::new(0);

/// Storage container for actor-local values.
///
/// Each actor instance has its own [`ActorLocalStorage`], which maps
/// [`ActorLocalKey`]s to boxed values. The storage is automatically
/// cleaned up when the actor instance is dropped.
#[derive(Default)]
pub struct ActorLocalStorage {
    pub(crate) storage: Mutex<HashMap<ActorLocalKey, Box<dyn Any + Send + Sync + 'static>>>,
}

impl ActorLocalStorage {
    /// Create a new empty storage.
    pub fn new() -> Self {
        Self {
            storage: Mutex::new(HashMap::new()),
        }
    }
}

/// Entry into actor-local storage. Holds the lock until dropped.
///
/// This follows the same pattern as [`std::collections::hash_map::Entry`].
pub enum Entry<'a, T: Send + Sync + 'static> {
    /// Value exists for this key.
    Occupied(OccupiedEntry<'a, T>),
    /// No value for this key.
    Vacant(VacantEntry<'a, T>),
}

/// Entry for an occupied actor-local slot.
///
/// Provides access to the stored value and allows replacing or removing it.
pub struct OccupiedEntry<'a, T: Send + Sync + 'static> {
    guard: MutexGuard<'a, HashMap<ActorLocalKey, Box<dyn Any + Send + Sync + 'static>>>,
    key: ActorLocalKey,
    _marker: PhantomData<T>,
}

/// Entry for a vacant actor-local slot.
///
/// Allows inserting a value into the slot.
pub struct VacantEntry<'a, T: Send + Sync + 'static> {
    guard: MutexGuard<'a, HashMap<ActorLocalKey, Box<dyn Any + Send + Sync + 'static>>>,
    key: ActorLocalKey,
    _marker: PhantomData<T>,
}

impl<'a, T: Send + Sync + 'static> Entry<'a, T> {
    /// Ensures a value is in the entry by inserting the default if empty,
    /// and returns an [`OccupiedEntry`].
    pub fn or_insert(self, default: T) -> OccupiedEntry<'a, T> {
        match self {
            Entry::Occupied(o) => o,
            Entry::Vacant(v) => v.insert(default),
        }
    }

    /// Ensures a value is in the entry by inserting the result of the default
    /// function if empty, and returns an [`OccupiedEntry`].
    pub fn or_insert_with<F: FnOnce() -> T>(self, f: F) -> OccupiedEntry<'a, T> {
        match self {
            Entry::Occupied(o) => o,
            Entry::Vacant(v) => v.insert(f()),
        }
    }

    /// Provides in-place mutable access to an occupied entry before any
    /// potential inserts into the map.
    pub fn and_modify<F: FnOnce(&mut T)>(mut self, f: F) -> Self {
        if let Entry::Occupied(ref mut o) = self {
            f(o.get_mut());
        }
        self
    }
}

impl<'a, T: Send + Sync + Default + 'static> Entry<'a, T> {
    /// Ensures a value is in the entry by inserting the default value if empty,
    /// and returns an [`OccupiedEntry`].
    pub fn or_default(self) -> OccupiedEntry<'a, T> {
        self.or_insert_with(T::default)
    }
}

impl<'a, T: Send + Sync + 'static> OccupiedEntry<'a, T> {
    /// Gets a reference to the value in the entry.
    pub fn get(&self) -> &T {
        self.guard
            .get(&self.key)
            .and_then(|b| b.downcast_ref::<T>())
            .expect("type mismatch in ActorLocal storage")
    }

    /// Gets a mutable reference to the value in the entry.
    pub fn get_mut(&mut self) -> &mut T {
        self.guard
            .get_mut(&self.key)
            .and_then(|b| b.downcast_mut::<T>())
            .expect("type mismatch in ActorLocal storage")
    }

    /// Sets the value of the entry with the [`OccupiedEntry`]'s key,
    /// and returns the entry's old value.
    pub fn insert(&mut self, value: T) -> T {
        let old = self
            .guard
            .insert(self.key, Box::new(value))
            .expect("OccupiedEntry should have value");
        *old.downcast::<T>()
            .expect("type mismatch in ActorLocal storage")
    }

    /// Takes the value of the entry out of the map, and returns it.
    pub fn remove(mut self) -> T {
        let old = self
            .guard
            .remove(&self.key)
            .expect("OccupiedEntry should have value");
        *old.downcast::<T>()
            .expect("type mismatch in ActorLocal storage")
    }
}

impl<'a, T: Send + Sync + 'static> VacantEntry<'a, T> {
    /// Sets the value of the entry with the [`VacantEntry`]'s key,
    /// and returns an [`OccupiedEntry`].
    pub fn insert(mut self, value: T) -> OccupiedEntry<'a, T> {
        self.guard.insert(self.key, Box::new(value));
        OccupiedEntry {
            guard: self.guard,
            key: self.key,
            _marker: PhantomData,
        }
    }
}

/// Per-actor local storage slot.
///
/// Declare as a static and access from handler methods via the context:
///
/// ```ignore
/// use hyperactor::actor_local::Entry;
///
/// static MY_LOCAL: ActorLocal<MyType> = ActorLocal::new();
///
/// fn handle(&mut self, cx: &Context<Self>, msg: MyMessage) {
///     // Get an entry (holds the lock)
///     let mut entry = MY_LOCAL.entry(cx);
///
///     // Use fluent API
///     *MY_LOCAL.entry(cx).or_default().get_mut() += 1;
///
///     // Or pattern match
///     match MY_LOCAL.entry(cx) {
///         Entry::Occupied(mut o) => {
///             let value = o.get_mut();
///             // modify value...
///         }
///         Entry::Vacant(v) => {
///             v.insert(initial_value);
///         }
///     }
/// }
/// ```
///
/// Each `ActorLocal` static gets a unique key at first access, so multiple
/// `ActorLocal<String>` statics will have separate storage slots.
pub struct ActorLocal<T: Send + Sync + 'static> {
    key: OnceLock<ActorLocalKey>,
    _marker: PhantomData<fn() -> T>,
}

// SAFETY: ActorLocal only stores a key (behind OnceLock) and phantom data.
// The actual values are stored in ActorLocalStorage which is Send + Sync.
unsafe impl<T: Send + Sync + 'static> Send for ActorLocal<T> {}
// SAFETY: ActorLocal only stores a key (behind OnceLock) and phantom data.
// The actual values are stored in ActorLocalStorage which is Send + Sync.
unsafe impl<T: Send + Sync + 'static> Sync for ActorLocal<T> {}

impl<T: Send + Sync + 'static> ActorLocal<T> {
    /// Create a new actor-local storage slot.
    pub const fn new() -> Self {
        Self {
            key: OnceLock::new(),
            _marker: PhantomData,
        }
    }

    /// Get the unique key for this static, initializing it if needed.
    fn key(&self) -> ActorLocalKey {
        *self
            .key
            .get_or_init(|| ActorLocalKey(NEXT_KEY.fetch_add(1, Ordering::Relaxed)))
    }

    /// Get an entry into actor-local storage.
    ///
    /// The returned [`Entry`] holds the lock until dropped, allowing
    /// mutable access without requiring `Clone`.
    ///
    /// # Warning
    ///
    /// All [`ActorLocal`] statics share the same lock per actor instance.
    /// Do not hold multiple entries simultaneously or the code will deadlock.
    /// See the [module-level documentation](self) for details.
    pub fn entry<'a, Cx: context::Actor>(&self, cx: &'a Cx) -> Entry<'a, T> {
        let guard = cx.instance().locals().storage.lock().unwrap();
        let key = self.key();
        if guard.contains_key(&key) {
            Entry::Occupied(OccupiedEntry {
                guard,
                key,
                _marker: PhantomData,
            })
        } else {
            Entry::Vacant(VacantEntry {
                guard,
                key,
                _marker: PhantomData,
            })
        }
    }
}

// Can't use derive(Clone) because it enforces T: Clone which is not necessary.
impl<T: Send + Sync + 'static> Clone for ActorLocal<T> {
    /// Clones only the key, not the value. If this clone is used from a different
    /// context it'll get a different value.
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_local_key_uniqueness() {
        static LOCAL1: ActorLocal<String> = ActorLocal::new();
        static LOCAL2: ActorLocal<String> = ActorLocal::new();

        let key1 = LOCAL1.key();
        let key2 = LOCAL2.key();

        assert_ne!(key1, key2, "each ActorLocal should have a unique key");
    }

    #[test]
    fn test_actor_local_storage_entry_vacant() {
        let storage = ActorLocalStorage::new();
        let key = ActorLocalKey(100);

        // Check that it's vacant
        {
            let guard = storage.storage.lock().unwrap();
            assert!(!guard.contains_key(&key));
        }

        // Insert via entry
        {
            let mut guard = storage.storage.lock().unwrap();
            guard.insert(key, Box::new("hello".to_string()));
        }

        // Check that it's now occupied
        {
            let guard = storage.storage.lock().unwrap();
            let value = guard
                .get(&key)
                .and_then(|b| b.downcast_ref::<String>())
                .unwrap();
            assert_eq!(value, "hello");
        }
    }

    #[test]
    fn test_actor_local_storage_entry_or_insert() {
        let storage = ActorLocalStorage::new();
        let key = ActorLocalKey(101);

        // Use or_insert pattern
        {
            let guard = storage.storage.lock().unwrap();
            if !guard.contains_key(&key) {
                drop(guard);
                let mut guard = storage.storage.lock().unwrap();
                guard.insert(key, Box::new(42u64));
            }
        }

        // Verify
        {
            let guard = storage.storage.lock().unwrap();
            let value = guard
                .get(&key)
                .and_then(|b| b.downcast_ref::<u64>())
                .unwrap();
            assert_eq!(*value, 42);
        }
    }

    #[test]
    fn test_actor_local_storage_entry_get_mut() {
        let storage = ActorLocalStorage::new();
        let key = ActorLocalKey(102);

        // Insert initial value
        {
            let mut guard = storage.storage.lock().unwrap();
            guard.insert(key, Box::new(10u64));
        }

        // Modify via get_mut
        {
            let mut guard = storage.storage.lock().unwrap();
            if let Some(boxed) = guard.get_mut(&key) {
                if let Some(value) = boxed.downcast_mut::<u64>() {
                    *value += 5;
                }
            }
        }

        // Verify
        {
            let guard = storage.storage.lock().unwrap();
            let value = guard
                .get(&key)
                .and_then(|b| b.downcast_ref::<u64>())
                .unwrap();
            assert_eq!(*value, 15);
        }
    }

    #[test]
    fn test_actor_local_storage_entry_remove() {
        let storage = ActorLocalStorage::new();
        let key = ActorLocalKey(103);

        // Insert value
        {
            let mut guard = storage.storage.lock().unwrap();
            guard.insert(key, Box::new("to be removed".to_string()));
        }

        // Remove and get old value
        let old_value = {
            let mut guard = storage.storage.lock().unwrap();
            let old = guard.remove(&key).unwrap();
            *old.downcast::<String>().unwrap()
        };

        assert_eq!(old_value, "to be removed");

        // Verify it's gone
        {
            let guard = storage.storage.lock().unwrap();
            assert!(!guard.contains_key(&key));
        }
    }

    #[test]
    fn test_actor_local_storage_entry_insert_replaces() {
        let storage = ActorLocalStorage::new();
        let key = ActorLocalKey(104);

        // Insert initial value
        {
            let mut guard = storage.storage.lock().unwrap();
            guard.insert(key, Box::new("first".to_string()));
        }

        // Replace with new value
        let old_value = {
            let mut guard = storage.storage.lock().unwrap();
            let old = guard.insert(key, Box::new("second".to_string())).unwrap();
            *old.downcast::<String>().unwrap()
        };

        assert_eq!(old_value, "first");

        // Verify new value
        {
            let guard = storage.storage.lock().unwrap();
            let value = guard
                .get(&key)
                .and_then(|b| b.downcast_ref::<String>())
                .unwrap();
            assert_eq!(value, "second");
        }
    }

    #[test]
    fn test_actor_local_storage_multiple_keys() {
        let storage = ActorLocalStorage::new();
        let key1 = ActorLocalKey(105);
        let key2 = ActorLocalKey(106);

        {
            let mut guard = storage.storage.lock().unwrap();
            guard.insert(key1, Box::new(42u64));
            guard.insert(key2, Box::new(100u64));
        }

        {
            let guard = storage.storage.lock().unwrap();
            assert_eq!(
                guard.get(&key1).and_then(|b| b.downcast_ref::<u64>()),
                Some(&42)
            );
            assert_eq!(
                guard.get(&key2).and_then(|b| b.downcast_ref::<u64>()),
                Some(&100)
            );
        }

        // Remove one
        {
            let mut guard = storage.storage.lock().unwrap();
            guard.remove(&key1);
        }

        {
            let guard = storage.storage.lock().unwrap();
            assert!(!guard.contains_key(&key1));
            assert_eq!(
                guard.get(&key2).and_then(|b| b.downcast_ref::<u64>()),
                Some(&100)
            );
        }
    }

    #[test]
    fn test_actor_local_storage_type_mismatch() {
        let storage = ActorLocalStorage::new();
        let key = ActorLocalKey(107);

        {
            let mut guard = storage.storage.lock().unwrap();
            guard.insert(key, Box::new("a string".to_string()));
        }

        {
            let guard = storage.storage.lock().unwrap();
            // Wrong type returns None on downcast
            assert!(
                guard
                    .get(&key)
                    .and_then(|b| b.downcast_ref::<u64>())
                    .is_none()
            );
            // Correct type works
            assert_eq!(
                guard
                    .get(&key)
                    .and_then(|b| b.downcast_ref::<String>())
                    .map(|s| s.as_str()),
                Some("a string")
            );
        }
    }
}
