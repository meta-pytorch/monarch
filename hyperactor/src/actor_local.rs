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
//! # Thread Safety
//!
//! Each [`ActorLocal`] has its own internal lock, so accessing different
//! `ActorLocal` statics concurrently is safe and does not cause deadlocks.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::OnceLock;

use crate::context;

#[allow(dead_code)]
mod weak_map {
    //! Internal weak map implementation for actor-local storage.
    //!
    //! This module provides a typed map keyed by weak references, where entries
    //! are automatically removed when the key is dropped.

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::MutexGuard;
    use std::sync::Weak;

    /// Type-erased trait for WeakMap so WeakKeyInner can hold refs to any WeakMap<T>.
    trait ErasedWeakMap: Send + Sync {
        /// Remove entry by raw pointer to WeakKeyInner.
        fn remove_by_ptr(&self, ptr: *const WeakKeyInner);
    }

    /// Wrapper around raw pointer for use as HashMap key.
    /// Implements Hash/Eq based on data pointer address only, ignoring vtable.
    /// This is necessary because vtables can be duplicated across codegen units.
    #[derive(Clone, Copy)]
    struct MapPtr(*const dyn ErasedWeakMap);

    impl MapPtr {
        /// Extract just the data pointer, ignoring the vtable.
        fn data_ptr(&self) -> *const () {
            self.0 as *const ()
        }
    }

    impl PartialEq for MapPtr {
        fn eq(&self, other: &Self) -> bool {
            self.data_ptr() == other.data_ptr()
        }
    }

    impl Eq for MapPtr {}

    impl std::hash::Hash for MapPtr {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.data_ptr().hash(state);
        }
    }

    // SAFETY: MapPtr is only used as a lookup key; we never dereference
    // without first upgrading the accompanying Weak reference.
    unsafe impl Send for MapPtr {}
    // SAFETY: MapPtr is only used as a lookup key; we never dereference
    // without first upgrading the accompanying Weak reference.
    unsafe impl Sync for MapPtr {}

    /// Inner key type, wrapped in Arc and tracked by WeakMaps.
    struct WeakKeyInner {
        /// WeakMaps that contain entries for this key.
        /// On drop, we remove ourselves from all these maps.
        maps: Mutex<HashMap<MapPtr, Weak<dyn ErasedWeakMap>>>,
    }

    impl WeakKeyInner {
        fn unregister_map(&self, map_ptr: *const dyn ErasedWeakMap) {
            let mut maps = self.maps.lock().unwrap();
            maps.remove(&MapPtr(map_ptr));
        }
    }

    impl Drop for WeakKeyInner {
        // No deadlock possible: we hold self.maps and call map.remove_by_ptr (which
        // locks map.entries). The reverse order is in WeakMapInner::drop. However,
        // deadlock requires both upgrades to succeed, meaning both strong counts > 0.
        // But we're in Drop, so our strong count is 0, and any Weak::upgrade to us
        // from the other side will fail. At most one direction's upgrade succeeds.
        fn drop(&mut self) {
            let maps = self.maps.lock().unwrap();
            let self_ptr = self as *const WeakKeyInner;
            for weak_map in maps.values() {
                if let Some(map) = weak_map.upgrade() {
                    map.remove_by_ptr(self_ptr);
                }
            }
        }
    }

    /// Key type allocated per storage. On drop, clears entries from all WeakMaps.
    #[derive(Clone)]
    pub struct WeakKey {
        inner: Arc<WeakKeyInner>,
    }

    impl WeakKey {
        pub fn new() -> Self {
            Self {
                inner: Arc::new(WeakKeyInner {
                    maps: Mutex::new(HashMap::new()),
                }),
            }
        }

        fn as_ptr(&self) -> *const WeakKeyInner {
            Arc::as_ptr(&self.inner)
        }

        fn downgrade(&self) -> Weak<WeakKeyInner> {
            Arc::downgrade(&self.inner)
        }

        fn register_map(&self, map: Weak<dyn ErasedWeakMap>) {
            let mut maps = self.inner.maps.lock().unwrap();
            maps.insert(MapPtr(map.as_ptr()), map);
        }

        fn unregister_map(&self, map_ptr: *const dyn ErasedWeakMap) {
            self.inner.unregister_map(map_ptr);
        }

        #[cfg(test)]
        pub fn maps_len(&self) -> usize {
            self.inner.maps.lock().unwrap().len()
        }
    }

    /// Inner state of a WeakMap.
    struct WeakMapInner<T: Send + 'static> {
        entries: Mutex<HashMap<*const WeakKeyInner, (Weak<WeakKeyInner>, T)>>,
    }

    // SAFETY: The raw pointer is only used as a key for lookup and is always
    // validated via the accompanying Weak<WeakKeyInner> before use.
    unsafe impl<T: Send + 'static> Send for WeakMapInner<T> {}
    // SAFETY: The raw pointer is only used as a key for lookup. Access to T
    // is through Mutex which provides exclusive access, so T: Sync is not required.
    unsafe impl<T: Send + 'static> Sync for WeakMapInner<T> {}

    impl<T: Send + 'static> ErasedWeakMap for WeakMapInner<T> {
        fn remove_by_ptr(&self, ptr: *const WeakKeyInner) {
            let mut entries = self.entries.lock().unwrap();
            entries.remove(&ptr);
        }
    }

    impl<T: Send + 'static> Drop for WeakMapInner<T> {
        // No deadlock possible: see comment on WeakKeyInner::drop. We hold self.entries
        // and call key.unregister_map (which locks key.maps). Since we're dropping,
        // our strong count is 0, so any Weak::upgrade to us will fail.
        fn drop(&mut self) {
            let entries = self.entries.lock().unwrap();
            let self_ptr = self as *const WeakMapInner<T> as *const dyn ErasedWeakMap;
            for (_, (weak_key, _)) in entries.iter() {
                if let Some(key_inner) = weak_key.upgrade() {
                    key_inner.unregister_map(self_ptr);
                }
            }
        }
    }

    fn as_erased<T: Send + 'static>(inner: &Arc<WeakMapInner<T>>) -> Weak<dyn ErasedWeakMap> {
        Arc::downgrade(inner) as Weak<dyn ErasedWeakMap>
    }

    /// Typed weak map with internal locking.
    pub struct WeakMap<T: Send + 'static> {
        inner: Arc<WeakMapInner<T>>,
    }

    impl<T: Send + 'static> WeakMap<T> {
        pub fn new() -> Self {
            Self {
                inner: Arc::new(WeakMapInner {
                    entries: Mutex::new(HashMap::new()),
                }),
            }
        }

        /// Get an entry into the weak map.
        pub fn entry<'a>(&'a self, key: &'a WeakKey) -> Entry<'a, T> {
            let key_ptr = key.as_ptr();
            let guard = self.inner.entries.lock().unwrap();

            if guard.contains_key(&key_ptr) {
                Entry::Occupied(OccupiedEntry {
                    guard,
                    key_ptr,
                    key,
                    map_inner: &self.inner,
                })
            } else {
                Entry::Vacant(VacantEntry {
                    guard,
                    key_ptr,
                    key,
                    map_inner: &self.inner,
                })
            }
        }

        #[cfg(test)]
        fn contains_key(&self, key: &WeakKey) -> bool {
            let guard = self.inner.entries.lock().unwrap();
            guard.contains_key(&key.as_ptr())
        }

        #[cfg(test)]
        fn get(&self, key: &WeakKey) -> Option<T>
        where
            T: Clone,
        {
            let guard = self.inner.entries.lock().unwrap();
            guard.get(&key.as_ptr()).map(|(_, v)| v.clone())
        }

        #[cfg(test)]
        fn len(&self) -> usize {
            let guard = self.inner.entries.lock().unwrap();
            guard.len()
        }
    }

    /// Entry into weak map storage. Holds the lock until dropped.
    ///
    /// This follows the same pattern as [`std::collections::hash_map::Entry`].
    pub enum Entry<'a, T: Send + 'static> {
        /// Value exists for this key.
        Occupied(OccupiedEntry<'a, T>),
        /// No value for this key.
        Vacant(VacantEntry<'a, T>),
    }

    /// Entry for an occupied weak map slot.
    ///
    /// Provides access to the stored value and allows replacing or removing it.
    pub struct OccupiedEntry<'a, T: Send + 'static> {
        guard: MutexGuard<'a, HashMap<*const WeakKeyInner, (Weak<WeakKeyInner>, T)>>,
        key_ptr: *const WeakKeyInner,
        key: &'a WeakKey,
        map_inner: &'a Arc<WeakMapInner<T>>,
    }

    /// Entry for a vacant weak map slot.
    ///
    /// Allows inserting a value into the slot.
    pub struct VacantEntry<'a, T: Send + 'static> {
        guard: MutexGuard<'a, HashMap<*const WeakKeyInner, (Weak<WeakKeyInner>, T)>>,
        key_ptr: *const WeakKeyInner,
        key: &'a WeakKey,
        map_inner: &'a Arc<WeakMapInner<T>>,
    }

    impl<'a, T: Send + 'static> Entry<'a, T> {
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

    impl<'a, T: Send + Default + 'static> Entry<'a, T> {
        /// Ensures a value is in the entry by inserting the default value if empty,
        /// and returns an [`OccupiedEntry`].
        pub fn or_default(self) -> OccupiedEntry<'a, T> {
            self.or_insert_with(T::default)
        }
    }

    impl<'a, T: Send + 'static> OccupiedEntry<'a, T> {
        /// Gets a reference to the value in the entry.
        pub fn get(&self) -> &T {
            &self
                .guard
                .get(&self.key_ptr)
                .expect("OccupiedEntry should have value")
                .1
        }

        /// Gets a mutable reference to the value in the entry.
        pub fn get_mut(&mut self) -> &mut T {
            &mut self
                .guard
                .get_mut(&self.key_ptr)
                .expect("OccupiedEntry should have value")
                .1
        }

        /// Sets the value of the entry with the [`OccupiedEntry`]'s key,
        /// and returns the entry's old value.
        pub fn insert(&mut self, value: T) -> T {
            let entry = self
                .guard
                .get_mut(&self.key_ptr)
                .expect("OccupiedEntry should have value");
            std::mem::replace(&mut entry.1, value)
        }

        /// Takes the value of the entry out of the map, and returns it.
        pub fn remove(mut self) -> T {
            let (_, value) = self
                .guard
                .remove(&self.key_ptr)
                .expect("OccupiedEntry should have value");

            // Unregister this map from the key
            let map_ptr = Arc::as_ptr(self.map_inner) as *const dyn ErasedWeakMap;
            self.key.unregister_map(map_ptr);

            value
        }
    }

    impl<'a, T: Send + 'static> VacantEntry<'a, T> {
        /// Sets the value of the entry with the [`VacantEntry`]'s key,
        /// and returns an [`OccupiedEntry`].
        pub fn insert(mut self, value: T) -> OccupiedEntry<'a, T> {
            // Register this map with the key for cleanup on key drop
            self.key.register_map(as_erased(self.map_inner));

            self.guard
                .insert(self.key_ptr, (self.key.downgrade(), value));

            OccupiedEntry {
                guard: self.guard,
                key_ptr: self.key_ptr,
                key: self.key,
                map_inner: self.map_inner,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_weak_key_creation() {
            let key1 = WeakKey::new();
            let key2 = WeakKey::new();

            // Each key should have a unique pointer
            assert_ne!(key1.as_ptr(), key2.as_ptr());
        }

        #[test]
        fn test_weak_map_basic_operations() {
            let map: WeakMap<u64> = WeakMap::new();
            let key = WeakKey::new();

            // Initially empty
            assert!(!map.contains_key(&key));

            // Insert via entry
            match map.entry(&key) {
                Entry::Vacant(v) => {
                    v.insert(42);
                }
                Entry::Occupied(_) => panic!("expected vacant"),
            }

            // Verify insertion
            assert!(map.contains_key(&key));
            assert_eq!(map.get(&key), Some(42));

            // Modify via entry
            match map.entry(&key) {
                Entry::Occupied(mut o) => {
                    *o.get_mut() = 100;
                }
                Entry::Vacant(_) => panic!("expected occupied"),
            }

            // Verify modification
            assert_eq!(map.get(&key), Some(100));

            // Remove via entry
            match map.entry(&key) {
                Entry::Occupied(o) => {
                    let removed = o.remove();
                    assert_eq!(removed, 100);
                }
                Entry::Vacant(_) => panic!("expected occupied"),
            }

            // Verify removal
            assert!(!map.contains_key(&key));
        }

        #[test]
        fn test_key_drop_clears_map_entries() {
            let map: WeakMap<String> = WeakMap::new();

            {
                let key = WeakKey::new();

                // Insert value via entry
                map.entry(&key).or_insert("hello".to_string());

                // Verify present
                assert!(map.contains_key(&key));
                assert_eq!(map.len(), 1);

                // key drops here
            }

            // After key drop, entry should be removed
            assert_eq!(map.len(), 0);
        }

        #[test]
        fn test_key_drop_clears_map_entries_with_scoped_key() {
            let map: WeakMap<String> = WeakMap::new();

            let weak_ref = {
                let key = WeakKey::new();

                // Insert value via entry
                map.entry(&key).or_insert("hello".to_string());

                // Verify present
                assert!(map.contains_key(&key));

                // Return a weak ref so we can verify cleanup happened
                key.downgrade()
                // key drops here
            };

            // The weak ref should now be dead
            assert!(weak_ref.upgrade().is_none());
        }

        #[test]
        fn test_multiple_maps_same_key() {
            let map1: WeakMap<u64> = WeakMap::new();
            let map2: WeakMap<String> = WeakMap::new();

            {
                let key = WeakKey::new();

                // Insert into both maps via entry
                map1.entry(&key).or_insert(42);
                map2.entry(&key).or_insert("test".to_string());

                // Verify both present
                assert!(map1.contains_key(&key));
                assert!(map2.contains_key(&key));
                assert_eq!(map1.len(), 1);
                assert_eq!(map2.len(), 1);

                // key drops here
            }

            // After key drop, both maps should be cleared
            assert_eq!(map1.len(), 0);
            assert_eq!(map2.len(), 0);
        }

        #[test]
        fn test_no_duplicate_map_registration() {
            let map: WeakMap<u64> = WeakMap::new();
            let key = WeakKey::new();

            // Insert same key multiple times (should only register map once)
            map.entry(&key).or_insert(1);
            // Re-entry on occupied doesn't register again
            *map.entry(&key).or_insert(2).get_mut() = 3;

            // Should only have one map registered
            assert_eq!(key.maps_len(), 1);
        }

        #[test]
        fn test_unregister_map_on_remove() {
            let map: WeakMap<u64> = WeakMap::new();
            let key = WeakKey::new();

            // Insert
            map.entry(&key).or_insert(42);
            assert_eq!(key.maps_len(), 1);

            // Remove via entry
            if let Entry::Occupied(o) = map.entry(&key) {
                o.remove();
            }

            // Map should be unregistered
            assert_eq!(key.maps_len(), 0);
        }

        #[test]
        fn test_concurrent_access_different_maps() {
            let map1: WeakMap<u64> = WeakMap::new();
            let map2: WeakMap<u64> = WeakMap::new();
            let key = WeakKey::new();

            // Set up entries in both maps
            map1.entry(&key).or_insert(1);
            map2.entry(&key).or_insert(2);

            // Verify both have correct values
            assert_eq!(map1.get(&key), Some(1));
            assert_eq!(map2.get(&key), Some(2));
        }

        #[test]
        fn test_entry_or_default() {
            let map: WeakMap<u64> = WeakMap::new();
            let key = WeakKey::new();

            // or_default on vacant
            *map.entry(&key).or_default().get_mut() = 42;
            assert_eq!(map.get(&key), Some(42));
        }

        #[test]
        fn test_entry_and_modify() {
            let map: WeakMap<u64> = WeakMap::new();
            let key = WeakKey::new();

            // and_modify on vacant (no-op), then or_insert
            map.entry(&key).and_modify(|v| *v += 10).or_insert(5);
            assert_eq!(map.get(&key), Some(5));

            // and_modify on occupied
            map.entry(&key).and_modify(|v| *v += 10).or_insert(0);
            assert_eq!(map.get(&key), Some(15));
        }

        #[test]
        fn test_map_drop_unregisters_from_keys() {
            let key = WeakKey::new();

            {
                let map: WeakMap<u64> = WeakMap::new();
                map.entry(&key).or_insert(42);

                // Key should have the map registered
                assert_eq!(key.maps_len(), 1);

                // map drops here
            }

            // After map drop, key should have no maps registered
            assert_eq!(key.maps_len(), 0);
        }

        #[test]
        fn test_multiple_maps_drop_unregisters_all() {
            let key = WeakKey::new();

            {
                let map1: WeakMap<u64> = WeakMap::new();
                let map2: WeakMap<String> = WeakMap::new();

                map1.entry(&key).or_insert(42);
                map2.entry(&key).or_insert("test".to_string());

                // Key should have both maps registered
                assert_eq!(key.maps_len(), 2);

                // Drop map1 first
                drop(map1);
                assert_eq!(key.maps_len(), 1);

                // map2 drops here
            }

            // After all maps dropped, key should have no maps registered
            assert_eq!(key.maps_len(), 0);
        }
    }
}

use weak_map::WeakKey;
use weak_map::WeakMap;

/// Type alias for the type-erased value stored in ActorLocalStorage.
type ErasedValue = Box<dyn Any + Send>;

/// Storage container for actor-local values.
///
/// Each actor instance has its own [`ActorLocalStorage`], which holds a
/// type-erased map. When the storage is dropped, all entries in the map
/// are automatically cleaned up.
pub struct ActorLocalStorage {
    map: WeakMap<ErasedValue>,
}

impl Default for ActorLocalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorLocalStorage {
    /// Create a new empty storage.
    pub fn new() -> Self {
        Self {
            map: WeakMap::new(),
        }
    }

    /// Get a reference to the map.
    fn map(&self) -> &WeakMap<ErasedValue> {
        &self.map
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
/// Each `ActorLocal` has its own internal lock, so accessing multiple
/// `ActorLocal` statics simultaneously is safe and will not deadlock.
pub struct ActorLocal<T: Send + 'static> {
    key: OnceLock<WeakKey>,
    _marker: PhantomData<fn() -> T>,
}

// SAFETY: ActorLocal stores a WeakKey (behind OnceLock) which is Send + Sync.
unsafe impl<T: Send + 'static> Send for ActorLocal<T> {}
// SAFETY: ActorLocal stores a WeakKey (behind OnceLock) which is Send + Sync.
unsafe impl<T: Send + 'static> Sync for ActorLocal<T> {}

impl<T: Send + 'static> ActorLocal<T> {
    /// Create a new actor-local storage slot.
    pub const fn new() -> Self {
        Self {
            key: OnceLock::new(),
            _marker: PhantomData,
        }
    }

    /// Get or initialize the WeakKey.
    fn key(&self) -> &WeakKey {
        self.key.get_or_init(WeakKey::new)
    }

    /// Get an entry into actor-local storage.
    ///
    /// The returned [`Entry`] holds the lock until dropped, allowing
    /// mutable access without requiring `Clone`.
    pub fn entry<'a, Cx: context::Actor>(&'a self, cx: &'a Cx) -> Entry<'a, T> {
        let map = cx.instance().locals().map();
        let key = self.key();
        Entry::new(map.entry(key))
    }
}

impl<T: Send + 'static> Default for ActorLocal<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> Clone for ActorLocal<T> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            _marker: PhantomData,
        }
    }
}

/// Entry into actor-local storage with type-safe access.
///
/// This wraps the underlying type-erased entry and provides typed access
/// through boxing and downcasting.
pub enum Entry<'a, T: Send + 'static> {
    /// Value exists for this key.
    Occupied(OccupiedEntry<'a, T>),
    /// No value for this key.
    Vacant(VacantEntry<'a, T>),
}

impl<'a, T: Send + 'static> Entry<'a, T> {
    fn new(entry: weak_map::Entry<'a, ErasedValue>) -> Self {
        match entry {
            weak_map::Entry::Occupied(o) => Entry::Occupied(OccupiedEntry(o, PhantomData)),
            weak_map::Entry::Vacant(v) => Entry::Vacant(VacantEntry(v, PhantomData)),
        }
    }

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

impl<'a, T: Send + Default + 'static> Entry<'a, T> {
    /// Ensures a value is in the entry by inserting the default value if empty,
    /// and returns an [`OccupiedEntry`].
    pub fn or_default(self) -> OccupiedEntry<'a, T> {
        self.or_insert_with(T::default)
    }
}

/// Entry for an occupied actor-local slot with typed access.
pub struct OccupiedEntry<'a, T: Send + 'static>(
    weak_map::OccupiedEntry<'a, ErasedValue>,
    PhantomData<fn() -> T>,
);

impl<'a, T: Send + 'static> OccupiedEntry<'a, T> {
    /// Gets a reference to the value in the entry.
    pub fn get(&self) -> &T {
        self.0
            .get()
            .downcast_ref::<T>()
            .expect("type mismatch in actor-local storage")
    }

    /// Gets a mutable reference to the value in the entry.
    pub fn get_mut(&mut self) -> &mut T {
        self.0
            .get_mut()
            .downcast_mut::<T>()
            .expect("type mismatch in actor-local storage")
    }

    /// Sets the value of the entry, returning the old value.
    pub fn insert(&mut self, value: T) -> T {
        let old = self.0.insert(Box::new(value));
        *old.downcast::<T>()
            .expect("type mismatch in actor-local storage")
    }

    /// Takes the value out of the entry.
    pub fn remove(self) -> T {
        let value = self.0.remove();
        *value
            .downcast::<T>()
            .expect("type mismatch in actor-local storage")
    }
}

/// Entry for a vacant actor-local slot with typed access.
pub struct VacantEntry<'a, T: Send + 'static>(
    weak_map::VacantEntry<'a, ErasedValue>,
    PhantomData<fn() -> T>,
);

impl<'a, T: Send + 'static> VacantEntry<'a, T> {
    /// Sets the value of the entry, returning an occupied entry.
    pub fn insert(self, value: T) -> OccupiedEntry<'a, T> {
        OccupiedEntry(self.0.insert(Box::new(value)), PhantomData)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_local_storage_creation() {
        let storage = ActorLocalStorage::new();
        // Just verify it can be created
        drop(storage);
    }

    #[test]
    fn test_storage_drop_clears_all_actor_locals() {
        let local1: ActorLocal<u64> = ActorLocal::new();
        let local2: ActorLocal<String> = ActorLocal::new();

        {
            let storage = ActorLocalStorage::new();
            let map = storage.map();

            // Insert into both locals via entry
            map.entry(local1.key()).or_insert(Box::new(42u64));
            map.entry(local2.key())
                .or_insert(Box::new("test".to_string()));

            // Verify present and keys registered with the map
            assert!(matches!(
                map.entry(local1.key()),
                weak_map::Entry::Occupied(_)
            ));
            assert!(matches!(
                map.entry(local2.key()),
                weak_map::Entry::Occupied(_)
            ));
            assert_eq!(local1.key().maps_len(), 1);
            assert_eq!(local2.key().maps_len(), 1);

            // storage drops here
        }

        // After storage drop, keys should have no maps registered
        assert_eq!(local1.key().maps_len(), 0);
        assert_eq!(local2.key().maps_len(), 0);
    }

    #[test]
    fn test_multiple_storages_same_local() {
        let local: ActorLocal<u64> = ActorLocal::new();

        let storage1 = ActorLocalStorage::new();
        let storage2 = ActorLocalStorage::new();

        let map1 = storage1.map();
        let map2 = storage2.map();
        let key = local.key();

        // Insert for both storages via entry
        map1.entry(key).or_insert(Box::new(100u64));
        map2.entry(key).or_insert(Box::new(200u64));

        // Verify both present with correct values
        assert_eq!(
            *map1
                .entry(key)
                .or_insert(Box::new(0u64))
                .get()
                .downcast_ref::<u64>()
                .unwrap(),
            100
        );
        assert_eq!(
            *map2
                .entry(key)
                .or_insert(Box::new(0u64))
                .get()
                .downcast_ref::<u64>()
                .unwrap(),
            200
        );

        // Drop storage1
        drop(storage1);

        // Only storage2's entry should remain
        assert_eq!(
            *map2
                .entry(key)
                .or_insert(Box::new(0u64))
                .get()
                .downcast_ref::<u64>()
                .unwrap(),
            200
        );
    }

    #[test]
    fn test_concurrent_access_different_locals() {
        static LOCAL1: ActorLocal<u64> = ActorLocal::new();
        static LOCAL2: ActorLocal<u64> = ActorLocal::new();

        let storage = ActorLocalStorage::new();
        let map = storage.map();

        // Set up entries via entry API
        map.entry(LOCAL1.key()).or_insert(Box::new(1u64));
        map.entry(LOCAL2.key()).or_insert(Box::new(2u64));

        // Verify correct values
        assert_eq!(
            *map.entry(LOCAL1.key())
                .or_insert(Box::new(0u64))
                .get()
                .downcast_ref::<u64>()
                .unwrap(),
            1
        );
        assert_eq!(
            *map.entry(LOCAL2.key())
                .or_insert(Box::new(0u64))
                .get()
                .downcast_ref::<u64>()
                .unwrap(),
            2
        );
    }

    #[test]
    fn test_actor_local_clone_shares_key() {
        let local1: ActorLocal<u64> = ActorLocal::new();
        // Initialize the key before cloning
        let _ = local1.key();
        let local2 = local1.clone();

        let storage = ActorLocalStorage::new();
        let map = storage.map();

        // Insert via local1's key
        map.entry(local1.key()).or_insert(Box::new(42u64));

        // Should be visible via local2's key (same key)
        assert_eq!(
            *map.entry(local2.key())
                .or_insert(Box::new(0u64))
                .get()
                .downcast_ref::<u64>()
                .unwrap(),
            42
        );
    }

    #[test]
    fn test_entry_fluent_api() {
        let local: ActorLocal<u64> = ActorLocal::new();
        let storage = ActorLocalStorage::new();
        let map = storage.map();
        let key = local.key();

        // Test or_default via Entry
        let entry = Entry::<u64>::new(map.entry(key));
        *entry.or_default().get_mut() = 42;

        let entry = Entry::<u64>::new(map.entry(key));
        assert_eq!(*entry.or_default().get(), 42);

        // Test and_modify
        let entry = Entry::<u64>::new(map.entry(key));
        entry.and_modify(|v| *v += 10).or_default();

        let entry = Entry::<u64>::new(map.entry(key));
        assert_eq!(*entry.or_default().get(), 52);
    }

    #[test]
    fn test_entry_pattern_matching() {
        let local: ActorLocal<u64> = ActorLocal::new();
        let storage = ActorLocalStorage::new();
        let map = storage.map();
        let key = local.key();

        // Initially vacant
        let entry = Entry::<u64>::new(map.entry(key));
        match entry {
            Entry::Vacant(v) => {
                v.insert(100);
            }
            Entry::Occupied(_) => panic!("expected vacant"),
        }

        // Now occupied
        let entry = Entry::<u64>::new(map.entry(key));
        match entry {
            Entry::Occupied(mut o) => {
                assert_eq!(*o.get(), 100);
                *o.get_mut() = 200;
            }
            Entry::Vacant(_) => panic!("expected occupied"),
        }

        // Verify modification
        let entry = Entry::<u64>::new(map.entry(key));
        assert_eq!(*entry.or_default().get(), 200);

        // Test remove
        let entry = Entry::<u64>::new(map.entry(key));
        match entry {
            Entry::Occupied(o) => {
                assert_eq!(o.remove(), 200);
            }
            Entry::Vacant(_) => panic!("expected occupied"),
        }

        // Should be vacant again
        let entry = Entry::<u64>::new(map.entry(key));
        assert!(matches!(entry, Entry::Vacant(_)));
    }

    #[test]
    fn test_occupied_entry_insert() {
        let local: ActorLocal<String> = ActorLocal::new();
        let storage = ActorLocalStorage::new();
        let map = storage.map();
        let key = local.key();

        // Insert initial value
        let entry = Entry::<String>::new(map.entry(key));
        entry.or_insert("hello".to_string());

        // Replace via insert and get old value
        let entry = Entry::<String>::new(map.entry(key));
        match entry {
            Entry::Occupied(mut o) => {
                let old = o.insert("world".to_string());
                assert_eq!(old, "hello");
            }
            Entry::Vacant(_) => panic!("expected occupied"),
        }

        // Verify new value
        let entry = Entry::<String>::new(map.entry(key));
        assert_eq!(*entry.or_insert("".to_string()).get(), "world");
    }
}
