/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This crate defines traits for diffing and patching Rust structs,
//! implements these traits for common types, and provides macros for
//! deriving them on structs.

pub mod watch;

use std::collections::HashMap;
use std::collections::hash_map;
use std::hash::Hash;

pub use struct_diff_patch_macros::Diff;
pub use struct_diff_patch_macros::Patch;

/// Represents a patch operating targeting values of type `T`.
pub trait Patch<T> {
    /// Apply this patch to the provided value, consuming the patch.
    fn apply(self, value: &mut T);
}

/// Implements the "diff" operation, which produces a patch given
/// two instances of the same type.
pub trait Diff: Sized {
    /// The type of patch produced by this diff operation.
    type Patch: Patch<Self>;

    /// Implements the "diff" operation, which produces a patch given
    /// two instances of the same type. Specifically, when the returned
    /// patch is applied to the original value, it should produce the
    /// second value.
    fn diff(&self, other: &Self) -> Self::Patch;
}

impl<T> Patch<T> for Option<T> {
    fn apply(self, value: &mut T) {
        if let Some(new) = self {
            *value = new;
        }
    }
}

impl<P, T> Patch<Vec<T>> for Vec<P>
where
    P: Patch<T>,
    T: Default,
{
    fn apply(self, value: &mut Vec<T>) {
        value.truncate(self.len());
        for (idx, patch) in self.into_iter().enumerate() {
            if idx < value.len() {
                patch.apply(&mut value[idx]);
            } else {
                value.push(T::default());
                patch.apply(&mut value[idx]);
            }
        }
    }
}

impl<T: Diff + Clone + Default> Diff for Vec<T>
where
    T::Patch: From<T>,
{
    type Patch = Vec<T::Patch>;

    fn diff(&self, other: &Self) -> Self::Patch {
        // Don't try to be clever here (e.g., using some kind of edit algorithm);
        // rather optimize for in-place edits, or just replace.
        //
        // Possibly we should also include prepend/append operations.
        let mut patch = Vec::with_capacity(other.len());
        for (idx, value) in other.iter().enumerate() {
            if idx < self.len() {
                patch.push(self[idx].diff(value));
            } else {
                patch.push(value.clone().into());
            }
        }
        patch
    }
}

/// Vector of key edits. `None` denotes a key to be removed.
pub type HashMapPatch<K, P> = Vec<(K, Option<P>)>;

impl<K, V, P> Patch<HashMap<K, V>> for HashMapPatch<K, P>
where
    K: Eq + Hash,
    V: Default,
    P: Patch<V>,
{
    fn apply(self, value: &mut HashMap<K, V>) {
        for (key, patch) in self {
            match patch {
                Some(patch) => match value.entry(key) {
                    hash_map::Entry::Occupied(mut entry) => {
                        patch.apply(entry.get_mut());
                    }
                    hash_map::Entry::Vacant(entry) => {
                        let mut v = V::default();
                        patch.apply(&mut v);
                        entry.insert(v);
                    }
                },
                None => {
                    value.remove(&key);
                }
            }
        }
    }
}

impl<K, V> Diff for HashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Diff + Clone + Default,
    V::Patch: From<V>,
{
    type Patch = HashMapPatch<K, V::Patch>;

    fn diff(&self, other: &Self) -> Self::Patch {
        let mut changes = Vec::new();

        for (key, new_value) in other.iter() {
            match self.get(key) {
                Some(value) => {
                    changes.push((key.clone(), Some(value.diff(new_value))));
                }
                None => {
                    changes.push((key.clone(), Some(new_value.clone().into())));
                }
            }
        }

        for key in self.keys() {
            if !other.contains_key(key) {
                changes.push((key.clone(), None));
            }
        }

        changes
    }
}

#[macro_export]
macro_rules! impl_simple_diff {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl $crate::Diff for $ty {
                type Patch = Option<$ty>;

                fn diff(&self, other: &Self) -> Self::Patch {
                    if self == other {
                        None
                    } else {
                        Some(other.clone())
                    }
                }
            }
        )+
    };
}

impl_simple_diff!(
    (),
    bool,
    char,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    f32,
    f64,
    String
);

#[macro_export]
macro_rules! impl_tuple_diff_patch {
    ($($idx:tt),+ $(,)?) => {
        ::paste::paste! {
            impl<$( [<P$idx>], [<V$idx>] ),+> $crate::Patch<($( [<V$idx>], )+)> for ($( [<P$idx>], )+)
            where
                $( [<P$idx>]: $crate::Patch<[<V$idx>]>, )+
            {
                fn apply(self, value: &mut ($( [<V$idx>], )+)) {
                    #[allow(non_snake_case)]
                    let ($( [<patch_$idx>], )+) = self;
                    $(
                        [<patch_$idx>].apply(&mut value.$idx);
                    )+
                }
            }

            impl<$( [<T$idx>]: $crate::Diff ),+> $crate::Diff for ($( [<T$idx>], )+) {
                type Patch = ($( <[<T$idx>] as $crate::Diff>::Patch, )+);

                fn diff(&self, other: &Self) -> Self::Patch {
                    (
                        $( self.$idx.diff(&other.$idx), )+
                    )
                }
            }
        }
    };
}

impl_tuple_diff_patch!(0);
impl_tuple_diff_patch!(0, 1);
impl_tuple_diff_patch!(0, 1, 2);
impl_tuple_diff_patch!(0, 1, 2, 3);
impl_tuple_diff_patch!(0, 1, 2, 3, 4);
impl_tuple_diff_patch!(0, 1, 2, 3, 4, 5);
impl_tuple_diff_patch!(0, 1, 2, 3, 4, 5, 6);
impl_tuple_diff_patch!(0, 1, 2, 3, 4, 5, 6, 7);
impl_tuple_diff_patch!(0, 1, 2, 3, 4, 5, 6, 7, 8);
impl_tuple_diff_patch!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
impl_tuple_diff_patch!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
impl_tuple_diff_patch!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);

#[cfg(test)]
mod tests {
    use super::*;
    use crate as struct_diff_patch; // for macros

    #[derive(Debug, Clone, PartialEq, Diff, Patch)]
    struct DerivedStruct {
        name: String,
        count: u32,
    }

    #[derive(Debug, Clone, PartialEq, Diff, Patch)]
    struct DerivedTuple(String, bool);

    #[derive(Debug, Clone, PartialEq, Diff, Patch)]
    struct DerivedUnit;

    #[test]
    fn bool_diff_is_none_when_equal() {
        assert_eq!(false.diff(&false), None);
    }

    #[test]
    fn string_diff_and_apply_replace_value() {
        let patch = String::from("bar").diff(&String::from("baz"));
        let mut value = String::from("bar");
        patch.apply(&mut value);
        assert_eq!(value, "baz");
    }

    #[test]
    fn tuple_diff_tracks_each_field() {
        let original = (false, String::from("foo"));
        let target = (true, String::from("bar"));
        let patch = original.diff(&target);

        let mut working = original;
        patch.apply(&mut working);
        assert_eq!(working, target);
    }

    #[test]
    fn vec_patch() {
        let mut orig = vec![1, 2, 3, 4, 5];
        let target = vec![1, 20, 3, 40, 5];

        let patch = orig.diff(&target);
        assert_eq!(patch, vec![None, Some(20), None, Some(40), None]);

        patch.apply(&mut orig);
        assert_eq!(orig, target);
    }

    #[test]
    fn hashmap_diff_patch_handles_insert_update_and_remove() {
        use std::collections::HashMap;

        let mut original = HashMap::new();
        original.insert("keep".to_string(), 1_u32);
        original.insert("remove".to_string(), 2_u32);

        let mut target = HashMap::new();
        target.insert("keep".to_string(), 10);
        target.insert("insert".to_string(), 3);

        let patch = original.diff(&target);

        let mut saw_insert = false;
        let mut saw_update = false;
        let mut saw_remove = false;

        for (key, change) in patch.iter() {
            match (key.as_str(), change) {
                ("insert", Some(Some(3))) => saw_insert = true,
                ("keep", Some(Some(10))) => saw_update = true,
                ("remove", None) => saw_remove = true,
                _ => {}
            }
        }

        assert!(saw_insert);
        assert!(saw_update);
        assert!(saw_remove);

        let mut working = original;
        patch.apply(&mut working);
        assert_eq!(working, target);
    }

    #[test]
    fn derive_macro_generates_struct_and_patch_impls() {
        let mut original = DerivedStruct {
            name: "foo".into(),
            count: 1,
        };
        let target = DerivedStruct {
            name: "bar".into(),
            count: 2,
        };

        let patch = original.diff(&target);
        patch.apply(&mut original);
        assert_eq!(original, target);

        let tuple_patch = DerivedTuple("foo".into(), true).diff(&DerivedTuple("baz".into(), false));
        let mut tuple_value = DerivedTuple("foo".into(), true);
        tuple_patch.apply(&mut tuple_value);
        assert_eq!(tuple_value, DerivedTuple("baz".into(), false));

        let mut unit = DerivedUnit;
        let unit_patch = unit.diff(&DerivedUnit);
        unit_patch.apply(&mut unit);
    }
}
