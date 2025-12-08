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
use std::fmt::Debug;
use std::hash::Hash;

pub use struct_diff_patch_macros::Diff;
pub use struct_diff_patch_macros::Patch;

/// The common error type returned by diff and patch operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing key '{0}'")]
    MissingKey(String),

    #[error("duplicate key '{0}'")]
    DuplicateKey(String),

    #[error("missing value")]
    MissingValue,

    #[error("index out of bounds")]
    IndexOutOfBounds,
}

/// Standard Result type used by this crate.
type Result<T> = std::result::Result<T, Error>;

/// Represents a patch operating targeting values of type `T`.
pub trait Patch<T> {
    /// Apply this patch to the provided value, consuming the patch.
    fn apply(self, value: &mut T) -> Result<()>;
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

/// Option<T> patches simple values. `Some` represents setting the value;
/// `None` means no new value should be set.
impl<T> Patch<T> for Option<T> {
    fn apply(self, value: &mut T) -> Result<()> {
        if let Some(new) = self {
            *value = new;
        }
        Ok(())
    }
}

/// A patch of an option.
#[derive(Debug, Clone, PartialEq)]
pub enum OptionPatch<T: Diff> {
    /// Set a new value.
    Set(T),
    /// Patch an existing value.
    Patch(T::Patch),
    /// Clear the value (set to `None`).
    Clear,
}

impl<T: Diff> Patch<Option<T>> for OptionPatch<T> {
    fn apply(self, value: &mut Option<T>) -> Result<()> {
        match self {
            OptionPatch::Set(new) => *value = Some(new),
            OptionPatch::Patch(patch) => {
                if let Some(value) = value {
                    patch.apply(value)?;
                } else {
                    return Err(Error::MissingValue);
                }
            }
            OptionPatch::Clear => *value = None,
        }
        Ok(())
    }
}

impl<T: Diff + Clone> Diff for Option<T> {
    type Patch = OptionPatch<T>;

    fn diff(&self, other: &Self) -> Self::Patch {
        match (self, other) {
            (Some(left), Some(right)) => OptionPatch::Patch(left.diff(right)),
            (_, None) => OptionPatch::Clear,
            (None, Some(right)) => OptionPatch::Set(right.clone()),
        }
    }
}

/// Vector patches.
#[derive(Debug, Clone, PartialEq)]
pub struct VecPatch<T: Diff> {
    /// Truncate the vector to this length.
    len: usize,
    /// Patches to existing elements.
    patches: Vec<(usize, T::Patch)>,
    /// Append new elements to the vector.
    append: Vec<T>,
}

impl<T: Diff> Patch<Vec<T>> for VecPatch<T> {
    fn apply(self, vec: &mut Vec<T>) -> Result<()> {
        vec.truncate(self.len);
        for (idx, patch) in self.patches {
            if idx < vec.len() {
                patch.apply(&mut vec[idx])?;
            } else {
                return Err(Error::IndexOutOfBounds);
            }
        }

        for value in self.append {
            vec.push(value)
        }

        Ok(())
    }
}

impl<T: Diff + Clone> Diff for Vec<T> {
    type Patch = VecPatch<T>;

    fn diff(&self, other: &Self) -> Self::Patch {
        // Don't try to be clever here (e.g., using some kind of edit algorithm);
        // rather optimize for in-place edits, or just replace.
        //
        // Possibly we should also include prepend/append operations.

        let len = other.len();
        let mut patches = Vec::with_capacity(self.len().min(len));
        let mut append = Vec::with_capacity(other.len().saturating_sub(self.len()));

        for (idx, value) in other.iter().enumerate() {
            if idx < self.len() {
                // TODO: future optimization to make this sparse
                patches.push((idx, self[idx].diff(value)));
            } else {
                append.push(value.clone());
            }
        }

        VecPatch {
            len,
            patches,
            append,
        }
    }
}

/// HashMap patches.
#[derive(Debug, Clone, PartialEq)]
pub struct HashMapPatch<K, V: Diff> {
    /// Remove the following keys.
    remove: Vec<K>,
    /// Patches to existing values.
    patches: Vec<(K, V::Patch)>,
    /// Insert new key-values.
    insert: Vec<(K, V)>,
}

impl<K, V> Patch<HashMap<K, V>> for HashMapPatch<K, V>
where
    K: Debug + Eq + Hash,
    V: Diff,
{
    fn apply(self, map: &mut HashMap<K, V>) -> Result<()> {
        for key in self.remove {
            map.remove(&key);
        }

        for (key, patch) in self.patches {
            match map.entry(key) {
                hash_map::Entry::Occupied(mut entry) => {
                    patch.apply(entry.get_mut())?;
                }
                hash_map::Entry::Vacant(entry) => {
                    return Err(Error::MissingKey(format!("{:?}", entry.key())));
                }
            }
        }

        for (key, val) in self.insert {
            match map.entry(key) {
                hash_map::Entry::Occupied(entry) => {
                    return Err(Error::DuplicateKey(format!("{:?}", entry.key())));
                }
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(val);
                }
            }
        }

        Ok(())
    }
}

impl<K, V> Diff for HashMap<K, V>
where
    K: Debug + Eq + Hash + Clone,
    V: Diff + Clone,
{
    type Patch = HashMapPatch<K, V>;

    fn diff(&self, other: &Self) -> Self::Patch {
        let mut remove = Vec::new();
        let mut patches = Vec::new();
        let mut insert = Vec::new();

        for (key, new_value) in other.iter() {
            match self.get(key) {
                Some(value) => {
                    patches.push((key.clone(), value.diff(new_value)));
                }
                None => {
                    insert.push((key.clone(), new_value.clone()));
                }
            }
        }

        for key in self.keys() {
            if !other.contains_key(key) {
                remove.push(key.clone());
            }
        }

        HashMapPatch {
            remove,
            patches,
            insert,
        }
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
                fn apply(self, value: &mut ($( [<V$idx>], )+)) -> $crate::Result<()> {
                    #[allow(non_snake_case)]
                    let ($( [<patch_$idx>], )+) = self;
                    $(
                        [<patch_$idx>].apply(&mut value.$idx)?;
                    )+
                    Ok(())
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
        opt: Option<bool>,
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
        patch.apply(&mut value).unwrap();
        assert_eq!(value, "baz");
    }

    #[test]
    fn tuple_diff_tracks_each_field() {
        let original = (false, String::from("foo"));
        let target = (true, String::from("bar"));
        let patch = original.diff(&target);

        let mut working = original;
        patch.apply(&mut working).unwrap();
        assert_eq!(working, target);
    }

    #[test]
    fn vec_patch() {
        let mut orig = vec![1, 2, 3, 4, 5];
        let target = vec![1, 20, 3, 40, 5];

        let patch = orig.diff(&target);

        assert_eq!(
            patch,
            VecPatch {
                len: 5,
                patches: vec![
                    (0, None),
                    (1, Some(20)),
                    (2, None),
                    (3, Some(40)),
                    (4, None)
                ],
                append: vec![],
            }
        );

        patch.apply(&mut orig).unwrap();
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

        assert_eq!(
            patch,
            HashMapPatch {
                remove: vec!["remove".to_string()],
                patches: vec![("keep".to_string(), Some(10))],
                insert: vec![("insert".to_string(), 3)]
            }
        );

        let mut working = original;
        patch.apply(&mut working).unwrap();
        assert_eq!(working, target);
    }

    #[test]
    fn derive_macro_generates_struct_and_patch_impls() {
        let mut original = DerivedStruct {
            name: "foo".into(),
            count: 1,
            opt: None,
        };
        let target = DerivedStruct {
            name: "bar".into(),
            count: 2,
            opt: Some(true),
        };

        let patch = original.diff(&target);
        patch.apply(&mut original).unwrap();
        assert_eq!(original, target);

        let tuple_patch = DerivedTuple("foo".into(), true).diff(&DerivedTuple("baz".into(), false));
        let mut tuple_value = DerivedTuple("foo".into(), true);
        tuple_patch.apply(&mut tuple_value).unwrap();
        assert_eq!(tuple_value, DerivedTuple("baz".into(), false));

        let mut unit = DerivedUnit;
        let unit_patch = unit.diff(&DerivedUnit);
        unit_patch.apply(&mut unit).unwrap();
    }

    #[test]
    fn option_patch_error_when_patching_none() {
        let mut value: Option<u32> = None;
        let patch = OptionPatch::Patch(Some(42));

        let result = patch.apply(&mut value);
        assert!(matches!(result, Err(Error::MissingValue)));
    }

    #[test]
    fn vec_patch_error_when_index_out_of_bounds() {
        let mut vec = vec![1, 2, 3];
        let patch = VecPatch {
            len: 3,
            patches: vec![(5, Some(100))], // Index 5 is out of bounds
            append: vec![],
        };

        let result = patch.apply(&mut vec);
        assert!(matches!(result, Err(Error::IndexOutOfBounds)));
    }

    #[test]
    fn vec_patch_error_when_patching_after_truncate() {
        let mut vec = vec![1, 2, 3, 4, 5];
        let patch = VecPatch {
            len: 2,                        // Truncate to 2 elements
            patches: vec![(3, Some(100))], // Try to patch index 3 which no longer exists
            append: vec![],
        };

        let result = patch.apply(&mut vec);
        assert!(matches!(result, Err(Error::IndexOutOfBounds)));
        // Verify the vec was truncated before the error
        assert_eq!(vec.len(), 2);
    }

    #[test]
    fn hashmap_patch_error_on_missing_key() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert("exists".to_string(), 1_u32);

        let patch = HashMapPatch {
            remove: vec![],
            patches: vec![("missing".to_string(), Some(10))],
            insert: vec![],
        };

        let result = patch.apply(&mut map);
        assert!(matches!(result, Err(Error::MissingKey(_))));

        if let Err(Error::MissingKey(key)) = result {
            assert!(key.contains("missing"));
        }
    }

    #[test]
    fn hashmap_patch_error_on_duplicate_key() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert("exists".to_string(), 1_u32);

        let patch = HashMapPatch {
            remove: vec![],
            patches: vec![],
            insert: vec![("exists".to_string(), 10)],
        };

        let result = patch.apply(&mut map);
        assert!(matches!(result, Err(Error::DuplicateKey(_))));

        if let Err(Error::DuplicateKey(key)) = result {
            assert!(key.contains("exists"));
        }
    }

    #[test]
    fn hashmap_patch_partial_application_on_error() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert("key1".to_string(), 1_u32);
        map.insert("key2".to_string(), 2_u32);

        // This patch will succeed on remove, then fail on patches
        let patch = HashMapPatch {
            remove: vec!["key2".to_string()],
            patches: vec![("nonexistent".to_string(), Some(10))],
            insert: vec![],
        };

        let result = patch.apply(&mut map);
        assert!(matches!(result, Err(Error::MissingKey(_))));

        // Verify that the remove operation was applied before the error
        assert!(!map.contains_key("key2"));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn nested_option_patch_error_propagation() {
        let mut value: Option<Option<u32>> = Some(None);

        // Try to patch the inner None value
        let inner_patch = OptionPatch::Patch(Some(42));
        let outer_patch = OptionPatch::Patch(inner_patch);

        let result = outer_patch.apply(&mut value);
        assert!(matches!(result, Err(Error::MissingValue)));
    }

    #[test]
    fn vec_of_options_patch_error() {
        let mut vec: Vec<Option<u32>> = vec![Some(1), None, Some(3)];

        // Create a patch that tries to modify the None value
        let patch = VecPatch {
            len: 3,
            patches: vec![(1, OptionPatch::Patch(Some(100)))],
            append: vec![],
        };

        let result = patch.apply(&mut vec);
        assert!(matches!(result, Err(Error::MissingValue)));
    }

    #[test]
    fn hashmap_with_nested_vec_patch_error() {
        use std::collections::HashMap;

        let mut map: HashMap<String, Vec<u32>> = HashMap::new();
        map.insert("key".to_string(), vec![1, 2, 3]);

        // Create a patch with an out-of-bounds index for the nested vec
        let vec_patch = VecPatch {
            len: 3,
            patches: vec![(10, Some(100))], // Index 10 is out of bounds
            append: vec![],
        };

        let patch = HashMapPatch {
            remove: vec![],
            patches: vec![("key".to_string(), vec_patch)],
            insert: vec![],
        };

        let result = patch.apply(&mut map);
        assert!(matches!(result, Err(Error::IndexOutOfBounds)));
    }
}
