/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Named trait for types with globally unique type URIs.

use std::any::TypeId;
use std::collections::HashMap;

// Re-export cityhasher for use in the derive macro
pub use cityhasher;
// Re-export dashmap so that the intern_typename macro can use $crate::dashmap
pub use dashmap;
// Re-export the Named derive macro from typeuri_macros
pub use typeuri_macros::Named;

/// Actor handler port should have its most significant bit set to 1.
pub static ACTOR_PORT_BIT: u64 = 1 << 63;

/// A [`Named`] type is a type that has a globally unique name.
pub trait Named: Sized + 'static {
    /// The globally unique type name for the type.
    /// This should typically be the fully qualified Rust name of the type.
    fn typename() -> &'static str;

    /// A globally unique hash for this type.
    /// TODO: actually enforce perfect hashing
    fn typehash() -> u64 {
        // The `Named` macro overrides this implementation with one that
        // memoizes the hash.
        cityhasher::hash(Self::typename())
    }

    /// The TypeId for this type. TypeIds are unique only within a binary,
    /// and should not be used for global identification.
    fn typeid() -> TypeId {
        TypeId::of::<Self>()
    }

    /// The globally unique port for this type. Typed ports are in the range
    /// of 1<<63..1<<64-1.
    fn port() -> u64 {
        Self::typehash() | ACTOR_PORT_BIT
    }

    /// If the named type is an enum, this returns the name of the arm
    /// of the value self.
    fn arm(&self) -> Option<&'static str> {
        None
    }

    /// An unsafe version of 'arm', accepting a pointer to the value,
    /// for use in type-erased settings.
    unsafe fn arm_unchecked(self_: *const ()) -> Option<&'static str> {
        // SAFETY: This isn't safe. We're passing it on.
        unsafe { &*(self_ as *const Self) }.arm()
    }
}

macro_rules! impl_basic {
    ($t:ty) => {
        impl Named for $t {
            fn typename() -> &'static str {
                stringify!($t)
            }
        }
    };
}

impl_basic!(());
impl_basic!(bool);
impl_basic!(i8);
impl_basic!(u8);
impl_basic!(i16);
impl_basic!(u16);
impl_basic!(i32);
impl_basic!(u32);
impl_basic!(i64);
impl_basic!(u64);
impl_basic!(i128);
impl_basic!(u128);
impl_basic!(isize);
impl_basic!(usize);
impl_basic!(f32);
impl_basic!(f64);
impl_basic!(String);
impl_basic!(std::net::IpAddr);
impl_basic!(std::net::Ipv4Addr);
impl_basic!(std::net::Ipv6Addr);
impl_basic!(std::time::Duration);
impl_basic!(std::time::SystemTime);
impl_basic!(bytes::Bytes);

impl Named for &'static str {
    fn typename() -> &'static str {
        "&str"
    }
}

// A macro that implements type-keyed interning of typenames. This is useful
// for implementing [`Named`] for generic types.
#[doc(hidden)] // not part of the public API
#[macro_export]
macro_rules! intern_typename {
    ($key:ty, $format_string:expr, $($args:ty),+) => {
        {
            static CACHE: std::sync::LazyLock<$crate::dashmap::DashMap<std::any::TypeId, &'static str>> =
              std::sync::LazyLock::new($crate::dashmap::DashMap::new);

            // Don't use entry, because typename() might re-enter intern_typename
            // for nested types like Option<Option<T>>
            let typeid = std::any::TypeId::of::<$key>();
            if let Some(value) = CACHE.get(&typeid) {
                *value
            } else {
                let typename = format!($format_string, $(<$args>::typename()),+).leak();
                CACHE.insert(typeid, typename);
                typename
            }
        }
    };
}

macro_rules! tuple_format_string {
    ($a:ident,) => { "{}" };
    ($a:ident, $($rest_a:ident,)+) => { concat!("{}, ", tuple_format_string!($($rest_a,)+)) };
}

macro_rules! impl_tuple_peel {
    ($name:ident, $($other:ident,)*) => (impl_tuple! { $($other,)* })
}

macro_rules! impl_tuple {
    () => ();
    ( $($name:ident,)+ ) => (
        impl<$($name:Named + 'static),+> Named for ($($name,)+) {
            fn typename() -> &'static str {
                intern_typename!(Self, concat!("(", tuple_format_string!($($name,)+), ")"), $($name),+)
            }
        }
        impl_tuple_peel! { $($name,)+ }
    )
}

impl_tuple! { E, D, C, B, A, Z, Y, X, W, V, U, T, }

impl<T: Named + 'static> Named for Option<T> {
    fn typename() -> &'static str {
        intern_typename!(Self, "Option<{}>", T)
    }
}

impl<T: Named + 'static> Named for Vec<T> {
    fn typename() -> &'static str {
        intern_typename!(Self, "Vec<{}>", T)
    }
}

impl<K: Named + 'static, V: Named + 'static> Named for HashMap<K, V> {
    fn typename() -> &'static str {
        intern_typename!(Self, "HashMap<{}, {}>", K, V)
    }
}

impl<T: Named + 'static, E: Named + 'static> Named for Result<T, E> {
    fn typename() -> &'static str {
        intern_typename!(Self, "Result<{}, {}>", T, E)
    }
}

impl<T: Named + 'static> Named for std::ops::Range<T> {
    fn typename() -> &'static str {
        intern_typename!(Self, "std::ops::Range<{}>", T)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_names() {
        assert_eq!(String::typename(), "String");
        assert_eq!(Option::<String>::typename(), "Option<String>");
        assert_eq!(Vec::<String>::typename(), "Vec<String>");
        assert_eq!(Vec::<Vec::<String>>::typename(), "Vec<Vec<String>>");
        assert_eq!(
            Vec::<Vec::<Vec::<String>>>::typename(),
            "Vec<Vec<Vec<String>>>"
        );
        assert_eq!(
            <(u64, String, Option::<isize>)>::typename(),
            "(u64, String, Option<isize>)"
        );
    }

    #[test]
    fn test_ports() {
        assert_eq!(String::typehash(), 3947244799002047352u64);
        assert_eq!(String::port(), 13170616835856823160u64);
        assert_ne!(
            Vec::<Vec::<Vec::<String>>>::typehash(),
            Vec::<Vec::<Vec::<Vec::<String>>>>::typehash(),
        );
    }
}
