/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// This crate implements scoped lifetime-erasure.
///
/// Its intended use is when you hold a reference with a well-defined scope,
/// but you want to pass it around to APIs where threading through lifetime
/// parameters is awkward, for example across Rust/Python FFI boundaries, but
/// you can nevertheless guarantee that the reference is not used after the
/// the scope exits.
///
/// Erased references are safe to use, but incorrect usage causes the relevant
/// thread to halt, as we cannot safely drop a keepalive associated with an
/// extant reference.
///
/// The crate provides a single function: [`erase`], which returns a [`Keepalive`]
/// token, with the lifetime of the erased reference, and [`Ref`], the reference
/// itself. The user must guarantee that the reference does not outlive the the
/// keepalive token; otherwise the program halts. Because [`Keepalive`] is parameterized
/// on the reference lifetime, it is guaranteed by the compiler not to outlive it,
/// and thus transitively we can guarantee that [`Ref`] access is safe.
///
/// [`Ref`] *only* provides lifetime erasure, and can be combined with other cell
/// types to provide interior mutability, dynamic borrow checking, etc.
///
/// ## Usage
///
/// ```
/// fn outer(x: &u32) {
///     let (keepalive, ref_) = erased_lifetime::erase(x);
///     inner(ref_);
/// }
///
/// fn inner(x: erased_lifetime::Ref<u32>) {
///     assert_eq!(*x, 42);
/// }
///
/// let data = 42u32;
/// outer(&data);
/// ```
use std::backtrace::Backtrace;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Panic instead of halting. Only used for testing in this crate.
static PANIC: AtomicBool = AtomicBool::new(false);

/// Erase the lifetime `'a` from the referand. See crate documentation for a discussion
/// of usage.
pub fn erase<'a, T: ?Sized>(x: &'a T) -> (Keepalive<'a>, Ref<T>) {
    let borrowed = Arc::new(AtomicBool::new(true));
    (
        Keepalive {
            borrowed: Arc::clone(&borrowed),
            _phantom: PhantomData,
        },
        Ref {
            ptr: NonNull::from(x),
            borrowed,
        },
    )
}

/// A keepalive token for a reference returned from [`erase`].
pub struct Keepalive<'a> {
    borrowed: Arc<AtomicBool>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> fmt::Debug for Keepalive<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Keepalive")
            .field("borrowed", &self.borrowed.load(Ordering::Relaxed))
            .finish()
    }
}

impl Drop for Keepalive<'_> {
    fn drop(&mut self) {
        if self.borrowed.load(Ordering::Acquire) {
            if PANIC.load(Ordering::Relaxed) {
                panic!("keepalive dropped with extant reference");
            }
            // Here, we have to just just block the thread.
            eprintln!("keepalive dropped with extant reference; continuing would result in UB");
            eprintln!("{}", Backtrace::force_capture());
            loop {
                std::thread::park();
            }
        }
    }
}

/// A wrapper type for an erased reference.
pub struct Ref<T: ?Sized> {
    ptr: NonNull<T>,
    borrowed: Arc<AtomicBool>,
}

impl<T: ?Sized> Deref for Ref<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: The pointer is valid while `Ref` is in scope.
        unsafe { self.ptr.as_ref() }
    }
}

impl<T: ?Sized> Drop for Ref<T> {
    fn drop(&mut self) {
        self.borrowed.store(false, Ordering::Release);
    }
}

// SAFETY: Ref is Sync if T is Send by Rust rules.
unsafe impl<T: Sync> Send for Ref<T> {}
// SAFETY: Ref is Sync if T is Sync by Rust rules.
unsafe impl<T: Sync> Sync for Ref<T> {}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use super::*;

    fn assert_panics<F: FnOnce() -> R + std::panic::UnwindSafe, R>(f: F) -> String {
        PANIC.store(true, Ordering::Relaxed);
        match catch_unwind(f) {
            Ok(_) => panic!("expected panic, but function returned normally"),
            Err(e) => {
                if let Some(s) = e.downcast_ref::<&'static str>() {
                    (*s).to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "<non-string panic payload>".to_string()
                }
            }
        }
    }

    #[test]
    fn test_basic() {
        let data = Box::new(42usize);
        let x: &usize = &data;

        let (_keepalive, ref_) = erase(x);

        assert_eq!(*ref_, 42);

        // Free of panics or thread parking. Compiler will drop in this order:
        // drop(ref_);
        // drop(keepalive);
    }

    #[test]
    fn test_early_keepalive_drop() {
        let data = Box::new(42usize);
        let x: &usize = &data;

        let (keepalive, ref_) = erase(x);

        assert_eq!(*ref_, 42);

        // Drop before reference.
        let msg = assert_panics(|| {
            drop(keepalive);
        });
        assert!(
            msg.contains("keepalive dropped with extant reference"),
            "{}",
            msg
        );
    }
}
