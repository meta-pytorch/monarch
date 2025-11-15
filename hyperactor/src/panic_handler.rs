/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Used to capture the backtrace from panic and store it in a task_local, so
//! that it can be retrieved later when the panic is catched.

use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::future::Future;
use std::ops::Deref;
use std::panic;

/// A struct to store the message and backtrace from a panic.
#[derive(Clone)]
pub struct PanicInfo {
    /// The message from the panic.
    pub message: String,
    /// The backtrace from the panic.
    pub backtrace: String,
}

tokio::task_local! {
    /// A task_local variable to store the backtrace from a panic, so it can be
    /// retrieved later.
    static BACKTRACE: RefCell<Option<PanicInfo>>;
}

/// Call this from the main method of your application, and use it in conjunction
/// with [[with_backtrace_tracking]] and [[take_panic_backtrace]], in order to
/// capture the backtrace from a panic.
pub fn set_panic_hook() {
    panic::update_hook(move |prev, info| {
        // Ignore AccessError, which would happen if panic occurred outside of
        // BACKTRACE's scope.
        let backtrace = Backtrace::force_capture();
        let loc = info.location().map_or_else(
            || "unavailable".to_owned(),
            |loc: &panic::Location<'_>| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()),
        );
        // Extract the panic message from the payload
        let panic_msg = if let Some(&s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "panic message cannot be downcasted".to_string()
        };

        tracing::error!("stacktrace"=%backtrace, "panic at {loc}: {panic_msg}");

        let _result = BACKTRACE.try_with(|entry| match entry.try_borrow_mut() {
            Ok(mut entry_ref) => {
                *entry_ref = Some(PanicInfo {
                    message: panic_msg,
                    backtrace: format!("panicked at {loc}\n{backtrace}"),
                });
            }
            Err(borrow_mut_error) => {
                eprintln!(
                    "failed to store backtrace to task_local: {:?}",
                    borrow_mut_error
                );
            }
        });

        // Execute the default hood to preserve the default behavior.
        prev(info);
    });
}

/// Set a task_local variable for this future f, so any panic occurred in f can
/// be stored and retrieved later.
pub async fn with_backtrace_tracking<F>(f: F) -> F::Output
where
    F: Future,
{
    BACKTRACE.scope(RefCell::new(None), f).await
}

/// Take the backtrace from the task_local variable, and reset the task_local to
/// None. Return error if the backtrace is not stored, or cannot be retrieved.
pub fn take_panic_backtrace() -> Result<PanicInfo, anyhow::Error> {
    BACKTRACE.try_with(|entry| {
        entry.try_borrow_mut().map(|mut entry_ref| {
            let result = match entry_ref.deref() {
                Some(bt) => Ok(bt.clone()),
                None => Err(anyhow::anyhow!("nothing is stored in task_local")),
            };
            // Clear the task_local because the backtrace has been retrieve.
            if result.is_ok() {
                *entry_ref = None;
            }
            result
        })
    })??
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    async fn execute_panic() {
        let result = async {
            panic!("boom!");
        }
        .catch_unwind()
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_with_tracking() {
        set_panic_hook();
        with_backtrace_tracking(async {
            execute_panic().await;
            // Verify backtrace can be taken successfully.
            assert!(take_panic_backtrace().is_ok());
            // Cannot take backtrace again because task_local is reset in the
            // previous take.
            assert!(take_panic_backtrace().is_err());
        })
        .await;

        // Cannot get backtrace because this is out of the set task_local's
        // scope.
        assert!(take_panic_backtrace().is_err());
    }

    #[tokio::test]
    async fn test_without_tracking() {
        set_panic_hook();
        async {
            execute_panic().await;
            // Cannot get backtrace because task_local is not set.
            assert!(take_panic_backtrace().is_err());
        }
        .await;
    }

    #[tokio::test]
    async fn test_without_init() {
        // set_panic_hook() was not called.
        with_backtrace_tracking(async {
            execute_panic().await;
            // Cannot get backtrace because the custom panic hook is not set.
            assert!(take_panic_backtrace().is_err());
        })
        .await;
    }

    #[tokio::test]
    async fn test_nested_tasks() {
        async fn verify_inner_panic(backtrace_captured: bool) {
            let result = async {
                panic!("wow!");
            }
            .catch_unwind()
            .await;
            assert!(result.is_err());
            if backtrace_captured {
                let info = take_panic_backtrace().unwrap();
                assert_eq!(info.message, "wow!");
                assert!(info.backtrace.contains("verify_inner_panic"));
            } else {
                assert!(take_panic_backtrace().is_err());
            }
        }

        set_panic_hook();
        with_backtrace_tracking(async {
            execute_panic().await;
            // Execute a nested task without tracking, and verify it cannot get backtrace.
            let result = tokio::task::spawn(async {
                verify_inner_panic(false).await;
            })
            .await;
            assert!(result.is_ok());

            // Execute a nested task with tracking, and verify it can get its own backtrace.
            let result =
                tokio::task::spawn(with_backtrace_tracking(verify_inner_panic(true))).await;
            assert!(result.is_ok());

            // Verify the outer task can get its own backtrace.
            let info = take_panic_backtrace().unwrap();
            assert_eq!(info.message, "boom!");
            assert!(info.backtrace.contains("test_nested_tasks"));
        })
        .await;
    }
}
