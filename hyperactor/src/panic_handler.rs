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
use std::panic;

/// A struct to store the message and backtrace from a panic.
pub(crate) struct PanicInfo {
    /// The message from the panic.
    message: String,
    /// The location where the panic occurred.
    location: Option<PanicLocation>,
    /// The backtrace from the panic.
    backtrace: Backtrace,
}

impl std::fmt::Display for PanicInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "panic at ")?;
        match &self.location {
            Some(loc) => write!(f, "{}", loc)?,
            None => write!(f, "unavailable")?,
        }
        write!(f, ": {}\n{}", self.message, self.backtrace)
    }
}

/// A struct to store location information from a panic with owned data
#[derive(Clone, Debug)]
struct PanicLocation {
    file: String,
    line: u32,
    column: u32,
}

impl From<&panic::Location<'_>> for PanicLocation {
    fn from(loc: &panic::Location<'_>) -> Self {
        Self {
            file: loc.file().to_string(),
            line: loc.line(),
            column: loc.column(),
        }
    }
}

impl std::fmt::Display for PanicLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line, self.column)
    }
}

tokio::task_local! {
    /// A task_local variable to store the backtrace from a panic, so it can be
    /// retrieved later.
    static BACKTRACE: RefCell<Option<PanicInfo>>;
}

/// Call this from the main method of your application, and use it in conjunction
/// with [[with_backtrace_tracking]] and [[take_panic_info]], in order to
/// capture the backtrace from a panic.
pub fn set_panic_hook() {
    panic::update_hook(move |prev, info| {
        let backtrace = Backtrace::force_capture();

        // Extract the panic message from the payload
        let panic_msg = if let Some(s) = info.payload_as_str() {
            s.to_string()
        } else {
            "panic message was not a string".to_string()
        };

        let location = info.location().map(PanicLocation::from);
        let loc_str = location
            .as_ref()
            .map_or_else(|| "unavailable".to_owned(), |l| l.to_string());
        tracing::error!("stacktrace"=%backtrace, "panic at {loc_str}: {panic_msg}");

        let _result = BACKTRACE.try_with(|entry| match entry.try_borrow_mut() {
            Ok(mut entry_ref) => {
                *entry_ref = Some(PanicInfo {
                    message: panic_msg,
                    location,
                    backtrace,
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
pub(crate) async fn with_backtrace_tracking<F>(f: F) -> F::Output
where
    F: Future,
{
    BACKTRACE.scope(RefCell::new(None), f).await
}

/// Take the backtrace from the task_local variable, and reset the task_local to
/// None. Return error if the backtrace is not stored, or cannot be retrieved.
pub(crate) fn take_panic_info() -> Result<PanicInfo, anyhow::Error> {
    BACKTRACE
        .try_with(|entry| {
            entry
                .try_borrow_mut()
                .map_err(|e| anyhow::anyhow!("failed to borrow task_local: {:?}", e))
                .and_then(|mut entry_ref| {
                    // Use take because we want to clear the task_local after
                    // the panic info has been retrieve.
                    entry_ref
                        .take()
                        .ok_or_else(|| anyhow::anyhow!("nothing is stored in task_local"))
                })
        })
        .map_err(|e| anyhow::anyhow!("failed to access task_local: {:?}", e))?
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
            assert!(take_panic_info().is_ok());
            // Cannot take backtrace again because task_local is reset in the
            // previous take.
            assert!(take_panic_info().is_err());
        })
        .await;

        // Cannot get backtrace because this is out of the set task_local's
        // scope.
        assert!(take_panic_info().is_err());
    }

    #[tokio::test]
    async fn test_without_tracking() {
        set_panic_hook();
        async {
            execute_panic().await;
            // Cannot get backtrace because task_local is not set.
            assert!(take_panic_info().is_err());
        }
        .await;
    }

    #[tokio::test]
    async fn test_without_init() {
        // set_panic_hook() was not called.
        with_backtrace_tracking(async {
            execute_panic().await;
            // Cannot get backtrace because the custom panic hook is not set.
            assert!(take_panic_info().is_err());
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
                let info = take_panic_info().unwrap();
                assert_eq!(info.message, "wow!");
                assert!(info.backtrace.to_string().contains("verify_inner_panic"));
            } else {
                assert!(take_panic_info().is_err());
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
            let info = take_panic_info().unwrap();
            assert_eq!(info.message, "boom!");
            assert!(info.backtrace.to_string().contains("test_nested_tasks"));
        })
        .await;
    }
}
