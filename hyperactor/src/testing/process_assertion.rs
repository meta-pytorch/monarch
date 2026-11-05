/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::process::Command;

const CHILD_TEST: &str = "HYPERACTOR_TERMINATION_TEST_CHILD";

/// Re-run the current libtest test in a fresh process and verify its exit code.
///
/// The child executes `f` with its own Tokio runtime. Forking an existing runtime
/// would inherit invalid I/O driver state on macOS. Call this once per test;
/// setup before this call is repeated in the child process.
/// Returning from `f` instead of terminating the process fails the assertion.
pub async fn assert_termination<F, Fut>(f: F, expected_code: i32) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    let thread = std::thread::current();
    let test_name = thread
        .name()
        .ok_or_else(|| anyhow::anyhow!("termination assertion requires a named libtest thread"))?;
    if std::env::var(CHILD_TEST).ok().as_deref() == Some(test_name) {
        f().await;
        eprintln!("termination callback returned without exiting");
        std::process::exit(if expected_code == 0 { 1 } else { 0 });
    }

    let status = Command::new(std::env::current_exe()?)
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_TEST, test_name)
        .status()?;
    anyhow::ensure!(
        status.code() == Some(expected_code),
        "expected exit code {expected_code}, got {status}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_return_without_termination_is_rejected() {
        let result = assert_termination(|| async {}, 0).await;
        if std::env::var(CHILD_TEST).is_err() {
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn test_explicit_zero_exit_succeeds() {
        assert_termination(|| async { std::process::exit(0) }, 0)
            .await
            .unwrap();
    }
}
