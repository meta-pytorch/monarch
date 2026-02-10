/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;

use nix::sys::wait::WaitStatus;
use nix::sys::wait::waitpid;
use nix::unistd::ForkResult;
use nix::unistd::fork;

/// Fork a child process, execute the given function in that process, and verify
/// that the process exits with the given exit code.
pub async fn assert_termination<F, Fut>(f: F, expected_code: i32) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    // SAFETY: for unit test process assertion.
    unsafe {
        match fork() {
            Ok(ForkResult::Parent { child, .. }) => match waitpid(child, None)? {
                WaitStatus::Exited(_, exit_code) => {
                    anyhow::ensure!(exit_code == expected_code);
                    Ok(())
                }
                status => Err(anyhow::anyhow!(
                    "didn't receive expected status, got: {:?}",
                    status
                )),
            },
            Ok(ForkResult::Child) => {
                let _: () = f().await;
                Ok(())
            }
            Err(_) => Err(anyhow::anyhow!("fork failed")),
        }
    }
}
