/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// This is an "empty shell" bootstrap process,
/// simply invoking [`hyperactor_mesh::bootstrap_or_die`].
#[tokio::main]
async fn main() {
    // This causes folly to intercept SIGTERM. When run in
    // '@fbcode//mode/dev-nosan' that translates into SEGFAULTs.
    hyperactor::initialize_with_current_runtime();
    // SAFETY: Does not derefrence pointers or rely on undefined
    // memory. No other threads are likely to be modifying it
    // concurrently.
    unsafe {
        libc::signal(libc::SIGTERM, libc::SIG_DFL);
    }

    hyperactor_mesh::bootstrap_or_die().await;
}
