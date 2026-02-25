/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use monarch_hyperactor as _; // Avoid "unused depdency" lint.

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

    // Initialize the embedded Python interpreter before any actor
    // code runs. Some per-proc actors (e.g. LoggerRuntimeActor) call
    // into Python during `new()`. If Python isn't initialized yet,
    // PyO3 will panic ("The Python interpreter is not initialized").
    pyo3::Python::initialize();

    // Enter the hyperactor-mesh bootstrap protocol.
    hyperactor_mesh::bootstrap_or_die().await;
}
