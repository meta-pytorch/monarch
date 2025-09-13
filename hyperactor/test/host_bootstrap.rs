/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future;

/// This is an "empty shell" bootstrap process,
/// simply invoking [`hyperactor_mesh::bootstrap_or_die`].
#[tokio::main]
async fn main() {
    hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

    let proc = hyperactor::host::ProcessProcManager::boot_proc::<
        hyperactor::host::testing::EchoActor,
    >("echo", ())
    .await
    .unwrap();

    tracing::info!("booted proc {}", proc.proc_id());

    future::pending::<()>().await;
    unreachable!();
}
