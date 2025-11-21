/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future;

use hyperactor::host::ProcessProcManager;

/// This is a bootstrap process to test `hyperactor::host::ProcessProcManager`.
/// It just boots a proc with `hyperactor::host::testing::EchoActor`.
#[tokio::main]
async fn main() {
    hyperactor_telemetry::initialize_logging(hyperactor::clock::ClockKind::default());

    let proc =
        ProcessProcManager::<hyperactor::host::testing::EchoActor>::boot_proc(|proc| async move {
            proc.spawn("echo", hyperactor::host::testing::EchoActor)
        })
        .await
        .unwrap();

    tracing::info!("booted proc {}", proc.proc_id());

    future::pending::<()>().await;
    unreachable!();
}
