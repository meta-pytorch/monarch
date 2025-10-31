/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// This program is meant as a test bed for exercising the various
/// (v1) mesh APIs.
///
/// It can also be used as the basis for benchmarks, functionality testing,
/// etc.
use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::Unbind;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor_mesh::bootstrap::BootstrapCommand;
use hyperactor_mesh::comm::multicast::CastInfo;
use hyperactor_mesh::proc_mesh::global_root_client;
use hyperactor_mesh::v1::host_mesh::HostMesh;
use ndslice::Point;
use ndslice::ViewExt;
use ndslice::extent;
use serde::Deserialize;
use serde::Serialize;

#[derive(Actor, Default, Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        TestMessage { cast = true },
    ],
)]
struct TestActor {}

#[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
enum TestMessage {
    Ping(#[binding(include)] PortRef<Point>),
}

#[async_trait]
impl Handler<TestMessage> for TestActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: TestMessage,
    ) -> Result<(), anyhow::Error> {
        match message {
            TestMessage::Ping(reply) => reply.send(cx, cx.cast_point())?,
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    hyperactor_telemetry::initialize_logging_for_test();

    let host_mesh = HostMesh::process(extent!(hosts = 8), BootstrapCommand::current().unwrap())
        .await
        .unwrap();

    let instance = global_root_client();

    let proc_mesh = host_mesh
        .spawn(instance, "test", extent!(procs = 2))
        .await
        .unwrap();

    let actor_mesh = proc_mesh
        .spawn::<TestActor>(instance, "test", &())
        .await
        .unwrap();

    loop {
        let mut received = HashSet::new();
        let (port, mut rx) = instance.open_port();
        let begin = RealClock.now();
        actor_mesh
            .cast(instance, TestMessage::Ping(port.bind()))
            .unwrap();
        while received.len() < actor_mesh.extent().num_ranks() {
            received.insert(rx.recv().await.unwrap());
        }

        eprintln!("ping {}ms", begin.elapsed().as_millis());
        RealClock.sleep(Duration::from_secs(1)).await;
    }
}
