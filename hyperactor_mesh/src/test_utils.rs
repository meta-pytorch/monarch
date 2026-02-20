/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Unbind;
use hyperactor::channel::ChannelTransport;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::host_mesh::HostMesh;

/// Message that can be sent to an EmptyActor.
#[derive(Serialize, Deserialize, Debug, Named, Clone, Bind, Unbind)]
pub struct EmptyMessage();

#[derive(Debug, PartialEq, Default)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        EmptyMessage { cast = true },
    ],
)]
pub struct EmptyActor();

impl Actor for EmptyActor {}

#[async_trait]
impl Handler<EmptyMessage> for EmptyActor {
    async fn handle(&mut self, _: &Context<Self>, _: EmptyMessage) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// Create a local in-process host mesh with `n` hosts, all running in
/// the current process using `Local` channel transport.
///
/// This is similar to [`HostMesh::local_in_process`] but supports
/// multiple hosts. All hosts use [`LocalProcManager`] with
/// [`ChannelTransport::Local`], so there is no IPC overhead.
///
/// # Examples
///
/// ```ignore
/// let mut host_mesh = test_utils::local_host_mesh(4).await;
/// let proc_mesh = host_mesh
///     .spawn(instance, "test", ndslice::extent!(gpu = 8))
///     .await
///     .unwrap();
/// // ... do something with the proc mesh ...
/// // shutdown the host mesh.
/// let _ = host_mesh.shutdown(&instance).await;
/// ```
pub async fn local_host_mesh(n: usize) -> HostMesh {
    let addrs = (0..n).map(|_| ChannelTransport::Local.any()).collect();
    let host_mesh = HostMesh::local_n_in_process(addrs).await.unwrap();
    HostMesh::take(host_mesh)
}
