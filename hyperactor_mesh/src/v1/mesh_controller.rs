/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt::Debug;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Instance;
use hyperactor::ProcId;
use hyperactor::actor::ActorError;
use hyperactor::actor::Referable;
use ndslice::ViewExt;
use ndslice::view::Ranked;

use crate::v1::actor_mesh::ActorMeshRef;
use crate::v1::host_mesh::HostMeshRef;
use crate::v1::proc_mesh::ProcMeshRef;

#[hyperactor::export(spawn = false)]
pub(crate) struct ActorMeshController<A>
where
    A: Referable,
{
    mesh: ActorMeshRef<A>,
}

impl<A: Referable> Debug for ActorMeshController<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshController")
            .field("mesh", &self.mesh)
            .finish()
    }
}

#[async_trait]
impl<A: Referable> Actor for ActorMeshController<A> {
    type Params = ActorMeshRef<A>;
    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        Ok(Self { mesh: params })
    }

    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        // Cannot use "ActorMesh::stop" as it's only defined on ActorMesh, not ActorMeshRef.
        self.mesh
            .proc_mesh()
            .stop_actor_by_name(this, self.mesh.name().clone())
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
#[hyperactor::export(spawn = true)]
pub(crate) struct ProcMeshController {
    mesh: ProcMeshRef,
}

#[async_trait]
impl Actor for ProcMeshController {
    type Params = ProcMeshRef;
    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        Ok(Self { mesh: params })
    }

    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        // Cannot use "ProcMesh::stop" as it's only defined on ProcMesh, not ProcMeshRef.
        let names = self.mesh.proc_ids().collect::<Vec<ProcId>>();
        let region = self.mesh.region().clone();
        if let Some(hosts) = self.mesh.hosts() {
            hosts
                .stop_proc_mesh(this, self.mesh.name(), names, region)
                .await
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
#[hyperactor::export(spawn = true)]
pub(crate) struct HostMeshController {
    mesh: HostMeshRef,
}

#[async_trait]
impl Actor for HostMeshController {
    type Params = HostMeshRef;
    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        Ok(Self { mesh: params })
    }

    async fn cleanup(
        &mut self,
        this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        // Cannot use "HostMesh::shutdown" as it's only defined on HostMesh, not HostMeshRef.
        for host in self.mesh.values() {
            if let Err(e) = host.shutdown(this).await {
                tracing::warn!(host = %host, error = %e, "host shutdown failed");
            }
        }
        Ok(())
    }
}
