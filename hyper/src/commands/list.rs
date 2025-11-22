/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::ActorRef;
use hyperactor::channel::ChannelAddr;
use hyperactor::reference::ProcId;
use hyperactor_mesh::proc_mesh::global_root_client;
use hyperactor_mesh::resource::ListClient;
use hyperactor_mesh::v1::host_mesh::mesh_agent::HostMeshAgent;

#[derive(clap::Args, Debug)]
pub struct ListCommand {
    /// The reference to the resource to list.
    /// TODO: this is a temporary workaround:  
    /// formalize parsing host refs, etc.
    reference: String,
}

impl ListCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        let host: ChannelAddr = self.reference.parse().map_err(|e| {
            anyhow::anyhow!(
                "could not parse '{}' as a host reference: {}",
                self.reference,
                e
            )
        })?;

        let client = global_root_client();

        // Codify obtaining a proc's agent in `hyperactor_mesh` somewhere.
        let agent: ActorRef<HostMeshAgent> =
            ActorRef::attest(ProcId::Direct(host, "service".to_string()).actor_id("agent", 0));

        let resources = agent.list(&client).await?;
        println!("{}", serde_json::to_string_pretty(&resources)?);

        Ok(())
    }
}
