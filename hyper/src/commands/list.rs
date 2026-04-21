/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::channel::ChannelAddr;
use hyperactor::host::SERVICE_PROC_NAME;
use hyperactor::reference;
use hyperactor_mesh::context;
use hyperactor_mesh::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
use hyperactor_mesh::host_mesh::host_agent::HostAgent;
use hyperactor_mesh::resource::ListClient;

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

        let cx = context().await;
        let client = cx.actor_instance;

        // Codify obtaining a proc's agent in `hyperactor_mesh` somewhere.
        let agent: reference::ActorRef<HostAgent> = reference::ActorRef::attest(
            reference::ProcId::with_name(host, SERVICE_PROC_NAME)
                .actor_id(HOST_MESH_AGENT_ACTOR_NAME, 0),
        );

        let resources = agent.list(&client).await?;
        println!("{}", serde_json::to_string_pretty(&resources)?);

        Ok(())
    }
}
