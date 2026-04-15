/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::host::SERVICE_PROC_NAME;
use hyperactor::reference;
use hyperactor_mesh::context;
use hyperactor_mesh::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
use hyperactor_mesh::host_mesh::host_agent::HostAgent;
use hyperactor_mesh::resource::GetStateClient;

#[derive(clap::Args, Debug)]
pub struct ShowCommand {
    /// The string repsentation of what we want to show, such as world, proc,
    /// actor, etc.
    reference: reference::Reference,
}

impl ShowCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        match self.reference {
            reference::Reference::Proc(proc_id) => {
                let host = proc_id.addr().clone();
                let proc = proc_id.id().to_string();
                let cx = context().await;
                let client = cx.actor_instance;

                // Codify obtaining a proc's agent in `hyperactor_mesh` somewhere.
                let agent: reference::ActorRef<HostAgent> = reference::ActorRef::attest(
                    reference::ProcId::with_name(host, SERVICE_PROC_NAME)
                        .actor_id(HOST_MESH_AGENT_ACTOR_NAME),
                );

                let state = agent.get_state(&client, proc.parse().unwrap()).await?;
                println!("{}", serde_json::to_string_pretty(&state)?);
            }

            ref_ => {
                anyhow::bail!(
                    "cannot show reference {}: unsupported reference kind '{}'",
                    ref_,
                    ref_.kind()
                );
            }
        }

        Ok(())
    }
}
