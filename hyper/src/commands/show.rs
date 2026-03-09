/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::ActorRef;
use hyperactor::host::SERVICE_PROC_NAME;
use hyperactor::reference::ProcId;
use hyperactor::reference::Reference;
use hyperactor_mesh::context;
use hyperactor_mesh::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
use hyperactor_mesh::host_mesh::host_agent::HostAgent;
use hyperactor_mesh::resource::GetStateClient;

#[derive(clap::Args, Debug)]
pub struct ShowCommand {
    /// The string repsentation of what we want to show, such as world, proc,
    /// actor, etc.
    reference: Reference,
}

impl ShowCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        match self.reference {
            Reference::Proc(proc_id) => {
                let host = proc_id.addr().clone();
                let proc = proc_id.name().to_string();
                let cx = context().await;
                let client = cx.actor_instance;

                // Codify obtaining a proc's agent in `hyperactor_mesh` somewhere.
                let agent: ActorRef<HostAgent> = ActorRef::attest(
                    ProcId(host, SERVICE_PROC_NAME.to_string())
                        .actor_id(HOST_MESH_AGENT_ACTOR_NAME, 0),
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
