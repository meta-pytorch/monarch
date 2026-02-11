/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::ActorRef;
use hyperactor::reference::ProcId;
use hyperactor::reference::Reference;
use hyperactor_mesh::global_root_client;
use hyperactor_mesh::host_mesh::mesh_agent::HostMeshAgent;
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
                let client = global_root_client();

                // Codify obtaining a proc's agent in `hyperactor_mesh` somewhere.
                let agent: ActorRef<HostMeshAgent> =
                    ActorRef::attest(ProcId(host, "service".to_string()).actor_id("agent", 0));

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
