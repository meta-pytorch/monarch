/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Admin introspection message handlers for [`HostMeshAgent`].
//!
//! This module defines the [`HostAdminQueryMessage`] protocol and its
//! handler implementation, keeping admin-related logic separate from the
//! core mesh agent code. System/infrastructure procs are queried directly
//! via local [`hyperactor::Proc`] references, while user-spawned procs
//! are forwarded to their [`ProcMeshAgent`].

use async_trait::async_trait;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use super::mesh_agent::HostMeshAgent;
use crate::proc_mesh::mesh_agent::AdminQueryMessageClient;
use crate::proc_mesh::mesh_agent::AdminQueryResponse;
use crate::v1::Name;

/// Messages for querying admin introspection data from a `HostMeshAgent`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    hyperactor::Handler,
    HandleClient,
    RefClient,
    Named
)]
pub enum HostAdminQueryMessage {
    /// Query details about this host.
    GetHostDetails {
        #[reply]
        reply: OncePortRef<AdminQueryResponse>,
    },
    /// Query details about a specific proc by name, forwarded to its `ProcMeshAgent`.
    GetProcDetails {
        proc_name: String,
        #[reply]
        reply: OncePortRef<AdminQueryResponse>,
    },
    /// Query details about a specific actor on a proc, forwarded to its `ProcMeshAgent`.
    GetActorDetails {
        proc_name: String,
        actor_name: String,
        #[reply]
        reply: OncePortRef<AdminQueryResponse>,
    },
}
wirevalue::register_type!(HostAdminQueryMessage);

// ---- Helper methods on HostMeshAgent for admin queries ----

impl HostMeshAgent {
    /// Look up a system proc (system or local) by its ProcId string.
    fn find_system_proc(&self, proc_id_str: &str) -> Option<&hyperactor::Proc> {
        let host = self.host.as_ref()?;
        let system = host.system_proc();
        if system.proc_id().to_string() == proc_id_str {
            return Some(system);
        }
        let local = host.local_proc();
        if local.proc_id().to_string() == proc_id_str {
            return Some(local);
        }
        None
    }

    /// Return proc details for a system/infrastructure proc.
    fn get_system_proc_details(
        &self,
        proc_id_str: &str,
    ) -> Result<AdminQueryResponse, anyhow::Error> {
        let proc = match self.find_system_proc(proc_id_str) {
            Some(p) => p,
            None => {
                tracing::warn!(
                    "admin: get_system_proc_details: proc not found: {}",
                    proc_id_str
                );
                return Ok(AdminQueryResponse { json: None });
            }
        };
        let details = hyperactor::admin::query_proc_details(proc);
        match serde_json::to_string(&details) {
            Ok(json) => Ok(AdminQueryResponse { json: Some(json) }),
            Err(e) => {
                tracing::warn!(
                    "admin: get_system_proc_details: serialization failed: {}",
                    e
                );
                Ok(AdminQueryResponse { json: None })
            }
        }
    }

    /// Return actor details for an actor in a system/infrastructure proc.
    fn get_system_actor_details(
        &self,
        proc_id_str: &str,
        actor_name: &str,
    ) -> Result<AdminQueryResponse, anyhow::Error> {
        let proc = match self.find_system_proc(proc_id_str) {
            Some(p) => p,
            None => {
                tracing::warn!(
                    "admin: get_system_actor_details: proc not found: {}",
                    proc_id_str
                );
                return Ok(AdminQueryResponse { json: None });
            }
        };
        let details = match hyperactor::admin::query_actor_details(proc, actor_name) {
            Some(d) => d,
            None => {
                tracing::warn!(
                    "admin: get_system_actor_details: actor '{}' not found in proc '{}'",
                    actor_name,
                    proc_id_str
                );
                return Ok(AdminQueryResponse { json: None });
            }
        };
        match serde_json::to_string(&details) {
            Ok(json) => Ok(AdminQueryResponse { json: Some(json) }),
            Err(e) => {
                tracing::warn!(
                    "admin: get_system_actor_details: serialization failed: {}",
                    e
                );
                Ok(AdminQueryResponse { json: None })
            }
        }
    }
}

// ---- HostAdminQueryMessage handler implementation ----

#[async_trait]
#[hyperactor::forward(HostAdminQueryMessage)]
impl HostAdminQueryMessageHandler for HostMeshAgent {
    // NOTE: These admin query handlers must NEVER return Err. A handler
    // error would crash the HostMeshAgent actor, and because the system
    // proc has no supervision coordinator, that triggers
    // `std::process::exit(1)` in `handle_unhandled_supervision_event`.
    // Instead, errors are logged and returned as `AdminQueryResponse`
    // with `json: None`, which the HTTP layer translates into a 404.

    async fn get_host_details(
        &mut self,
        _cx: &Context<Self>,
    ) -> Result<AdminQueryResponse, anyhow::Error> {
        let host = match self.host.as_ref() {
            Some(h) => h,
            None => {
                tracing::warn!("admin: get_host_details: host has been shut down");
                return Ok(AdminQueryResponse { json: None });
            }
        };
        let host_addr = host.addr().to_string();

        // System/infrastructure procs
        let mut procs: Vec<hyperactor::admin::HostProcEntry> = vec![
            hyperactor::admin::HostProcEntry {
                name: format!("[system] {}", host.system_proc().proc_id()),
                num_actors: host.system_proc().all_actor_ids().len(),
                url: String::new(),
            },
            hyperactor::admin::HostProcEntry {
                name: format!("[system] {}", host.local_proc().proc_id()),
                num_actors: host.local_proc().all_actor_ids().len(),
                url: String::new(),
            },
        ];

        // User-spawned procs
        for name in self.created.keys() {
            let name_str = name.to_string();
            let num_actors = hyperactor::admin::local_proc_details(&name_str)
                .map(|pd| pd.actors.len())
                .unwrap_or(0);
            procs.push(hyperactor::admin::HostProcEntry {
                name: name_str,
                num_actors,
                url: format!("/v1/hosts/{}/procs/{}", host_addr, name),
            });
        }
        let details = hyperactor::admin::HostDetails {
            addr: host_addr,
            procs,
            agent_url: None,
        };
        match serde_json::to_string(&details) {
            Ok(json) => Ok(AdminQueryResponse { json: Some(json) }),
            Err(e) => {
                tracing::warn!("admin: get_host_details: serialization failed: {}", e);
                Ok(AdminQueryResponse { json: None })
            }
        }
    }

    async fn get_proc_details(
        &mut self,
        cx: &Context<Self>,
        proc_name: String,
    ) -> Result<AdminQueryResponse, anyhow::Error> {
        // System/infrastructure procs are prefixed with "[system] " and
        // can be queried directly via the local Proc reference rather
        // than forwarding to a ProcMeshAgent.
        if let Some(proc_id_str) = proc_name.strip_prefix("[system] ") {
            return self.get_system_proc_details(proc_id_str);
        }

        let name = match proc_name.parse::<Name>() {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(
                    "admin: get_proc_details: invalid name '{}': {}",
                    proc_name,
                    e
                );
                return Ok(AdminQueryResponse { json: None });
            }
        };
        let state = match self.created.get(&name) {
            Some(s) => s,
            None => {
                tracing::warn!("admin: get_proc_details: proc not found: {}", proc_name);
                return Ok(AdminQueryResponse { json: None });
            }
        };
        let (_proc_id, agent_ref) = match state.created.as_ref() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    "admin: get_proc_details: proc '{}' creation failed: {}",
                    proc_name,
                    e
                );
                return Ok(AdminQueryResponse { json: None });
            }
        };
        match agent_ref.get_proc_details(cx).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                tracing::warn!(
                    "admin: get_proc_details: query failed for '{}': {}",
                    proc_name,
                    e
                );
                Ok(AdminQueryResponse { json: None })
            }
        }
    }

    async fn get_actor_details(
        &mut self,
        cx: &Context<Self>,
        proc_name: String,
        actor_name: String,
    ) -> Result<AdminQueryResponse, anyhow::Error> {
        // System/infrastructure procs are prefixed with "[system] " and
        // can be queried directly via the local Proc reference.
        if let Some(proc_id_str) = proc_name.strip_prefix("[system] ") {
            return self.get_system_actor_details(proc_id_str, &actor_name);
        }

        let name = match proc_name.parse::<Name>() {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(
                    "admin: get_actor_details: invalid name '{}': {}",
                    proc_name,
                    e
                );
                return Ok(AdminQueryResponse { json: None });
            }
        };
        let state = match self.created.get(&name) {
            Some(s) => s,
            None => {
                tracing::warn!("admin: get_actor_details: proc not found: {}", proc_name);
                return Ok(AdminQueryResponse { json: None });
            }
        };
        let (_proc_id, agent_ref) = match state.created.as_ref() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    "admin: get_actor_details: proc '{}' creation failed: {}",
                    proc_name,
                    e
                );
                return Ok(AdminQueryResponse { json: None });
            }
        };
        match agent_ref.get_actor_details(cx, actor_name.clone()).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                tracing::warn!(
                    "admin: get_actor_details: query failed for '{}/{}': {}",
                    proc_name,
                    actor_name,
                    e
                );
                Ok(AdminQueryResponse { json: None })
            }
        }
    }
}
