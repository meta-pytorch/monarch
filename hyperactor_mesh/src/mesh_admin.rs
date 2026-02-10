/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mesh-level admin HTTP server for introspecting a host mesh.
//!
//! The `MeshAdminAgent` actor aggregates admin queries across all
//! `HostMeshAgent` instances in a mesh, providing a single HTTP
//! endpoint for mesh-wide introspection. Individual host, proc, and
//! actor details are fetched by forwarding requests to the
//! appropriate `HostMeshAgent` via actor messaging.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::routing::get;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::admin::ActorDetails;
use hyperactor::admin::ApiError;
use hyperactor::admin::HostDetails;
use hyperactor::admin::HostSummary;
use hyperactor::admin::ProcDetails;
use serde::Deserialize;
use serde::Serialize;
use tokio::net::TcpListener;
use typeuri::Named;

use crate::global_root_client;
use crate::host_mesh::host_admin::HostAdminQueryMessageClient;
use crate::host_mesh::mesh_agent::HostMeshAgent;

/// Timeout for fan-out queries that hit every host in the mesh.
/// Kept short so that a few slow or dead hosts don't block the entire
/// response. Hosts that don't respond within this window are reported
/// as unreachable and skipped.
const FANOUT_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for targeted queries that hit a single, specific host.
/// Longer than the fan-out timeout because the caller explicitly chose
/// this host and is willing to wait for a response.
const SINGLE_HOST_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared state for mesh admin HTTP handlers.
struct MeshAdminState {
    hosts: HashMap<String, ActorRef<HostMeshAgent>>,
}

/// Response to a `GetAdminAddr` query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct MeshAdminAddrResponse {
    pub addr: Option<String>,
}
wirevalue::register_type!(MeshAdminAddrResponse);

/// Messages handled by the `MeshAdminAgent`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Handler,
    HandleClient,
    RefClient,
    Named
)]
pub enum MeshAdminMessage {
    /// Query the HTTP admin address.
    GetAdminAddr {
        #[reply]
        reply: OncePortRef<MeshAdminAddrResponse>,
    },
}
wirevalue::register_type!(MeshAdminMessage);

/// Actor that serves a mesh-level admin HTTP endpoint.
///
/// The agent holds references to all `HostMeshAgent` instances in the
/// mesh and forwards HTTP requests to them via actor messaging.
#[hyperactor::export(handlers = [MeshAdminMessage])]
pub struct MeshAdminAgent {
    hosts: HashMap<String, ActorRef<HostMeshAgent>>,
    admin_addr: Option<std::net::SocketAddr>,
}

impl MeshAdminAgent {
    /// Create a new mesh admin agent from a list of (host_addr, agent_ref) pairs.
    pub fn new(hosts: Vec<(String, ActorRef<HostMeshAgent>)>) -> Self {
        Self {
            hosts: hosts.into_iter().collect(),
            admin_addr: None,
        }
    }
}

impl std::fmt::Debug for MeshAdminAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshAdminAgent")
            .field("hosts", &self.hosts.keys().collect::<Vec<_>>())
            .field("admin_addr", &self.admin_addr)
            .finish()
    }
}

#[async_trait]
impl Actor for MeshAdminAgent {
    async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        self.admin_addr = Some(addr);

        let state = Arc::new(MeshAdminState {
            hosts: self.hosts.clone(),
        });
        let router = create_mesh_admin_router(state);
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                tracing::error!("mesh admin server error: {}", e);
            }
        });

        tracing::info!("mesh admin server listening on http://{}", addr);
        Ok(())
    }
}

#[async_trait]
#[hyperactor::forward(MeshAdminMessage)]
impl MeshAdminMessageHandler for MeshAdminAgent {
    async fn get_admin_addr(
        &mut self,
        _cx: &Context<Self>,
    ) -> Result<MeshAdminAddrResponse, anyhow::Error> {
        Ok(MeshAdminAddrResponse {
            addr: self.admin_addr.map(|a| a.to_string()),
        })
    }
}

fn create_mesh_admin_router(state: Arc<MeshAdminState>) -> Router {
    Router::new()
        .route("/v1/hosts", get(list_hosts))
        .route("/v1/hosts/{host_addr}", get(get_host))
        .route("/v1/hosts/{host_addr}/procs/{proc_name}", get(get_proc))
        .route(
            "/v1/hosts/{host_addr}/procs/{proc_name}/{actor_name}",
            get(get_actor),
        )
        .route("/v1/tree", get(tree_dump))
        .with_state(state)
}

/// Response from the `/v1/hosts` endpoint. Wraps the list of
/// responsive hosts with metadata about the overall mesh state.
#[derive(Debug, Serialize, Deserialize)]
struct MeshHostsResponse {
    /// Total number of hosts in the mesh.
    total: usize,
    /// Hosts that responded within the fan-out timeout.
    hosts: Vec<HostSummary>,
    /// Addresses of hosts that were unreachable or timed out.
    unreachable: Vec<String>,
}

/// GET /v1/hosts -- list all hosts in the mesh, fanning out to each host agent.
///
/// Each host is queried in parallel. Hosts that don't respond within
/// `FANOUT_TIMEOUT` are reported in the `unreachable` field rather
/// than blocking the response.
///
/// Actor message calls are spawned as background tasks so that if the
/// timeout fires, the reply port remains alive — preventing
/// "undeliverable message" crashes on the remote actor.
async fn list_hosts(
    State(state): State<Arc<MeshAdminState>>,
) -> Result<Json<MeshHostsResponse>, ApiError> {
    let cx = global_root_client();
    let total = state.hosts.len();
    let futures: Vec<_> = state
        .hosts
        .iter()
        .map(|(addr, agent)| {
            let addr = addr.clone();
            let agent = agent.clone();
            // Spawn the actor call so the reply port survives timeout.
            let task = tokio::spawn(async move { agent.get_host_details(cx).await });
            async move {
                let resp = tokio::time::timeout(FANOUT_TIMEOUT, task).await;
                (addr, resp)
            }
        })
        .collect();

    let results = futures::future::join_all(futures).await;
    let mut hosts = Vec::new();
    let mut unreachable = Vec::new();
    for (addr, result) in results {
        match result {
            Ok(Ok(Ok(response))) => {
                if let Some(json) = response.json {
                    if let Ok(details) = serde_json::from_str::<HostDetails>(&json) {
                        hosts.push(HostSummary {
                            addr: details.addr,
                            num_procs: details.procs.len(),
                        });
                        continue;
                    }
                }
                // Host responded but no details; include with zero procs.
                hosts.push(HostSummary { addr, num_procs: 0 });
            }
            _ => {
                tracing::warn!("failed to query host {} within fan-out timeout", addr);
                unreachable.push(addr);
            }
        }
    }
    Ok(Json(MeshHostsResponse {
        total,
        hosts,
        unreachable,
    }))
}

/// GET /v1/hosts/{host_addr} -- details for a single host.
async fn get_host(
    State(state): State<Arc<MeshAdminState>>,
    Path(host_addr): Path<String>,
) -> Result<Json<HostDetails>, ApiError> {
    let agent = state.hosts.get(&host_addr).ok_or_else(|| {
        ApiError::not_found(
            "host not found",
            Some(serde_json::json!({ "addr": host_addr })),
        )
    })?;
    let cx = global_root_client();
    let agent = agent.clone();
    let task = tokio::spawn(async move { agent.get_host_details(cx).await });
    let response = tokio::time::timeout(SINGLE_HOST_TIMEOUT, task)
        .await
        .map_err(|_| ApiError {
            code: "gateway_timeout".to_string(),
            message: "timed out querying host agent".to_string(),
            details: None,
        })?
        .map_err(|e| ApiError {
            code: "internal_error".to_string(),
            message: format!("task join error: {}", e),
            details: None,
        })?
        .map_err(|e| ApiError {
            code: "internal_error".to_string(),
            message: format!("failed to query host agent: {}", e),
            details: None,
        })?;
    match response.json {
        Some(json) => {
            let details: HostDetails = serde_json::from_str(&json).map_err(|e| ApiError {
                code: "internal_error".to_string(),
                message: format!("failed to parse host details: {}", e),
                details: None,
            })?;
            Ok(Json(details))
        }
        None => Err(ApiError::not_found("host returned no details", None)),
    }
}

/// GET /v1/hosts/{host_addr}/procs/{proc_name} -- details for a proc on a host.
async fn get_proc(
    State(state): State<Arc<MeshAdminState>>,
    Path((host_addr, proc_name)): Path<(String, String)>,
) -> Result<Json<ProcDetails>, ApiError> {
    let agent = state.hosts.get(&host_addr).ok_or_else(|| {
        ApiError::not_found(
            "host not found",
            Some(serde_json::json!({ "addr": host_addr })),
        )
    })?;
    let cx = global_root_client();
    let agent = agent.clone();
    let pn = proc_name.clone();
    let task = tokio::spawn(async move { agent.get_proc_details(cx, pn).await });
    let response = tokio::time::timeout(SINGLE_HOST_TIMEOUT, task)
        .await
        .map_err(|_| ApiError {
            code: "gateway_timeout".to_string(),
            message: "timed out querying host agent".to_string(),
            details: None,
        })?
        .map_err(|e| ApiError {
            code: "internal_error".to_string(),
            message: format!("task join error: {}", e),
            details: None,
        })?
        .map_err(|e| ApiError {
            code: "internal_error".to_string(),
            message: format!("failed to query proc details: {}", e),
            details: None,
        })?;
    match response.json {
        Some(json) => {
            let details: ProcDetails = serde_json::from_str(&json).map_err(|e| ApiError {
                code: "internal_error".to_string(),
                message: format!("failed to parse proc details: {}", e),
                details: None,
            })?;
            Ok(Json(details))
        }
        None => Err(ApiError::not_found(
            "proc not found",
            Some(serde_json::json!({ "proc_name": proc_name })),
        )),
    }
}

/// GET /v1/hosts/{host_addr}/procs/{proc_name}/{actor_name} -- details for an actor.
async fn get_actor(
    State(state): State<Arc<MeshAdminState>>,
    Path((host_addr, proc_name, actor_name)): Path<(String, String, String)>,
) -> Result<Json<ActorDetails>, ApiError> {
    let agent = state.hosts.get(&host_addr).ok_or_else(|| {
        ApiError::not_found(
            "host not found",
            Some(serde_json::json!({ "addr": host_addr })),
        )
    })?;
    let cx = global_root_client();
    let agent = agent.clone();
    let pn = proc_name.clone();
    let an = actor_name.clone();
    let task = tokio::spawn(async move { agent.get_actor_details(cx, pn, an).await });
    let response = tokio::time::timeout(SINGLE_HOST_TIMEOUT, task)
        .await
        .map_err(|_| ApiError {
            code: "gateway_timeout".to_string(),
            message: "timed out querying host agent".to_string(),
            details: None,
        })?
        .map_err(|e| ApiError {
            code: "internal_error".to_string(),
            message: format!("task join error: {}", e),
            details: None,
        })?
        .map_err(|e| ApiError {
            code: "internal_error".to_string(),
            message: format!("failed to query actor details: {}", e),
            details: None,
        })?;
    match response.json {
        Some(json) => {
            let details: ActorDetails = serde_json::from_str(&json).map_err(|e| ApiError {
                code: "internal_error".to_string(),
                message: format!("failed to parse actor details: {}", e),
                details: None,
            })?;
            Ok(Json(details))
        }
        None => Err(ApiError::not_found(
            "actor not found",
            Some(serde_json::json!({
                "proc_name": proc_name,
                "actor_name": actor_name,
            })),
        )),
    }
}

/// GET /v1/tree -- ASCII tree of the entire mesh topology.
async fn tree_dump(State(state): State<Arc<MeshAdminState>>) -> Result<String, ApiError> {
    let cx = global_root_client();
    let futures: Vec<_> = state
        .hosts
        .iter()
        .map(|(addr, agent)| {
            let addr = addr.clone();
            let agent = agent.clone();
            let task = tokio::spawn(async move { agent.get_host_details(cx).await });
            async move {
                let resp = tokio::time::timeout(FANOUT_TIMEOUT, task).await;
                (addr, resp)
            }
        })
        .collect();

    let results = futures::future::join_all(futures).await;
    let mut output = String::new();
    for (addr, result) in results {
        match result {
            Ok(Ok(Ok(response))) => {
                if let Some(json) = response.json {
                    if let Ok(details) = serde_json::from_str::<HostDetails>(&json) {
                        output.push_str(&format!("{}\n", details.addr));
                        for (i, proc_entry) in details.procs.iter().enumerate() {
                            let connector = if i == details.procs.len() - 1 {
                                "└── "
                            } else {
                                "├── "
                            };
                            output.push_str(&format!("{}{}\n", connector, proc_entry.name));
                        }
                        output.push('\n');
                        continue;
                    }
                }
                output.push_str(&format!("{} (no details)\n\n", addr));
            }
            _ => {
                output.push_str(&format!("{} (unreachable)\n\n", addr));
            }
        }
    }
    Ok(output)
}
