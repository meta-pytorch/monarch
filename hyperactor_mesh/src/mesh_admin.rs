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
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
use hyperactor::ProcId;
use hyperactor::RefClient;
use hyperactor::admin::ActorDetails;
use hyperactor::admin::ApiError;
use hyperactor::admin::HostDetails;
use hyperactor::admin::HostSummary;
use hyperactor::admin::ProcDetails;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::reference::Reference;
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

/// Typed properties for each kind of node in the mesh topology.
///
/// This is a wire-friendly enum (no `serde_json::Value`) so that it
/// survives wirevalue's bincode-based encoding. The HTTP layer gets
/// structured JSON for free via `Serialize`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub enum NodeProperties {
    Root {
        num_hosts: usize,
    },
    Host {
        addr: String,
        num_procs: usize,
    },
    Proc {
        proc_name: String,
        num_actors: usize,
    },
    Actor {
        actor_status: String,
        actor_type: String,
        messages_processed: u64,
        created_at: String,
        last_message_handler: Option<String>,
        total_processing_time_us: u64,
        flight_recorder: Option<String>,
    },
}
wirevalue::register_type!(NodeProperties);

/// Uniform response for any node in the mesh topology.
///
/// Every addressable entity (root, host, proc, actor) is represented
/// as a `NodePayload`. The client navigates the mesh by fetching a
/// node and following its `children` references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct NodePayload {
    /// Canonical reference string for this node.
    pub identity: String,
    /// Node-specific metadata (type, status, metrics, etc.).
    pub properties: NodeProperties,
    /// Reference strings the client can GET next to descend the tree.
    pub children: Vec<String>,
    /// Parent node reference for upward navigation.
    pub parent: Option<String>,
}
wirevalue::register_type!(NodePayload);

/// Newtype wrapper around `Result<NodePayload, String>` for the
/// resolve reply port (`OncePortRef` requires `Named`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct ResolveReferenceResponse(pub Result<NodePayload, String>);
wirevalue::register_type!(ResolveReferenceResponse);

/// Message for resolving an opaque reference string into a
/// `NodePayload`.
///
/// Sent by the HTTP bridge handler to `MeshAdminAgent`, which parses
/// the reference, routes to the appropriate host/proc/actor, and
/// assembles the response inside the actor message loop.
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
pub enum ResolveReferenceMessage {
    /// Resolve a reference string to a `NodePayload`.
    Resolve {
        reference_string: String,
        #[reply]
        reply: OncePortRef<ResolveReferenceResponse>,
    },
}
wirevalue::register_type!(ResolveReferenceMessage);

/// Actor that serves a mesh-level admin HTTP endpoint.
///
/// The agent holds references to all `HostMeshAgent` instances in the
/// mesh and forwards HTTP requests to them via actor messaging.
#[hyperactor::export(handlers = [MeshAdminMessage, ResolveReferenceMessage])]
pub struct MeshAdminAgent {
    hosts: HashMap<String, ActorRef<HostMeshAgent>>,
    /// Reverse index: HostMeshAgent ActorId → host address string.
    /// Used to distinguish host agent actors from regular actors when
    /// routing reference resolution.
    host_agents_by_actor_id: HashMap<ActorId, String>,
    admin_addr: Option<std::net::SocketAddr>,
}

impl MeshAdminAgent {
    /// Create a new mesh admin agent from a list of (host_addr,
    /// agent_ref) pairs.
    pub fn new(hosts: Vec<(String, ActorRef<HostMeshAgent>)>) -> Self {
        let host_agents_by_actor_id: HashMap<ActorId, String> = hosts
            .iter()
            .map(|(addr, agent_ref)| (agent_ref.actor_id().clone(), addr.clone()))
            .collect();
        Self {
            hosts: hosts.into_iter().collect(),
            host_agents_by_actor_id,
            admin_addr: None,
        }
    }
}

impl std::fmt::Debug for MeshAdminAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshAdminAgent")
            .field("hosts", &self.hosts.keys().collect::<Vec<_>>())
            .field("host_agents", &self.host_agents_by_actor_id.len())
            .field("admin_addr", &self.admin_addr)
            .finish()
    }
}

/// Shared state for the new reference-based HTTP bridge routes. Holds
/// an `ActorRef<MeshAdminAgent>` so the thin bridge handler can send
/// `ResolveReferenceMessage` into the actor message loop.
struct BridgeState {
    admin_ref: ActorRef<MeshAdminAgent>,
}

#[async_trait]
impl Actor for MeshAdminAgent {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        self.admin_addr = Some(addr);

        let legacy_state = Arc::new(MeshAdminState {
            hosts: self.hosts.clone(),
        });
        let bridge_state = Arc::new(BridgeState {
            admin_ref: ActorRef::attest(this.self_id().clone()),
        });
        let router = create_mesh_admin_router(legacy_state, bridge_state);
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

#[async_trait]
#[hyperactor::forward(ResolveReferenceMessage)]
impl ResolveReferenceMessageHandler for MeshAdminAgent {
    async fn resolve(
        &mut self,
        cx: &Context<Self>,
        reference_string: String,
    ) -> Result<ResolveReferenceResponse, anyhow::Error> {
        // Errors are returned in the response, never as Err —
        // returning Err from a handler crashes the actor.
        Ok(ResolveReferenceResponse(
            self.resolve_reference(cx, &reference_string)
                .await
                .map_err(|e| format!("{:#}", e)),
        ))
    }
}

impl MeshAdminAgent {
    /// Resolve a reference string into a `NodePayload` by dispatching
    /// to the appropriate host agent via existing admin query
    /// messages.
    async fn resolve_reference(
        &self,
        cx: &Context<'_, Self>,
        reference_string: &str,
    ) -> Result<NodePayload, anyhow::Error> {
        if reference_string == "root" {
            return Ok(self.build_root_payload());
        }

        let reference: Reference = reference_string
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid reference '{}': {}", reference_string, e))?;

        match &reference {
            Reference::Actor(actor_id) if self.host_agents_by_actor_id.contains_key(actor_id) => {
                self.resolve_host_node(cx, actor_id).await
            }
            Reference::Proc(proc_id) => self.resolve_proc_node(cx, proc_id).await,
            Reference::Actor(actor_id) => self.resolve_actor_node(cx, actor_id).await,
            _ => Err(anyhow::anyhow!(
                "unsupported reference type: {}",
                reference_string
            )),
        }
    }

    /// Build the root node payload from the hosts map.
    fn build_root_payload(&self) -> NodePayload {
        let children: Vec<String> = self
            .hosts
            .values()
            .map(|agent| agent.actor_id().to_string())
            .collect();
        NodePayload {
            identity: "root".to_string(),
            properties: NodeProperties::Root {
                num_hosts: self.hosts.len(),
            },
            children,
            parent: None,
        }
    }

    /// Resolve a host agent ActorId into a host NodePayload.
    async fn resolve_host_node(
        &self,
        cx: &Context<'_, Self>,
        actor_id: &ActorId,
    ) -> Result<NodePayload, anyhow::Error> {
        let host_addr = self
            .host_agents_by_actor_id
            .get(actor_id)
            .ok_or_else(|| anyhow::anyhow!("host agent not found for {}", actor_id))?;

        let agent = self
            .hosts
            .get(host_addr)
            .ok_or_else(|| anyhow::anyhow!("host not found: {}", host_addr))?;
        let response = RealClock
            .timeout(SINGLE_HOST_TIMEOUT, agent.get_host_details(cx))
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying host agent"))?
            .map_err(|e| anyhow::anyhow!("failed to query host agent: {}", e))?;

        let json = response
            .json
            .ok_or_else(|| anyhow::anyhow!("host returned no details"))?;
        let details: HostDetails = serde_json::from_str(&json)?;

        // Build children: ProcId strings for each proc on this host.
        let children: Vec<String> = details
            .procs
            .iter()
            .map(|p| {
                if let Some(proc_id_str) = p.name.strip_prefix("[system] ") {
                    // System procs already have a ProcId string after
                    // the prefix.
                    proc_id_str.to_string()
                } else {
                    // User procs: construct ProcId::Direct string as
                    // "addr,name".
                    format!("{},{}", host_addr, p.name)
                }
            })
            .collect();

        Ok(NodePayload {
            identity: actor_id.to_string(),
            properties: NodeProperties::Host {
                addr: details.addr,
                num_procs: details.procs.len(),
            },
            children,
            parent: Some("root".to_string()),
        })
    }

    /// Resolve a ProcId into a proc NodePayload.
    async fn resolve_proc_node(
        &self,
        cx: &Context<'_, Self>,
        proc_id: &ProcId,
    ) -> Result<NodePayload, anyhow::Error> {
        let (host_addr, proc_name) = match proc_id {
            ProcId::Direct(addr, name) => (addr.to_string(), name.clone()),
            ProcId::Ranked(world_id, _rank) => {
                return Err(anyhow::anyhow!(
                    "ranked proc references not yet supported: {}",
                    world_id
                ));
            }
        };

        let agent = self
            .hosts
            .get(&host_addr)
            .ok_or_else(|| anyhow::anyhow!("host not found: {}", host_addr))?;

        // Find the host agent ActorId for parent.
        let host_agent_id = agent.actor_id().to_string();

        let response = RealClock
            .timeout(
                SINGLE_HOST_TIMEOUT,
                agent.get_proc_details(cx, proc_name.clone()),
            )
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying proc details"))?
            .map_err(|e| anyhow::anyhow!("failed to query proc details: {}", e))?;

        // If the plain name lookup failed, this may be a system/
        // infrastructure proc. HostAdminQueryMessage::GetProcDetails
        // expects the "[system] <proc_id>" format for those.
        let response = if response.json.is_some() {
            response
        } else {
            let system_name = format!("[system] {}", proc_id);
            RealClock
                .timeout(SINGLE_HOST_TIMEOUT, agent.get_proc_details(cx, system_name))
                .await
                .map_err(|_| anyhow::anyhow!("timed out querying system proc details"))?
                .map_err(|e| anyhow::anyhow!("failed to query system proc details: {}", e))?
        };

        let json = response
            .json
            .ok_or_else(|| anyhow::anyhow!("proc not found: {}", proc_name))?;
        let details: ProcDetails = serde_json::from_str(&json)?;

        Ok(NodePayload {
            identity: proc_id.to_string(),
            properties: NodeProperties::Proc {
                proc_name: details.proc_name,
                num_actors: details.actors.len(),
            },
            // ProcDetails.actors contains full ActorId.to_string() values.
            children: details.actors,
            parent: Some(host_agent_id),
        })
    }

    /// Resolve an ActorId (not a host agent) into an actor NodePayload.
    async fn resolve_actor_node(
        &self,
        cx: &Context<'_, Self>,
        actor_id: &ActorId,
    ) -> Result<NodePayload, anyhow::Error> {
        let proc_id = actor_id.proc_id();
        let (host_addr, proc_name) = match proc_id {
            ProcId::Direct(addr, name) => (addr.to_string(), name.clone()),
            ProcId::Ranked(world_id, _rank) => {
                return Err(anyhow::anyhow!(
                    "ranked proc references not yet supported: {}",
                    world_id
                ));
            }
        };

        let agent = self
            .hosts
            .get(&host_addr)
            .ok_or_else(|| anyhow::anyhow!("host not found: {}", host_addr))?;
        let actor_name = actor_id.name().to_string();
        let response = RealClock
            .timeout(
                SINGLE_HOST_TIMEOUT,
                agent.get_actor_details(cx, proc_name.clone(), actor_name),
            )
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying actor details"))?
            .map_err(|e| anyhow::anyhow!("failed to query actor details: {}", e))?;

        let json = response
            .json
            .ok_or_else(|| anyhow::anyhow!("actor not found: {}", actor_id))?;
        let details: ActorDetails = serde_json::from_str(&json)?;

        Ok(NodePayload {
            identity: actor_id.to_string(),
            properties: NodeProperties::Actor {
                actor_status: details.actor_status,
                actor_type: details.actor_type,
                messages_processed: details.messages_processed,
                created_at: details.created_at,
                last_message_handler: details.last_message_handler,
                total_processing_time_us: details.total_processing_time_us,
                flight_recorder: serde_json::to_string(&details.flight_recorder).ok(),
            },
            // ActorDetails.children contains full ActorId.to_string() values.
            children: details.children,
            parent: Some(proc_id.to_string()),
        })
    }
}

fn create_mesh_admin_router(
    legacy_state: Arc<MeshAdminState>,
    bridge_state: Arc<BridgeState>,
) -> Router {
    let legacy_routes = Router::new()
        .route("/v1/hosts", get(list_hosts))
        .route("/v1/hosts/{host_addr}", get(get_host))
        .route("/v1/hosts/{host_addr}/procs/{proc_name}", get(get_proc))
        .route(
            "/v1/hosts/{host_addr}/procs/{proc_name}/{actor_name}",
            get(get_actor),
        )
        .route("/v1/tree", get(tree_dump))
        .with_state(legacy_state);

    let bridge_routes = Router::new()
        .route("/v1/{*reference}", get(resolve_reference_bridge))
        .with_state(bridge_state);

    // Legacy routes are more specific and take precedence over the
    // wildcard bridge route.
    legacy_routes.merge(bridge_routes)
}

/// Decode percent-encoded characters in a reference path extracted
/// from an axum wildcard route. Axum does not decode `{*wildcard}`
/// captures, so characters like `:`, `,`, `[`, and `]` arrive as
/// `%3A`, `%2C`, `%5B`, and `%5D` respectively.
fn decode_reference_path(raw: &str) -> Result<String, ApiError> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(ApiError::bad_request(
                    "malformed percent-encoding: incomplete escape at end of string",
                    None,
                ));
            }
            let hi = hex_digit(bytes[i + 1]).ok_or_else(|| {
                ApiError::bad_request("malformed percent-encoding: invalid hex digit", None)
            })?;
            let lo = hex_digit(bytes[i + 2]).ok_or_else(|| {
                ApiError::bad_request("malformed percent-encoding: invalid hex digit", None)
            })?;
            out.push(hi << 4 | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| {
        ApiError::bad_request(
            "malformed percent-encoding: decoded bytes are not valid UTF-8",
            None,
        )
    })
}

/// Convert an ASCII hex digit to its numeric value.
fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// GET /v1/{*reference} -- resolve any reference string to a NodePayload.
///
/// Thin bridge handler: sends `ResolveReferenceMessage` to the
/// `MeshAdminAgent` actor and returns the result as JSON.
async fn resolve_reference_bridge(
    State(state): State<Arc<BridgeState>>,
    Path(reference): Path<String>,
) -> Result<Json<NodePayload>, ApiError> {
    // Axum's wildcard may include a leading slash; strip it.
    let reference = reference.trim_start_matches('/');
    if reference.is_empty() {
        return Err(ApiError::bad_request("empty reference", None));
    }
    let reference = decode_reference_path(reference)?;

    let cx = global_root_client();
    let response = RealClock
        .timeout(SINGLE_HOST_TIMEOUT, state.admin_ref.resolve(cx, reference))
        .await
        .map_err(|_| ApiError {
            code: "gateway_timeout".to_string(),
            message: "timed out resolving reference".to_string(),
            details: None,
        })?
        .map_err(|e| ApiError {
            code: "internal_error".to_string(),
            message: format!("failed to resolve reference: {}", e),
            details: None,
        })?;

    match response.0 {
        Ok(payload) => Ok(Json(payload)),
        Err(error) => Err(ApiError::not_found(error, None)),
    }
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
                let resp = RealClock.timeout(FANOUT_TIMEOUT, task).await;
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
    let response = RealClock
        .timeout(SINGLE_HOST_TIMEOUT, task)
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
    let response = RealClock
        .timeout(SINGLE_HOST_TIMEOUT, task)
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
    let response = RealClock
        .timeout(SINGLE_HOST_TIMEOUT, task)
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
                let resp = RealClock.timeout(FANOUT_TIMEOUT, task).await;
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use hyperactor::channel::ChannelAddr;

    use super::*;

    #[test]
    fn test_decode_comma() {
        assert_eq!(decode_reference_path("%2C").unwrap(), ",");
    }

    #[test]
    fn test_decode_colon() {
        assert_eq!(decode_reference_path("%3A").unwrap(), ":");
    }

    #[test]
    fn test_decode_brackets() {
        assert_eq!(decode_reference_path("%5B0%5D").unwrap(), "[0]");
    }

    #[test]
    fn test_decode_full_reference() {
        let encoded = "tcp%3A127.0.0.1%3A8080%2Cdp%2Cphilosopher%5B0%5D";
        let expected = "tcp:127.0.0.1:8080,dp,philosopher[0]";
        assert_eq!(decode_reference_path(encoded).unwrap(), expected);
    }

    #[test]
    fn test_decode_malformed_truncated() {
        assert!(decode_reference_path("abc%2").is_err());
    }

    #[test]
    fn test_decode_malformed_bad_hex() {
        assert!(decode_reference_path("%ZZ").is_err());
    }

    #[test]
    fn test_decode_passthrough_already_decoded() {
        let plain = "tcp:127.0.0.1:8080,dp,philosopher[0]";
        assert_eq!(decode_reference_path(plain).unwrap(), plain);
    }

    #[test]
    fn test_build_root_payload() {
        let addr1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9002".parse().unwrap();

        let proc1 = ProcId::Direct(ChannelAddr::Tcp(addr1), "host1".to_string());
        let proc2 = ProcId::Direct(ChannelAddr::Tcp(addr2), "host2".to_string());

        let actor_id1 = ActorId::root(proc1, "mesh_agent".to_string());
        let actor_id2 = ActorId::root(proc2, "mesh_agent".to_string());

        let ref1: ActorRef<HostMeshAgent> = ActorRef::attest(actor_id1.clone());
        let ref2: ActorRef<HostMeshAgent> = ActorRef::attest(actor_id2.clone());

        let agent = MeshAdminAgent::new(vec![
            ("host_a".to_string(), ref1),
            ("host_b".to_string(), ref2),
        ]);

        let payload = agent.build_root_payload();
        assert_eq!(payload.identity, "root");
        assert_eq!(payload.parent, None);
        assert_eq!(payload.properties, NodeProperties::Root { num_hosts: 2 });
        assert_eq!(payload.children.len(), 2);
        // Children should be the actor ID strings.
        assert!(payload.children.contains(&actor_id1.to_string()));
        assert!(payload.children.contains(&actor_id2.to_string()));
    }

    /// Integration test: resolve references at each level of the mesh
    /// topology (root -> host -> proc -> actor) by sending
    /// `ResolveReferenceMessage` through the actor mailbox.
    #[tokio::test]
    async fn test_resolve_reference_tree_walk() {
        use hyperactor::Proc;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::host::Host;
        use hyperactor::host::LocalProcManager;

        use crate::host_mesh::mesh_agent::HostAgentMode;
        use crate::host_mesh::mesh_agent::ProcManagerSpawnFn;
        use crate::mesh_agent::ProcMeshAgent;

        // -- 1. Stand up a local in-process Host with a HostMeshAgent --
        // Use Unix transport for all procs — Local transport does not
        // support cross-proc message routing.
        let spawn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcMeshAgent::boot_v1(proc))));
        let manager: LocalProcManager<ProcManagerSpawnFn> = LocalProcManager::new(spawn);
        let host = Host::new(manager, ChannelTransport::Unix.any())
            .await
            .unwrap();
        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_agent_handle = system_proc
            .spawn("agent", HostMeshAgent::new(HostAgentMode::Local(host)))
            .unwrap();
        let host_agent_ref: ActorRef<HostMeshAgent> = host_agent_handle.bind();
        let host_addr_str = host_addr.to_string();

        // -- 2. Spawn MeshAdminAgent on a separate proc --
        let admin_proc = Proc::direct(ChannelTransport::Unix.any(), "admin".to_string()).unwrap();
        // The admin proc has no supervision coordinator by default.
        // Without one, actor teardown triggers std::process::exit(1).
        use hyperactor::test_utils::proc_supervison::ProcSupervisionCoordinator;
        let _supervision = ProcSupervisionCoordinator::set(&admin_proc).await.unwrap();
        let admin_handle = admin_proc
            .spawn(
                "mesh_admin",
                MeshAdminAgent::new(vec![(host_addr_str.clone(), host_agent_ref.clone())]),
            )
            .unwrap();
        let admin_ref: ActorRef<MeshAdminAgent> = admin_handle.bind();

        // -- 3. Create a bare client instance for sending messages --
        // Only a mailbox is needed for reply ports — no actor message
        // loop required.
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _handle) = client_proc.instance("client").unwrap();

        // -- 4. Resolve "root" --
        let root_resp = admin_ref
            .resolve(&client, "root".to_string())
            .await
            .unwrap();
        let root = root_resp.0.unwrap();
        assert_eq!(root.identity, "root");
        assert_eq!(root.properties, NodeProperties::Root { num_hosts: 1 });
        assert_eq!(root.parent, None);
        assert_eq!(root.children.len(), 1);

        // -- 5. Resolve the host child --
        let host_child_ref_str = &root.children[0];
        let host_resp = admin_ref
            .resolve(&client, host_child_ref_str.clone())
            .await
            .unwrap();
        let host_node = host_resp.0.unwrap();
        assert_eq!(host_node.identity, *host_child_ref_str);
        assert!(
            matches!(host_node.properties, NodeProperties::Host { .. }),
            "expected Host properties, got {:?}",
            host_node.properties
        );
        assert_eq!(host_node.parent, Some("root".to_string()));
        // A local host always has at least the system and local procs.
        assert!(
            !host_node.children.is_empty(),
            "host should have at least one proc child"
        );

        // -- 6. Resolve a system proc child --
        // System proc children are ProcId strings (the "[system] "
        // prefix is stripped by resolve_host_node).
        let proc_ref_str = &host_node.children[0];
        let proc_resp = admin_ref
            .resolve(&client, proc_ref_str.clone())
            .await
            .unwrap();
        let proc_node = proc_resp.0.unwrap();
        assert!(
            matches!(proc_node.properties, NodeProperties::Proc { .. }),
            "expected Proc properties, got {:?}",
            proc_node.properties
        );
        assert_eq!(proc_node.parent, Some(host_child_ref_str.clone()));
        // The system proc should have at least the "agent" actor.
        assert!(
            !proc_node.children.is_empty(),
            "proc should have at least one actor child"
        );

        // -- 7. Resolve an actor child --
        // The first child of the system proc is typically the
        // HostMeshAgent actor itself, which resolves as a Host node
        // (the reverse index correctly identifies it). The test
        // verifies that the reference is resolvable and produces a
        // valid node — the specific node type depends on whether the
        // actor is a host agent or a regular actor.
        let actor_ref_str = &proc_node.children[0];
        let actor_resp = admin_ref
            .resolve(&client, actor_ref_str.clone())
            .await
            .unwrap();
        let actor_node = actor_resp.0.unwrap();
        assert_eq!(actor_node.identity, *actor_ref_str);
        assert!(
            matches!(
                actor_node.properties,
                NodeProperties::Actor { .. } | NodeProperties::Host { .. }
            ),
            "expected Actor or Host properties, got {:?}",
            actor_node.properties
        );
    }
}
