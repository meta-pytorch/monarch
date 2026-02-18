/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Mesh-level admin surface for topology introspection and reference
//! walking.
//!
//! This module defines `MeshAdminAgent`, an actor that exposes a
//! uniform, reference-based HTTP API over an entire host mesh. Rather
//! than requiring clients to understand host/proc/actor-specific
//! routes, every addressable entity in the mesh is represented as a
//! `NodePayload` and resolved via an opaque reference string.
//!
//! Incoming HTTP requests are bridged into the actor message loop
//! using `ResolveReferenceMessage`, ensuring that all topology
//! resolution and data collection happens through actor messaging.
//! The agent fans out to `HostMeshAgent` instances to fetch host,
//! proc, and actor details, then normalizes them into a single
//! tree-shaped model (`NodeProperties` + children references)
//! suitable for topology-agnostic clients such as the admin TUI.
//!
//! Legacy structured routes are still provided for now (will soon be
//! removed) but the primary abstraction is the reference-walking API:
//! clients fetch `root`, follow child references, and progressively
//! explore the mesh without embedding topology knowledge.

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
use hyperactor::introspect::NodePayload;
use hyperactor::introspect::NodeProperties;
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

/// Shared state for the legacy (host-address keyed) mesh admin HTTP
/// routes.
///
/// Handlers use this to look up the `HostMeshAgent` for a given host
/// address and forward per-host admin queries over actor messaging.
struct MeshAdminState {
    hosts: HashMap<String, ActorRef<HostMeshAgent>>,
}

/// Response payload for `MeshAdminMessage::GetAdminAddr`.
///
/// `addr` is `None` until the admin HTTP server has successfully
/// bound a listening socket during `MeshAdminAgent::init`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct MeshAdminAddrResponse {
    pub addr: Option<String>,
}
wirevalue::register_type!(MeshAdminAddrResponse);

/// Messages handled by the `MeshAdminAgent`.
///
/// These are mesh-admin control-plane queries (as opposed to topology
/// resolution). They’re wirevalue-serializable and come with
/// generated client/ref helpers via `HandleClient`/`RefClient`.
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
    /// Return the HTTP admin server address that this agent bound in
    /// `init`.
    ///
    /// The reply contains `None` if the server hasn't started yet.
    GetAdminAddr {
        #[reply]
        reply: OncePortRef<MeshAdminAddrResponse>,
    },
}
wirevalue::register_type!(MeshAdminMessage);

/// Newtype wrapper around `Result<NodePayload, String>` for the
/// resolve reply port (`OncePortRef` requires `Named`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct ResolveReferenceResponse(pub Result<NodePayload, String>);
wirevalue::register_type!(ResolveReferenceResponse);

/// Message for resolving an opaque reference string into a
/// `NodePayload`.
///
/// This is the primary “navigation” request used by the admin HTTP
/// bridge: the caller provides a reference (e.g. `"root"`, a `ProcId`
/// string, or an `ActorId` string) and the `MeshAdminAgent` returns a
/// uniformly shaped `NodePayload` plus child references to continue
/// walking the topology.
///
/// The work happens inside the admin actor's message loop so
/// resolution can:
/// - parse and validate the reference format,
/// - dispatch to the right host/proc/actor via existing admin
///   queries, and
/// - return a structured payload without blocking HTTP handlers on
///   mesh logic.
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
    /// Resolve `reference_string` to a `NodePayload`.
    ///
    /// On success the reply contains `payload=Some(..), error=None`; on failure
    /// it contains `payload=None, error=Some(..)`.
    Resolve {
        /// Opaque reference string identifying a root/host/proc/actor
        /// node.
        reference_string: String,
        /// Reply port receiving the resolution result.
        #[reply]
        reply: OncePortRef<ResolveReferenceResponse>,
    },
}
wirevalue::register_type!(ResolveReferenceMessage);

/// Actor that serves a mesh-level admin HTTP endpoint.
///
/// `MeshAdminAgent` is the mesh-wide aggregation point for
/// introspection: it holds `ActorRef<HostMeshAgent>` handles for each
/// host, and answers admin queries by forwarding targeted requests to
/// the appropriate host agent and assembling a uniform `NodePayload`
/// response for the client.
///
/// The agent also exposes an HTTP server (spawned from `init`) and
/// supports reference-based navigation (`GET /v1/{reference}`) by
/// resolving opaque reference strings into typed `NodeProperties`
/// plus child references.
#[hyperactor::export(handlers = [MeshAdminMessage, ResolveReferenceMessage])]
pub struct MeshAdminAgent {
    /// Map of host address string → `HostMeshAgent` reference used to
    /// fan out or target admin queries.
    hosts: HashMap<String, ActorRef<HostMeshAgent>>,

    /// Reverse index: `HostMeshAgent` `ActorId` → host address
    /// string.
    ///
    /// The host agent itself is an actor that can appear in multiple
    /// places (e.g., as a host node and as a child actor under a
    /// system proc). This index lets reference resolution treat that
    /// `ActorId` as a *Host* node (via `resolve_host_node`) rather
    /// than a generic *Actor* node, avoiding cycles / dropped nodes
    /// in clients like the TUI.
    host_agents_by_actor_id: HashMap<ActorId, String>,

    /// Bound address of the admin HTTP server once started in `init`.
    admin_addr: Option<std::net::SocketAddr>,
}

impl MeshAdminAgent {
    /// Construct a `MeshAdminAgent` from a list of `(host_addr,
    /// host_agent_ref)` pairs.
    ///
    /// Builds both:
    /// - `hosts`: the forward map used to route admin queries to the
    ///   correct `HostMeshAgent`, and
    /// - `host_agents_by_actor_id`: a reverse index used during
    ///   reference resolution to recognize host-agent `ActorId`s and
    ///   resolve them as `NodeProperties::Host` rather than as
    ///   generic actors.
    ///
    /// The HTTP listen address is initialized to `None` and populated
    /// during `init()` after the server socket is bound.
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

/// Shared state for the reference-based `/v1/{*reference}` bridge
/// route.
///
/// The HTTP handler itself is intentionally thin and does not perform
/// any routing logic. Instead, it forwards each request into the
/// `MeshAdminAgent` actor via `ResolveReferenceMessage`, ensuring
/// resolution happens inside the actor message loop (with access to
/// actor messaging, timeouts, and indices).
struct BridgeState {
    /// Reference to the `MeshAdminAgent` actor that performs
    /// reference resolution.
    admin_ref: ActorRef<MeshAdminAgent>,
}

#[async_trait]
impl Actor for MeshAdminAgent {
    /// Initializes the mesh admin HTTP server.
    ///
    /// Binds an ephemeral local TCP listener, builds the axum router
    /// (legacy host/proc/actor routes plus the reference-based bridge
    /// route), and spawns the server in a background task. The chosen
    /// listen address is stored in `admin_addr` so it can be returned
    /// via `GetAdminAddr`.
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
    /// Returns the socket address the admin HTTP server is listening
    /// on (if started).
    ///
    /// This is populated during `init()` after binding the listener.
    /// If the agent hasn't been initialized yet (or failed to bind),
    /// the address is `None`.
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
    /// Resolves an opaque reference string into a `NodePayload` for
    /// TUI/HTTP consumers.
    ///
    /// Important: this handler never returns `Err`, because a handler
    /// error would crash the actor. Instead, failures are surfaced as
    /// `ResolveReferenceResponse(Err(..))`.
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
    /// Core resolver for the reference-based admin API.
    ///
    /// Parses the caller-provided `reference_string` (or handles the
    /// special `"root"` case), then dispatches to
    /// `resolve_host_node`, `resolve_proc_node`, or
    /// `resolve_actor_node` to assemble a fully-populated
    /// `NodePayload` (properties + child references).
    ///
    /// Note: this returns `Err` for internal use; the public
    /// `resolve` handler converts failures into
    /// `ResolveReferenceResponse(Err(..))` so the actor never crashes
    /// on
    /// lookup errors.
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

    /// Construct the synthetic root node for the reference tree.
    ///
    /// The root is not a real actor/proc; it's a convenience node
    /// that anchors navigation. Its children are the configured
    /// `HostMeshAgent` actor IDs (as reference strings), and its
    /// properties summarize the mesh at a glance (currently just
    /// `num_hosts`).
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

    /// Resolve a `HostMeshAgent` actor reference into a host-level
    /// `NodePayload`.
    ///
    /// Host nodes are identified by the `HostMeshAgent`’s `ActorId`,
    /// but their payload is derived by querying that agent for
    /// `HostDetails` and translating the result into a stable,
    /// navigable shape for the TUI/HTTP clients.
    ///
    /// Children are emitted as `ProcId` reference strings for every
    /// proc on the host: system/local procs are returned as plain
    /// `ProcId` strings (with the `[system] ` prefix stripped), while
    /// user procs are synthesized as `"{host_addr},{proc_name}"` to
    /// match the `ProcId::Direct` textual format. The navigation
    /// parent is `"root"`.
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

    /// Resolve a `ProcId` reference into a proc-level `NodePayload`.
    ///
    /// This looks up the owning host via the proc's `ProcId::Direct`
    /// address, queries the corresponding `HostMeshAgent` for
    /// `ProcDetails`, and converts that into a `NodePayload` with
    /// `NodeProperties::Proc`.
    ///
    /// The `is_system` flag is inferred by retrying the lookup using
    /// the host-admin convention for system/local procs (`"[system]
    /// <proc_id>"`) when the plain proc-name query returns no JSON.
    /// Children are the proc’s actor IDs (already serialized as full
    /// `ActorId` strings), and `parent` is set to the host-agent
    /// `ActorId` string for the owning host.
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
        let is_system;
        let response = if response.json.is_some() {
            is_system = false;
            response
        } else {
            is_system = true;
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
                is_system,
            },
            // ProcDetails.actors contains full ActorId.to_string() values.
            children: details.actors,
            parent: Some(host_agent_id),
        })
    }

    /// Resolve a non-host-agent `ActorId` reference into an
    /// actor-level `NodePayload`.
    ///
    /// Determines the owning proc/host from `actor_id.proc_id()`,
    /// routes the request to the corresponding `HostMeshAgent`, and
    /// fetches `ActorDetails` to populate `NodeProperties::Actor`
    /// (status/type/metrics plus an optional serialized flight
    /// recorder).
    ///
    /// The query uses the full `ActorId` string for an exact match
    /// (avoiding ambiguous name-only resolution), and if the initial
    /// lookup returns no JSON it retries using the system-proc naming
    /// convention (`"[system] <proc_id>"`) to support actors living
    /// under infrastructure procs. The returned payload links
    /// `parent` to the proc node (`proc_id.to_string()`) and uses
    /// `ActorDetails.children` as the next-hop references.
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
        // Pass the full ActorId string so that query_actor_details can
        // do an exact match via ActorId::from_str, rather than falling
        // back to a name-only match which may pick the wrong actor.
        let actor_name = actor_id.to_string();
        let response = RealClock
            .timeout(
                SINGLE_HOST_TIMEOUT,
                agent.get_actor_details(cx, proc_name.clone(), actor_name.clone()),
            )
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying actor details"))?
            .map_err(|e| anyhow::anyhow!("failed to query actor details: {}", e))?;

        // If the plain name lookup failed, this may be an actor in a
        // system/infrastructure proc. Retry with the "[system] " prefix.
        let response = if response.json.is_some() {
            response
        } else {
            let system_proc_name = format!("[system] {}", proc_id);
            RealClock
                .timeout(
                    SINGLE_HOST_TIMEOUT,
                    agent.get_actor_details(cx, system_proc_name, actor_name),
                )
                .await
                .map_err(|_| anyhow::anyhow!("timed out querying system actor details"))?
                .map_err(|e| anyhow::anyhow!("failed to query system actor details: {}", e))?
        };

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

/// Build the Axum router for the mesh admin HTTP server.
///
/// Exposes two route families:
/// - **Legacy, structured endpoints** under `/v1/hosts/...`
///   (host/proc/actor detail and tree dump), backed by
///   `MeshAdminState`.
/// - A **reference-based bridge** at `/v1/{*reference}` that resolves
///   any opaque reference string into a `NodePayload` by messaging
///   `MeshAdminAgent` via `BridgeState`.
///
/// The legacy routes are mounted first and are more specific, so they
/// take precedence over the wildcard bridge route when paths overlap.
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

/// Resolve an opaque reference string to a `NodePayload` via the
/// actor-based resolver.
///
/// Implements `GET /v1/{*reference}` for the reference-walking client
/// (e.g. the TUI):
/// - Decodes the wildcard path segment into the original reference
///   string (Axum does not percent-decode `{*reference}` captures).
/// - Sends `ResolveReferenceMessage::Resolve` to `MeshAdminAgent` and
///   awaits the reply.
/// - Maps resolver failures into appropriate `ApiError`s
///   (`bad_request`, `not_found`, `gateway_timeout`, or
///   `internal_error`).
async fn resolve_reference_bridge(
    State(state): State<Arc<BridgeState>>,
    Path(reference): Path<String>,
) -> Result<Json<NodePayload>, ApiError> {
    // Axum's wildcard may include a leading slash; strip it.
    let reference = reference.trim_start_matches('/');
    if reference.is_empty() {
        return Err(ApiError::bad_request("empty reference", None));
    }
    let reference = urlencoding::decode(reference)
        .map(|cow| cow.into_owned())
        .map_err(|_| {
            ApiError::bad_request(
                "malformed percent-encoding: decoded bytes are not valid UTF-8",
                None,
            )
        })?;

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

// Legacy

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

/// GET /v1/hosts -- list all hosts in the mesh, fanning out to each
/// host agent.
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

    // Verifies that MeshAdminAgent::build_root_payload constructs the
    // expected root node: identity/root metadata, correct Root
    // properties (num_hosts), and child links populated with the
    // stringified IDs of the configured host mesh-agent ActorRefs.
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

    // End-to-end smoke test for MeshAdminAgent::resolve that walks
    // the reference tree: root → host → system proc → host-agent
    // cross-reference. Verifies the reverse index routes the
    // HostMeshAgent ActorId to NodeProperties::Host (not Actor),
    // preventing the TUI's cycle detection from dropping that node.
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
        let host: Host<LocalProcManager<ProcManagerSpawnFn>> =
            Host::new(manager, ChannelTransport::Unix.any())
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
        if let NodeProperties::Proc { is_system, .. } = &proc_node.properties {
            assert!(is_system, "system proc should have is_system=true");
        } else {
            panic!("expected Proc properties, got {:?}", proc_node.properties);
        }
        assert_eq!(proc_node.parent, Some(host_child_ref_str.clone()));
        // The system proc should have at least the "agent" actor.
        assert!(
            !proc_node.children.is_empty(),
            "proc should have at least one actor child"
        );

        // -- 7. Cross-reference: system proc child is the host agent --
        //
        // The service proc's actor (agent[0]) IS the HostMeshAgent, so
        // it appears both as a host node (step 5) and as a child of
        // the system proc. The `host_agents_by_actor_id` reverse index
        // must route it to `resolve_host_node`, producing
        // `NodeProperties::Host` — not `NodeProperties::Actor`. This
        // is the scenario that caused the TUI's cycle detection to
        // silently drop the node.

        // The system proc must list the host agent among its children.
        let host_agent_id_str = host_agent_ref.actor_id().to_string();
        assert!(
            proc_node.children.contains(&host_agent_id_str),
            "system proc children {:?} should contain the host agent {}",
            proc_node.children,
            host_agent_id_str
        );

        // Resolve that child reference.
        let xref_resp = admin_ref
            .resolve(&client, host_agent_id_str.clone())
            .await
            .unwrap();
        let xref_node = xref_resp.0.unwrap();

        // It must resolve as Host, not Actor, because the reverse
        // index identifies it as a host agent.
        assert!(
            matches!(xref_node.properties, NodeProperties::Host { .. }),
            "host agent child should resolve as Host, got {:?}",
            xref_node.properties
        );

        // The identity must match the host node resolved in step 5 —
        // same ActorId appears at both levels of the tree.
        assert_eq!(
            xref_node.identity, host_node.identity,
            "cross-referenced host agent identity should match the host node from step 5"
        );

        // Sanity: same properties as the host node from step 5.
        assert_eq!(xref_node.properties, host_node.properties);
        assert_eq!(xref_node.children, host_node.children);
    }

    // Verifies MeshAdminAgent::resolve correctly sets
    // NodeProperties::Proc.is_system for both built-in host procs
    // (system/local) and a dynamically created user proc. Spawns a
    // user proc via CreateOrUpdate<ProcSpec>, resolves all host
    // proc-children, and asserts only the user proc reports
    // is_system=false while the others report is_system=true.
    #[tokio::test]
    async fn test_is_system_flag_for_system_and_user_procs() {
        use std::time::Duration;

        use hyperactor::Proc;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::clock::Clock;
        use hyperactor::clock::RealClock;
        use hyperactor::host::Host;
        use hyperactor::host::LocalProcManager;

        use crate::Name;
        use crate::host_mesh::mesh_agent::HostAgentMode;
        use crate::host_mesh::mesh_agent::ProcManagerSpawnFn;
        use crate::mesh_agent::ProcMeshAgent;
        use crate::resource;
        use crate::resource::ProcSpec;
        use crate::resource::Rank;

        // Stand up a local in-process Host with a HostMeshAgent.
        let spawn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcMeshAgent::boot_v1(proc))));
        let manager: LocalProcManager<ProcManagerSpawnFn> = LocalProcManager::new(spawn);
        let host: Host<LocalProcManager<ProcManagerSpawnFn>> =
            Host::new(manager, ChannelTransport::Unix.any())
                .await
                .unwrap();
        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_agent_handle = system_proc
            .spawn("agent", HostMeshAgent::new(HostAgentMode::Local(host)))
            .unwrap();
        let host_agent_ref: ActorRef<HostMeshAgent> = host_agent_handle.bind();
        let host_addr_str = host_addr.to_string();

        // Spawn MeshAdminAgent on a separate proc.
        let admin_proc = Proc::direct(ChannelTransport::Unix.any(), "admin".to_string()).unwrap();
        use hyperactor::test_utils::proc_supervison::ProcSupervisionCoordinator;
        let _supervision = ProcSupervisionCoordinator::set(&admin_proc).await.unwrap();
        let admin_handle = admin_proc
            .spawn(
                "mesh_admin",
                MeshAdminAgent::new(vec![(host_addr_str.clone(), host_agent_ref.clone())]),
            )
            .unwrap();
        let admin_ref: ActorRef<MeshAdminAgent> = admin_handle.bind();

        // Create a bare client instance for sending messages.
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _handle) = client_proc.instance("client").unwrap();

        // Spawn a user proc via CreateOrUpdate<ProcSpec>.
        let user_proc_name = Name::new("user_proc").unwrap();
        host_agent_ref
            .send(
                &client,
                resource::CreateOrUpdate {
                    name: user_proc_name.clone(),
                    rank: Rank::new(0),
                    spec: ProcSpec::default(),
                },
            )
            .unwrap();

        // Wait for the user proc to boot.
        RealClock.sleep(Duration::from_secs(2)).await;

        // Resolve the host to get its children (system + user procs).
        let host_resp = admin_ref
            .resolve(&client, host_agent_ref.actor_id().to_string())
            .await
            .unwrap();
        let host_node = host_resp.0.unwrap();

        // The host should have at least 3 children: system proc,
        // local proc, and our user proc.
        assert!(
            host_node.children.len() >= 3,
            "expected at least 3 proc children (2 system + 1 user), got {}",
            host_node.children.len()
        );

        // Resolve each proc child and verify is_system.
        let user_proc_name_str = user_proc_name.to_string();
        let mut found_system = false;
        let mut found_user = false;
        for child_ref_str in &host_node.children {
            let resp = admin_ref
                .resolve(&client, child_ref_str.clone())
                .await
                .unwrap();
            let node = resp.0.unwrap();
            if let NodeProperties::Proc {
                is_system,
                proc_name,
                ..
            } = &node.properties
            {
                if proc_name.contains(&user_proc_name_str) {
                    assert!(
                        !is_system,
                        "user proc '{}' should have is_system=false",
                        proc_name
                    );
                    found_user = true;
                } else {
                    assert!(
                        *is_system,
                        "system proc '{}' should have is_system=true",
                        proc_name
                    );
                    found_system = true;
                }
            } else {
                // Host agent cross-reference — skip.
            }
        }
        assert!(
            found_system,
            "should have resolved at least one system proc"
        );
        assert!(found_user, "should have resolved the user proc");
    }
}
