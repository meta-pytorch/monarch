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
//! uniform, reference-based HTTP API over an entire host mesh. Every addressable entity in the mesh is represented as a
//! `NodePayload` and resolved via an opaque reference string.
//!
//! Incoming HTTP requests are bridged into the actor message loop
//! using `ResolveReferenceMessage`, ensuring that all topology
//! resolution and data collection happens through actor messaging.
//! The agent fans out to `HostMeshAgent` instances to fetch host,
//! proc, and actor details, then normalizes them into a single
//! tree-shaped model (`NodeProperties` + children references)
//! suitable for topology-agnostic clients such as the admin TUI.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
use hyperactor::PortRef;
use hyperactor::ProcId;
use hyperactor::RefClient;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::introspect::IntrospectMessage;
use hyperactor::introspect::NodePayload;
use hyperactor::introspect::NodeProperties;
use hyperactor::mailbox::open_once_port;
use hyperactor::reference::Reference;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpListener;
use typeuri::Named;

use crate::global_root_client;
use crate::host_mesh::mesh_agent::HostMeshAgent;
use crate::host_mesh::mesh_agent::parse_system_proc_ref;
use crate::host_mesh::mesh_agent::system_proc_ref;

/// Timeout for targeted queries that hit a single, specific host.
/// Longer than the fan-out timeout because the caller explicitly chose
/// this host and is willing to wait for a response.
const SINGLE_HOST_TIMEOUT: Duration = Duration::from_secs(30);

/// Structured error response following the gateway RFC envelope
/// pattern.
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    /// Machine-readable error code (e.g. "not_found", "bad_request").
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Additional context about the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Wrapper for the structured error envelope.
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiErrorEnvelope {
    pub error: ApiError,
}

impl ApiError {
    /// Create a "not_found" error.
    pub fn not_found(message: impl Into<String>, details: Option<Value>) -> Self {
        Self {
            code: "not_found".to_string(),
            message: message.into(),
            details,
        }
    }

    /// Create a "bad_request" error.
    pub fn bad_request(message: impl Into<String>, details: Option<Value>) -> Self {
        Self {
            code: "bad_request".to_string(),
            message: message.into(),
            details,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let status = match self.code.as_str() {
            "not_found" => StatusCode::NOT_FOUND,
            "bad_request" => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let envelope = ApiErrorEnvelope { error: self };
        (status, Json(envelope)).into_response()
    }
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
    /// Binds an ephemeral local TCP listener, builds the axum router,
    /// and spawns the server in a background task. The chosen
    /// listen address is stored in `admin_addr` so it can be returned
    /// via `GetAdminAddr`.
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        self.admin_addr = Some(addr);

        let bridge_state = Arc::new(BridgeState {
            admin_ref: ActorRef::attest(this.self_id().clone()),
        });
        let router = create_mesh_admin_router(bridge_state);
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                tracing::error!("mesh admin server error: {}", e);
            }
        });

        tracing::info!("mesh admin server listening on http://{}", addr);
        Ok(())
    }

    /// Swallow undeliverable bounces instead of crashing.
    ///
    /// The admin agent sends IntrospectMessage to actors that may have
    /// exited or whose ports are not bound.  When the reply bounces
    /// back as `Undeliverable`, the default `bail!()` implementation
    /// would kill this agent — taking down the entire admin HTTP
    /// server.  We log and move on.
    async fn handle_undeliverable_message(
        &mut self,
        _cx: &Instance<Self>,
        hyperactor::mailbox::Undeliverable(envelope): hyperactor::mailbox::Undeliverable<
            hyperactor::mailbox::MessageEnvelope,
        >,
    ) -> Result<(), anyhow::Error> {
        tracing::debug!(
            "admin agent: undeliverable message to {} (port not bound?), ignoring",
            envelope.dest(),
        );
        Ok(())
    }
}

/// Manual Handler impl — swallows `reply.send()` failures so the
/// admin agent stays alive when the HTTP caller disconnects.
#[async_trait]
impl Handler<MeshAdminMessage> for MeshAdminAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: MeshAdminMessage,
    ) -> Result<(), anyhow::Error> {
        match msg {
            MeshAdminMessage::GetAdminAddr { reply } => {
                let resp = MeshAdminAddrResponse {
                    addr: self.admin_addr.map(|a| a.to_string()),
                };
                if let Err(e) = reply.send(cx, resp) {
                    tracing::debug!("GetAdminAddr reply failed (caller gone?): {e}");
                }
            }
        }
        Ok(())
    }
}

/// Manual Handler impl — swallows `reply.send()` failures so the
/// admin agent stays alive when the HTTP caller disconnects.
#[async_trait]
impl Handler<ResolveReferenceMessage> for MeshAdminAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        msg: ResolveReferenceMessage,
    ) -> Result<(), anyhow::Error> {
        match msg {
            ResolveReferenceMessage::Resolve {
                reference_string,
                reply,
            } => {
                let response = ResolveReferenceResponse(
                    self.resolve_reference(cx, &reference_string)
                        .await
                        .map_err(|e| format!("{:#}", e)),
                );
                if let Err(e) = reply.send(cx, response) {
                    tracing::debug!("Resolve reply failed (caller gone?): {e}");
                }
            }
        }
        Ok(())
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

        // System proc refs are non-addressable children of a
        // HostMeshAgent. They use the "[system] <proc_id>" format
        // which does not parse as a Reference, so intercept them
        // before attempting Reference::from_str.
        if parse_system_proc_ref(reference_string).is_some() {
            return self.resolve_system_proc_node(cx, reference_string).await;
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
    /// Sends `IntrospectMessage::Query` directly to the
    /// `HostMeshAgent`, which returns a `NodePayload` with
    /// `NodeProperties::Host` and the host's children. The resolver
    /// overrides `parent` to `"root"` since the host agent
    /// doesn't know its position in the navigation tree.
    async fn resolve_host_node(
        &self,
        cx: &Context<'_, Self>,
        actor_id: &ActorId,
    ) -> Result<NodePayload, anyhow::Error> {
        let introspect_port = PortRef::<IntrospectMessage>::attest_message_port(actor_id);
        let (reply_handle, reply_rx) = open_once_port::<NodePayload>(cx);
        introspect_port.send(
            cx,
            IntrospectMessage::Query {
                reply: reply_handle.bind(),
            },
        )?;

        let mut payload = RealClock
            .timeout(SINGLE_HOST_TIMEOUT, reply_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying host agent"))?
            .map_err(|e| anyhow::anyhow!("failed to receive host introspection: {}", e))?;

        payload.parent = Some("root".to_string());
        Ok(payload)
    }

    /// Resolve a system proc reference (e.g. `"[system]
    /// tcp!127.0.0.1:12345,system"`) into a proc-level `NodePayload`.
    ///
    /// System procs are non-addressable children of a
    /// `HostMeshAgent`. The resolver strips the `"[system] "` prefix,
    /// parses the embedded `ProcId`, finds the owning
    /// `HostMeshAgent`, and sends `IntrospectMessage::QueryChild`
    /// with `Reference::Proc` so the host can answer on the proc's
    /// behalf.
    async fn resolve_system_proc_node(
        &self,
        cx: &Context<'_, Self>,
        reference_string: &str,
    ) -> Result<NodePayload, anyhow::Error> {
        let proc_id_str = parse_system_proc_ref(reference_string)
            .ok_or_else(|| anyhow::anyhow!("not a system proc reference: {}", reference_string))?;
        let proc_id: ProcId = proc_id_str.parse().map_err(|e| {
            anyhow::anyhow!(
                "invalid proc id in system ref '{}': {}",
                reference_string,
                e
            )
        })?;
        let host_addr = match &proc_id {
            ProcId::Direct(addr, _) => addr.to_string(),
            ProcId::Ranked(world_id, _) => {
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

        let child_ref = Reference::Proc(proc_id);
        let introspect_port = PortRef::<IntrospectMessage>::attest_message_port(agent.actor_id());
        let (reply_handle, reply_rx) = open_once_port::<NodePayload>(cx);
        introspect_port.send(
            cx,
            IntrospectMessage::QueryChild {
                child_ref,
                reply: reply_handle.bind(),
            },
        )?;

        let mut payload = RealClock
            .timeout(SINGLE_HOST_TIMEOUT, reply_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying system proc"))?
            .map_err(|e| anyhow::anyhow!("failed to receive system proc introspection: {}", e))?;

        // Override the identity to match the child reference string
        // from the host's children list, so that the TUI can
        // correlate navigated nodes with their parent's child
        // entries.
        payload.identity = reference_string.to_string();
        Ok(payload)
    }

    /// Resolve a `ProcId` reference into a proc-level `NodePayload`.
    ///
    /// First tries `IntrospectMessage::QueryChild` against the owning
    /// `HostMeshAgent` (system procs). If that returns an error
    /// payload, falls back to sending `IntrospectMessage::Query` to
    /// the conventional `ProcMeshAgent` actor (`<proc_id>/agent[0]`)
    /// for user procs.
    async fn resolve_proc_node(
        &self,
        cx: &Context<'_, Self>,
        proc_id: &ProcId,
    ) -> Result<NodePayload, anyhow::Error> {
        let (host_addr, _proc_name) = match proc_id {
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

        // Try as a system proc first (QueryChild to the host agent).
        let child_ref = Reference::Proc(proc_id.clone());
        let introspect_port = PortRef::<IntrospectMessage>::attest_message_port(agent.actor_id());
        let (reply_handle, reply_rx) = open_once_port::<NodePayload>(cx);
        introspect_port.send(
            cx,
            IntrospectMessage::QueryChild {
                child_ref,
                reply: reply_handle.bind(),
            },
        )?;

        let payload = RealClock
            .timeout(SINGLE_HOST_TIMEOUT, reply_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying proc details"))?
            .map_err(|e| anyhow::anyhow!("failed to receive proc introspection: {}", e))?;

        // If the host recognized the proc, use its response directly.
        if !matches!(payload.properties, NodeProperties::Error { .. }) {
            return Ok(payload);
        }

        // Fall back to querying the ProcMeshAgent directly (user
        // procs). The conventional ProcMeshAgent ActorId is
        // <proc_id>/agent[0].
        let mesh_agent_id = proc_id.actor_id("agent", 0);
        let agent_port = PortRef::<IntrospectMessage>::attest_message_port(&mesh_agent_id);
        let (reply_handle, reply_rx) = open_once_port::<NodePayload>(cx);
        agent_port.send(
            cx,
            IntrospectMessage::Query {
                reply: reply_handle.bind(),
            },
        )?;

        let mut payload = RealClock
            .timeout(SINGLE_HOST_TIMEOUT, reply_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying proc mesh agent"))?
            .map_err(|e| {
                anyhow::anyhow!("failed to receive proc mesh agent introspection: {}", e)
            })?;

        // Set parent to the host agent.
        let host_agent_id = agent.actor_id().to_string();
        payload.parent = Some(host_agent_id);
        Ok(payload)
    }

    /// Resolve a non-host-agent `ActorId` reference into an
    /// actor-level `NodePayload`.
    ///
    /// Sends `IntrospectMessage::Query` directly to the target actor
    /// via `PortRef::attest_message_port`. The blanket handler
    /// returns a `NodePayload` with `NodeProperties::Actor` (or a
    /// domain-specific override like `NodeProperties::Proc` for
    /// `ProcMeshAgent`).
    ///
    /// The resolver sets `parent` based on the actor's position
    /// in the topology: if the actor lives in a system proc, the
    /// parent is the system proc ref; otherwise it's the
    /// `ProcMeshAgent` ActorId.
    async fn resolve_actor_node(
        &self,
        cx: &Context<'_, Self>,
        actor_id: &ActorId,
    ) -> Result<NodePayload, anyhow::Error> {
        let introspect_port = PortRef::<IntrospectMessage>::attest_message_port(actor_id);
        let (reply_handle, reply_rx) = open_once_port::<NodePayload>(cx);
        introspect_port.send(
            cx,
            IntrospectMessage::Query {
                reply: reply_handle.bind(),
            },
        )?;

        let mut payload = RealClock
            .timeout(SINGLE_HOST_TIMEOUT, reply_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying actor {}", actor_id))?
            .map_err(|e| anyhow::anyhow!("failed to receive actor introspection: {}", e))?;

        // Set parent based on topology. If the actor returns Proc
        // properties (ProcMeshAgent override), its parent is the host
        // agent. Otherwise, it's a regular actor and its parent is
        // the proc.
        let proc_id = actor_id.proc_id();
        match &payload.properties {
            NodeProperties::Proc { .. } => {
                // ProcMeshAgent: parent is the host agent.
                let host_addr = match proc_id {
                    ProcId::Direct(addr, _) => addr.to_string(),
                    _ => proc_id.to_string(),
                };
                if let Some(agent) = self.hosts.get(&host_addr) {
                    payload.parent = Some(agent.actor_id().to_string());
                }
            }
            _ => {
                // Regular actor: parent is the proc. We use the
                // system proc ref format if the proc is a known
                // system/local proc, otherwise the ProcMeshAgent
                // ActorId.
                let host_addr = match proc_id {
                    ProcId::Direct(addr, _) => Some(addr.to_string()),
                    _ => None,
                };

                // Check if this is a system proc by seeing if the
                // host lists it as a system proc child.
                let is_system = host_addr
                    .as_ref()
                    .and_then(|addr| self.hosts.get(addr))
                    .map(|agent| {
                        // If the actor's proc is the same proc the
                        // host agent lives in (system proc) or any
                        // other known infrastructure proc, treat it
                        // as system.
                        agent.actor_id().proc_id() == proc_id
                    })
                    .unwrap_or(false);

                if is_system {
                    payload.parent = Some(system_proc_ref(&proc_id.to_string()));
                } else {
                    // User proc: parent is ProcMeshAgent.
                    let mesh_agent_id = proc_id.actor_id("agent", 0).to_string();
                    payload.parent = Some(mesh_agent_id);
                }
            }
        }

        Ok(payload)
    }
}

/// Build the Axum router for the mesh admin HTTP server.
///
/// Serves two routes, both backed by the introspection-based
/// resolver:
/// - `GET /v1/tree` — ASCII topology dump (walks the reference graph
///   and formats the result as a human-readable tree; intended for
///   quick `curl` inspection).
/// - `GET /v1/{*reference}` — JSON `NodePayload` for a single
///   reference (the primary API used by the TUI and programmatic
///   clients).
fn create_mesh_admin_router(bridge_state: Arc<BridgeState>) -> Router {
    Router::new()
        // `/v1/tree` is more specific than the wildcard and takes
        // precedence in Axum's router.
        .route("/v1/tree", get(tree_dump))
        .route("/v1/{*reference}", get(resolve_reference_bridge))
        .with_state(bridge_state)
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

/// Timeout for the tree dump fan-out. Kept short so that slow or dead
/// hosts don't block the response.
const TREE_TIMEOUT: Duration = Duration::from_secs(10);

/// `GET /v1/tree` — ASCII topology dump.
///
/// Walks the reference graph starting from `"root"`, resolving each
/// host and its proc children, and formats the result as a
/// human-readable ASCII tree suitable for quick `curl` inspection.
/// Each line includes a clickable URL for drilling into that node via
/// the reference API. Built on top of the same
/// `ResolveReferenceMessage` protocol used by the TUI.
///
/// Output format:
/// ```text
/// unix:@hash  ->  http://127.0.0.1:port/v1/...
/// ├── service  ->  http://127.0.0.1:port/v1/...
/// │   ├── agent[0]  ->  http://127.0.0.1:port/v1/...
/// │   └── client[0]  ->  http://127.0.0.1:port/v1/...
/// ├── local  ->  http://127.0.0.1:port/v1/...
/// └── philosophers_0  ->  http://127.0.0.1:port/v1/...
///     ├── agent[0]  ->  http://127.0.0.1:port/v1/...
///     └── philosopher[0]  ->  http://127.0.0.1:port/v1/...
/// ```
async fn tree_dump(
    State(state): State<Arc<BridgeState>>,
    headers: axum::http::header::HeaderMap,
) -> Result<String, ApiError> {
    let cx = global_root_client();

    // Build base URL from the Host header for clickable links.
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    let base_url = format!("{}://{}", scheme, host);

    // Resolve root.
    let root_resp = RealClock
        .timeout(
            TREE_TIMEOUT,
            state.admin_ref.resolve(cx, "root".to_string()),
        )
        .await
        .map_err(|_| ApiError {
            code: "gateway_timeout".to_string(),
            message: "timed out resolving root".to_string(),
            details: None,
        })?
        .map_err(|e| ApiError {
            code: "internal_error".to_string(),
            message: format!("failed to resolve root: {}", e),
            details: None,
        })?;

    let root = root_resp.0.map_err(|e| ApiError {
        code: "internal_error".to_string(),
        message: e,
        details: None,
    })?;

    let mut output = String::new();

    // Resolve each host, its proc children, and their actor children.
    for host_ref in &root.children {
        let resp = RealClock
            .timeout(TREE_TIMEOUT, state.admin_ref.resolve(cx, host_ref.clone()))
            .await;

        let payload = match resp {
            Ok(Ok(r)) => r.0.ok(),
            _ => None,
        };

        match payload {
            Some(host_node) => {
                // Host header: show the addr from NodeProperties::Host
                // if available, otherwise fall back to the reference
                // string.
                let header = match &host_node.properties {
                    NodeProperties::Host { addr, .. } => addr.clone(),
                    _ => host_ref.clone(),
                };
                let host_url = format!("{}/v1/{}", base_url, urlencoding::encode(host_ref));
                output.push_str(&format!("{}  ->  {}\n", header, host_url));

                // Proc children with box-drawing connectors.
                let num_procs = host_node.children.len();
                for (i, proc_ref) in host_node.children.iter().enumerate() {
                    let is_last_proc = i == num_procs - 1;
                    let proc_connector = if is_last_proc {
                        "└── "
                    } else {
                        "├── "
                    };
                    let proc_name = derive_tree_label(proc_ref);
                    let proc_url = format!("{}/v1/{}", base_url, urlencoding::encode(proc_ref));
                    output.push_str(&format!(
                        "{}{}  ->  {}\n",
                        proc_connector, proc_name, proc_url
                    ));

                    // Resolve the proc to get its actor children.
                    let proc_resp = RealClock
                        .timeout(TREE_TIMEOUT, state.admin_ref.resolve(cx, proc_ref.clone()))
                        .await;
                    let proc_payload = match proc_resp {
                        Ok(Ok(r)) => r.0.ok(),
                        _ => None,
                    };
                    if let Some(proc_node) = proc_payload {
                        let num_actors = proc_node.children.len();
                        let child_prefix = if is_last_proc { "    " } else { "│   " };
                        for (j, actor_ref) in proc_node.children.iter().enumerate() {
                            let actor_connector = if j == num_actors - 1 {
                                "└── "
                            } else {
                                "├── "
                            };
                            let actor_label = derive_actor_label(actor_ref);
                            let actor_url =
                                format!("{}/v1/{}", base_url, urlencoding::encode(actor_ref));
                            output.push_str(&format!(
                                "{}{}{}  ->  {}\n",
                                child_prefix, actor_connector, actor_label, actor_url
                            ));
                        }
                    }
                }
                output.push('\n');
            }
            _ => {
                output.push_str(&format!("{} (unreachable)\n\n", host_ref));
            }
        }
    }
    Ok(output)
}

/// Derive a short display label from a reference string for the ASCII
/// tree.
///
/// Extracts the proc name — the meaningful identifier for tree
/// display — from the various reference formats emitted by
/// `HostMeshAgent`'s children list:
///
/// - System proc ref `"[system] unix:@hash,service"` → `"service"`
/// - ProcMeshAgent ActorId `"unix:@hash,my_proc,agent[0]"` →
///   `"my_proc"`
/// - Bare ProcId `"unix:@hash,my_proc"` → `"my_proc"`
///
/// Note: `ActorId::Display` for `ProcId::Direct` uses commas as
/// separators (`proc_id,actor_name[idx]`), not slashes.
fn derive_tree_label(reference: &str) -> String {
    // System proc refs: "[system] transport!addr,name" → name.
    if let Some(proc_id_str) = parse_system_proc_ref(reference) {
        if let Some((_addr, name)) = proc_id_str.rsplit_once(',') {
            return name.to_string();
        }
        return proc_id_str.to_string();
    }
    // ActorId (Direct): "transport!addr,proc_name,actor[idx]"
    // ProcId (Direct): "transport!addr,proc_name"
    // In both cases, split on ',' and take the second segment (the
    // proc name).
    let parts: Vec<&str> = reference.splitn(3, ',').collect();
    match parts.len() {
        // "addr,proc_name,actor[idx]" → proc_name
        3 => parts[1].to_string(),
        // "addr,proc_name" → proc_name
        2 => parts[1].to_string(),
        _ => reference.to_string(),
    }
}

/// Derive a short display label for an actor reference.
///
/// Actor references are `ActorId` strings in the format
/// `"transport!addr,proc_name,actor_name[idx]"`. This extracts the
/// actor name with index (e.g. `"philosopher[0]"`).
fn derive_actor_label(reference: &str) -> String {
    let parts: Vec<&str> = reference.splitn(3, ',').collect();
    match parts.len() {
        // "addr,proc_name,actor[idx]" → actor[idx]
        3 => parts[2].to_string(),
        // "addr,name" → name
        2 => parts[1].to_string(),
        _ => reference.to_string(),
    }
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
