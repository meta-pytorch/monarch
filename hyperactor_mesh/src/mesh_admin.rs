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
//! uniform, reference-based HTTP API over an entire host mesh. Every
//! addressable entity in the mesh is represented as a `NodePayload`
//! and resolved via an opaque reference string.
//!
//! Incoming HTTP requests are bridged into the actor message loop
//! using `ResolveReferenceMessage`, ensuring that all topology
//! resolution and data collection happens through actor messaging.
//! The agent fans out to `HostMeshAgent` instances to fetch host,
//! proc, and actor details, then normalizes them into a single
//! tree-shaped model (`NodeProperties` + children references)
//! suitable for topology-agnostic clients such as the admin TUI.
//!
//! # Introspection visibility policy
//!
//! Admin tooling only displays **introspectable** nodes: entities
//! that are reachable via actor messaging and respond to
//! [`IntrospectMessage`]. Infrastructure procs that are
//! **non-routable** are intentionally **opaque** to introspection and
//! are omitted from the navigation graph.
//!
//! ## Definitions
//!
//! **Routable** — an entity is routable if the system can address it
//! via the routing layer and successfully deliver a message to it
//! using a `Reference` / `ActorId` (i.e., there exists a live mailbox
//! sender reachable through normal routing). Practical test: "can I
//! send `IntrospectMessage::Query` to it and get a reply?"
//!
//! **Non-routable** — an entity is non-routable if it has no
//! externally reachable mailbox sender in the routing layer, so
//! message delivery is impossible by construction (even if you know
//! its name). Examples: `hyperactor_runtime[0]`, `mailbox_server[N]`,
//! `local[N]` — these use `PanickingMailboxSender` and are never
//! bound to the router.
//!
//! **Introspectable** — tooling can obtain a `NodePayload` for this
//! node by sending `IntrospectMessage` to a routable actor.
//!
//! **Opaque** — the node exists but is not introspectable via
//! messaging; tooling cannot observe it through the introspection
//! protocol.
//!
//! ## Proc visibility
//!
//! A proc is not directly introspected; actors are. Tooling
//! synthesizes proc-level nodes by grouping introspectable actors by
//! `ProcId`.
//!
//! A proc is visible iff there exists at least one actor on that proc
//! whose `ActorId` is deliverable via the routing layer (i.e., the
//! actor has a bound mailbox sender reachable through normal routing)
//! and responds to `IntrospectMessage`.
//!
//! The rule is: **if an entity is routable via the mesh routing layer
//! (i.e., tooling can deliver `IntrospectMessage::Query` to one of its
//! actors), then it is introspectable and appears in the admin graph.**
//!
//! ## Navigation identity invariant
//!
//! Every `NodePayload` in the topology tree satisfies two properties:
//!
//! 1. **Identity = reference**: a node's `identity` field must equal the
//!    reference string that was used to resolve it. If the TUI asks for
//!    reference `R` and gets back a payload, `payload.identity == R`.
//!
//! 2. **Parent coherence**: a node's `parent` field must equal the
//!    `identity` of the node it appears under. If node `P` lists `R` in
//!    its `children`, then the payload for `R` must have
//!    `payload.parent == Some(P.identity)`.
//!
//! Together these ensure that the TUI can correlate responses to tree
//! nodes, and that upward/downward navigation is consistent.

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
use crate::host_mesh::mesh_agent::HostId;
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

    /// `ActorId` of the process-global root client (`client[0]` on
    /// `mesh_root_client_proc`), exposed as a first-class child of the
    /// root node. Routable and introspectable via the blanket
    /// `Handler<IntrospectMessage>`.
    root_client_actor_id: Option<ActorId>,

    /// This agent's own `ActorId`, captured during `init`. Used to
    /// include the admin proc as a visible node in the introspection
    /// tree (the principle: "if you can send it a message, you can
    /// introspect it").
    self_actor_id: Option<ActorId>,

    /// Bound address of the admin HTTP server once started in `init`.
    admin_addr: Option<std::net::SocketAddr>,

    /// When the mesh was started (ISO-8601 timestamp).
    started_at: String,

    /// Username who started the mesh.
    started_by: String,
}

impl MeshAdminAgent {
    /// Construct a `MeshAdminAgent` from a list of `(host_addr,
    /// host_agent_ref)` pairs and an optional root client `ActorId`.
    ///
    /// Builds both:
    /// - `hosts`: the forward map used to route admin queries to the
    ///   correct `HostMeshAgent`, and
    /// - `host_agents_by_actor_id`: a reverse index used during
    ///   reference resolution to recognize host-agent `ActorId`s and
    ///   resolve them as `NodeProperties::Host` rather than as
    ///   generic actors.
    ///
    /// When `root_client_actor_id` is `Some`, the root client appears
    /// as a first-class child of the root node in the introspection
    /// tree.
    ///
    /// The HTTP listen address is initialized to `None` and populated
    /// during `init()` after the server socket is bound.
    pub fn new(
        hosts: Vec<(String, ActorRef<HostMeshAgent>)>,
        root_client_actor_id: Option<ActorId>,
    ) -> Self {
        let host_agents_by_actor_id: HashMap<ActorId, String> = hosts
            .iter()
            .map(|(addr, agent_ref)| (agent_ref.actor_id().clone(), addr.clone()))
            .collect();

        // Capture start time and username
        let started_at = chrono::Utc::now().to_rfc3339();
        let started_by = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "unknown".to_string());

        Self {
            hosts: hosts.into_iter().collect(),
            host_agents_by_actor_id,
            root_client_actor_id,
            self_actor_id: None,
            admin_addr: None,
            started_at,
            started_by,
        }
    }
}

impl std::fmt::Debug for MeshAdminAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshAdminAgent")
            .field("hosts", &self.hosts.keys().collect::<Vec<_>>())
            .field("host_agents", &self.host_agents_by_actor_id.len())
            .field("root_client_actor_id", &self.root_client_actor_id)
            .field("self_actor_id", &self.self_actor_id)
            .field("admin_addr", &self.admin_addr)
            .field("started_at", &self.started_at)
            .field("started_by", &self.started_by)
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
        self.self_actor_id = Some(this.self_id().clone());

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

    /// Swallow undeliverable message bounces instead of crashing.
    ///
    /// The admin agent sends `IntrospectMessage` to actors that may
    /// not have the introspection port bound (e.g. actors spawned
    /// via `cx.spawn()` whose `#[export]` list does not include it).
    /// When the message cannot be delivered, the routing layer
    /// bounces an `Undeliverable` back to the sender. The default
    /// `Actor::handle_undeliverable_message` calls `bail!()`, which
    /// would kill this admin agent and — via supervision cascade —
    /// take down the entire admin process with `exit(1)`.
    ///
    /// Since the admin agent is best-effort infrastructure, an
    /// undeliverable introspection probe is not a fatal error.
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
    /// Dispatches `MeshAdminMessage` variants.
    ///
    /// Reply-send failures are swallowed because a dropped receiver
    /// (e.g. the HTTP bridge timed out) is not an error — the caller
    /// simply went away. Propagating the failure would crash the admin
    /// agent and take down the entire process.
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
    /// Dispatches `ResolveReferenceMessage` variants.
    ///
    /// The inner `resolve_reference` call never returns `Err` to the
    /// handler — failures are captured in the response payload.
    /// Reply-send failures are swallowed for the same reason as
    /// `MeshAdminMessage`: a dropped receiver means the caller (HTTP
    /// bridge) went away, which must not crash the admin agent.
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
    /// The returned payload satisfies the **navigation identity
    /// invariant** (see module docs): `payload.identity ==
    /// reference_string`, and `payload.parent` equals the identity of
    /// the node this one appears under.
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

        // Host refs use the "host:<actor_id>" format so they are
        // unambiguous from plain actor references. The same
        // HostMeshAgent ActorId can appear both as a host (from root)
        // and as an actor (from a proc's children list).
        if let Ok(host_id) = reference_string.parse::<HostId>() {
            return self.resolve_host_node(cx, &host_id.0).await;
        }

        let reference: Reference = reference_string
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid reference '{}': {}", reference_string, e))?;

        match &reference {
            Reference::Proc(proc_id) if self.standalone_proc_anchor(proc_id).is_some() => {
                self.resolve_standalone_proc_node(cx, proc_id).await
            }
            Reference::Proc(proc_id) => self.resolve_proc_node(cx, proc_id).await,
            Reference::Actor(actor_id) => self.resolve_actor_node(cx, actor_id).await,
            _ => Err(anyhow::anyhow!(
                "unsupported reference type: {}",
                reference_string
            )),
        }
    }

    /// Returns the known actors on standalone procs — procs not
    /// managed by any host but whose actors are routable and
    /// introspectable. Each proc appears as a root child; the
    /// actor is the "anchor" used to discover the proc's contents.
    fn standalone_proc_actors(&self) -> impl Iterator<Item = &ActorId> {
        self.root_client_actor_id
            .iter()
            .chain(self.self_actor_id.iter())
    }

    /// If `proc_id` belongs to a standalone proc, return the anchor
    /// actor on that proc. Returns `None` for host-managed procs.
    fn standalone_proc_anchor(&self, proc_id: &ProcId) -> Option<&ActorId> {
        self.standalone_proc_actors()
            .find(|actor_id| *actor_id.proc_id() == *proc_id)
    }

    /// Returns true if `actor_id` lives on a standalone proc.
    fn is_standalone_proc_actor(&self, actor_id: &ActorId) -> bool {
        self.standalone_proc_actors()
            .any(|a| *a.proc_id() == *actor_id.proc_id())
    }

    /// Construct the synthetic root node for the reference tree.
    ///
    /// The root is not a real actor/proc; it's a convenience node
    /// that anchors navigation. Its children are the configured
    /// `HostMeshAgent` actor IDs (as reference strings) plus any
    /// standalone procs (root client proc, admin proc).
    fn build_root_payload(&self) -> NodePayload {
        let mut children: Vec<String> = self
            .hosts
            .values()
            .map(|agent| HostId(agent.actor_id().clone()).to_string())
            .collect();
        for actor_id in self.standalone_proc_actors() {
            children.push(actor_id.proc_id().to_string());
        }
        NodePayload {
            identity: "root".to_string(),
            properties: NodeProperties::Root {
                num_hosts: self.hosts.len(),
                started_at: self.started_at.clone(),
                started_by: self.started_by.clone(),
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
                view: hyperactor::introspect::IntrospectView::Entity,
                reply: reply_handle.bind(),
            },
        )?;

        let mut payload = RealClock
            .timeout(SINGLE_HOST_TIMEOUT, reply_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out querying host agent"))?
            .map_err(|e| anyhow::anyhow!("failed to receive host introspection: {}", e))?;

        payload.identity = HostId(actor_id.clone()).to_string();
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
        // Set parent to the host reference (host:<actor_id>).
        payload.parent = Some(HostId(agent.actor_id().clone()).to_string());
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
                view: hyperactor::introspect::IntrospectView::Entity,
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

        // The ProcMeshAgent sets identity to its own ActorId, but the
        // caller asked for the proc by ProcId. Override so the
        // payload matches the reference the TUI navigated to.
        payload.identity = proc_id.to_string();
        // Set parent to the host reference (host:<actor_id>).
        let host_ref = HostId(agent.actor_id().clone()).to_string();
        payload.parent = Some(host_ref);
        Ok(payload)
    }

    /// Resolve a standalone proc into a proc-level `NodePayload`.
    ///
    /// Standalone procs (e.g. `mesh_root_client_proc`, the admin
    /// proc) are not managed by any `HostMeshAgent`, so
    /// `resolve_proc_node` cannot resolve them. Instead, we query the
    /// anchor actor on the proc for its introspection data, collect
    /// its supervision children, and build a synthetic proc node.
    ///
    /// Special case: when the anchor actor is this agent itself, we
    /// build the children list directly (just `[self]`) to avoid a
    /// self-deadlock — the actor loop cannot process an
    /// `IntrospectMessage` it sends to itself while handling a
    /// resolve request.
    async fn resolve_standalone_proc_node(
        &self,
        cx: &Context<'_, Self>,
        proc_id: &ProcId,
    ) -> Result<NodePayload, anyhow::Error> {
        let actor_id = self
            .standalone_proc_anchor(proc_id)
            .ok_or_else(|| anyhow::anyhow!("no anchor actor for standalone proc {}", proc_id))?;

        let children = if self.self_actor_id.as_ref() == Some(actor_id) {
            // Self-proc: we are the only actor; no message needed.
            vec![actor_id.to_string()]
        } else {
            // Query the anchor actor for its supervision children.
            let introspect_port = PortRef::<IntrospectMessage>::attest_message_port(actor_id);
            let (reply_handle, reply_rx) = open_once_port::<NodePayload>(cx);
            introspect_port.send(
                cx,
                IntrospectMessage::Query {
                    view: hyperactor::introspect::IntrospectView::Actor,
                    reply: reply_handle.bind(),
                },
            )?;

            let actor_payload = RealClock
                .timeout(SINGLE_HOST_TIMEOUT, reply_rx.recv())
                .await
                .map_err(|_| anyhow::anyhow!("timed out querying anchor actor on {}", proc_id))?
                .map_err(|e| {
                    anyhow::anyhow!("failed to receive anchor actor introspection: {}", e)
                })?;

            // Anchor actor + its supervision children.
            let mut children = vec![actor_id.to_string()];
            children.extend(actor_payload.children);
            children
        };

        let proc_name = match proc_id {
            ProcId::Direct(_, name) => name.clone(),
            _ => proc_id.to_string(),
        };

        Ok(NodePayload {
            identity: proc_id.to_string(),
            properties: NodeProperties::Proc {
                proc_name,
                num_actors: children.len(),
                is_system: false,
            },
            children,
            parent: Some("root".to_string()),
        })
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
    /// parent is the system proc ref; otherwise it's the proc's
    /// `ProcId` string.
    async fn resolve_actor_node(
        &self,
        cx: &Context<'_, Self>,
        actor_id: &ActorId,
    ) -> Result<NodePayload, anyhow::Error> {
        // Self-resolution: we cannot send IntrospectMessage to our
        // own actor loop while handling a resolve request (deadlock).
        // Use introspect_payload() to snapshot our own state
        // directly.
        let mut payload = if self.self_actor_id.as_ref() == Some(actor_id) {
            cx.introspect_payload()
        } else {
            let introspect_port = PortRef::<IntrospectMessage>::attest_message_port(actor_id);
            let (reply_handle, reply_rx) = open_once_port::<NodePayload>(cx);
            introspect_port.send(
                cx,
                IntrospectMessage::Query {
                    view: hyperactor::introspect::IntrospectView::Actor,
                    reply: reply_handle.bind(),
                },
            )?;

            RealClock
                .timeout(SINGLE_HOST_TIMEOUT, reply_rx.recv())
                .await
                .map_err(|_| anyhow::anyhow!("timed out querying actor {}", actor_id))?
                .map_err(|e| anyhow::anyhow!("failed to receive actor introspection: {}", e))?
        };

        // Actors on standalone procs: parent is the proc.
        if self.is_standalone_proc_actor(actor_id) {
            payload.parent = Some(actor_id.proc_id().to_string());
            return Ok(payload);
        }

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
                    payload.parent = Some(HostId(agent.actor_id().clone()).to_string());
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
                    // User proc: parent is the proc node, whose
                    // identity is the ProcId string.
                    payload.parent = Some(proc_id.to_string());
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
/// - `GET /SKILL.md` — self-describing API documentation (markdown).
fn create_mesh_admin_router(bridge_state: Arc<BridgeState>) -> Router {
    Router::new()
        .route("/SKILL.md", get(serve_skill_md))
        // `/v1/tree` is more specific than the wildcard and takes
        // precedence in Axum's router.
        .route("/v1/tree", get(tree_dump))
        .route("/v1/{*reference}", get(resolve_reference_bridge))
        .with_state(bridge_state)
}

/// Raw markdown template for the SKILL.md API document.
const SKILL_MD_TEMPLATE: &str = include_str!("mesh_admin_skill.md");

/// Serves the self-describing API document with the base URL
/// interpolated so examples are copy-pasteable.
async fn serve_skill_md(headers: axum::http::HeaderMap) -> impl axum::response::IntoResponse {
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let base = format!("http://{}", host);
    let body = SKILL_MD_TEMPLATE.replace("{base}", &base);
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/markdown; charset=utf-8",
        )],
        body,
    )
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

    // Resolve each root child. Hosts get the full host→proc→actor
    // subtree; non-host children (e.g. the root client actor) are
    // rendered as single leaf lines.
    for child_ref in &root.children {
        let resp = RealClock
            .timeout(TREE_TIMEOUT, state.admin_ref.resolve(cx, child_ref.clone()))
            .await;

        let payload = match resp {
            Ok(Ok(r)) => r.0.ok(),
            _ => None,
        };

        match payload {
            Some(node) if matches!(node.properties, NodeProperties::Host { .. }) => {
                // Host header: show the addr from NodeProperties::Host.
                let header = match &node.properties {
                    NodeProperties::Host { addr, .. } => addr.clone(),
                    _ => child_ref.clone(),
                };
                let host_url = format!("{}/v1/{}", base_url, urlencoding::encode(child_ref));
                output.push_str(&format!("{}  ->  {}\n", header, host_url));

                // Proc children with box-drawing connectors.
                let num_procs = node.children.len();
                for (i, proc_ref) in node.children.iter().enumerate() {
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
            Some(node) if matches!(node.properties, NodeProperties::Proc { .. }) => {
                // Non-host proc (e.g. root client proc). Render as a
                // proc-level subtree with its actor children.
                let proc_name = match &node.properties {
                    NodeProperties::Proc { proc_name, .. } => proc_name.clone(),
                    _ => child_ref.clone(),
                };
                let proc_url = format!("{}/v1/{}", base_url, urlencoding::encode(child_ref));
                output.push_str(&format!("{}  ->  {}\n", proc_name, proc_url));

                let num_actors = node.children.len();
                for (j, actor_ref) in node.children.iter().enumerate() {
                    let actor_connector = if j == num_actors - 1 {
                        "└── "
                    } else {
                        "├── "
                    };
                    let actor_label = derive_actor_label(actor_ref);
                    let actor_url = format!("{}/v1/{}", base_url, urlencoding::encode(actor_ref));
                    output.push_str(&format!(
                        "{}{}  ->  {}\n",
                        actor_connector, actor_label, actor_url
                    ));
                }
                output.push('\n');
            }
            Some(_node) => {
                // Non-host root child (e.g. root client actor).
                // Render as a single leaf line.
                let label = derive_actor_label(child_ref);
                let url = format!("{}/v1/{}", base_url, urlencoding::encode(child_ref));
                output.push_str(&format!("{}  ->  {}\n\n", label, url));
            }
            _ => {
                output.push_str(&format!("{} (unreachable)\n\n", child_ref));
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

    /// Minimal introspectable actor for tests. The `#[export]`
    /// attribute generates `Named + Referable + Binds` so that
    /// `handle.bind()` registers the `IntrospectMessage` port for
    /// remote delivery.
    #[derive(Debug)]
    #[hyperactor::export(handlers = [])]
    struct TestIntrospectableActor;
    impl Actor for TestIntrospectableActor {}

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

        let agent = MeshAdminAgent::new(
            vec![("host_a".to_string(), ref1), ("host_b".to_string(), ref2)],
            None,
        );

        let payload = agent.build_root_payload();
        assert_eq!(payload.identity, "root");
        assert_eq!(payload.parent, None);
        assert!(matches!(
            payload.properties,
            NodeProperties::Root { num_hosts: 2, .. }
        ));
        assert_eq!(payload.children.len(), 2);
        // Children should be host: reference strings.
        assert!(
            payload
                .children
                .contains(&HostId(actor_id1.clone()).to_string())
        );
        assert!(
            payload
                .children
                .contains(&HostId(actor_id2.clone()).to_string())
        );
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
                MeshAdminAgent::new(vec![(host_addr_str.clone(), host_agent_ref.clone())], None),
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
        assert!(matches!(
            root.properties,
            NodeProperties::Root { num_hosts: 1, .. }
        ));
        assert_eq!(root.parent, None);
        assert_eq!(root.children.len(), 2); // host + admin proc

        // -- 5. Resolve the host child --
        let _host_agent_id_str = host_agent_ref.actor_id().to_string();
        let host_ref_str = HostId(host_agent_ref.actor_id().clone()).to_string();
        let host_child_ref_str = root
            .children
            .iter()
            .find(|c| **c == host_ref_str)
            .expect("root children should contain the host agent (as host: ref)");
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
        // it appears both as a host node (from root, via HostId) and
        // as an actor (from a proc's children list, via plain ActorId).
        // HostId in root children makes resolution unambiguous: host
        // refs get Entity view, plain actor refs get Actor view.

        // The system proc must list the host agent among its children.
        let host_agent_id_str = host_agent_ref.actor_id().to_string();
        assert!(
            proc_node.children.contains(&host_agent_id_str),
            "system proc children {:?} should contain the host agent {}",
            proc_node.children,
            host_agent_id_str
        );

        // Resolve that child reference as a plain actor (no host: prefix).
        let xref_resp = admin_ref
            .resolve(&client, host_agent_id_str.clone())
            .await
            .unwrap();
        let xref_node = xref_resp.0.unwrap();

        // When resolved as a plain actor reference, it must return
        // Actor properties (not Host), because it has no host: prefix.
        assert!(
            matches!(xref_node.properties, NodeProperties::Actor { .. }),
            "host agent child resolved as plain actor should be Actor, got {:?}",
            xref_node.properties
        );
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
                MeshAdminAgent::new(vec![(host_addr_str.clone(), host_agent_ref.clone())], None),
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
        let host_ref_string = HostId(host_agent_ref.actor_id().clone()).to_string();
        let host_resp = admin_ref.resolve(&client, host_ref_string).await.unwrap();
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

    // Verifies that build_root_payload includes the root client proc
    // ID in the children list when provided, alongside the host
    // children.
    #[test]
    fn test_build_root_payload_with_root_client() {
        let addr1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let proc1 = ProcId::Direct(ChannelAddr::Tcp(addr1), "host1".to_string());
        let actor_id1 = ActorId::root(proc1, "mesh_agent".to_string());
        let ref1: ActorRef<HostMeshAgent> = ActorRef::attest(actor_id1.clone());

        let client_proc_id =
            ProcId::Direct(ChannelAddr::Tcp(addr1), "mesh_root_client_proc".to_string());
        let client_actor_id = client_proc_id.actor_id("client", 0);

        let agent = MeshAdminAgent::new(
            vec![("host_a".to_string(), ref1)],
            Some(client_actor_id.clone()),
        );

        let payload = agent.build_root_payload();
        assert!(matches!(
            payload.properties,
            NodeProperties::Root { num_hosts: 1, .. }
        ));
        // Should have 2 children: one host + one root client proc.
        assert_eq!(payload.children.len(), 2);
        assert!(
            payload
                .children
                .contains(&HostId(actor_id1.clone()).to_string())
        );
        assert!(payload.children.contains(&client_proc_id.to_string()));
    }

    // Verifies that resolving the root client ActorId via the
    // resolver returns a NodePayload with parent = "root",
    // confirming that the root client is treated as a first-class
    // child of root.
    #[tokio::test]
    async fn test_resolve_root_client_actor() {
        use hyperactor::Proc;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::host::Host;
        use hyperactor::host::LocalProcManager;

        use crate::host_mesh::mesh_agent::HostAgentMode;
        use crate::host_mesh::mesh_agent::ProcManagerSpawnFn;
        use crate::mesh_agent::ProcMeshAgent;

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

        // Create a genuinely introspectable root client actor.
        // proc.spawn() starts the message loop; handle.bind()
        // registers the IntrospectMessage port for remote delivery.
        let root_client_proc =
            Proc::direct(ChannelTransport::Unix.any(), "root_client".to_string()).unwrap();
        let _root_client_supervision = ProcSupervisionCoordinator::set(&root_client_proc)
            .await
            .unwrap();
        let root_client_handle = root_client_proc
            .spawn("client", TestIntrospectableActor)
            .unwrap();
        let root_client_ref: ActorRef<TestIntrospectableActor> = root_client_handle.bind();
        let root_client_actor_id = root_client_ref.actor_id().clone();

        // Spawn MeshAdminAgent with the root client ActorId.
        let admin_proc = Proc::direct(ChannelTransport::Unix.any(), "admin".to_string()).unwrap();
        use hyperactor::test_utils::proc_supervison::ProcSupervisionCoordinator;
        let _supervision = ProcSupervisionCoordinator::set(&admin_proc).await.unwrap();
        let admin_handle = admin_proc
            .spawn(
                "mesh_admin",
                MeshAdminAgent::new(
                    vec![(host_addr_str.clone(), host_agent_ref.clone())],
                    Some(root_client_actor_id.clone()),
                ),
            )
            .unwrap();
        let admin_ref: ActorRef<MeshAdminAgent> = admin_handle.bind();

        // Client for sending messages.
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _handle) = client_proc.instance("client").unwrap();

        // Resolve "root" — should include the root client proc in children.
        let root_resp = admin_ref
            .resolve(&client, "root".to_string())
            .await
            .unwrap();
        let root = root_resp.0.unwrap();
        let root_client_proc_id = root_client_actor_id.proc_id().to_string();
        assert!(
            root.children.contains(&root_client_proc_id),
            "root children {:?} should contain root client proc {}",
            root.children,
            root_client_proc_id
        );

        // Resolve the root client proc — should return Proc properties
        // with parent = "root" and the root client actor as a child.
        let proc_resp = admin_ref
            .resolve(&client, root_client_proc_id.clone())
            .await
            .unwrap();
        let proc_node = proc_resp.0.unwrap();
        assert!(
            matches!(proc_node.properties, NodeProperties::Proc { .. }),
            "expected Proc properties, got {:?}",
            proc_node.properties
        );
        assert_eq!(
            proc_node.parent,
            Some("root".to_string()),
            "root client proc parent should be 'root'"
        );
        assert!(
            proc_node
                .children
                .contains(&root_client_actor_id.to_string()),
            "proc children {:?} should contain root client actor {}",
            proc_node.children,
            root_client_actor_id
        );

        // Resolve the root client ActorId — should return Actor
        // properties with parent = the proc.
        let client_resp = admin_ref
            .resolve(&client, root_client_actor_id.to_string())
            .await
            .unwrap();
        let client_node = client_resp.0.unwrap();
        assert!(
            matches!(client_node.properties, NodeProperties::Actor { .. }),
            "expected Actor properties, got {:?}",
            client_node.properties
        );
        assert_eq!(
            client_node.parent,
            Some(root_client_proc_id),
            "root client parent should be the proc"
        );
    }

    // Verifies that the SKILL.md template contains the canonical
    // strings that agents and tests rely on. Prevents silent drift or
    // accidental removal.
    #[test]
    fn test_skill_md_contains_canonical_strings() {
        let template = SKILL_MD_TEMPLATE;
        assert!(
            template.contains("GET {base}/v1/root"),
            "SKILL.md must document the root endpoint"
        );
        assert!(
            template.contains("GET {base}/v1/{reference}"),
            "SKILL.md must document the reference endpoint"
        );
        assert!(
            template.contains("NodePayload"),
            "SKILL.md must mention the NodePayload response type"
        );
        assert!(
            template.contains("GET {base}/SKILL.md"),
            "SKILL.md must document itself"
        );
        assert!(
            template.contains("{base}"),
            "SKILL.md must use {{base}} placeholder for interpolation"
        );
    }

    // Verifies the navigation identity invariant (see module docs):
    //
    // 1. payload.identity == reference_string used to resolve it.
    // 2. For each child reference C of a resolved node P,
    //    resolve(C).parent == P.identity.
    //
    // Walks the entire tree starting from root, checking both
    // properties at every reachable node.
    #[tokio::test]
    async fn test_navigation_identity_invariant() {
        use hyperactor::Proc;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::host::Host;
        use hyperactor::host::LocalProcManager;

        use crate::host_mesh::mesh_agent::HostAgentMode;
        use crate::host_mesh::mesh_agent::ProcManagerSpawnFn;
        use crate::mesh_agent::ProcMeshAgent;

        // Stand up a local host with a HostMeshAgent.
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
                MeshAdminAgent::new(vec![(host_addr_str, host_agent_ref)], None),
            )
            .unwrap();
        let admin_ref: ActorRef<MeshAdminAgent> = admin_handle.bind();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _handle) = client_proc.instance("client").unwrap();

        // Walk the tree breadth-first, checking the invariant at every node.
        // Each entry is (reference_string, expected_parent_identity).
        let mut queue: std::collections::VecDeque<(String, Option<String>)> =
            std::collections::VecDeque::new();
        queue.push_back(("root".to_string(), None));

        let mut visited = std::collections::HashSet::new();
        while let Some((ref_str, expected_parent)) = queue.pop_front() {
            if !visited.insert(ref_str.clone()) {
                continue;
            }

            let resp = admin_ref.resolve(&client, ref_str.clone()).await.unwrap();
            let node = resp.0.unwrap();

            // Invariant 1: identity matches the reference used.
            assert_eq!(
                node.identity, ref_str,
                "identity mismatch: resolved '{}' but payload.identity = '{}'",
                ref_str, node.identity
            );

            // Invariant 2: parent matches the parent node's identity.
            assert_eq!(
                node.parent, expected_parent,
                "parent mismatch for '{}': expected {:?}, got {:?}",
                ref_str, expected_parent, node.parent
            );

            // Enqueue children with this node's identity as their
            // expected parent.
            for child_ref in &node.children {
                if !visited.contains(child_ref) {
                    queue.push_back((child_ref.clone(), Some(node.identity.clone())));
                }
            }
        }

        // Sanity: we should have visited at least root, host, a
        // proc, and an actor.
        assert!(
            visited.len() >= 4,
            "expected at least 4 nodes in the tree, visited {}",
            visited.len()
        );
    }
}
