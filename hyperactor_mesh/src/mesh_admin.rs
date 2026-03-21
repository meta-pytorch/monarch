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
//! The agent fans out to `HostAgent` instances to fetch host,
//! proc, and actor details, then normalizes them into a single
//! tree-shaped model (`NodeProperties` + children references)
//! suitable for topology-agnostic clients such as the admin TUI.
//!
//! # Schema strategy
//!
//! The external API contract is schema-first: the JSON Schema
//! (Draft 2020-12) served at `GET /v1/schema` is the
//! authoritative definition of the response shape, derived
//! directly from the Rust types (`NodePayload`,
//! `NodeProperties`, `FailureInfo`) via `schemars::JsonSchema`.
//! The error envelope schema is at `GET /v1/schema/error`.
//!
//! This follows the "Admin Gateway Pattern" RFC
//! ([doc](https://fburl.com/1dvah88uutaiyesebojouen2)):
//! schema is the product; transports and tooling are projections.
//!
//! ## Schema generation pipeline
//!
//! 1. `#[derive(JsonSchema)]` on `NodePayload`, `NodeProperties`,
//!    `FailureInfo`, `ApiError`, `ApiErrorEnvelope`.
//! 2. `schemars::schema_for!(T)` produces a `Schema` value at
//!    runtime (Draft 2020-12).
//! 3. The `serve_schema` / `serve_error_schema` handlers inject a
//!    `$id` field (SC-4) and serve the result as JSON.
//! 4. Snapshot tests in `introspect::tests` compare the raw
//!    schemars output (without `$id`) against checked-in golden
//!    files to detect drift (SC-2).
//! 5. Validation tests confirm that real `NodePayload` samples
//!    pass schema validation (SC-3).
//!
//! ## Regenerating snapshots
//!
//! After intentional type changes to `NodePayload`,
//! `NodeProperties`, `FailureInfo`, `ApiError`, or
//! `ApiErrorEnvelope`, regenerate the golden files:
//!
//! ```sh
//! buck run fbcode//monarch/hyperactor_mesh:generate_api_artifacts \
//!   @fbcode//mode/dev-nosan -- \
//!   fbcode/monarch/hyperactor_mesh/src/testdata
//! ```
//!
//! Or via cargo:
//!
//! ```sh
//! cargo run -p hyperactor_mesh --bin generate_api_artifacts -- \
//!   hyperactor_mesh/src/testdata
//! ```
//!
//! Then re-run tests to confirm the new snapshot passes.
//!
//! ## Schema invariants (SC-*)
//!
//! - **SC-1 (schema-derived):** Schema is derived from Rust
//!   types via `schemars::JsonSchema`, not hand-written.
//! - **SC-2 (schema-snapshot-stability):** Schema changes must
//!   be explicit — a snapshot test catches unintentional drift.
//! - **SC-3 (schema-payload-conformance):** Real `NodePayload`
//!   instances validate against the generated schema.
//! - **SC-4 (schema-version-identity):** Served schemas carry a
//!   `$id` tied to the API version (e.g.
//!   `https://monarch.meta.com/schemas/v1/node_payload`).
//! - **SC-5 (route-precedence):** Literal schema routes are
//!   matched by specificity before the `{*reference}` wildcard
//!   (axum 0.8 specificity-based routing).
//!
//! Note on `ApiError.details`: the derived schema is maximally
//! permissive for `details` (any valid JSON). This is intentional
//! for v1 — `details` is a domain-specific escape hatch.
//! Consumers must not assume a fixed shape.
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
//! ## Navigation identity invariants (NI-*)
//!
//! Every `NodePayload` in the topology tree satisfies:
//!
//! - **NI-1 (identity = reference):** A node's `identity` field must
//!   equal the reference string used to resolve it. If the TUI asks
//!   for reference `R`, `payload.identity == R`.
//!
//! - **NI-2 (parent coherence):** A node's `parent` field must equal
//!   the `identity` of the node it appears under. If node `P` lists
//!   `R` in its `children`, then `R.parent == Some(P.identity)`.
//!
//! Together these ensure that the TUI can correlate responses to tree
//! nodes, and that upward/downward navigation is consistent.
//!
//! ## Proc-resolution invariants (SP-*)
//!
//! When a proc reference is resolved, the returned `NodePayload`
//! satisfies:
//!
//! - **SP-1 (identity):** The identity matches the ProcId reference
//!   from the parent's children list.
//! - **SP-2 (properties):** The properties are `NodeProperties::Proc`.
//! - **SP-3 (parent):** The parent is set to the HostId format
//!   (`"host:<actor_id>"`).
//! - **SP-4 (as_of):** The `as_of` field is present and non-empty.
//!
//! Enforced by `test_system_proc_identity`.
//!
//! ## Proc-agent invariants (PA-*)
//!
//! - **PA-1 (live children):** Proc-node children used by admin/TUI
//!   must be derived from live proc state at query time. No
//!   additional publish event is required for a newly spawned actor
//!   to appear.
//!
//! Enforced by `test_proc_children_reflect_directly_spawned_actors`.
//!
//! ## Robustness invariant (MA-R1)
//!
//! - **MA-R1 (no-crash):** `MeshAdminAgent` must never crash the OS
//!   process it resides in. Every handler catches errors and converts
//!   them into structured error payloads
//!   (`ResolveReferenceResponse(Err(..))`, `NodeProperties::Error`,
//!   etc.) rather than propagating panics or unwinding. Failed reply
//!   sends (the caller went away) are silently swallowed.
//!
//! ## TLS transport invariant (MA-T1)
//!
//! - **MA-T1 (tls):** At Meta (`fbcode_build`), the admin HTTP
//!   server **requires** mutual TLS. At startup it probes for
//!   certificates via `try_tls_acceptor` with client cert
//!   enforcement enabled. If no usable certificate bundle is found,
//!   `init()` returns an error — no plain HTTP fallback. In OSS,
//!   TLS is best-effort with plain HTTP fallback.
//!
//! - **MA-T2 (scheme-in-url):** The URL returned by `GetAdminAddr`
//!   is always `https://host:port` or `http://host:port`, never a
//!   bare `host:port`. All callers receive and use this full URL
//!   directly.
//!
//! ## Client host invariants (CH-*)
//!
//! Let **A** denote the observed host mesh (the host mesh for which
//! this `MeshAdminAgent` was spawned), and let **C** denote the
//! process-global singleton client host mesh in the caller process
//! (whose local proc hosts the root client actor).
//!
//! - **CH-1 (deduplication):** When C ∈ A, the client host appears
//!   exactly once in the admin host list (deduplicated by `HostAgent`
//!   `ActorId` identity). When C ∉ A, `spawn_admin` includes C
//!   alongside A's hosts so the admin introspects C as a normal host
//!   subtree, not as a standalone proc.
//!
//! - **CH-2 (reachability):** In both cases, the root client actor
//!   is reachable through the standard host → proc → actor walk.
//!
//! - **CH-3 (ordering):** `spawn_admin` requires `cx: &impl
//!   context::Actor` (the caller's root client instance). Constructing
//!   that instance initializes C. Therefore C is available when
//!   `spawn_admin` executes. Any refactor must preserve this ordering.
//!
//! **Mechanism:** [`HostMeshRef::spawn_admin`] reads C from the
//! caller process (via `try_this_host()`), merges it with A's host
//! list, deduplicates by `HostAgent` `ActorId`, and sends the merged
//! list in `SpawnMeshAdmin`. This works for same-process and
//! cross-process setups because merge+dedeup happens in the caller
//! process before sending the spawn request.
//!
//! ## MAST resolution invariants (MC-*)
//!
//! CLI-based `mast_conda:///` resolution (OSS-compatible fallback):
//!
//! - **MC-1 (cli-contract):** `mast get-status --json <job>` must
//!   exit 0 and produce valid JSON. Missing binary → distinct error.
//!   Non-zero exit → includes exit code and stderr. Malformed JSON →
//!   parse error.
//! - **MC-2 (head-hostname):** `head_hostname` extracts the first
//!   hostname by ascending task index from the last attempt of each
//!   task group.
//! - **MC-3 (fqdn-idempotent):** `qualify_fqdn` passes through
//!   hostnames containing a dot. Short hostnames are qualified via
//!   `getaddrinfo(AI_CANONNAME)`. Failure falls back to the raw
//!   hostname.
//! - **MC-4 (fqdn-nonblocking):** `qualify_fqdn` runs the blocking
//!   `getaddrinfo` syscall via `spawn_blocking`.
//! - **MC-5 (admin-port):** `resolve_admin_port` uses the explicit
//!   override when provided, otherwise reads the port from
//!   `MESH_ADMIN_ADDR` config.
//!
//! Enforced by `test_head_hostname_*`, `test_qualify_fqdn_*`,
//! `test_resolve_mast_*`, `test_resolve_admin_port_*`.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::extract::Path as AxumPath;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::RefClient;
use hyperactor::channel::try_tls_acceptor;
use hyperactor::host::SERVICE_PROC_NAME;
use hyperactor::introspect::IntrospectMessage;
use hyperactor::introspect::IntrospectResult;
use hyperactor::introspect::IntrospectView;
use hyperactor::mailbox::open_once_port;
use hyperactor::reference as hyperactor_reference;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use typeuri::Named;

use crate::config_dump::ConfigDump;
use crate::config_dump::ConfigDumpResult;
use crate::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
use crate::host_mesh::host_agent::HostAgent;
use crate::host_mesh::host_agent::HostId;
use crate::introspect::NodePayload;
use crate::introspect::NodeProperties;
use crate::introspect::to_node_payload;
use crate::proc_agent::PROC_AGENT_ACTOR_NAME;
use crate::proc_agent::PySpyDump;
use crate::pyspy::PySpyResult;

/// Send an `IntrospectMessage` to an actor and receive the reply.
/// Encapsulates open_once_port + send + timeout + error handling.
async fn query_introspect(
    cx: &hyperactor::Context<'_, MeshAdminAgent>,
    actor_id: &hyperactor_reference::ActorId,
    view: hyperactor::introspect::IntrospectView,
    timeout: Duration,
    err_ctx: &str,
) -> Result<IntrospectResult, anyhow::Error> {
    let introspect_port =
        hyperactor_reference::PortRef::<IntrospectMessage>::attest_message_port(actor_id);
    let (reply_handle, reply_rx) = open_once_port::<IntrospectResult>(cx);
    introspect_port.send(
        cx,
        IntrospectMessage::Query {
            view,
            reply: reply_handle.bind(),
        },
    )?;
    tokio::time::timeout(timeout, reply_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out {}", err_ctx))?
        .map_err(|e| anyhow::anyhow!("failed to receive {}: {}", err_ctx, e))
}

/// Send an `IntrospectMessage::QueryChild` to an actor.
async fn query_child_introspect(
    cx: &hyperactor::Context<'_, MeshAdminAgent>,
    actor_id: &hyperactor_reference::ActorId,
    child_ref: hyperactor_reference::Reference,
    timeout: Duration,
    err_ctx: &str,
) -> Result<IntrospectResult, anyhow::Error> {
    let introspect_port =
        hyperactor_reference::PortRef::<IntrospectMessage>::attest_message_port(actor_id);
    let (reply_handle, reply_rx) = open_once_port::<IntrospectResult>(cx);
    introspect_port.send(
        cx,
        IntrospectMessage::QueryChild {
            child_ref,
            reply: reply_handle.bind(),
        },
    )?;
    tokio::time::timeout(timeout, reply_rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out {}", err_ctx))?
        .map_err(|e| anyhow::anyhow!("failed to receive {}: {}", err_ctx, e))
}

/// Actor name used when spawning the mesh admin agent.
pub const MESH_ADMIN_ACTOR_NAME: &str = "mesh_admin";

/// Actor name for the HTTP bridge client mailbox on the service proc.
///
/// Unlike `MESH_ADMIN_ACTOR_NAME`, this is not a full actor: it is a
/// client-mode `Instance<()>` created via
/// `Proc::introspectable_instance()` and driven by Axum's Tokio task
/// pool rather than an actor message loop. A separate instance is
/// required because `MeshAdminAgent`'s own `Instance<Self>` is only
/// accessible inside its message loop and cannot be shared with
/// external tasks. This instance gives the HTTP handlers a routable
/// proc identity so they can open one-shot reply ports
/// (`open_once_port`) to receive responses from `MeshAdminAgent`.
///
/// Unlike a plain `instance()`, this uses
/// `Proc::introspectable_instance()` so the bridge responds to
/// `IntrospectMessage::Query` and appears as a navigable node in the
/// mesh TUI rather than causing a 504 when selected.
pub const MESH_ADMIN_BRIDGE_NAME: &str = "mesh_admin_bridge";

/// Structured error response following the gateway RFC envelope
/// pattern.
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ApiError {
    /// Machine-readable error code (e.g. "not_found", "bad_request").
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Additional context about the error. Schema is permissive
    /// (any valid JSON) — `details` is a domain-specific escape
    /// hatch. Do not assume a fixed shape.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Wrapper for the structured error envelope.
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
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
            "gateway_timeout" => StatusCode::GATEWAY_TIMEOUT,
            "service_unavailable" => StatusCode::SERVICE_UNAVAILABLE,
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
        reply: hyperactor_reference::OncePortRef<MeshAdminAddrResponse>,
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
        reply: hyperactor_reference::OncePortRef<ResolveReferenceResponse>,
    },
}
wirevalue::register_type!(ResolveReferenceMessage);

/// Actor that serves a mesh-level admin HTTP endpoint.
///
/// `MeshAdminAgent` is the mesh-wide aggregation point for
/// introspection: it holds `hyperactor_reference::ActorRef<HostAgent>` handles for each
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
    /// Map of host address string → `HostAgent` reference used to
    /// fan out our target admin queries.
    hosts: HashMap<String, hyperactor_reference::ActorRef<HostAgent>>,

    /// Reverse index: `HostAgent` `ActorId` → host address
    /// string.
    ///
    /// The host agent itself is an actor that can appear in multiple
    /// places (e.g., as a host node and as a child actor under a
    /// system proc). This index lets reference resolution treat that
    /// `ActorId` as a *Host* node (via `resolve_host_node`) rather
    /// than a generic *Actor* node, avoiding cycles / dropped nodes
    /// in clients like the TUI.
    host_agents_by_actor_id: HashMap<hyperactor_reference::ActorId, String>,

    /// `ActorId` of the process-global root client (`client[0]` on
    /// the singleton Host's `local_proc`), exposed as a first-class
    /// child of the root node. Routable and introspectable via the
    /// blanket `Handler<IntrospectMessage>`.
    root_client_actor_id: Option<hyperactor_reference::ActorId>,

    /// This agent's own `ActorId`, captured during `init`. Used to
    /// include the admin proc as a visible node in the introspection
    /// tree (the principle: "if you can send it a message, you can
    /// introspect it").
    self_actor_id: Option<hyperactor_reference::ActorId>,

    // -- HTTP server address fields --
    //
    // The admin HTTP server has three address representations:
    //
    //   1. `admin_addr_override` — caller-supplied bind address
    //      (constructor param). When `None`, `init` reads
    //      `MESH_ADMIN_ADDR` from config instead.
    //
    //   2. `admin_addr` — the actual `SocketAddr` the OS assigned
    //      after `TcpListener::bind`. Populated during `init`.
    //
    //   3. `admin_host` — human-friendly URL with the machine
    //      hostname (not the raw IP) so it works with DNS and TLS
    //      certificate validation. Returned via `GetAdminAddr`.
    /// Caller-supplied bind address. When `None`, `init` reads
    /// `MESH_ADMIN_ADDR` from config.
    admin_addr_override: Option<std::net::SocketAddr>,

    /// Actual bound address after `TcpListener::bind`, populated
    /// during `init`.
    admin_addr: Option<std::net::SocketAddr>,

    /// Hostname-based URL (e.g. `"https://myhost.facebook.com:1729"`)
    /// for the admin HTTP server. Returned via `GetAdminAddr`.
    admin_host: Option<String>,

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
    ///   correct `HostAgent`, and
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
        hosts: Vec<(String, hyperactor_reference::ActorRef<HostAgent>)>,
        root_client_actor_id: Option<hyperactor_reference::ActorId>,
        admin_addr: Option<std::net::SocketAddr>,
    ) -> Self {
        let host_agents_by_actor_id: HashMap<hyperactor_reference::ActorId, String> = hosts
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
            admin_addr_override: admin_addr,
            admin_addr: None,
            admin_host: None,
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
            .field("admin_host", &self.admin_host)
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
    admin_ref: hyperactor_reference::ActorRef<MeshAdminAgent>,
    /// Dedicated client mailbox on system_proc for HTTP bridge reply
    /// ports. Using a separate `Instance<()>` avoids sharing the
    /// actor's own mailbox with the HTTP bridge and ensures the
    /// bridge context is routable via system_proc's frontend address.
    // Previous approach used `this.clone_for_py()` which cloned the
    // admin actor's Instance:
    //   bridge_cx: Instance<MeshAdminAgent>,
    bridge_cx: Instance<()>,
    /// Limits the number of in-flight resolve requests to prevent
    /// introspection queries from overwhelming the shared tokio
    /// runtime and starving user actor workloads.
    resolve_semaphore: tokio::sync::Semaphore,
    /// Keep the handle alive so the bridge mailbox is not dropped.
    _bridge_handle: ActorHandle<()>,
}

/// A TCP listener that performs a TLS handshake on each accepted
/// connection before handing it to axum.
///
/// Implements [`axum::serve::Listener`] so it can be passed directly
/// to [`axum::serve`].  Per the trait contract, `accept` handles
/// errors internally (logging + retrying) and never returns `Err`.
struct TlsListener {
    tcp: TcpListener,
    acceptor: TlsAcceptor,
}

impl axum::serve::Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.tcp.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::warn!("TCP accept error: {}", e);
                    continue;
                }
            };

            match self.acceptor.accept(stream).await {
                Ok(tls_stream) => return (tls_stream, addr),
                Err(e) => {
                    tracing::warn!("TLS handshake failed from {}: {}", addr, e);
                    continue;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

#[async_trait]
impl Actor for MeshAdminAgent {
    /// Initializes the mesh admin agent and its HTTP server.
    ///
    /// 1. Binds well-known message ports (`proc.spawn()` does not
    ///    call `bind()` — unlike `gspawn` — so the actor must do it
    ///    itself before becoming reachable).
    /// 2. Binds a TCP listener (ephemeral or fixed port).
    /// 3. Builds a TLS acceptor (explicit env vars, then Meta default
    ///    paths). At Meta (`fbcode_build`), mTLS is mandatory and
    ///    init fails if no certs are found. In OSS, falls back to
    ///    plain HTTP.
    /// 4. Creates a dedicated `Instance<()>` client mailbox on
    ///    system_proc for the HTTP bridge's reply ports, keeping
    ///    bridge traffic off the actor's own mailbox.
    /// 5. Spawns the axum server in a background task (HTTPS with
    ///    mTLS at Meta, HTTPS or HTTP in OSS depending on step 3).
    ///
    /// The hostname-based listen address is stored in `admin_host` so
    /// it can be returned via `GetAdminAddr`. The scheme (`https://`
    /// or `http://`) is included so clients know which protocol to
    /// use.
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // Bind well-known ports before the HTTP server is spawned, so
        // messages (including Undeliverable bounces) can be delivered
        // as soon as the admin is reachable.
        this.bind::<Self>();
        this.set_system();
        self.self_actor_id = Some(this.self_id().clone());

        let bind_addr = match self.admin_addr_override {
            Some(addr) => addr,
            None => hyperactor_config::global::get_cloned(crate::config::MESH_ADMIN_ADDR)
                .parse_socket_addr()
                .map_err(|e| anyhow::anyhow!("invalid MESH_ADMIN_ADDR config: {}", e))?,
        };
        let listener = TcpListener::bind(bind_addr).await?;
        let bound_addr = listener.local_addr()?;
        // Report the hostname (e.g. Tupperware container name) + port
        // rather than a raw IP, so the address works with DNS and TLS
        // certificate validation.
        let host = hostname::get()
            .unwrap_or_else(|_| "localhost".into())
            .into_string()
            .unwrap_or_else(|_| "localhost".to_string());
        self.admin_addr = Some(bound_addr);

        // At Meta: mTLS is mandatory — fail if no certs are found.
        // In OSS: TLS is best-effort with plain HTTP fallback.
        // See MA-T1 in module doc.
        let enforce_mtls = cfg!(fbcode_build);
        let tls_acceptor = try_tls_acceptor(enforce_mtls);

        if enforce_mtls && tls_acceptor.is_none() {
            return Err(anyhow::anyhow!(
                "mesh admin requires mTLS but no TLS certificates found; \
                 set HYPERACTOR_TLS_CERT/KEY/CA or ensure Meta cert paths exist \
                 (/var/facebook/x509_identities/server.pem, /var/facebook/rootcanal/ca.pem)"
            ));
        }

        let scheme = if tls_acceptor.is_some() {
            "https"
        } else {
            "http"
        };
        self.admin_host = Some(format!("{}://{}:{}", scheme, host, bound_addr.port()));

        // Create a dedicated client mailbox on system_proc for the
        // HTTP bridge's reply ports. This avoids sharing the admin
        // actor's own mailbox with async HTTP handlers.
        let (bridge_cx, bridge_handle) = this
            .proc()
            .introspectable_instance(MESH_ADMIN_BRIDGE_NAME)?;
        bridge_cx.set_system();
        let bridge_state = Arc::new(BridgeState {
            admin_ref: hyperactor_reference::ActorRef::attest(this.self_id().clone()),
            bridge_cx,
            resolve_semaphore: tokio::sync::Semaphore::new(hyperactor_config::global::get(
                crate::config::MESH_ADMIN_MAX_CONCURRENT_RESOLVES,
            )),
            _bridge_handle: bridge_handle,
        });
        let router = create_mesh_admin_router(bridge_state);

        if let Some(acceptor) = tls_acceptor {
            let tls_listener = TlsListener {
                tcp: listener,
                acceptor,
            };
            tokio::spawn(async move {
                if let Err(e) = axum::serve(tls_listener, router).await {
                    tracing::error!("mesh admin server (mTLS) error: {}", e);
                }
            });
        } else {
            // OSS fallback: plain HTTP (only reachable when !fbcode_build).
            tokio::spawn(async move {
                if let Err(e) = axum::serve(listener, router).await {
                    tracing::error!("mesh admin server error: {}", e);
                }
            });
        }

        tracing::info!(
            "mesh admin server listening on {}",
            self.admin_host.as_deref().unwrap_or("unknown")
        );
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
                    addr: self.admin_host.clone(),
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

        // Host refs use the "host:<actor_id>" format so they are
        // unambiguous from plain actor references. The same
        // HostAgent ActorId can appear both as a host (from root)
        // and as an actor (from a proc's children list).
        if let Ok(host_id) = reference_string.parse::<HostId>() {
            return self.resolve_host_node(cx, &host_id.0).await;
        }

        let reference: hyperactor_reference::Reference = reference_string
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid reference '{}': {}", reference_string, e))?;

        match &reference {
            hyperactor_reference::Reference::Proc(proc_id) => {
                // Try the host-managed path first (uses ProcAgent,
                // sees all actors). Fall back to the standalone
                // anchor path for procs truly off-mesh (A != C).
                match self.resolve_proc_node(cx, proc_id).await {
                    Ok(payload) => Ok(payload),
                    Err(_) if self.standalone_proc_anchor(proc_id).is_some() => {
                        self.resolve_standalone_proc_node(cx, proc_id).await
                    }
                    Err(e) => Err(e),
                }
            }
            hyperactor_reference::Reference::Actor(actor_id) => {
                self.resolve_actor_node(cx, actor_id).await
            }
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
    ///
    /// The root client is no longer standalone: spawn_admin registers
    /// C (the bootstrap host) as a normal host entry (A/C invariant).
    fn standalone_proc_actors(&self) -> impl Iterator<Item = &hyperactor_reference::ActorId> {
        std::iter::empty()
    }

    /// If `proc_id` belongs to a standalone proc, return the anchor
    /// actor on that proc. Returns `None` for host-managed procs.
    fn standalone_proc_anchor(
        &self,
        proc_id: &hyperactor_reference::ProcId,
    ) -> Option<&hyperactor_reference::ActorId> {
        self.standalone_proc_actors()
            .find(|actor_id| *actor_id.proc_id() == *proc_id)
    }

    /// Returns true if `actor_id` lives on a standalone proc.
    fn is_standalone_proc_actor(&self, actor_id: &hyperactor_reference::ActorId) -> bool {
        self.standalone_proc_actors()
            .any(|a| *a.proc_id() == *actor_id.proc_id())
    }

    /// Construct the synthetic root node for the reference tree.
    ///
    /// The root is not a real actor/proc; it's a convenience node
    /// that anchors navigation. Its children are the configured
    /// `HostAgent` actor IDs (as reference strings) plus any
    /// standalone procs (root client proc, admin proc).
    fn build_root_payload(&self) -> NodePayload {
        let children: Vec<String> = self
            .hosts
            .values()
            .map(|agent| HostId(agent.actor_id().clone()).to_string())
            .collect();
        let system_children: Vec<String> = Vec::new();
        let mut attrs = hyperactor_config::Attrs::new();
        attrs.set(crate::introspect::NODE_TYPE, "root".to_string());
        attrs.set(crate::introspect::NUM_HOSTS, self.hosts.len());
        if let Ok(t) = humantime::parse_rfc3339(&self.started_at) {
            attrs.set(crate::introspect::STARTED_AT, t);
        }
        attrs.set(crate::introspect::STARTED_BY, self.started_by.clone());
        attrs.set(crate::introspect::SYSTEM_CHILDREN, system_children.clone());
        let attrs_json = serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());
        NodePayload {
            identity: "root".to_string(),
            properties: crate::introspect::derive_properties(&attrs_json),
            children,
            parent: None,
            as_of: hyperactor::introspect::format_timestamp(std::time::SystemTime::now()),
        }
    }

    /// Resolve a `HostAgent` actor reference into a host-level
    /// `NodePayload`.
    ///
    /// Sends `IntrospectMessage::Query` directly to the
    /// `HostAgent`, which returns a `NodePayload` with
    /// `NodeProperties::Host` and the host's children. The resolver
    /// overrides `parent` to `"root"` since the host agent
    /// doesn't know its position in the navigation tree.
    async fn resolve_host_node(
        &self,
        cx: &Context<'_, Self>,
        actor_id: &hyperactor_reference::ActorId,
    ) -> Result<NodePayload, anyhow::Error> {
        let result = query_introspect(
            cx,
            actor_id,
            hyperactor::introspect::IntrospectView::Entity,
            hyperactor_config::global::get(crate::config::MESH_ADMIN_SINGLE_HOST_TIMEOUT),
            "querying host agent",
        )
        .await?;
        Ok(crate::introspect::to_node_payload_with(
            result,
            HostId(actor_id.clone()).to_string(),
            Some("root".to_string()),
        ))
    }

    /// Resolve a `ProcId` reference into a proc-level `NodePayload`.
    ///
    /// First tries `IntrospectMessage::QueryChild` against the owning
    /// `HostAgent` (which recognizes service and local procs). If
    /// that returns an error payload, falls back to `ProcAgent` for
    /// user procs by querying
    /// `QueryChild(hyperactor_reference::Reference::Proc(proc_id))`
    /// on `<proc_id>/proc_agent[0]`.
    ///
    /// See PA-1 in module doc.
    async fn resolve_proc_node(
        &self,
        cx: &Context<'_, Self>,
        proc_id: &hyperactor_reference::ProcId,
    ) -> Result<NodePayload, anyhow::Error> {
        let host_addr = proc_id.addr().to_string();

        let agent = self
            .hosts
            .get(&host_addr)
            .ok_or_else(|| anyhow::anyhow!("host not found: {}", host_addr))?;

        // Try the host agent's QueryChild first.
        let result = query_child_introspect(
            cx,
            agent.actor_id(),
            hyperactor_reference::Reference::Proc(proc_id.clone()),
            hyperactor_config::global::get(crate::config::MESH_ADMIN_QUERY_CHILD_TIMEOUT),
            "querying proc details",
        )
        .await?;

        // If the host recognized the proc, use its response directly.
        // No identity/parent normalization — QueryChild sets them correctly.
        let payload = to_node_payload(result);
        if !matches!(payload.properties, NodeProperties::Error { .. }) {
            return Ok(payload);
        }

        // Fall back to querying the ProcAgent directly (user procs).
        let mesh_agent_id = proc_id.actor_id(PROC_AGENT_ACTOR_NAME, 0);
        let result = query_child_introspect(
            cx,
            &mesh_agent_id,
            hyperactor_reference::Reference::Proc(proc_id.clone()),
            hyperactor_config::global::get(crate::config::MESH_ADMIN_RESOLVE_ACTOR_TIMEOUT),
            "querying proc mesh agent",
        )
        .await?;

        Ok(crate::introspect::to_node_payload_with(
            result,
            proc_id.to_string(),
            Some(HostId(agent.actor_id().clone()).to_string()),
        ))
    }

    /// Resolve a standalone proc into a proc-level `NodePayload`.
    ///
    /// Standalone procs (e.g. the admin proc) are not managed by any
    /// `HostAgent`, so
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
        proc_id: &hyperactor_reference::ProcId,
    ) -> Result<NodePayload, anyhow::Error> {
        let actor_id = self
            .standalone_proc_anchor(proc_id)
            .ok_or_else(|| anyhow::anyhow!("no anchor actor for standalone proc {}", proc_id))?;

        let (children, system_children) = if self.self_actor_id.as_ref() == Some(actor_id) {
            // Self-proc: we are the only actor; no message needed.
            // The admin agent is a system actor.
            let self_ref = actor_id.to_string();
            (vec![self_ref.clone()], vec![self_ref])
        } else {
            // Query the anchor actor for its supervision children.
            let actor_result = query_introspect(
                cx,
                actor_id,
                hyperactor::introspect::IntrospectView::Actor,
                hyperactor_config::global::get(crate::config::MESH_ADMIN_SINGLE_HOST_TIMEOUT),
                &format!("querying anchor actor on {}", proc_id),
            )
            .await?;
            // No identity/parent normalization — only reading properties for is_system check.
            let actor_payload = to_node_payload(actor_result);
            // Check if anchor actor is system.
            let anchor_ref = actor_id.to_string();
            let anchor_is_system = matches!(
                &actor_payload.properties,
                NodeProperties::Actor {
                    is_system: true,
                    ..
                }
            );

            let mut children = vec![anchor_ref.clone()];
            let mut system_children = Vec::new();
            if anchor_is_system {
                system_children.push(anchor_ref);
            }

            // Query each supervision child to check is_system.
            for child_ref in actor_payload.children {
                if let Ok(child_actor_id) = child_ref.parse::<hyperactor_reference::ActorId>() {
                    let child_is_system = if let Ok(r) = query_introspect(
                        cx,
                        &child_actor_id,
                        hyperactor::introspect::IntrospectView::Actor,
                        hyperactor_config::global::get(
                            crate::config::MESH_ADMIN_RESOLVE_ACTOR_TIMEOUT,
                        ),
                        "querying child actor is_system",
                    )
                    .await
                    {
                        let p = to_node_payload(r);
                        matches!(
                            &p.properties,
                            NodeProperties::Actor {
                                is_system: true,
                                ..
                            }
                        )
                    } else {
                        false
                    };
                    if child_is_system {
                        system_children.push(child_ref.clone());
                    }
                }
                children.push(child_ref);
            }
            (children, system_children)
        };

        let proc_name = proc_id.name().to_string();

        // Build attrs for standalone proc.
        let mut attrs = hyperactor_config::Attrs::new();
        attrs.set(crate::introspect::NODE_TYPE, "proc".to_string());
        attrs.set(crate::introspect::PROC_NAME, proc_name.clone());
        attrs.set(crate::introspect::NUM_ACTORS, children.len());
        attrs.set(crate::introspect::SYSTEM_CHILDREN, system_children.clone());
        let attrs_json = serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());

        Ok(NodePayload {
            identity: proc_id.to_string(),
            properties: crate::introspect::derive_properties(&attrs_json),
            children,
            as_of: hyperactor::introspect::format_timestamp(std::time::SystemTime::now()),
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
    /// `ProcAgent`).
    ///
    /// The resolver sets `parent` based on the actor's position
    /// in the topology: if the actor lives in a system proc, the
    /// parent is the system proc ref; otherwise it's the proc's
    /// `ProcId` string.
    async fn resolve_actor_node(
        &self,
        cx: &Context<'_, Self>,
        actor_id: &hyperactor_reference::ActorId,
    ) -> Result<NodePayload, anyhow::Error> {
        // Self-resolution: we cannot send IntrospectMessage to our
        // own actor loop while handling a resolve request (deadlock).
        // Use introspect_payload() to snapshot our own state
        // directly.
        let result = if self.self_actor_id.as_ref() == Some(actor_id) {
            cx.introspect_payload()
        } else if self.is_standalone_proc_actor(actor_id) {
            // Standalone procs have no ProcAgent — query directly.
            query_introspect(
                cx,
                actor_id,
                hyperactor::introspect::IntrospectView::Actor,
                hyperactor_config::global::get(crate::config::MESH_ADMIN_SINGLE_HOST_TIMEOUT),
                &format!("querying actor {}", actor_id),
            )
            .await?
        } else {
            // Check terminated snapshots first — fast, no ambiguity.
            let proc_id = actor_id.proc_id();
            let mesh_agent_id = proc_id.actor_id(PROC_AGENT_ACTOR_NAME, 0);
            let terminated = query_child_introspect(
                cx,
                &mesh_agent_id,
                hyperactor_reference::Reference::Actor(actor_id.clone()),
                hyperactor_config::global::get(crate::config::MESH_ADMIN_QUERY_CHILD_TIMEOUT),
                "querying terminated snapshot",
            )
            .await
            .ok()
            .filter(|r| {
                let p = crate::introspect::derive_properties(&r.attrs);
                !matches!(p, NodeProperties::Error { .. })
            });

            match terminated {
                Some(snapshot) => snapshot,
                None => {
                    // Not terminated — query the live actor.
                    query_introspect(
                        cx,
                        actor_id,
                        hyperactor::introspect::IntrospectView::Actor,
                        hyperactor_config::global::get(
                            crate::config::MESH_ADMIN_RESOLVE_ACTOR_TIMEOUT,
                        ),
                        &format!("querying actor {}", actor_id),
                    )
                    .await?
                }
            }
        };
        let mut payload = to_node_payload(result);

        // Actors on standalone procs: parent is the proc.
        if self.is_standalone_proc_actor(actor_id) {
            payload.parent = Some(actor_id.proc_id().to_string());
            return Ok(payload);
        }

        // Set parent based on topology. If the actor returns Proc
        // properties (ProcAgent override), its parent is the host
        // agent. Otherwise, it's a regular actor and its parent is
        // the proc.
        let proc_id = actor_id.proc_id();
        match &payload.properties {
            NodeProperties::Proc { .. } => {
                // ProcAgent: parent is the host agent.
                let host_addr = proc_id.addr().to_string();
                if let Some(agent) = self.hosts.get(&host_addr) {
                    payload.parent = Some(HostId(agent.actor_id().clone()).to_string());
                }
            }
            _ => {
                // Regular actor: parent is the proc. We use the
                // system proc ref format if the proc is a known
                // system/local proc, otherwise the ProcAgent
                // ActorId.
                // Parent is the proc node, whose identity is the
                // ProcId string (same for system and user procs).
                payload.parent = Some(proc_id.to_string());
            }
        }

        Ok(payload)
    }
}

/// Build the Axum router for the mesh admin HTTP server.
///
/// Routes:
/// - `GET /v1/schema` — JSON Schema (Draft 2020-12) for `NodePayload`.
/// - `GET /v1/schema/error` — JSON Schema for `ApiErrorEnvelope`.
/// - `GET /v1/openapi.json` — OpenAPI 3.1 spec (embeds JSON Schemas).
/// - `GET /v1/tree` — ASCII topology dump.
/// - `GET /v1/pyspy/{*proc_reference}` — py-spy stack dump for a proc.
/// - `GET /v1/config/{*proc_reference}` — config snapshot for a proc.
/// - `GET /v1/{*reference}` — JSON `NodePayload` for a single reference.
/// - `GET /SKILL.md` — agent-facing API documentation (markdown).
fn create_mesh_admin_router(bridge_state: Arc<BridgeState>) -> Router {
    Router::new()
        .route("/SKILL.md", get(serve_skill_md))
        // Literal paths matched by specificity before wildcard (SC-5).
        .route("/v1/schema", get(serve_schema))
        .route("/v1/schema/error", get(serve_error_schema))
        .route("/v1/openapi.json", get(serve_openapi))
        .route("/v1/tree", get(tree_dump))
        .route("/v1/pyspy/{*proc_reference}", get(pyspy_bridge))
        .route("/v1/config/{*proc_reference}", get(config_bridge))
        .route("/v1/{*reference}", get(resolve_reference_bridge))
        .with_state(bridge_state)
}

/// Raw markdown template for the SKILL.md API document.
const SKILL_MD_TEMPLATE: &str = include_str!("mesh_admin_skill.md");

/// Extract base URL from request headers.
///
/// Defaults to `https` when `x-forwarded-proto` is absent — the
/// admin server uses TLS in production, so `http` is the wrong
/// default for direct connections.
fn extract_base_url(headers: &axum::http::HeaderMap) -> String {
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("https");
    format!("{scheme}://{host}")
}

/// Serves the self-describing API document with the base URL
/// interpolated so examples are copy-pasteable.
async fn serve_skill_md(headers: axum::http::HeaderMap) -> impl axum::response::IntoResponse {
    let base = extract_base_url(&headers);
    let body = SKILL_MD_TEMPLATE.replace("{base}", &base);
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/markdown; charset=utf-8",
        )],
        body,
    )
}

/// Build a JSON Schema value with a `$id` field.
fn schema_with_id<T: schemars::JsonSchema>(id: &str) -> Result<serde_json::Value, ApiError> {
    let schema = schemars::schema_for!(T);
    let mut value = serde_json::to_value(schema).map_err(|e| ApiError {
        code: "internal_error".to_string(),
        message: format!("failed to serialize schema: {e}"),
        details: None,
    })?;
    if let Some(obj) = value.as_object_mut() {
        obj.insert("$id".into(), serde_json::Value::String(id.into()));
    }
    Ok(value)
}

/// JSON Schema for the `NodePayload` response type.
async fn serve_schema() -> Result<axum::response::Json<serde_json::Value>, ApiError> {
    Ok(axum::response::Json(schema_with_id::<
        crate::introspect::NodePayload,
    >(
        "https://monarch.meta.com/schemas/v1/node_payload",
    )?))
}

/// JSON Schema for the `ApiErrorEnvelope` error response.
async fn serve_error_schema() -> Result<axum::response::Json<serde_json::Value>, ApiError> {
    Ok(axum::response::Json(schema_with_id::<ApiErrorEnvelope>(
        "https://monarch.meta.com/schemas/v1/error",
    )?))
}

/// Hoist `$defs` from a schemars-generated schema into a shared
/// map and rewrite internal `$ref` pointers from `#/$defs/X` to
/// `#/components/schemas/X` so OpenAPI tools can resolve them.
fn hoist_defs(
    schema: &mut serde_json::Value,
    shared: &mut serde_json::Map<String, serde_json::Value>,
) {
    if let Some(obj) = schema.as_object_mut() {
        if let Some(defs) = obj.remove("$defs") {
            if let Some(defs_map) = defs.as_object() {
                for (k, v) in defs_map {
                    shared.insert(k.clone(), v.clone());
                }
            }
        }
        // Also remove $schema from embedded schemas — it's
        // only valid at the root of a JSON Schema document,
        // not inside an OpenAPI components/schemas entry.
        obj.remove("$schema");
    }
    rewrite_refs(schema);
}

/// Recursively rewrite `$ref: "#/$defs/X"` →
/// `$ref: "#/components/schemas/X"`.
fn rewrite_refs(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(r)) = map.get_mut("$ref") {
                if r.starts_with("#/$defs/") {
                    *r = r.replace("#/$defs/", "#/components/schemas/");
                }
            }
            for v in map.values_mut() {
                rewrite_refs(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                rewrite_refs(v);
            }
        }
        _ => {}
    }
}

/// Build the OpenAPI 3.1 spec, embedding schemars-derived JSON
/// Schemas into `components/schemas`.
pub fn build_openapi_spec() -> serde_json::Value {
    let mut node_schema =
        serde_json::to_value(schemars::schema_for!(crate::introspect::NodePayload))
            .expect("NodePayload schema must be serializable");
    let mut error_schema = serde_json::to_value(schemars::schema_for!(ApiErrorEnvelope))
        .expect("ApiErrorEnvelope schema must be serializable");

    // Hoist $defs into a shared components/schemas map so
    // OpenAPI tools can resolve references.
    let mut shared_schemas = serde_json::Map::new();
    hoist_defs(&mut node_schema, &mut shared_schemas);
    hoist_defs(&mut error_schema, &mut shared_schemas);
    shared_schemas.insert("NodePayload".into(), node_schema);
    shared_schemas.insert("ApiErrorEnvelope".into(), error_schema);

    let error_response = |desc: &str| -> serde_json::Value {
        serde_json::json!({
            "description": desc,
            "content": {
                "application/json": {
                    "schema": { "$ref": "#/components/schemas/ApiErrorEnvelope" }
                }
            }
        })
    };

    let success_payload = serde_json::json!({
        "description": "Resolved NodePayload",
        "content": {
            "application/json": {
                "schema": { "$ref": "#/components/schemas/NodePayload" }
            }
        }
    });

    serde_json::json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Monarch Mesh Admin API",
            "version": "1.0.0",
            "description": "Reference-walking introspection API for a Monarch actor mesh. See the Admin Gateway Pattern RFC."
        },
        "paths": {
            "/v1/root": {
                "get": {
                    "summary": "Fetch root node",
                    "operationId": "getRoot",
                    "responses": {
                        "200": success_payload,
                        "500": error_response("Internal error"),
                        "503": error_response("Service unavailable (at capacity, retry with backoff)"),
                        "504": error_response("Gateway timeout (downstream host unresponsive)")
                    }
                }
            },
            "/v1/{reference}": {
                "get": {
                    "summary": "Resolve a reference to a NodePayload",
                    "operationId": "resolveReference",
                    "parameters": [{
                        "name": "reference",
                        "in": "path",
                        "required": true,
                        "description": "URL-encoded opaque reference string",
                        "schema": { "type": "string" }
                    }],
                    "responses": {
                        "200": success_payload,
                        "400": error_response("Bad request (malformed reference)"),
                        "404": error_response("Reference not found"),
                        "500": error_response("Internal error"),
                        "503": error_response("Service unavailable (at capacity, retry with backoff)"),
                        "504": error_response("Gateway timeout (downstream host unresponsive)")
                    }
                }
            },
            "/v1/schema": {
                "get": {
                    "summary": "JSON Schema for NodePayload (Draft 2020-12)",
                    "operationId": "getSchema",
                    "responses": {
                        "200": {
                            "description": "JSON Schema document",
                            "content": { "application/json": {} }
                        }
                    }
                }
            },
            "/v1/schema/error": {
                "get": {
                    "summary": "JSON Schema for ApiErrorEnvelope (Draft 2020-12)",
                    "operationId": "getErrorSchema",
                    "responses": {
                        "200": {
                            "description": "JSON Schema document",
                            "content": { "application/json": {} }
                        }
                    }
                }
            },
            "/v1/tree": {
                "get": {
                    "summary": "ASCII topology dump (debug)",
                    "operationId": "getTree",
                    "responses": {
                        "200": {
                            "description": "Human-readable topology tree",
                            "content": { "text/plain": {} }
                        }
                    }
                }
            },
            "/v1/config/{proc_reference}": {
                "get": {
                    "summary": "Config snapshot for a proc",
                    "operationId": "getConfig",
                    "description": "Returns the effective CONFIG-marked configuration entries from the target process. Routes to ProcAgent (worker procs) or HostAgent (service proc).",
                    "parameters": [{
                        "name": "proc_reference",
                        "in": "path",
                        "required": true,
                        "description": "URL-encoded proc reference (ProcId)",
                        "schema": { "type": "string" }
                    }],
                    "responses": {
                        "200": {
                            "description": "ConfigDumpResult — sorted list of config entries",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "entries": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "name": { "type": "string" },
                                                        "value": { "type": "string" },
                                                        "default_value": { "type": ["string", "null"] },
                                                        "source": { "type": "string" },
                                                        "changed_from_default": { "type": "boolean" },
                                                        "env_var": { "type": ["string", "null"] }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "404": error_response("Proc not found or handler not reachable"),
                        "500": error_response("Internal error"),
                        "504": error_response("Gateway timeout")
                    }
                }
            }
        },
        "components": {
            "schemas": serde_json::Value::Object(shared_schemas)
        }
    })
}

/// OpenAPI 3.1 spec for the mesh admin API.
async fn serve_openapi() -> Result<axum::response::Json<serde_json::Value>, ApiError> {
    Ok(axum::response::Json(build_openapi_spec()))
}

/// Validate and parse a raw proc reference path segment into a
/// decoded reference string and `ProcId`. Extracted for testability.
fn parse_pyspy_proc_reference(
    raw: &str,
) -> Result<(String, hyperactor_reference::ProcId), ApiError> {
    let trimmed = raw.trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(ApiError::bad_request("empty proc reference", None));
    }
    let decoded = urlencoding::decode(trimmed)
        .map(|cow| cow.into_owned())
        .map_err(|_| {
            ApiError::bad_request(
                "malformed percent-encoding: decoded bytes are not valid UTF-8",
                None,
            )
        })?;
    let proc_id: hyperactor_reference::ProcId = decoded
        .parse()
        .map_err(|e| ApiError::bad_request(format!("invalid proc reference: {}", e), None))?;
    Ok((decoded, proc_id))
}

/// Probe whether an actor is reachable by sending a lightweight
/// introspect query bounded by `MESH_ADMIN_QUERY_CHILD_TIMEOUT`.
///
/// Returns `Ok(true)` if the actor responds, `Ok(false)` if the
/// actor is absent or unresponsive (timeout / recv error).
/// Returns `Err(ApiError)` on bridge-side send failure — a real
/// infrastructure problem, not an absent actor.
async fn probe_actor(
    cx: &Instance<()>,
    agent_id: &hyperactor_reference::ActorId,
) -> Result<bool, ApiError> {
    let port = hyperactor_reference::PortRef::<IntrospectMessage>::attest_message_port(agent_id);
    let (handle, rx) = open_once_port::<IntrospectResult>(cx);
    port.send(
        cx,
        IntrospectMessage::Query {
            view: IntrospectView::Entity,
            reply: handle.bind(),
        },
    )
    .map_err(|e| {
        tracing::warn!(
            name = "pyspy_probe_send_failed",
            %agent_id,
            error = %e,
        );
        ApiError {
            code: "internal_error".to_string(),
            message: format!("failed to send probe to {}: {}", agent_id, e),
            details: None,
        }
    })?;

    let timeout = hyperactor_config::global::get(crate::config::MESH_ADMIN_QUERY_CHILD_TIMEOUT);
    match tokio::time::timeout(timeout, rx.recv()).await {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(e)) => {
            tracing::debug!(
                name = "pyspy_probe_recv_failed",
                %agent_id,
                error = %e,
            );
            Ok(false)
        }
        Err(_elapsed) => {
            tracing::debug!(
                name = "pyspy_probe_timeout",
                %agent_id,
            );
            Ok(false)
        }
    }
}

/// HTTP bridge for py-spy stack dump requests.
///
/// Parses the proc reference, routes to the appropriate actor
/// (ProcAgent on worker procs, HostAgent on the service proc),
/// probes for reachability, and sends `PySpyDump` directly.
/// See PS-12, PS-13 in `introspect` module doc.
async fn pyspy_bridge(
    State(state): State<Arc<BridgeState>>,
    AxumPath(proc_reference): AxumPath<String>,
) -> Result<Json<PySpyResult>, ApiError> {
    let (proc_reference, proc_id) = parse_pyspy_proc_reference(&proc_reference)?;

    // PS-12: route by proc name — service proc → HostAgent, all others → ProcAgent.
    let agent_id = if proc_id.base_name() == SERVICE_PROC_NAME {
        proc_id.actor_id(HOST_MESH_AGENT_ACTOR_NAME, 0)
    } else {
        proc_id.actor_id(PROC_AGENT_ACTOR_NAME, 0)
    };

    // PS-13: defensive probe — verify the target actor is reachable
    // before committing to the full py-spy timeout.
    let cx = &state.bridge_cx;
    if !probe_actor(cx, &agent_id).await? {
        return Err(ApiError::not_found(
            format!(
                "proc {} does not have a reachable py-spy handler (expected {} actor)",
                proc_reference,
                if proc_id.base_name() == SERVICE_PROC_NAME {
                    HOST_MESH_AGENT_ACTOR_NAME
                } else {
                    PROC_AGENT_ACTOR_NAME
                },
            ),
            None,
        ));
    }

    let port = hyperactor_reference::PortRef::<PySpyDump>::attest_message_port(&agent_id);
    let (reply_handle, reply_rx) = open_once_port::<PySpyResult>(cx);
    // Native frames are essential for diagnosing hangs in C
    // extensions and CUDA calls — the primary py-spy use case in
    // Monarch. These defaults match the old hyperactor_multiprocess
    // battle-tested diagnostics.
    port.send(
        cx,
        PySpyDump {
            threads: false,
            native: true,
            native_all: true,
            nonblocking: false,
            result: reply_handle.bind(),
        },
    )
    .map_err(|e| ApiError {
        code: "internal_error".to_string(),
        message: format!("failed to send PySpyDump: {}", e),
        details: None,
    })?;

    let wire_result = tokio::time::timeout(
        hyperactor_config::global::get(crate::config::MESH_ADMIN_PYSPY_BRIDGE_TIMEOUT),
        reply_rx.recv(),
    )
    .await
    .map_err(|_| {
        tracing::warn!(
            proc_reference = %proc_reference,
            "mesh admin: py-spy dump timed out (gateway_timeout)",
        );
        ApiError {
            code: "gateway_timeout".to_string(),
            message: format!("timed out waiting for py-spy dump from {}", proc_reference),
            details: None,
        }
    })?
    .map_err(|e| ApiError {
        code: "internal_error".to_string(),
        message: format!("failed to receive PySpyResult: {}", e),
        details: None,
    })?;

    Ok(Json(wire_result))
}

/// HTTP bridge for config dump requests.
///
/// Parses the proc reference, routes to the appropriate actor
/// (ProcAgent on worker procs, HostAgent on the service proc),
/// probes for reachability, and sends `ConfigDump` directly.
/// See CFG-4 in `admin_tui/main.rs`.
async fn config_bridge(
    State(state): State<Arc<BridgeState>>,
    AxumPath(proc_reference): AxumPath<String>,
) -> Result<Json<ConfigDumpResult>, ApiError> {
    let (proc_reference, proc_id) = parse_pyspy_proc_reference(&proc_reference)?;

    // Route by proc name — service proc → HostAgent, all others → ProcAgent.
    let agent_id = if proc_id.base_name() == SERVICE_PROC_NAME {
        proc_id.actor_id(HOST_MESH_AGENT_ACTOR_NAME, 0)
    } else {
        proc_id.actor_id(PROC_AGENT_ACTOR_NAME, 0)
    };

    // Defensive probe — verify the target actor is reachable.
    let cx = &state.bridge_cx;
    if !probe_actor(cx, &agent_id).await? {
        return Err(ApiError::not_found(
            format!(
                "proc {} does not have a reachable config handler (expected {} actor)",
                proc_reference,
                if proc_id.base_name() == SERVICE_PROC_NAME {
                    HOST_MESH_AGENT_ACTOR_NAME
                } else {
                    PROC_AGENT_ACTOR_NAME
                },
            ),
            None,
        ));
    }

    let port = hyperactor_reference::PortRef::<ConfigDump>::attest_message_port(&agent_id);
    let (reply_handle, reply_rx) = open_once_port::<ConfigDumpResult>(cx);
    port.send(
        cx,
        ConfigDump {
            result: reply_handle.bind(),
        },
    )
    .map_err(|e| ApiError {
        code: "internal_error".to_string(),
        message: format!("failed to send ConfigDump: {}", e),
        details: None,
    })?;

    // Short timeout — config_entries() is instantaneous.
    let wire_result = tokio::time::timeout(
        hyperactor_config::global::get(crate::config::MESH_ADMIN_QUERY_CHILD_TIMEOUT),
        reply_rx.recv(),
    )
    .await
    .map_err(|_| {
        tracing::warn!(
            proc_reference = %proc_reference,
            "mesh admin: config dump timed out (gateway_timeout)",
        );
        ApiError {
            code: "gateway_timeout".to_string(),
            message: format!("timed out waiting for config dump from {}", proc_reference),
            details: None,
        }
    })?
    .map_err(|e| ApiError {
        code: "internal_error".to_string(),
        message: format!("failed to receive ConfigDumpResult: {}", e),
        details: None,
    })?;

    Ok(Json(wire_result))
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
    AxumPath(reference): AxumPath<String>,
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

    // Limit concurrent resolves to avoid starving user workloads
    // that share this tokio runtime.
    let _permit = state.resolve_semaphore.try_acquire().map_err(|_| {
        tracing::warn!("mesh admin: rejecting resolve request (503): too many concurrent requests");
        ApiError {
            code: "service_unavailable".to_string(),
            message: "too many concurrent introspection requests".to_string(),
            details: None,
        }
    })?;

    let cx = &state.bridge_cx;
    let resolve_start = std::time::Instant::now();
    let response = tokio::time::timeout(
        hyperactor_config::global::get(crate::config::MESH_ADMIN_SINGLE_HOST_TIMEOUT),
        state.admin_ref.resolve(cx, reference.clone()),
    )
    .await
    .map_err(|_| {
        tracing::warn!(
            reference = %reference,
            elapsed_ms = resolve_start.elapsed().as_millis() as u64,
            "mesh admin: resolve timed out (gateway_timeout)",
        );
        ApiError {
            code: "gateway_timeout".to_string(),
            message: "timed out resolving reference".to_string(),
            details: None,
        }
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

// TODO: MESH_ADMIN_TREE_TIMEOUT is applied per-call, not as a total
// budget. On a mesh with N hosts and M procs, the worst case is
// N*(1+M) sequential calls each up to 10s. This should use a single
// deadline for the entire walk.
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
/// unix:@hash  ->  https://host:port/v1/...  (or http:// in OSS)
/// ├── service  ->  https://host:port/v1/...
/// │   ├── agent[0]  ->  https://host:port/v1/...
/// │   └── client[0]  ->  https://host:port/v1/...
/// ├── local  ->  https://host:port/v1/...
/// └── philosophers_0  ->  https://host:port/v1/...
///     ├── agent[0]  ->  https://host:port/v1/...
///     └── philosopher[0]  ->  https://host:port/v1/...
/// ```
async fn tree_dump(
    State(state): State<Arc<BridgeState>>,
    headers: axum::http::header::HeaderMap,
) -> Result<String, ApiError> {
    // Limit concurrent resolves to avoid starving user workloads.
    let _permit = state.resolve_semaphore.try_acquire().map_err(|_| {
        tracing::warn!(
            "mesh admin: rejecting tree_dump request (503): too many concurrent requests"
        );
        ApiError {
            code: "service_unavailable".to_string(),
            message: "too many concurrent introspection requests".to_string(),
            details: None,
        }
    })?;

    let cx = &state.bridge_cx;

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
    let root_resp = tokio::time::timeout(
        hyperactor_config::global::get(crate::config::MESH_ADMIN_TREE_TIMEOUT),
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
        let resp = tokio::time::timeout(
            hyperactor_config::global::get(crate::config::MESH_ADMIN_TREE_TIMEOUT),
            state.admin_ref.resolve(cx, child_ref.clone()),
        )
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
                    let proc_resp = tokio::time::timeout(
                        hyperactor_config::global::get(crate::config::MESH_ADMIN_TREE_TIMEOUT),
                        state.admin_ref.resolve(cx, proc_ref.clone()),
                    )
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
/// `HostAgent`'s children list:
///
/// - System proc ref `"[system] unix:@hash,service"` → `"service"`
/// - ProcAgent ActorId `"unix:@hash,my_proc,agent[0]"` →
///   `"my_proc"`
/// - Bare ProcId `"unix:@hash,my_proc"` → `"my_proc"`
///
/// Note: `ActorId::Display` for `ProcId` uses commas as
/// separators (`proc_id,actor_name[idx]`), not slashes.
fn derive_tree_label(reference: &str) -> String {
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

// -- CLI-based mast_conda:/// resolution --
//
// See module doc for MC-1..MC-5 invariants.

/// Top-level response from `mast get-status --json`.
#[derive(serde::Deserialize)]
struct MastStatusResponse {
    #[serde(rename = "latestAttempt")]
    latest_attempt: MastAttempt,
}

/// A single execution attempt containing task groups.
#[derive(serde::Deserialize)]
struct MastAttempt {
    #[serde(rename = "taskGroupExecutionAttempts")]
    task_groups: std::collections::HashMap<String, Vec<MastTaskGroup>>,
}

/// A task group execution attempt containing per-task attempts.
#[derive(serde::Deserialize)]
struct MastTaskGroup {
    #[serde(rename = "taskExecutionAttempts")]
    tasks: std::collections::HashMap<String, Vec<MastTaskAttempt>>,
}

/// A single task execution attempt with an optional hostname.
#[derive(serde::Deserialize)]
struct MastTaskAttempt {
    hostname: Option<String>,
}

/// Extract the head node hostname from a parsed MAST status response
/// (MC-2).
///
/// For each task group, the last attempt is selected. Within that
/// attempt, each task's last execution attempt provides the hostname.
/// Task indices (the map keys) are parsed as integers and sorted
/// ascending; the first hostname is the head node.
fn head_hostname(response: &MastStatusResponse) -> Result<String, String> {
    let mut hosts: Vec<(i64, String)> = Vec::new();
    for attempts in response.latest_attempt.task_groups.values() {
        let group = match attempts.last() {
            Some(g) => g,
            None => continue,
        };
        for (index_str, task_attempts) in &group.tasks {
            let attempt = match task_attempts.last() {
                Some(a) => a,
                None => continue,
            };
            if let Some(ref hostname) = attempt.hostname {
                let index = index_str.parse::<i64>().unwrap_or(i64::MAX);
                hosts.push((index, hostname.clone()));
            }
        }
    }
    hosts.sort_by_key(|(idx, _)| *idx);
    hosts
        .into_iter()
        .next()
        .map(|(_, h)| h)
        .ok_or_else(|| "no hostnames found in MAST response".to_string())
}

/// Qualify a short hostname to an FQDN via
/// `getaddrinfo(AI_CANONNAME)` (MC-3,
/// MC-4).
///
/// Called via `spawn_blocking` to avoid blocking tokio workers. Falls
/// back to the raw hostname on any failure.
async fn qualify_fqdn(hostname: &str) -> String {
    if hostname.contains('.') {
        return hostname.to_string();
    }
    let owned = hostname.to_string();
    let fallback = owned.clone();
    tokio::task::spawn_blocking(move || qualify_fqdn_blocking(&owned))
        .await
        .unwrap_or(fallback)
}

fn qualify_fqdn_blocking(hostname: &str) -> String {
    use std::ffi::CStr;
    use std::ffi::CString;
    use std::ptr;

    let c_host = match CString::new(hostname) {
        Ok(c) => c,
        Err(_) => return hostname.to_string(),
    };
    // SAFETY: zeroed addrinfo is a valid hints struct (all-zero is
    // AF_UNSPEC with no flags, which we then override).
    let mut hints: libc::addrinfo = unsafe { std::mem::zeroed() };
    hints.ai_flags = libc::AI_CANONNAME;
    hints.ai_family = libc::AF_UNSPEC;

    let mut result: *mut libc::addrinfo = ptr::null_mut();
    // SAFETY: c_host is a valid NUL-terminated string, hints is
    // initialized, and result is a valid out-pointer.
    let rc = unsafe { libc::getaddrinfo(c_host.as_ptr(), ptr::null(), &hints, &mut result) };
    if rc != 0 || result.is_null() {
        return hostname.to_string();
    }

    // RAII guard ensures freeaddrinfo is called.
    struct AddrInfoGuard(*mut libc::addrinfo);
    impl Drop for AddrInfoGuard {
        fn drop(&mut self) {
            // SAFETY: self.0 was returned by a successful getaddrinfo.
            unsafe { libc::freeaddrinfo(self.0) }
        }
    }
    let _guard = AddrInfoGuard(result);

    // SAFETY: result is non-null and was returned by getaddrinfo.
    let canon = unsafe { (*result).ai_canonname };
    if canon.is_null() {
        return hostname.to_string();
    }
    // SAFETY: canon is non-null and NUL-terminated per getaddrinfo
    // contract.
    unsafe { CStr::from_ptr(canon) }
        .to_str()
        .unwrap_or(hostname)
        .to_string()
}

/// Resolve admin port from an explicit override or `MESH_ADMIN_ADDR`
/// config (MC-5).
///
/// When `port_override` is `Some`, that port is used directly. When
/// `None`, the port is read from the `MESH_ADMIN_ADDR` configuration
/// attribute (parsed as a `SocketAddr`).
fn resolve_admin_port(port_override: Option<u16>) -> Result<u16, anyhow::Error> {
    match port_override {
        Some(p) => Ok(p),
        None => {
            let config_addr = hyperactor_config::global::get_cloned(crate::config::MESH_ADMIN_ADDR);
            Ok(config_addr
                .parse_socket_addr()
                .map_err(|e| anyhow::anyhow!("invalid MESH_ADMIN_ADDR config: {}", e))?
                .port())
        }
    }
}

/// Resolve a `mast_conda:///<job-name>` handle into an
/// `https://<fqdn>:<port>` base URL by shelling out to the `mast` CLI
/// (MC-1).
///
/// This is the OSS-compatible counterpart to
/// `hyperactor_meta::mesh_admin::resolve_mast_handle`. The `cmd`
/// parameter names the CLI binary; production passes `"mast"`, tests
/// substitute a synthetic command.
async fn try_resolve_mast_handle(
    handle: &str,
    port_override: Option<u16>,
    cmd: &str,
) -> anyhow::Result<String> {
    let port = resolve_admin_port(port_override)?;
    let job_name = handle
        .strip_prefix("mast_conda:///")
        .ok_or_else(|| anyhow::anyhow!("expected mast_conda:/// prefix, got '{}'", handle))?;

    let output = match tokio::process::Command::new(cmd)
        .args(["get-status", "--json", job_name])
        .output()
        .await
    {
        Ok(o) => o,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            anyhow::bail!(
                "MAST CLI not found; install via `sudo feature install --persist mast_cli`"
            );
        }
        Err(e) => {
            anyhow::bail!("failed to run '{}': {}", cmd, e);
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "'mast get-status' exited with {}: {}",
            output.status,
            stderr.trim()
        );
    }

    let response: MastStatusResponse = serde_json::from_slice(&output.stdout)
        .map_err(|e| anyhow::anyhow!("failed to parse mast JSON output: {}", e))?;

    let hostname =
        head_hostname(&response).map_err(|e| anyhow::anyhow!("MAST job '{}': {}", job_name, e))?;

    let fqdn = qualify_fqdn(&hostname).await;
    Ok(format!("https://{}:{}", fqdn, port))
}

/// Resolve a `mast_conda:///<job-name>` handle into an
/// `https://<fqdn>:<port>` base URL using the `mast` CLI.
///
/// This is the CLI/OSS path. The thrift-based equivalent lives in
/// `hyperactor_meta::mesh_admin::resolve_mast_handle`. Binaries
/// match on `MastResolver` to select which to call.
pub async fn resolve_mast_handle(
    handle: &str,
    port_override: Option<u16>,
) -> anyhow::Result<String> {
    try_resolve_mast_handle(handle, port_override, "mast").await
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use hyperactor::channel::ChannelAddr;
    use hyperactor::testing::ids::test_proc_id_with_addr;

    use super::*;

    // Integration tests that spawn MeshAdminAgent must pass
    // `Some("[::]:0".parse().unwrap())` as the admin_addr to get an
    // ephemeral port. The default (`None`) reads MESH_ADMIN_ADDR
    // config which is `[::]:1729` — a fixed port that causes bind
    // conflicts when tests run concurrently.

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

        let proc1 = test_proc_id_with_addr(ChannelAddr::Tcp(addr1), "host1");
        let proc2 = test_proc_id_with_addr(ChannelAddr::Tcp(addr2), "host2");

        let actor_id1 = proc1.actor_id("mesh_agent", 0);
        let actor_id2 = proc2.actor_id("mesh_agent", 0);

        let ref1: hyperactor_reference::ActorRef<HostAgent> =
            hyperactor_reference::ActorRef::attest(actor_id1.clone());
        let ref2: hyperactor_reference::ActorRef<HostAgent> =
            hyperactor_reference::ActorRef::attest(actor_id2.clone());

        let agent = MeshAdminAgent::new(
            vec![("host_a".to_string(), ref1), ("host_b".to_string(), ref2)],
            None,
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

        // Verify root properties derived from attrs.
        match &payload.properties {
            NodeProperties::Root {
                num_hosts,
                started_by,
                ..
            } => {
                assert_eq!(*num_hosts, 2);
                assert!(!started_by.is_empty());
            }
            other => panic!("expected Root, got {:?}", other),
        }
    }

    // End-to-end smoke test for MeshAdminAgent::resolve that walks
    // the reference tree: root → host → system proc → host-agent
    // cross-reference. Verifies the reverse index routes the
    // HostAgent ActorId to NodeProperties::Host (not Actor),
    // preventing the TUI's cycle detection from dropping that node.
    #[tokio::test]
    async fn test_resolve_reference_tree_walk() {
        use hyperactor::Proc;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::host::Host;
        use hyperactor::host::LocalProcManager;

        use crate::host_mesh::host_agent::HostAgentMode;
        use crate::host_mesh::host_agent::ProcManagerSpawnFn;
        use crate::proc_agent::ProcAgent;

        // -- 1. Stand up a local in-process Host with a HostAgent --
        // Use Unix transport for all procs — Local transport does not
        // support cross-proc message routing.
        let spawn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
        let manager: LocalProcManager<ProcManagerSpawnFn> = LocalProcManager::new(spawn);
        let host: Host<LocalProcManager<ProcManagerSpawnFn>> =
            Host::new(manager, ChannelTransport::Unix.any())
                .await
                .unwrap();
        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_agent_handle = system_proc
            .spawn(
                crate::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Local(host)),
            )
            .unwrap();
        let host_agent_ref: hyperactor_reference::ActorRef<HostAgent> = host_agent_handle.bind();
        let host_addr_str = host_addr.to_string();

        // -- 2. Spawn MeshAdminAgent on a separate proc --
        let admin_proc = Proc::direct(ChannelTransport::Unix.any(), "admin".to_string()).unwrap();
        // The admin proc has no supervision coordinator by default.
        // Without one, actor teardown triggers std::process::exit(1).
        use hyperactor::testing::proc_supervison::ProcSupervisionCoordinator;
        let _supervision = ProcSupervisionCoordinator::set(&admin_proc).await.unwrap();
        let admin_handle = admin_proc
            .spawn(
                MESH_ADMIN_ACTOR_NAME,
                MeshAdminAgent::new(
                    vec![(host_addr_str.clone(), host_agent_ref.clone())],
                    None,
                    Some("[::]:0".parse().unwrap()),
                ),
            )
            .unwrap();
        let admin_ref: hyperactor_reference::ActorRef<MeshAdminAgent> = admin_handle.bind();

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
        assert_eq!(root.children.len(), 1); // host only (admin proc no longer standalone)

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
        assert!(
            matches!(proc_node.properties, NodeProperties::Proc { .. }),
            "expected Proc properties, got {:?}",
            proc_node.properties
        );
        assert_eq!(proc_node.parent, Some(host_child_ref_str.clone()));
        // The system proc should have at least the "host_agent" actor.
        assert!(
            !proc_node.children.is_empty(),
            "proc should have at least one actor child"
        );

        // -- 7. Cross-reference: system proc child is the host agent --
        //
        // The service proc's actor (agent[0]) IS the HostAgent, so
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

    // Verifies MeshAdminAgent::resolve returns NodeProperties::Proc
    // for all proc children. Spawns a user proc via
    // CreateOrUpdate<ProcSpec>, resolves all host proc-children, and
    // asserts every proc returns Proc properties.
    #[tokio::test]
    async fn test_proc_properties_for_all_procs() {
        use std::time::Duration;

        use hyperactor::Proc;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::host::Host;
        use hyperactor::host::LocalProcManager;

        use crate::Name;
        use crate::host_mesh::host_agent::HostAgentMode;
        use crate::host_mesh::host_agent::ProcManagerSpawnFn;
        use crate::proc_agent::ProcAgent;
        use crate::resource;
        use crate::resource::ProcSpec;
        use crate::resource::Rank;

        // Stand up a local in-process Host with a HostAgent.
        let spawn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
        let manager: LocalProcManager<ProcManagerSpawnFn> = LocalProcManager::new(spawn);
        let host: Host<LocalProcManager<ProcManagerSpawnFn>> =
            Host::new(manager, ChannelTransport::Unix.any())
                .await
                .unwrap();
        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_agent_handle = system_proc
            .spawn(
                crate::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Local(host)),
            )
            .unwrap();
        let host_agent_ref: hyperactor_reference::ActorRef<HostAgent> = host_agent_handle.bind();
        let host_addr_str = host_addr.to_string();

        // Spawn MeshAdminAgent on a separate proc.
        let admin_proc = Proc::direct(ChannelTransport::Unix.any(), "admin".to_string()).unwrap();
        use hyperactor::testing::proc_supervison::ProcSupervisionCoordinator;
        let _supervision = ProcSupervisionCoordinator::set(&admin_proc).await.unwrap();
        let admin_handle = admin_proc
            .spawn(
                MESH_ADMIN_ACTOR_NAME,
                MeshAdminAgent::new(
                    vec![(host_addr_str.clone(), host_agent_ref.clone())],
                    None,
                    Some("[::]:0".parse().unwrap()),
                ),
            )
            .unwrap();
        let admin_ref: hyperactor_reference::ActorRef<MeshAdminAgent> = admin_handle.bind();

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
        tokio::time::sleep(Duration::from_secs(2)).await;

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

        // Resolve each proc child and verify it has Proc properties.
        let user_proc_name_str = user_proc_name.to_string();
        let mut found_system = false;
        let mut found_user = false;
        for child_ref_str in &host_node.children {
            let resp = admin_ref
                .resolve(&client, child_ref_str.clone())
                .await
                .unwrap();
            let node = resp.0.unwrap();
            if let NodeProperties::Proc { proc_name, .. } = &node.properties {
                if proc_name.contains(&user_proc_name_str) {
                    found_user = true;
                } else {
                    found_system = true;
                }
                // Properties derived from attrs — verified by derive_properties tests.
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

    // Verifies that build_root_payload lists only the host as a
    // child. The root client is visible under its host's local proc,
    // not at root level.
    #[test]
    fn test_build_root_payload_with_root_client() {
        let addr1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let proc1 = hyperactor_reference::ProcId::with_name(ChannelAddr::Tcp(addr1), "host1");
        let actor_id1 = hyperactor_reference::ActorId::root(proc1, "mesh_agent".to_string());
        let ref1: hyperactor_reference::ActorRef<HostAgent> =
            hyperactor_reference::ActorRef::attest(actor_id1.clone());

        let client_proc_id =
            hyperactor_reference::ProcId::with_name(ChannelAddr::Tcp(addr1), "local");
        let client_actor_id = client_proc_id.actor_id("client", 0);

        let agent = MeshAdminAgent::new(
            vec![("host_a".to_string(), ref1)],
            Some(client_actor_id.clone()),
            None,
        );

        let payload = agent.build_root_payload();
        assert!(matches!(
            payload.properties,
            NodeProperties::Root { num_hosts: 1, .. }
        ));
        // Only the host; root client is under host → local proc.
        assert_eq!(payload.children.len(), 1);
        assert!(
            payload
                .children
                .contains(&HostId(actor_id1.clone()).to_string())
        );
    }

    // Verifies that the root client actor is visible through the
    // host → local proc → actor path, not as a standalone child of
    // root.
    #[tokio::test]
    async fn test_resolve_root_client_actor() {
        use hyperactor::channel::ChannelTransport;
        use hyperactor::host::Host;
        use hyperactor::host::LocalProcManager;

        use crate::host_mesh::host_agent::HostAgentMode;
        use crate::host_mesh::host_agent::ProcManagerSpawnFn;
        use crate::proc_agent::ProcAgent;

        // Stand up a local in-process Host with a HostAgent.
        let spawn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
        let manager: LocalProcManager<ProcManagerSpawnFn> = LocalProcManager::new(spawn);
        let host: Host<LocalProcManager<ProcManagerSpawnFn>> =
            Host::new(manager, ChannelTransport::Unix.any())
                .await
                .unwrap();
        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();

        // Spawn the root client on the host's local proc (before
        // moving the host into HostAgentMode).
        let local_proc = host.local_proc();
        let local_proc_id = local_proc.proc_id().clone();
        let root_client_handle = local_proc.spawn("client", TestIntrospectableActor).unwrap();
        let root_client_ref: hyperactor_reference::ActorRef<TestIntrospectableActor> =
            root_client_handle.bind();
        let root_client_actor_id = root_client_ref.actor_id().clone();

        let host_agent_handle = system_proc
            .spawn(
                crate::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Local(host)),
            )
            .unwrap();
        let host_agent_ref: hyperactor_reference::ActorRef<HostAgent> = host_agent_handle.bind();
        let host_addr_str = host_addr.to_string();

        // Spawn MeshAdminAgent with the root client ActorId.
        let admin_proc =
            hyperactor::Proc::direct(ChannelTransport::Unix.any(), "admin".to_string()).unwrap();
        use hyperactor::testing::proc_supervison::ProcSupervisionCoordinator;
        let _supervision = ProcSupervisionCoordinator::set(&admin_proc).await.unwrap();
        let admin_handle = admin_proc
            .spawn(
                MESH_ADMIN_ACTOR_NAME,
                MeshAdminAgent::new(
                    vec![(host_addr_str.clone(), host_agent_ref.clone())],
                    Some(root_client_actor_id.clone()),
                    Some("[::]:0".parse().unwrap()),
                ),
            )
            .unwrap();
        let admin_ref: hyperactor_reference::ActorRef<MeshAdminAgent> = admin_handle.bind();

        // Client for sending messages.
        let client_proc =
            hyperactor::Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _handle) = client_proc.instance("client").unwrap();

        // Resolve "root" — should contain only the host.
        let root_resp = admin_ref
            .resolve(&client, "root".to_string())
            .await
            .unwrap();
        let root = root_resp.0.unwrap();
        let host_id_str = HostId(host_agent_ref.actor_id().clone()).to_string();
        assert!(
            root.children.contains(&host_id_str),
            "root children {:?} should contain host {}",
            root.children,
            host_id_str
        );

        // Resolve the host — should list the local proc in children.
        let host_resp = admin_ref
            .resolve(&client, host_id_str.clone())
            .await
            .unwrap();
        let host_node = host_resp.0.unwrap();
        let local_proc_str = local_proc_id.to_string();
        assert!(
            host_node.children.contains(&local_proc_str),
            "host children {:?} should contain local proc {}",
            host_node.children,
            local_proc_str
        );

        // Resolve the local proc — should contain the root client actor.
        let proc_resp = admin_ref
            .resolve(&client, local_proc_str.clone())
            .await
            .unwrap();
        let proc_node = proc_resp.0.unwrap();
        assert!(
            matches!(proc_node.properties, NodeProperties::Proc { .. }),
            "expected Proc properties, got {:?}",
            proc_node.properties
        );
        assert!(
            proc_node
                .children
                .contains(&root_client_actor_id.to_string()),
            "local proc children {:?} should contain root client actor {}",
            proc_node.children,
            root_client_actor_id
        );

        // Resolve the root client actor — parent should be the local proc.
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
            Some(local_proc_str),
            "root client parent should be the local proc"
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

        use crate::host_mesh::host_agent::HostAgentMode;
        use crate::host_mesh::host_agent::ProcManagerSpawnFn;
        use crate::proc_agent::ProcAgent;

        // Stand up a local host with a HostAgent.
        let spawn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
        let manager: LocalProcManager<ProcManagerSpawnFn> = LocalProcManager::new(spawn);
        let host: Host<LocalProcManager<ProcManagerSpawnFn>> =
            Host::new(manager, ChannelTransport::Unix.any())
                .await
                .unwrap();
        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let host_agent_handle = system_proc
            .spawn(
                crate::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Local(host)),
            )
            .unwrap();
        let host_agent_ref: hyperactor_reference::ActorRef<HostAgent> = host_agent_handle.bind();
        let host_addr_str = host_addr.to_string();

        // Spawn MeshAdminAgent on a separate proc.
        let admin_proc = Proc::direct(ChannelTransport::Unix.any(), "admin".to_string()).unwrap();
        use hyperactor::testing::proc_supervison::ProcSupervisionCoordinator;
        let _supervision = ProcSupervisionCoordinator::set(&admin_proc).await.unwrap();
        let admin_handle = admin_proc
            .spawn(
                MESH_ADMIN_ACTOR_NAME,
                MeshAdminAgent::new(
                    vec![(host_addr_str, host_agent_ref)],
                    None,
                    Some("[::]:0".parse().unwrap()),
                ),
            )
            .unwrap();
        let admin_ref: hyperactor_reference::ActorRef<MeshAdminAgent> = admin_handle.bind();

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

            // NI-1: identity matches the reference used.
            assert_eq!(
                node.identity, ref_str,
                "identity mismatch: resolved '{}' but payload.identity = '{}'",
                ref_str, node.identity
            );

            // NI-2: parent matches the parent node's identity.
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

    // Exercises SP-1..SP-4 for host/proc payloads.
    #[tokio::test]
    async fn test_system_proc_identity() {
        use hyperactor::Proc;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::host::Host;
        use hyperactor::host::LocalProcManager;

        use crate::host_mesh::host_agent::HostAgentMode;
        use crate::host_mesh::host_agent::ProcManagerSpawnFn;
        use crate::proc_agent::ProcAgent;

        // -- 1. Stand up a local in-process Host with a HostAgent --
        let spawn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
        let manager: LocalProcManager<ProcManagerSpawnFn> = LocalProcManager::new(spawn);
        let host: Host<LocalProcManager<ProcManagerSpawnFn>> =
            Host::new(manager, ChannelTransport::Unix.any())
                .await
                .unwrap();
        let host_addr = host.addr().clone();
        let system_proc = host.system_proc().clone();
        let system_proc_id = system_proc.proc_id().clone();
        let host_agent_handle = system_proc
            .spawn(
                crate::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Local(host)),
            )
            .unwrap();
        let host_agent_ref: hyperactor_reference::ActorRef<HostAgent> = host_agent_handle.bind();
        let host_addr_str = host_addr.to_string();

        // -- 2. Spawn MeshAdminAgent on a separate proc --
        let admin_proc = Proc::direct(ChannelTransport::Unix.any(), "admin".to_string()).unwrap();
        use hyperactor::testing::proc_supervison::ProcSupervisionCoordinator;
        let _supervision = ProcSupervisionCoordinator::set(&admin_proc).await.unwrap();
        let admin_handle = admin_proc
            .spawn(
                MESH_ADMIN_ACTOR_NAME,
                MeshAdminAgent::new(
                    vec![(host_addr_str.clone(), host_agent_ref.clone())],
                    None,
                    Some("[::]:0".parse().unwrap()),
                ),
            )
            .unwrap();
        let admin_ref: hyperactor_reference::ActorRef<MeshAdminAgent> = admin_handle.bind();

        // -- 3. Create a bare client instance for sending messages --
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _handle) = client_proc.instance("client").unwrap();

        // -- 4. Resolve the host to get its children --
        let host_ref_str = HostId(host_agent_ref.actor_id().clone()).to_string();
        let host_resp = admin_ref
            .resolve(&client, host_ref_str.clone())
            .await
            .unwrap();
        let host_node = host_resp.0.unwrap();
        assert!(
            !host_node.children.is_empty(),
            "host should have at least one proc child"
        );

        // -- 5. Find a system proc child via system_children --
        let system_children = match &host_node.properties {
            NodeProperties::Host {
                system_children, ..
            } => system_children.clone(),
            other => panic!("expected Host properties, got {:?}", other),
        };
        // Procs are never system — host system_children should be empty.
        assert!(
            system_children.is_empty(),
            "host system_children should be empty (procs are never system), got {:?}",
            system_children
        );
        // Verify host properties derived from attrs.
        assert!(
            matches!(&host_node.properties, NodeProperties::Host { .. }),
            "expected Host properties"
        );

        // -- 6. Verify host children contain the system proc --
        let expected_system_ref = system_proc_id.to_string();
        assert!(
            host_node.children.contains(&expected_system_ref),
            "host children {:?} should contain the system proc ref '{}'",
            host_node.children,
            expected_system_ref
        );

        // -- 7. Resolve a proc child --
        let proc_child_ref = &host_node.children[0];
        let proc_resp = admin_ref
            .resolve(&client, proc_child_ref.clone())
            .await
            .unwrap();
        let proc_node = proc_resp.0.unwrap();

        assert_eq!(
            proc_node.identity, *proc_child_ref,
            "identity must match the proc ref from the host's children list"
        );

        assert!(
            matches!(proc_node.properties, NodeProperties::Proc { .. }),
            "expected NodeProperties::Proc, got {:?}",
            proc_node.properties
        );

        assert_eq!(
            proc_node.parent,
            Some(host_ref_str.clone()),
            "proc parent should be the host reference (host:<actor_id>)"
        );

        assert!(
            !proc_node.as_of.is_empty(),
            "as_of should be present and non-empty"
        );

        // Verify proc properties derived from attrs.
        assert!(
            matches!(&proc_node.properties, NodeProperties::Proc { .. }),
            "expected Proc properties"
        );
    }

    // -- MAST CLI resolver tests --
    //
    // These tests exercise the invariants introduced by the CLI-based
    // MAST handle resolution. Each test names the invariant it
    // establishes.

    /// Helper: build a `MastStatusResponse` from a list of
    /// `(task_index, hostname)` pairs in one task group.
    fn mast_response_from_hosts(hosts: &[(i64, &str)]) -> super::MastStatusResponse {
        let mut tasks = std::collections::HashMap::new();
        for (idx, host) in hosts {
            tasks.insert(
                idx.to_string(),
                vec![super::MastTaskAttempt {
                    hostname: Some(host.to_string()),
                }],
            );
        }
        super::MastStatusResponse {
            latest_attempt: super::MastAttempt {
                task_groups: std::collections::HashMap::from([(
                    "trainers".to_string(),
                    vec![super::MastTaskGroup { tasks }],
                )]),
            },
        }
    }

    // MC-2: single group, tasks sorted by ascending index.
    #[test]
    fn test_head_hostname_single_group() {
        let response = mast_response_from_hosts(&[(2, "host2"), (0, "host0"), (1, "host1")]);
        let head = super::head_hostname(&response).unwrap();
        assert_eq!(head, "host0");
    }

    // MC-2: last attempt selected per task.
    #[test]
    fn test_head_hostname_last_attempt_wins() {
        let mut tasks = std::collections::HashMap::new();
        tasks.insert(
            "0".to_string(),
            vec![
                super::MastTaskAttempt {
                    hostname: Some("old_host".to_string()),
                },
                super::MastTaskAttempt {
                    hostname: Some("new_host".to_string()),
                },
            ],
        );
        let response = super::MastStatusResponse {
            latest_attempt: super::MastAttempt {
                task_groups: std::collections::HashMap::from([(
                    "trainers".to_string(),
                    vec![super::MastTaskGroup { tasks }],
                )]),
            },
        };
        let head = super::head_hostname(&response).unwrap();
        assert_eq!(head, "new_host");
    }

    // MC-2: multiple groups merged and sorted.
    #[test]
    fn test_head_hostname_multiple_groups() {
        let mut tasks_a = std::collections::HashMap::new();
        tasks_a.insert(
            "1".to_string(),
            vec![super::MastTaskAttempt {
                hostname: Some("host_a1".to_string()),
            }],
        );
        let mut tasks_b = std::collections::HashMap::new();
        tasks_b.insert(
            "0".to_string(),
            vec![super::MastTaskAttempt {
                hostname: Some("host_b0".to_string()),
            }],
        );
        let response = super::MastStatusResponse {
            latest_attempt: super::MastAttempt {
                task_groups: std::collections::HashMap::from([
                    (
                        "group_a".to_string(),
                        vec![super::MastTaskGroup { tasks: tasks_a }],
                    ),
                    (
                        "group_b".to_string(),
                        vec![super::MastTaskGroup { tasks: tasks_b }],
                    ),
                ]),
            },
        };
        let head = super::head_hostname(&response).unwrap();
        assert_eq!(head, "host_b0");
    }

    // MC-2: no hostnames → error.
    #[test]
    fn test_head_hostname_empty() {
        let response = super::MastStatusResponse {
            latest_attempt: super::MastAttempt {
                task_groups: std::collections::HashMap::new(),
            },
        };
        assert!(super::head_hostname(&response).is_err());
    }

    // MC-2: hostname field is None (task not yet
    // allocated) → skipped.
    #[test]
    fn test_head_hostname_skips_unallocated() {
        let mut tasks = std::collections::HashMap::new();
        tasks.insert(
            "0".to_string(),
            vec![super::MastTaskAttempt { hostname: None }],
        );
        tasks.insert(
            "1".to_string(),
            vec![super::MastTaskAttempt {
                hostname: Some("allocated_host".to_string()),
            }],
        );
        let response = super::MastStatusResponse {
            latest_attempt: super::MastAttempt {
                task_groups: std::collections::HashMap::from([(
                    "trainers".to_string(),
                    vec![super::MastTaskGroup { tasks }],
                )]),
            },
        };
        let head = super::head_hostname(&response).unwrap();
        assert_eq!(head, "allocated_host");
    }

    // MC-3: hostname with dot passes through
    // unchanged (no DNS lookup).
    #[tokio::test]
    async fn test_qualify_fqdn_already_qualified() {
        let fqdn = super::qualify_fqdn("fake.nonexistent.tld").await;
        assert_eq!(fqdn, "fake.nonexistent.tld");
    }

    // MC-3, MC-4: short hostname
    // goes through getaddrinfo via spawn_blocking. The result is
    // environment-dependent, but must be non-empty and the call
    // must complete without hanging.
    #[tokio::test]
    async fn test_qualify_fqdn_short_hostname() {
        let fqdn = super::qualify_fqdn("localhost").await;
        assert!(!fqdn.is_empty(), "qualify_fqdn returned empty string");
    }

    // MC-3: nonexistent short hostname falls back
    // to the raw input.
    #[tokio::test]
    async fn test_qualify_fqdn_nonexistent_fallback() {
        let input = "__nonexistent_host_for_test";
        let fqdn = super::qualify_fqdn(input).await;
        assert_eq!(fqdn, input);
    }

    // MC-5: explicit override is used directly.
    #[test]
    fn test_resolve_admin_port_override() {
        assert_eq!(super::resolve_admin_port(Some(8080)).unwrap(), 8080);
    }

    // MC-5: falls back to MESH_ADMIN_ADDR config
    // (default [::]:1729).
    #[test]
    fn test_resolve_admin_port_from_config() {
        let port = super::resolve_admin_port(None).unwrap();
        assert_eq!(port, 1729);
    }

    // MC-1: missing binary produces a "not found" error.
    #[tokio::test]
    async fn test_cli_missing_binary() {
        let result = super::try_resolve_mast_handle(
            "mast_conda:///test-job",
            Some(1729),
            "__nonexistent_mast_test_bin",
        )
        .await;
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("not found"),
            "expected 'not found' in error, got: {}",
            err
        );
    }

    /// Write a shell script to a temp directory and return the
    /// directory guard and script path.
    fn write_test_script(content: &str) -> (tempfile::TempDir, String) {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("mast_stub.sh");
        std::fs::write(&script_path, content).unwrap();
        std::fs::set_permissions(&script_path, PermissionsExt::from_mode(0o755)).unwrap();
        let path_str = script_path.to_str().unwrap().to_string();
        (dir, path_str)
    }

    // MC-1: valid JSON, happy path end-to-end.
    #[tokio::test]
    async fn test_cli_happy_path() {
        let json = r#"{"latestAttempt":{"taskGroupExecutionAttempts":{"trainers":[{"taskExecutionAttempts":{"0":[{"hostname":"devgpu042"}]}}]}}}"#;
        let (_dir, script_path) = write_test_script(&format!("#!/bin/sh\necho '{}'\n", json));
        let url =
            super::try_resolve_mast_handle("mast_conda:///test-job", Some(1729), &script_path)
                .await
                .unwrap();
        assert!(url.starts_with("https://"), "url: {}", url);
        assert!(url.ends_with(":1729"), "url: {}", url);
    }

    // MC-1: malformed JSON produces a parse error.
    #[tokio::test]
    async fn test_cli_malformed_json() {
        let (_dir, script_path) = write_test_script("#!/bin/sh\necho 'not json'\n");
        let result =
            super::try_resolve_mast_handle("mast_conda:///test-job", Some(1729), &script_path)
                .await;
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("failed to parse"),
            "expected parse error, got: {}",
            err
        );
    }

    // MC-1: non-zero exit includes code + stderr.
    #[tokio::test]
    async fn test_cli_nonzero_exit() {
        let (_dir, script_path) =
            write_test_script("#!/bin/sh\necho >&2 'job not found'\nexit 42\n");
        let result =
            super::try_resolve_mast_handle("mast_conda:///test-job", Some(1729), &script_path)
                .await;
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("job not found"),
            "expected stderr in error, got: {}",
            err
        );
    }

    // MC-1: handle without mast_conda:/// prefix is
    // rejected with a clear error.
    #[tokio::test]
    async fn test_cli_missing_prefix() {
        let result = super::try_resolve_mast_handle("bad_handle", Some(1729), "mast").await;
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("expected mast_conda:/// prefix"),
            "expected prefix error, got: {}",
            err
        );
    }

    // MC-2: a task group with zero attempts is skipped.
    #[test]
    fn test_head_hostname_empty_attempts_vec() {
        let response = super::MastStatusResponse {
            latest_attempt: super::MastAttempt {
                task_groups: std::collections::HashMap::from([(
                    "trainers".to_string(),
                    vec![], // no attempts in this group
                )]),
            },
        };
        assert!(super::head_hostname(&response).is_err());
    }

    // MC-2: non-numeric task index keys sort last
    // (i64::MAX fallback).
    #[test]
    fn test_head_hostname_non_numeric_index() {
        let mut tasks = std::collections::HashMap::new();
        tasks.insert(
            "abc".to_string(),
            vec![super::MastTaskAttempt {
                hostname: Some("host_abc".to_string()),
            }],
        );
        tasks.insert(
            "0".to_string(),
            vec![super::MastTaskAttempt {
                hostname: Some("host_0".to_string()),
            }],
        );
        let response = super::MastStatusResponse {
            latest_attempt: super::MastAttempt {
                task_groups: std::collections::HashMap::from([(
                    "trainers".to_string(),
                    vec![super::MastTaskGroup { tasks }],
                )]),
            },
        };
        let head = super::head_hostname(&response).unwrap();
        assert_eq!(head, "host_0");
    }

    // Verifies that GET /v1/{proc_id} reflects actors spawned directly
    // on a proc — bypassing ProcAgent's gspawn message and therefore
    // never triggering publish_introspect_properties — so that the
    // resolved children list is always derived from live proc state.
    //
    // Regression guard for the bug introduced in 9a08d559: the switch
    // from a live handle_introspect to a cached publish model made
    // supervision-spawned actors (e.g. every sieve actor after
    // sieve[0]) invisible to the TUI.
    //
    // Exercises PA-1 (see module doc). See also
    // proc_agent::tests::test_query_child_proc_returns_live_children.
    #[tokio::test]
    async fn test_proc_children_reflect_directly_spawned_actors() {
        use hyperactor::Proc;
        use hyperactor::actor::ActorStatus;
        use hyperactor::channel::ChannelTransport;
        use hyperactor::host::Host;
        use hyperactor::host::LocalProcManager;
        use hyperactor::testing::proc_supervison::ProcSupervisionCoordinator;

        use crate::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
        use crate::host_mesh::host_agent::HostAgent;
        use crate::host_mesh::host_agent::HostAgentMode;
        use crate::host_mesh::host_agent::ProcManagerSpawnFn;
        use crate::proc_agent::PROC_AGENT_ACTOR_NAME;
        use crate::proc_agent::ProcAgent;

        // Stand up a HostMeshAgent. The user proc gets its own
        // ephemeral address; we register that address in
        // MeshAdminAgent so resolve_proc_node can look it up.
        // HostMeshAgent won't know the user proc (it wasn't spawned
        // through it), so QueryChild returns Error and resolve falls
        // back to querying proc_agent[0] via QueryChild(Proc) — the
        // path being tested.
        let spawn_fn: ProcManagerSpawnFn =
            Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot_v1(proc, None))));
        let manager: LocalProcManager<ProcManagerSpawnFn> = LocalProcManager::new(spawn_fn);
        let host: Host<LocalProcManager<ProcManagerSpawnFn>> =
            Host::new(manager, ChannelTransport::Unix.any())
                .await
                .unwrap();
        let system_proc = host.system_proc().clone();
        let host_agent_handle = system_proc
            .spawn(
                HOST_MESH_AGENT_ACTOR_NAME,
                HostAgent::new(HostAgentMode::Local(host)),
            )
            .unwrap();
        let host_agent_ref: hyperactor_reference::ActorRef<HostAgent> = host_agent_handle.bind();

        // User proc: own ephemeral Unix socket, own ProcAgent.
        let user_proc =
            Proc::direct(ChannelTransport::Unix.any(), "user_proc".to_string()).unwrap();
        let user_proc_addr = user_proc.proc_id().addr().to_string();
        let agent_handle = ProcAgent::boot_v1(user_proc.clone(), None).unwrap();
        agent_handle
            .status()
            .wait_for(|s| matches!(s, ActorStatus::Idle))
            .await
            .unwrap();

        // MeshAdminAgent: register the user proc's addr as a "host"
        // pointing to host_agent_ref. That agent doesn't know the
        // user proc, so QueryChild → Error → fallback to proc_agent.
        let admin_proc = Proc::direct(ChannelTransport::Unix.any(), "admin".to_string()).unwrap();
        let _supervision = ProcSupervisionCoordinator::set(&admin_proc).await.unwrap();
        let admin_handle = admin_proc
            .spawn(
                MESH_ADMIN_ACTOR_NAME,
                MeshAdminAgent::new(
                    vec![(user_proc_addr, host_agent_ref.clone())],
                    None,
                    Some("[::]:0".parse().unwrap()),
                ),
            )
            .unwrap();
        let admin_ref: hyperactor_reference::ActorRef<MeshAdminAgent> = admin_handle.bind();

        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client".to_string()).unwrap();
        let (client, _client_handle) = client_proc.instance("client").unwrap();

        // Resolve the user proc via MeshAdminAgent. HostMeshAgent
        // returns Error for QueryChild → fallback to proc_agent[0]
        // QueryChild(Reference::Proc) → live NodeProperties::Proc.
        let user_proc_ref = user_proc.proc_id().to_string();
        let resp = admin_ref
            .resolve(&client, user_proc_ref.clone())
            .await
            .unwrap();
        let node = resp.0.unwrap();
        assert!(
            matches!(node.properties, NodeProperties::Proc { .. }),
            "expected Proc, got {:?}",
            node.properties
        );
        let initial_count = node.children.len();
        assert!(
            node.children
                .iter()
                .any(|c| c.contains(PROC_AGENT_ACTOR_NAME)),
            "initial children {:?} should contain proc_agent",
            node.children
        );

        // Spawn an actor directly on the user proc, bypassing gspawn.
        // This simulates how sieve[0] spawns sieve[1], sieve[2], etc.
        user_proc
            .spawn("extra_actor", TestIntrospectableActor)
            .unwrap();

        // Resolve again — the new actor must appear immediately
        // without any republish, proving PA-1 is satisfied.
        let resp2 = admin_ref
            .resolve(&client, user_proc_ref.clone())
            .await
            .unwrap();
        let node2 = resp2.0.unwrap();
        assert!(
            matches!(node2.properties, NodeProperties::Proc { .. }),
            "expected Proc, got {:?}",
            node2.properties
        );
        assert!(
            node2.children.iter().any(|c| c.contains("extra_actor")),
            "after direct spawn, children {:?} should contain extra_actor",
            node2.children
        );
        assert!(
            node2.children.len() > initial_count,
            "expected at least {} children after direct spawn, got {:?}",
            initial_count + 1,
            node2.children
        );
    }

    // -- pyspy bridge input validation tests --
    //
    // Tests for the v1 proc-reference strictness contract (see
    // introspect module doc): the py-spy bridge accepts only
    // ProcId-form references and rejects other forms as bad_request.

    #[test]
    fn pyspy_parse_empty_reference() {
        // v1 contract: empty input → bad_request.
        let err = parse_pyspy_proc_reference("").unwrap_err();
        assert_eq!(err.code, "bad_request");
        assert!(err.message.contains("empty"));
    }

    #[test]
    fn pyspy_parse_slash_only() {
        // v1 contract: slash-only (axum wildcard artifact) → bad_request.
        let err = parse_pyspy_proc_reference("/").unwrap_err();
        assert_eq!(err.code, "bad_request");
        assert!(err.message.contains("empty"));
    }

    #[test]
    fn pyspy_parse_malformed_percent_encoding() {
        // v1 contract: malformed encoding → bad_request.
        // %FF%FE is not valid UTF-8.
        let err = parse_pyspy_proc_reference("%FF%FE").unwrap_err();
        assert_eq!(err.code, "bad_request");
        assert!(err.message.contains("percent-encoding"));
    }

    #[test]
    fn pyspy_parse_invalid_proc_id() {
        // v1 contract: non-ProcId reference → bad_request.
        let err = parse_pyspy_proc_reference("not-a-valid-proc-id").unwrap_err();
        assert_eq!(err.code, "bad_request");
        assert!(err.message.contains("invalid proc reference"));
    }

    #[test]
    fn pyspy_parse_valid_proc_reference() {
        // v1 contract: valid ProcId → accepted.
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let proc_id = test_proc_id_with_addr(ChannelAddr::Tcp(addr), "myproc");
        let proc_id_str = proc_id.to_string();

        let (decoded, parsed) = parse_pyspy_proc_reference(&proc_id_str).unwrap();
        assert_eq!(decoded, proc_id_str);
        assert_eq!(parsed, proc_id);
    }

    #[test]
    fn pyspy_parse_strips_leading_slash() {
        // v1 contract: leading slash from axum wildcard is stripped.
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let proc_id = test_proc_id_with_addr(ChannelAddr::Tcp(addr), "myproc");
        let with_slash = format!("/{}", proc_id);

        let (_, parsed) = parse_pyspy_proc_reference(&with_slash).unwrap();
        assert_eq!(parsed, proc_id);
    }
}
