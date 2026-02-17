/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Admin proxy that routes HTTP requests to child procs via actor messaging.
//!
//! In multi-process mode, each child proc runs in its own OS process with
//! its own `AdminState`. The host's admin server can only see procs
//! registered locally. This module provides proxy handlers that forward
//! admin queries to remote child procs via their `ProcMeshAgent` actor refs.
//!
//! # Usage
//!
//! The proxy state is managed as a process-global singleton (mirroring
//! `hyperactor::admin::AdminState`). Child proc agents are registered
//! automatically by `BootstrapProcManager` when procs become ready.
//! Applications should use [`serve`] instead of `hyperactor::admin::serve`
//! to get proxy-aware admin endpoints.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use hyperactor::ActorRef;
use hyperactor::admin;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use tokio::net::TcpListener;

use crate::global_root_client;
use crate::mesh_agent::AdminQueryMessageClient;
use crate::mesh_agent::ProcMeshAgent;

/// Shared state for the admin proxy, holding references to remote proc agents.
#[derive(Clone, Default)]
struct AdminProxyState {
    /// Map from proc name → agent ref for child procs in separate processes.
    remote_procs: Arc<RwLock<HashMap<String, ActorRef<ProcMeshAgent>>>>,
}

/// Returns the global admin proxy state singleton.
fn global_proxy() -> &'static AdminProxyState {
    static PROXY_STATE: OnceLock<AdminProxyState> = OnceLock::new();
    PROXY_STATE.get_or_init(AdminProxyState::default)
}

/// Register a remote proc's agent ref for proxied admin queries.
///
/// Called by `BootstrapProcManager` when a child proc becomes ready.
pub fn register_remote_proc(proc_name: String, agent: ActorRef<ProcMeshAgent>) {
    global_proxy()
        .remote_procs
        .write()
        .unwrap()
        .insert(proc_name, agent);
}

/// Deregister a remote proc.
///
/// Called when a child proc exits (detected by the exit monitor).
pub fn deregister_remote_proc(proc_name: &str) {
    global_proxy()
        .remote_procs
        .write()
        .unwrap()
        .remove(proc_name);
}

/// Create the mesh-aware admin router that proxies requests to child procs.
///
/// Uses the base admin handlers for local-only routes (`/`, `/tree`,
/// `/v1/hosts`, etc.) and proxy-aware handlers for proc/actor routes
/// that may need to reach child procs in separate OS processes.
pub fn create_router() -> Router {
    let state = Arc::new(global_proxy().clone());
    Router::new()
        .route("/", get(admin::handlers::list_procs))
        .route("/tree", get(admin::handlers::tree_dump))
        .route("/procs/{proc_name}", get(proxy_get_proc))
        .route("/procs/{proc_name}/{actor_name}", get(proxy_get_actor))
        .route("/v1/hosts", get(admin::handlers::list_hosts))
        .route("/v1/hosts/{host_addr}", get(admin::handlers::get_host))
        .route("/{*reference}", get(admin::handlers::resolve_reference))
        .with_state(state)
}

/// Start serving the mesh-aware admin HTTP server.
///
/// This is a drop-in replacement for `hyperactor::admin::serve` that adds
/// proxy support for querying child procs in separate OS processes.
pub async fn serve(listener: TcpListener) -> std::io::Result<()> {
    let app = create_router();
    axum::serve(listener, app).await
}

/// Proxy-aware GET /procs/{proc_name}.
///
/// Checks local admin state first; if not found, queries the child proc's
/// `ProcMeshAgent` via actor messaging.
async fn proxy_get_proc(
    State(state): State<Arc<AdminProxyState>>,
    Path(proc_name): Path<String>,
) -> Result<Json<admin::ProcDetails>, StatusCode> {
    // Try local admin state first (covers in-process procs).
    if let Some(details) = admin::local_proc_details(&proc_name) {
        return Ok(Json(details));
    }

    // Try remote via actor messaging.
    let agent_ref = { state.remote_procs.read().unwrap().get(&proc_name).cloned() };

    if let Some(agent_ref) = agent_ref {
        let cx = global_root_client();
        let agent = agent_ref.clone();
        let query_future = agent.get_proc_details(cx);
        let response = RealClock
            .timeout(Duration::from_secs(2), query_future)
            .await
            .map_err(|_| StatusCode::GATEWAY_TIMEOUT)?
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        match response.json {
            Some(json) => {
                let details: admin::ProcDetails =
                    serde_json::from_str(&json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                Ok(Json(details))
            }
            None => Err(StatusCode::NOT_FOUND),
        }
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// Proxy-aware GET /procs/{proc_name}/{actor_name}.
///
/// Checks local admin state first; if not found, queries the child proc's
/// `ProcMeshAgent` via actor messaging.
async fn proxy_get_actor(
    State(state): State<Arc<AdminProxyState>>,
    Path((proc_name, actor_name)): Path<(String, String)>,
) -> Result<Json<admin::ActorDetails>, StatusCode> {
    // Try local admin state first.
    if let Some(details) = admin::local_actor_details(&proc_name, &actor_name) {
        return Ok(Json(details));
    }

    // Try remote via actor messaging.
    let agent_ref = { state.remote_procs.read().unwrap().get(&proc_name).cloned() };

    if let Some(agent_ref) = agent_ref {
        let cx = global_root_client();
        let agent = agent_ref.clone();
        let query_future = agent.get_actor_details(cx, actor_name);
        let response = RealClock
            .timeout(Duration::from_secs(2), query_future)
            .await
            .map_err(|_| StatusCode::GATEWAY_TIMEOUT)?
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        match response.json {
            Some(json) => {
                let details: admin::ActorDetails =
                    serde_json::from_str(&json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                Ok(Json(details))
            }
            None => Err(StatusCode::NOT_FOUND),
        }
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
