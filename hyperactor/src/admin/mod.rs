/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP admin server for introspecting procs, actors, and hosts.
//!
//! The admin server provides a REST API for querying registered procs,
//! their actor hierarchies, and hosts that manage them.
//!
//! # Example
//!
//! ```ignore
//! use hyperactor::admin;
//! use tokio::net::TcpListener;
//!
//! // Register a proc
//! admin::register_proc(&proc);
//!
//! // Start the admin server
//! let listener = TcpListener::bind("127.0.0.1:8080").await?;
//! admin::serve(listener).await?;
//! ```

pub mod handlers;
mod responses;
mod tree;

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;

use axum::Router;
use axum::routing::get;
use dashmap::DashMap;
pub use responses::ActorDetails;
pub use responses::ApiError;
pub use responses::HostDetails;
pub use responses::HostProcEntry;
pub use responses::HostSummary;
pub use responses::ProcDetails;
pub use responses::ProcSummary;
pub use responses::RecordedEvent;
pub use responses::ReferenceInfo;
use tokio::net::TcpListener;
pub use tree::format_proc_tree;
pub use tree::format_proc_tree_with_urls;

use crate::ActorId;
use crate::Proc;
use crate::ProcId;
use crate::channel::ChannelAddr;
use crate::proc::WeakProc;

/// Trait for type-erased host introspection.
///
/// Implemented by `HostAdminHandle` to allow the admin server to query
/// host state without knowing the concrete `ProcManager` type.
pub(crate) trait HostIntrospect: Send + Sync {
    /// The host's frontend channel address.
    fn addr(&self) -> ChannelAddr;
    /// Names of procs managed by this host.
    fn proc_names(&self) -> Vec<String>;
}

/// Shared handle for host admin introspection.
///
/// Created when a `Host<M>` is constructed, updated when procs are spawned,
/// and deregistered when the host is dropped. This provides a type-erased
/// view of the host's state for the admin server.
#[derive(Clone)]
pub(crate) struct HostAdminHandle {
    addr: ChannelAddr,
    procs: Arc<RwLock<HashSet<String>>>,
}

impl HostAdminHandle {
    /// Create a new admin handle for a host at the given address.
    pub(crate) fn new(addr: ChannelAddr) -> Self {
        Self {
            addr,
            procs: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Record that a proc with the given name has been spawned on this host.
    pub(crate) fn add_proc(&self, name: &str) {
        self.procs.write().unwrap().insert(name.to_string());
    }
}

impl HostIntrospect for HostAdminHandle {
    fn addr(&self) -> ChannelAddr {
        self.addr.clone()
    }

    fn proc_names(&self) -> Vec<String> {
        self.procs.read().unwrap().iter().cloned().collect()
    }
}

/// Global admin state holding registered procs and hosts.
struct AdminState {
    procs: DashMap<ProcId, WeakProc>,
    hosts: DashMap<String, Arc<dyn HostIntrospect>>,
}

/// Returns the global admin state singleton.
fn global() -> &'static AdminState {
    static ADMIN_STATE: OnceLock<AdminState> = OnceLock::new();
    ADMIN_STATE.get_or_init(|| AdminState {
        procs: DashMap::new(),
        hosts: DashMap::new(),
    })
}

/// Register a proc with the admin server.
///
/// Stores a weak reference so the admin server doesn't prevent the proc from
/// being dropped.
pub(crate) fn register_proc(proc: &Proc) {
    global()
        .procs
        .insert(proc.proc_id().clone(), proc.downgrade());
}

/// Deregister a proc from the admin server.
///
/// After deregistration, the proc will no longer appear in API responses.
pub(crate) fn deregister_proc(proc: &Proc) {
    deregister_proc_by_id(proc.proc_id());
}

/// Deregister a proc by its ID.
///
/// Called from `ProcState::drop` to clean up when the proc is dropped.
pub(crate) fn deregister_proc_by_id(proc_id: &ProcId) {
    global().procs.remove(proc_id);
}

/// Register a host with the admin server.
///
/// The handle is stored as an `Arc<dyn HostIntrospect>` keyed by the
/// host's address string.
pub(crate) fn register_host(handle: Arc<dyn HostIntrospect>) {
    global().hosts.insert(handle.addr().to_string(), handle);
}

/// Deregister a host from the admin server.
///
/// Called from `Host::drop` to clean up when the host is dropped.
pub(crate) fn deregister_host(addr: &ChannelAddr) {
    global().hosts.remove(&addr.to_string());
}

/// Creates the axum router for the admin server.
pub fn create_router() -> Router {
    Router::new()
        .route("/", get(handlers::list_procs))
        .route("/tree", get(handlers::tree_dump))
        .route("/procs/{proc_name}", get(handlers::get_proc))
        .route("/procs/{proc_name}/{actor_name}", get(handlers::get_actor))
        .route("/v1/hosts", get(handlers::list_hosts))
        .route("/v1/hosts/{host_addr}", get(handlers::get_host))
        .route("/{*reference}", get(handlers::resolve_reference))
}

/// Query proc details from a `Proc` reference.
///
/// Returns the proc name and all actor IDs (including dynamically
/// spawned children). This is the same data served by `GET /procs/{name}`
/// but accessible programmatically for use in actor-messaging-based
/// admin proxies.
pub fn query_proc_details(proc: &Proc) -> ProcDetails {
    let actors = proc
        .all_actor_ids()
        .into_iter()
        .map(|id| id.to_string())
        .collect();
    ProcDetails {
        proc_name: proc.proc_id().to_string(),
        actors,
    }
}

/// Query actor details from a `Proc` reference by actor name.
///
/// Searches the proc's actors to find one matching by name and returns
/// its details including status, flight recorder, and children. Returns
/// `None` if no actor with the given name is found.
pub fn query_actor_details(proc: &Proc, actor_name: &str) -> Option<ActorDetails> {
    // Try parsing as a full ActorId first, then fall back to name match.
    let actor_id = if let Ok(id) = ActorId::from_str(actor_name) {
        id
    } else {
        proc.all_actor_ids()
            .into_iter()
            .find(|id| id.name() == actor_name)?
    };
    let cell = proc.get_instance(&actor_id)?;
    Some(handlers::build_actor_details(&cell))
}

/// Look up proc details from the local admin state by proc name string.
///
/// Returns `None` if the proc name is not a valid `ProcId` or if the
/// proc is not registered locally (e.g. it's in another OS process).
pub fn local_proc_details(proc_name: &str) -> Option<ProcDetails> {
    let proc_id = ProcId::from_str(proc_name).ok()?;
    let state = global();
    let weak = state.procs.get(&proc_id)?;
    let proc = weak.upgrade()?;
    Some(query_proc_details(&proc))
}

/// Look up actor details from the local admin state by proc name and actor name.
///
/// Returns `None` if the proc or actor is not found locally.
pub fn local_actor_details(proc_name: &str, actor_name: &str) -> Option<ActorDetails> {
    let proc_id = ProcId::from_str(proc_name).ok()?;
    let state = global();
    let weak = state.procs.get(&proc_id)?;
    let proc = weak.upgrade()?;
    query_actor_details(&proc, actor_name)
}

/// Start serving the admin HTTP server.
///
/// This function runs indefinitely until the server encounters an error.
pub async fn serve(listener: TcpListener) -> std::io::Result<()> {
    let app = create_router();
    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auto_register_and_deregister_proc() {
        let proc = Proc::local();
        let proc_id = proc.proc_id().clone();

        // Proc should be auto-registered on creation
        assert!(global().procs.contains_key(&proc_id));

        // Deregister
        deregister_proc(&proc);
        assert!(!global().procs.contains_key(&proc_id));
    }

    #[tokio::test]
    async fn test_auto_deregister_on_drop() {
        let proc_id = {
            let proc = Proc::local();
            let proc_id = proc.proc_id().clone();

            // Proc should be auto-registered
            assert!(global().procs.contains_key(&proc_id));

            proc_id
            // proc dropped here, should auto-deregister
        };

        // Entry should be removed automatically on drop
        assert!(!global().procs.contains_key(&proc_id));
    }

    #[tokio::test]
    async fn test_list_procs_endpoint() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        // Proc is auto-registered on creation
        let proc = Proc::local();
        let _proc_id = proc.proc_id().to_string();

        let app = create_router();
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);

        // No cleanup needed; weak ref will be cleaned up automatically
    }

    #[tokio::test]
    async fn test_tree_endpoint() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        // Proc is auto-registered on creation
        let _proc = Proc::local();

        let app = create_router();
        let response = app
            .oneshot(Request::builder().uri("/tree").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);

        // No cleanup needed; weak ref will be cleaned up automatically
    }

    #[tokio::test]
    async fn test_register_and_deregister_host() {
        use std::str::FromStr;

        use crate::channel::ChannelAddr;

        let addr = ChannelAddr::from_str("local:99901").unwrap();
        let handle = HostAdminHandle::new(addr.clone());
        handle.add_proc("proc1");
        handle.add_proc("proc2");

        register_host(Arc::new(handle.clone()));

        // Host should be registered
        assert!(global().hosts.contains_key(&addr.to_string()));

        // Verify proc names (drop the Ref before deregistering to avoid deadlock)
        {
            let entry = global().hosts.get(&addr.to_string()).unwrap();
            let names = entry.value().proc_names();
            assert_eq!(names.len(), 2);
            assert!(names.contains(&"proc1".to_string()));
            assert!(names.contains(&"proc2".to_string()));
        }

        // Deregister
        deregister_host(&addr);
        assert!(!global().hosts.contains_key(&addr.to_string()));
    }

    #[tokio::test]
    async fn test_list_hosts_endpoint() {
        use std::str::FromStr;

        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let addr = ChannelAddr::from_str("local:99902").unwrap();
        let handle = HostAdminHandle::new(addr.clone());
        register_host(Arc::new(handle));

        let app = create_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/hosts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);

        // Clean up
        deregister_host(&addr);
    }

    #[tokio::test]
    async fn test_get_host_not_found() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let app = create_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/hosts/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
    }
}
