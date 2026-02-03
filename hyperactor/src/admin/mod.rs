/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP admin server for introspecting procs and actors.
//!
//! The admin server provides a REST API for querying registered procs
//! and their actor hierarchies.
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

mod handlers;
mod responses;
mod tree;

use std::sync::OnceLock;

use axum::Router;
use axum::routing::get;
use dashmap::DashMap;
pub use responses::ActorDetails;
pub use responses::ProcDetails;
pub use responses::ProcSummary;
pub use responses::RecordedEvent;
pub use responses::ReferenceInfo;
use tokio::net::TcpListener;
pub use tree::format_proc_tree;
pub use tree::format_proc_tree_with_urls;

use crate::Proc;
use crate::ProcId;
use crate::proc::WeakProc;

/// Global admin state holding registered procs.
struct AdminState {
    procs: DashMap<ProcId, WeakProc>,
}

/// Returns the global admin state singleton.
fn global() -> &'static AdminState {
    static ADMIN_STATE: OnceLock<AdminState> = OnceLock::new();
    ADMIN_STATE.get_or_init(|| AdminState {
        procs: DashMap::new(),
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

/// Creates the axum router for the admin server.
fn create_router() -> Router {
    Router::new()
        .route("/", get(handlers::list_procs))
        .route("/tree", get(handlers::tree_dump))
        .route("/procs/{proc_name}", get(handlers::get_proc))
        .route("/procs/{proc_name}/{actor_name}", get(handlers::get_actor))
        .route("/{*reference}", get(handlers::resolve_reference))
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
}
