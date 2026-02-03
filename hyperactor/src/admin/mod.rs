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
pub use responses::ReferenceInfo;
use tokio::net::TcpListener;
pub use tree::format_proc_tree;
pub use tree::format_proc_tree_with_urls;

use crate::Proc;

/// Global admin state holding registered procs.
struct AdminState {
    procs: DashMap<String, Proc>,
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
/// Once registered, the proc's actors can be queried via the HTTP API.
pub fn register_proc(proc: &Proc) {
    global()
        .procs
        .insert(proc.proc_id().to_string(), proc.clone());
}

/// Deregister a proc from the admin server.
///
/// After deregistration, the proc will no longer appear in API responses.
pub fn deregister_proc(proc: &Proc) {
    global().procs.remove(&proc.proc_id().to_string());
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
    async fn test_register_deregister_proc() {
        let proc = Proc::local();
        let proc_id = proc.proc_id().to_string();

        // Register
        register_proc(&proc);
        assert!(global().procs.contains_key(&proc_id));

        // Deregister
        deregister_proc(&proc);
        assert!(!global().procs.contains_key(&proc_id));
    }

    #[tokio::test]
    async fn test_list_procs_endpoint() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let proc = Proc::local();
        let proc_id = proc.proc_id().to_string();
        register_proc(&proc);

        let app = create_router();
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);

        // Cleanup
        deregister_proc(&proc);
    }

    #[tokio::test]
    async fn test_tree_endpoint() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let proc = Proc::local();
        register_proc(&proc);

        let app = create_router();
        let response = app
            .oneshot(Request::builder().uri("/tree").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);

        // Cleanup
        deregister_proc(&proc);
    }
}
