/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP route handlers for the admin server.

use std::str::FromStr;

use axum::Json;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::http::header::HeaderMap;
use axum::response::IntoResponse;

use super::global;
use super::responses::ActorDetails;
use super::responses::ProcDetails;
use super::responses::ProcSummary;
use super::responses::ReferenceInfo;
use super::tree::format_proc_tree_with_urls;
use crate::reference::Reference;

/// GET / - List all registered procs.
pub async fn list_procs() -> Json<Vec<ProcSummary>> {
    let state = global();
    let procs: Vec<ProcSummary> = state
        .procs
        .iter()
        .map(|entry| {
            let proc = entry.value();
            let mut num_actors = 0;
            proc.traverse(&mut |_, _| {
                num_actors += 1;
            });
            ProcSummary {
                name: entry.key().clone(),
                num_actors,
            }
        })
        .collect();
    Json(procs)
}

/// GET /tree - ASCII tree dump of all procs and actors with URLs.
pub async fn tree_dump(headers: HeaderMap) -> String {
    let state = global();

    // Extract host from headers
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");

    // Determine the scheme (default to http)
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");

    let base_url = format!("{}://{}", scheme, host);

    let mut output = String::new();
    for entry in state.procs.iter() {
        let proc = entry.value();
        output.push_str(&format_proc_tree_with_urls(proc, Some(&base_url)));
        output.push('\n');
    }
    output
}

/// GET /procs/{proc_name} - Get details for a specific proc.
pub async fn get_proc(Path(proc_name): Path<String>) -> Result<Json<ProcDetails>, StatusCode> {
    let state = global();
    let proc = state.procs.get(&proc_name).ok_or(StatusCode::NOT_FOUND)?;

    let root_actors: Vec<String> = proc
        .root_actor_ids()
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    Ok(Json(ProcDetails {
        proc_name,
        root_actors,
    }))
}

/// GET /procs/{proc_name}/{actor_name} - Get details for a specific actor.
pub async fn get_actor(
    Path((proc_name, actor_name)): Path<(String, String)>,
) -> Result<Json<ActorDetails>, StatusCode> {
    let state = global();
    let proc = state.procs.get(&proc_name).ok_or(StatusCode::NOT_FOUND)?;

    // Find actor by name by traversing all actors
    let mut found_actor_id = None;
    proc.traverse(&mut |cell, _| {
        if cell.actor_id().name() == actor_name && found_actor_id.is_none() {
            found_actor_id = Some(cell.actor_id().clone());
        }
    });

    let actor_id = found_actor_id.ok_or(StatusCode::NOT_FOUND)?;
    let cell = proc.get_instance(&actor_id).ok_or(StatusCode::NOT_FOUND)?;

    let status = cell.status().borrow().clone();
    let mut children = Vec::new();
    cell.traverse(&mut |child, depth| {
        if depth == 1 {
            children.push(child.actor_id().to_string());
        }
    });

    Ok(Json(ActorDetails {
        actor_status: status.to_string(),
        children,
    }))
}

/// GET /{reference} - Resolve a reference and show status.
pub async fn resolve_reference(Path(reference): Path<String>) -> impl IntoResponse {
    let parsed = match Reference::from_str(&reference) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid reference"})),
            );
        }
    };

    let state = global();

    match &parsed {
        Reference::Proc(proc_id) => {
            let proc_name = proc_id.to_string();
            match state.procs.get(&proc_name) {
                Some(proc) => {
                    let root_actors: Vec<String> = proc
                        .root_actor_ids()
                        .into_iter()
                        .map(|id| id.to_string())
                        .collect();
                    (
                        StatusCode::OK,
                        Json(serde_json::json!(ReferenceInfo::Proc(ProcDetails {
                            proc_name,
                            root_actors,
                        }))),
                    )
                }
                None => (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "proc not found"})),
                ),
            }
        }
        Reference::Actor(actor_id) => {
            let proc_id = actor_id.proc_id();
            let proc_name = proc_id.to_string();
            match state.procs.get(&proc_name) {
                Some(proc) => match proc.get_instance(actor_id) {
                    Some(cell) => {
                        let status = cell.status().borrow().clone();
                        let mut children = Vec::new();
                        cell.traverse(&mut |child, depth| {
                            if depth == 1 {
                                children.push(child.actor_id().to_string());
                            }
                        });
                        (
                            StatusCode::OK,
                            Json(serde_json::json!(ReferenceInfo::Actor(ActorDetails {
                                actor_status: status.to_string(),
                                children,
                            }))),
                        )
                    }
                    None => (
                        StatusCode::NOT_FOUND,
                        Json(serde_json::json!({"error": "actor not found"})),
                    ),
                },
                None => (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "proc not found"})),
                ),
            }
        }
        _ => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "unsupported reference type"})),
        ),
    }
}
