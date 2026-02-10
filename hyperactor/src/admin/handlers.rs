/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP route handlers for the admin server.

use std::str::FromStr;
use std::time::SystemTime;

use axum::Json;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::http::header::HeaderMap;
use axum::response::IntoResponse;
use chrono::DateTime;
use chrono::Utc;

use super::global;
use super::responses::ActorDetails;
use super::responses::ApiError;
use super::responses::HostDetails;
use super::responses::HostProcEntry;
use super::responses::HostSummary;
use super::responses::ProcDetails;
use super::responses::ProcSummary;
use super::responses::RecordedEvent;
use super::responses::ReferenceInfo;
use super::tree::format_proc_tree_with_urls;
use super::tree::url_encode_path;
use crate::ActorId;
use crate::ProcId;
use crate::proc::InstanceCell;
use crate::reference::Reference;

/// GET / - List all registered procs.
pub async fn list_procs() -> Json<Vec<ProcSummary>> {
    let state = global();
    let procs: Vec<ProcSummary> = state
        .procs
        .iter()
        .filter_map(|entry| {
            let proc = entry.value().upgrade()?;
            let num_actors = proc.all_actor_ids().len();
            Some(ProcSummary {
                name: entry.key().to_string(),
                num_actors,
            })
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
        if let Some(proc) = entry.value().upgrade() {
            output.push_str(&format_proc_tree_with_urls(&proc, Some(&base_url)));
            output.push('\n');
        }
    }
    output
}

/// GET /procs/{proc_name} - Get details for a specific proc.
pub async fn get_proc(Path(proc_name): Path<String>) -> Result<Json<ProcDetails>, StatusCode> {
    let state = global();
    let proc_id = ProcId::from_str(&proc_name).map_err(|_| StatusCode::BAD_REQUEST)?;
    let weak = state.procs.get(&proc_id).ok_or(StatusCode::NOT_FOUND)?;
    let proc = weak.upgrade().ok_or(StatusCode::NOT_FOUND)?;

    let actors: Vec<String> = proc
        .all_actor_ids()
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    Ok(Json(ProcDetails { proc_name, actors }))
}

/// GET /procs/{proc_name}/{actor_name} - Get details for a specific actor.
pub async fn get_actor(
    Path((proc_name, actor_name)): Path<(String, String)>,
) -> Result<Json<ActorDetails>, StatusCode> {
    let state = global();
    let proc_id = ProcId::from_str(&proc_name).map_err(|_| StatusCode::BAD_REQUEST)?;
    let weak = state.procs.get(&proc_id).ok_or(StatusCode::NOT_FOUND)?;
    let proc = weak.upgrade().ok_or(StatusCode::NOT_FOUND)?;

    // Try parsing as a full ActorId first, then fall back to name match.
    let actor_id = if let Ok(id) = ActorId::from_str(&actor_name) {
        id
    } else {
        // Find actor by name in the flat instances map (no recursion).
        let found = proc
            .all_actor_ids()
            .into_iter()
            .find(|id| id.name() == actor_name);
        found.ok_or(StatusCode::NOT_FOUND)?
    };

    let cell = proc.get_instance(&actor_id).ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(build_actor_details(&cell)))
}

/// Build ActorDetails from an InstanceCell.
pub(super) fn build_actor_details(cell: &InstanceCell) -> ActorDetails {
    let status = cell.status().borrow().clone();
    let children: Vec<String> = cell
        .child_actor_ids()
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    let events = cell.recording().tail();
    let flight_recorder: Vec<RecordedEvent> = events
        .into_iter()
        .map(|event| RecordedEvent {
            timestamp: format_timestamp(event.time),
            seq: event.seq,
            level: event.metadata.level().to_string(),
            target: event.metadata.target().to_string(),
            name: event.metadata.name().to_string(),
            fields: event.json_value(),
        })
        .collect();

    let parent = cell.parent().map(|p| p.actor_id().to_string());
    let messages_processed = cell.num_processed_messages();
    let created_at = format_timestamp(cell.created_at());
    let last_message_handler = cell.last_message_handler().map(|info| info.to_string());
    let total_processing_time_us = cell.total_processing_time_us();
    let actor_type = cell.actor_type_name().to_string();

    ActorDetails {
        actor_status: status.to_string(),
        actor_type,
        children,
        flight_recorder,
        parent,
        messages_processed,
        created_at,
        last_message_handler,
        total_processing_time_us,
    }
}

/// Look up an actor by ActorId and return its details.
fn lookup_actor_details(actor_id: &ActorId) -> Option<ActorDetails> {
    let state = global();
    let proc_id = actor_id.proc_id();
    state
        .procs
        .get(proc_id)
        .and_then(|weak| weak.upgrade())
        .and_then(|proc| proc.get_instance(actor_id))
        .map(|cell| build_actor_details(&cell))
}

/// Format a SystemTime as an ISO 8601 timestamp.
fn format_timestamp(time: SystemTime) -> String {
    let datetime: DateTime<Utc> = time.into();
    datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// GET /{reference} - Resolve a reference and return its details.
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
        Reference::Proc(proc_id) => match state.procs.get(proc_id).and_then(|w| w.upgrade()) {
            Some(proc) => {
                let actors: Vec<String> = proc
                    .all_actor_ids()
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect();
                (
                    StatusCode::OK,
                    Json(serde_json::json!(ReferenceInfo::Proc(ProcDetails {
                        proc_name: proc_id.to_string(),
                        actors,
                    }))),
                )
            }
            None => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "proc not found"})),
            ),
        },
        Reference::Actor(actor_id) => match lookup_actor_details(actor_id) {
            Some(details) => (
                StatusCode::OK,
                Json(serde_json::json!(ReferenceInfo::Actor(details))),
            ),
            None => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "actor not found"})),
            ),
        },
        _ => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "unsupported reference type"})),
        ),
    }
}

/// GET /v1/hosts - List all registered hosts.
pub async fn list_hosts() -> Json<Vec<HostSummary>> {
    let state = global();
    let hosts: Vec<HostSummary> = state
        .hosts
        .iter()
        .map(|entry| {
            let handle = entry.value();
            HostSummary {
                addr: handle.addr().to_string(),
                num_procs: handle.proc_names().len(),
            }
        })
        .collect();
    Json(hosts)
}

/// GET /v1/hosts/{host_addr} - Get details for a specific host.
pub async fn get_host(
    headers: HeaderMap,
    Path(host_addr): Path<String>,
) -> Result<Json<HostDetails>, ApiError> {
    let state = global();
    let entry = state.hosts.get(&host_addr).ok_or_else(|| {
        ApiError::not_found(
            "Host not found",
            Some(serde_json::json!({ "addr": host_addr })),
        )
    })?;

    let handle = entry.value();

    // Extract base URL from headers for building drill-down links
    let host_header = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    let base_url = format!("{}://{}", scheme, host_header);

    let procs: Vec<HostProcEntry> = handle
        .proc_names()
        .into_iter()
        .map(|name| {
            let url = format!("{}/procs/{}", base_url, url_encode_path(&name));
            HostProcEntry {
                name,
                num_actors: 0,
                url,
            }
        })
        .collect();

    Ok(Json(HostDetails {
        addr: handle.addr().to_string(),
        procs,
        agent_url: None, // Set by hyperactor_mesh layer when HostMeshAgent is available
    }))
}
