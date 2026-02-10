/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Response types for the admin HTTP API.

use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

/// Summary of a registered proc.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcSummary {
    /// The proc's name/ID.
    pub name: String,
    /// Number of actors in the proc.
    pub num_actors: usize,
}

/// Details about a specific proc.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcDetails {
    /// The proc's name/ID.
    pub proc_name: String,
    /// Names of all actors in this proc, including dynamically spawned children.
    pub actors: Vec<String>,
}

/// A recorded event from the flight recorder.
#[derive(Debug, Serialize, Deserialize)]
pub struct RecordedEvent {
    /// ISO 8601 formatted timestamp.
    pub timestamp: String,
    /// Sequence number for ordering.
    pub seq: usize,
    /// Event level (INFO, DEBUG, etc.).
    pub level: String,
    /// Event target (module path).
    pub target: String,
    /// Event name.
    pub name: String,
    /// Event fields as JSON.
    pub fields: Value,
}

/// Details about a specific actor.
#[derive(Debug, Serialize, Deserialize)]
pub struct ActorDetails {
    /// Current status of the actor.
    pub actor_status: String,
    /// The actor's type name.
    pub actor_type: String,
    /// Names of child actors.
    pub children: Vec<String>,
    /// Recent events from the flight recorder.
    pub flight_recorder: Vec<RecordedEvent>,
    /// Parent actor ID, if any.
    pub parent: Option<String>,
    /// Number of messages processed.
    pub messages_processed: u64,
    /// ISO 8601 formatted creation timestamp.
    pub created_at: String,
    /// Last message handler invoked.
    pub last_message_handler: Option<String>,
    /// Total processing time in microseconds.
    pub total_processing_time_us: u64,
}

/// Information about a resolved reference.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceInfo {
    /// A proc reference with details.
    Proc(ProcDetails),
    /// An actor reference with details.
    Actor(ActorDetails),
}

/// Summary of a registered host.
#[derive(Debug, Serialize, Deserialize)]
pub struct HostSummary {
    /// The host's address (e.g. "tcp:127.0.0.1:8080").
    pub addr: String,
    /// Number of procs managed by this host.
    pub num_procs: usize,
}

/// Details about a specific host.
#[derive(Debug, Serialize, Deserialize)]
pub struct HostDetails {
    /// The host's address.
    pub addr: String,
    /// Procs managed by this host, with drill-down URLs.
    pub procs: Vec<HostProcEntry>,
    /// URL to the HostMeshAgent's actor details endpoint. The
    /// HostMeshAgent's flight recorder captures host-level operations
    /// (proc spawns, stops, shutdowns) via instrumented handlers.
    pub agent_url: Option<String>,
}

/// A proc entry within a host's details, with a drill-down URL.
#[derive(Debug, Serialize, Deserialize)]
pub struct HostProcEntry {
    /// The proc's name.
    pub name: String,
    /// Number of actors in this proc.
    pub num_actors: usize,
    /// URL path to this proc's detail endpoint (e.g.
    /// "/procs/tcp%3A...").
    pub url: String,
}

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
