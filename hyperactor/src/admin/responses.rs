/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Response types for the admin HTTP API.

use serde::Serialize;
use serde_json::Value;

/// Summary of a registered proc.
#[derive(Debug, Serialize)]
pub struct ProcSummary {
    /// The proc's name/ID.
    pub name: String,
    /// Number of actors in the proc.
    pub num_actors: usize,
}

/// Details about a specific proc.
#[derive(Debug, Serialize)]
pub struct ProcDetails {
    /// The proc's name/ID.
    pub proc_name: String,
    /// Names of root actors (pid=0).
    pub root_actors: Vec<String>,
}

/// A recorded event from the flight recorder.
#[derive(Debug, Serialize)]
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
#[derive(Debug, Serialize)]
pub struct ActorDetails {
    /// Current status of the actor.
    pub actor_status: String,
    /// Names of child actors.
    pub children: Vec<String>,
    /// Recent events from the flight recorder.
    pub flight_recorder: Vec<RecordedEvent>,
}

/// Information about a resolved reference.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceInfo {
    /// A proc reference with details.
    Proc(ProcDetails),
    /// An actor reference with details.
    Actor(ActorDetails),
}
