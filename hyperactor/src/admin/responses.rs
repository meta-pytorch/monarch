/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Response types for the admin HTTP API.

use serde::Serialize;

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

/// Details about a specific actor.
#[derive(Debug, Serialize)]
pub struct ActorDetails {
    /// Current status of the actor.
    pub actor_status: String,
    /// Names of child actors.
    pub children: Vec<String>,
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
