/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Messages used in supervision of actor meshes.

use hyperactor::Bind;
use hyperactor::Unbind;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::context;
use hyperactor::supervision::ActorSupervisionEvent;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Message about a supervision failure on a mesh of actors instead of a single
/// actor.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Bind, Unbind)]
pub struct MeshFailure {
    /// Name of the mesh which the event originated from.
    pub actor_mesh_name: Option<String>,
    /// The supervision event on an actor located at mesh + rank.
    pub event: ActorSupervisionEvent,
    /// The set of crashed ranks in the mesh. Empty means the event
    /// applies to the whole mesh (e.g. mesh stop, controller timeout).
    pub crashed_ranks: Vec<usize>,
}
wirevalue::register_type!(MeshFailure);

impl MeshFailure {
    /// Returns true if the given rank is part of this failure.
    /// A whole-mesh event (empty crashed_ranks) contains every rank.
    pub fn contains_rank(&self, rank: usize) -> bool {
        self.crashed_ranks.is_empty() || self.crashed_ranks.contains(&rank)
    }

    /// Helper function to handle a message to an actor that just wants to forward
    /// it to the next owner.
    pub fn default_handler(&self, cx: &impl context::Actor) -> Result<(), anyhow::Error> {
        // If an actor spawned by this one fails, we can't handle it. We fail
        // ourselves with a chained error and bubble up to the next owner.
        let err = ActorErrorKind::UnhandledSupervisionEvent(Box::new(ActorSupervisionEvent::new(
            cx.instance().self_id().clone(),
            None,
            ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(Box::new(
                self.event.clone(),
            ))),
            None,
        )));
        Err(anyhow::Error::new(err))
    }
}

impl std::fmt::Display for MeshFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let actor_mesh_name = self
            .actor_mesh_name
            .as_ref()
            .map(|m| format!(" on mesh \"{}\"", m))
            .unwrap_or("".to_string());
        let ranks = if self.crashed_ranks.is_empty() {
            String::new()
        } else {
            format!(" at ranks {:?}", self.crashed_ranks)
        };
        write!(
            f,
            "failure{}{} with event: {}",
            actor_mesh_name, ranks, self.event
        )
    }
}

// Shared between mesh types.
#[derive(Debug, Clone)]
pub(crate) enum Unhealthy {
    StreamClosed(MeshFailure), // Event stream closed
    Crashed(MeshFailure),      // Bad health event received
}
