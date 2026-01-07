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
pub struct SupervisionFailureMessage {
    /// Name of the mesh which the event originated from.
    pub actor_mesh_name: Option<String>,
    /// Rank of the mesh from which the event originated.
    /// TODO: Point instead?
    pub rank: Option<usize>,
    /// The supervision event on an actor located at mesh + rank.
    pub event: ActorSupervisionEvent,
}
wirevalue::register_type!(SupervisionFailureMessage);

impl SupervisionFailureMessage {
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

impl std::fmt::Display for SupervisionFailureMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Supervision failure on mesh {:?} at rank {:?} with event: {}",
            self.actor_mesh_name, self.rank, self.event
        )
    }
}
