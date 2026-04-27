/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Messages used in supervision of actor meshes.
//!
//! ## Mesh-name propagation
//!
//! When a `MeshFailure` is constructed for a supervision event
//! whose constructing site has the mesh name locally in scope, the
//! mesh name is carried on `MeshFailure.actor_mesh_name`. The
//! constructing site does not perform a lookup to obtain the mesh
//! name; if the mesh name is not locally available at the site,
//! `None` is correct. `MeshFailure::Display` surfaces the mesh
//! name as an `on mesh "{name}"` segment when `actor_mesh_name`
//! is populated; stable identifiers continue to appear in detail
//! segments where the renderer already includes them.
//! Python-binding-specific plumbing for this carrier — how a
//! Python-spawned actor ends up with a mesh base-name string to
//! supply — lives in `monarch_hyperactor/src/actor.rs`
//! (`PythonActorParams.mesh_base_name`).

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
    /// Mesh name carried by the `MeshFailure` construction site,
    /// when locally available. On the direct actor-handled path
    /// this is the observing PythonActor's mesh base name. On
    /// controller-owned paths this is the monitored mesh name
    /// supplied by the controller path.
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

#[cfg(test)]
mod tests {
    //! Tests that pin `MeshFailure::Display` rendering. The
    //! `proof_*` tests capture exact rendered strings for three
    //! supervision-path shapes, paired for each path as
    //! `MeshFailure { actor_mesh_name: None, ... }` vs.
    //! `MeshFailure { actor_mesh_name: Some(...), ... }`, so the
    //! "with mesh name" and "without mesh name" rendered output is
    //! locked down and any regression surfaces here.
    //!
    //! The `assert_eq!` literals capture the `ActorId::Display`
    //! output of the checkout the tests were generated against. If
    //! the identifier encoding changes (e.g. a reference-stack
    //! refactor lands in the same tree), the literals need to be
    //! regenerated on the new baseline — the mesh-name-rendering
    //! behavior this module tests is independent of the id format.

    use hyperactor::actor::ActorErrorKind;
    use hyperactor::actor::ActorStatus;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::reference;

    use super::*;

    fn test_event(name: &str, display_name: Option<String>) -> ActorSupervisionEvent {
        let proc_id = reference::ProcId::from_resource_name(ChannelAddr::Local(0), "test_proc");
        ActorSupervisionEvent::new(
            proc_id.actor_id(name),
            display_name,
            ActorStatus::Failed(ActorErrorKind::Generic("boom".to_string())),
            None,
        )
    }

    // `MeshFailure::Display` renders the mesh name in its prose
    // when `actor_mesh_name` is Some, producing the "on mesh \"{name}\""
    // segment alongside the stable id-bearing event.
    #[test]
    fn mesh_failure_display_renders_mesh_name_when_populated() {
        let failure = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            event: test_event("actor_a", None),
            crashed_ranks: vec![],
        };
        let rendered = format!("{}", failure);
        assert!(
            rendered.contains("on mesh \"training\""),
            "expected rendered output to contain `on mesh \"training\"`; got: {rendered}"
        );
    }

    // When `actor_mesh_name` is None, the formatter omits the mesh
    // segment entirely — the absence degrades gracefully without
    // changing surrounding prose.
    #[test]
    fn mesh_failure_display_omits_mesh_segment_when_none() {
        let failure = MeshFailure {
            actor_mesh_name: None,
            event: test_event("actor_a", None),
            crashed_ranks: vec![],
        };
        let rendered = format!("{}", failure);
        assert!(
            !rendered.contains("on mesh"),
            "expected no `on mesh` segment when actor_mesh_name is None; got: {rendered}"
        );
    }

    // When both mesh name and the event's Python-class display_name
    // are populated, the rendered prose includes both, producing a
    // user-readable description alongside the stable identifier
    // carried in the event.
    #[test]
    fn mesh_failure_display_renders_mesh_and_python_class() {
        let failure = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            event: test_event(
                "actor_a",
                Some("instance0.<my_module.Philosopher training>".to_string()),
            ),
            crashed_ranks: vec![],
        };
        let rendered = format!("{}", failure);
        assert!(
            rendered.contains("on mesh \"training\""),
            "expected mesh name segment; got: {rendered}"
        );
        assert!(
            rendered.contains("my_module.Philosopher"),
            "expected Python-class segment from display_name; got: {rendered}"
        );
    }

    // Shared fixture for the proofs: the exact synthesized event shape
    // that `GlobalClientActor::handle_undeliverable_message` produces
    // (`hyperactor_mesh/src/global_context.rs:278`): display_name =
    // None, actor_status = generic_failure("message not delivered: ...").
    fn undeliverable_synthesized_event() -> ActorSupervisionEvent {
        let proc_id = reference::ProcId::from_resource_name(ChannelAddr::Local(0), "worker_proc");
        ActorSupervisionEvent::new(
            proc_id.actor_id("dead_actor"),
            None, // synthesized site has no PythonActor context; display_name stays None
            ActorStatus::generic_failure(
                "message not delivered: undeliverable message error: ... \
                 error: broken link: message returned to global root client"
                    .to_string(),
            ),
            None,
        )
    }

    // Root-client undeliverable path.
    //
    // Transport bounces an undeliverable back to the root client;
    // `GlobalClientActor::handle_undeliverable_message` synthesizes
    // an `ActorSupervisionEvent` with `display_name = None` and
    // `"message not delivered: ..."` status. That event propagates
    // to a `PythonActor::handle_supervision_event`, which wraps it
    // in a `MeshFailure`. At the wrap site the observing
    // `PythonActor`'s `mesh_base_name` is the mesh name locally
    // available; this test pins what `MeshFailure::Display` renders
    // when `actor_mesh_name` is `None` vs. `Some("training")` for
    // that exact synthesized inner event shape.
    #[test]
    fn proof_motivating_incident_root_client_undeliverable() {
        let without_mesh_name = MeshFailure {
            actor_mesh_name: None,
            event: undeliverable_synthesized_event(),
            crashed_ranks: vec![],
        };
        let with_mesh_name = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            event: undeliverable_synthesized_event(),
            crashed_ranks: vec![],
        };
        let expected_without = "failure with event: Supervision event: \
                                actor local:0,_worker_proc,dead_actor failed:\n  \
                                message not delivered: undeliverable message error: \
                                ... error: broken link: message returned to global \
                                root client";
        let expected_with = "failure on mesh \"training\" with event: \
                             Supervision event: actor \
                             local:0,_worker_proc,dead_actor failed:\n  \
                             message not delivered: undeliverable message error: \
                             ... error: broken link: message returned to global \
                             root client";
        assert_eq!(format!("{}", without_mesh_name), expected_without);
        assert_eq!(format!("{}", with_mesh_name), expected_with);

        // Note: the inner event here has `display_name = None` (the
        // synthesis site at `global_context.rs` has no PythonActor
        // context to populate it), so the inner actor mention
        // renders via raw `ActorId` text. That is a separate concern
        // from mesh-name plumbing.
    }

    // Direct actor-handled panic path.
    //
    // A `PythonActor` panics in a handler. `Proc::stop_actor`
    // constructs the `ActorSupervisionEvent` using
    // `actor.display_name()`, which on a `PythonActor` is the
    // Python-class-bearing `str(PyInstance)`. The event reaches a
    // supervising `PythonActor` through the propagation chain,
    // which wraps it in a `MeshFailure` at
    // `monarch_hyperactor/src/actor.rs:1072`. At that wrap site the
    // observing `PythonActor`'s `mesh_base_name` is the mesh name
    // locally available; this test pins what
    // `MeshFailure::Display` renders when `actor_mesh_name` is
    // `None` vs. `Some("training")` for a panicked-event inner
    // shape that already carries a Python-class `display_name`.
    #[test]
    fn proof_direct_actor_handled_panic() {
        let panicked_event = {
            let proc_id =
                reference::ProcId::from_resource_name(ChannelAddr::Local(0), "worker_proc");
            ActorSupervisionEvent::new(
                proc_id.actor_id("philosopher_1"),
                // `Proc::stop_actor` populates this via
                // `actor.display_name()` on a PythonActor — which
                // returns the Python-class-bearing `str(PyInstance)`.
                Some("instance0.<monarch_examples.dining.Philosopher training>".to_string()),
                ActorStatus::Failed(ActorErrorKind::Generic(
                    "IndexError: list index out of range".to_string(),
                )),
                None,
            )
        };
        let without_mesh_name = MeshFailure {
            actor_mesh_name: None,
            event: panicked_event.clone(),
            crashed_ranks: vec![],
        };
        let with_mesh_name = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            event: panicked_event,
            crashed_ranks: vec![],
        };
        let expected_without = "failure with event: Supervision event: actor \
                                instance0.<monarch_examples.dining.Philosopher \
                                training> failed:\n  \
                                IndexError: list index out of range";
        let expected_with = "failure on mesh \"training\" with event: \
                             Supervision event: actor \
                             instance0.<monarch_examples.dining.Philosopher \
                             training> failed:\n  \
                             IndexError: list index out of range";
        assert_eq!(format!("{}", without_mesh_name), expected_without);
        assert_eq!(format!("{}", with_mesh_name), expected_with);
    }

    // Controller-unreachable path.
    //
    // When the controller for a mesh becomes unreachable, code in
    // `actor_mesh.rs` synthesizes a `MeshFailure` with
    // `actor_mesh_name: Some(self.id().to_string())` — the slot is
    // already populated on this path, and the inner event's
    // `display_name` is `None` because the construction site has
    // no `PythonActor` context. This test pins the rendered string
    // for that exact shape.
    #[test]
    fn proof_controller_unreachable() {
        let controller_timeout_event = {
            let proc_id =
                reference::ProcId::from_resource_name(ChannelAddr::Local(0), "controller_proc");
            ActorSupervisionEvent::new(
                proc_id.actor_id("training_controller"),
                None,
                ActorStatus::generic_failure(
                    "timed out reaching controller ... Assuming controller's proc is dead"
                        .to_string(),
                ),
                None,
            )
        };
        let failure = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            event: controller_timeout_event,
            crashed_ranks: vec![],
        };
        let expected = "failure on mesh \"training\" with event: \
                        Supervision event: actor \
                        local:0,_controller_proc,training_controller \
                        failed:\n  \
                        timed out reaching controller ... Assuming \
                        controller's proc is dead";
        assert_eq!(format!("{}", failure), expected);
    }
}
