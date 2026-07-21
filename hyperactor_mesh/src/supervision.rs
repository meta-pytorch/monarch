/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Messages used in supervision of actor meshes.
//!
//! ## Mesh identity propagation
//!
//! When a `MeshFailure` is constructed for a supervision event
//! whose constructing site has a user-facing display name locally
//! in scope, that display name is carried on
//! `MeshFailure.actor_mesh_name`. The constructing site does not
//! use an internal ID as a fallback display name; if the display
//! name is not locally available at the site, `None` is correct.
//! `MeshFailure.mesh_id` carries the stable internal mesh ID when
//! the constructing site has it, and callers should compare this
//! field against the mesh object's public `id`.
//! `MeshFailure::Display` surfaces the display name as an
//! `on mesh "{name}"` segment when `actor_mesh_name` is
//! populated; stable identifiers continue to appear in detail
//! segments where the renderer already includes them.
//! Python-binding-specific plumbing for this carrier — how a
//! Python-spawned actor ends up with a mesh base-name string to
//! supply — lives in `monarch_hyperactor/src/actor.rs`
//! (`PythonActorParams.mesh_base_name`).
//!
//! ## Mesh failure controller-attribution invariants (MFCA-*)
//!
//! `MeshFailure.reporting_controller` names the mesh controller that
//! observed and reported the failure, distinct from `event.actor_id`
//! (the actor the event concerns).
//!
//! - **MFCA-1 (complete controller reports):** every `MeshFailure` a
//!   controller constructs stamps `reporting_controller =
//!   Some(controller_id)`; `send_state_change` is the sole controller-to-owner
//!   failure post and stamps before posting.
//! - **MFCA-2 (same identity):** the stamped id is
//!   `cx.instance().self_addr().id()`, which equals the id a consumer reads
//!   from the mesh's controller ref (`ActorMesh::controller`, or the proc-mesh
//!   controller).
//! - **MFCA-3 (subject/source separation):** `event.actor_id` (the event
//!   subject/root cause) and `reporting_controller` (the reporter) are separate
//!   roles; their values may coincide on a controller-originated event, but
//!   neither is overloaded to mean the other.
//! - **MFCA-4 (non-controller absence):** `None` is reserved for construction
//!   paths with no reporting mesh controller; such a failure carries no
//!   controller attribution.

use hyperactor::ActorId;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::context;
use hyperactor::supervision::ActorSupervisionEvent;
use ndslice::Point;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Message about a supervision failure on a mesh of actors instead of a single
/// actor.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq)]
pub struct MeshFailure {
    /// Optional user-facing display name carried by the `MeshFailure`
    /// construction site when locally available. This field is for
    /// diagnostics and display, not identity; construction sites must not
    /// use the internal mesh ID as a fallback display name.
    pub actor_mesh_name: Option<String>,
    /// Stable internal mesh ID (for example, `workers_abc123`) when available.
    /// Python callers should compare this to the mesh object's public `id`.
    pub mesh_id: Option<String>,
    /// The supervision event on an actor located at mesh + rank.
    pub event: ActorSupervisionEvent,
    /// The set of crashed ranks in the mesh. Empty means the event
    /// applies to the whole mesh (e.g. mesh stop, controller timeout).
    pub crashed_ranks: Vec<usize>,
    /// Identity of the mesh controller that reported this failure, when
    /// one did (MFCA-3, MFCA-4). A separate role from `event.actor_id`
    /// (the event subject); the two may hold the same `ActorId` on a
    /// controller-originated event. `None` on construction paths with no
    /// reporting controller, which carry no controller attribution.
    pub reporting_controller: Option<ActorId>,
    /// The coordinate of the point in the mesh where the failure occurred.
    /// For single-point failures, this is the coordinate of the failed point.
    /// For whole-mesh failures, this is None.
    pub coordinate: Option<Point>,
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
        // ourselves with a chained error and bubble up to the next owner. This
        // converts the mesh failure into a supervision event, keeping only
        // `event`; the mesh-level fields (`actor_mesh_name`, `crashed_ranks`,
        // `reporting_controller`) are dropped and the next controller
        // re-attributes (MFCA-1). Only a direct clone/re-post of a `MeshFailure`
        // preserves `reporting_controller`; this conversion is not one.
        let err = ActorErrorKind::UnhandledSupervisionEvent(Box::new(ActorSupervisionEvent::new(
            cx.instance().self_addr().clone(),
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
        let coordinate = if let Some(coord) = &self.coordinate {
            format!(" at coordinate {}", coord)
        } else {
            String::new()
        };
        write!(
            f,
            "failure{}{}{} with event: {}",
            actor_mesh_name, ranks, coordinate, self.event
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
    //! The `assert_eq!` literals capture the `ActorAddr::Display`
    //! output of the checkout the tests were generated against. If
    //! the identifier encoding changes (e.g. a reference-stack
    //! refactor lands in the same tree), the literals need to be
    //! regenerated on the new baseline — the mesh-name-rendering
    //! behavior this module tests is independent of the id format.

    use hyperactor::actor::ActorErrorKind;
    use hyperactor::actor::ActorStatus;
    use hyperactor::channel::ChannelAddr;

    use super::*;
    use crate::mesh_id::ResourceId;

    fn test_event(name: &str, display_name: Option<String>) -> ActorSupervisionEvent {
        let proc_id = ResourceId::proc_addr_from_name(ChannelAddr::Local(0), "test_proc");
        ActorSupervisionEvent::new(
            proc_id.actor_addr(name),
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
            mesh_id: Some("training_12345".to_string()),
            event: test_event("actor_a", None),
            crashed_ranks: vec![],
            reporting_controller: None,
            coordinate: None,
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
            mesh_id: None,
            event: test_event("actor_a", None),
            crashed_ranks: vec![],
            reporting_controller: None,
            coordinate: None,
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
            mesh_id: Some("training_12345".to_string()),
            event: test_event(
                "actor_a",
                Some("instance0.<my_module.Philosopher training>".to_string()),
            ),
            crashed_ranks: vec![],
            reporting_controller: None,
            coordinate: None,
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
        let proc_id = ResourceId::proc_addr_from_name(ChannelAddr::Local(0), "worker_proc");
        ActorSupervisionEvent::new(
            proc_id.actor_addr("dead_actor"),
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
            mesh_id: None,
            event: undeliverable_synthesized_event(),
            crashed_ranks: vec![],
            reporting_controller: None,
            coordinate: None,
        };
        let with_mesh_name = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            mesh_id: Some("training_12345".to_string()),
            event: undeliverable_synthesized_event(),
            crashed_ranks: vec![],
            reporting_controller: None,
            coordinate: None,
        };
        let expected_without = "failure with event: Supervision event: \
                                actor worker_proc@inproc://0,dead_actor failed:\n  \
                                message not delivered: undeliverable message error: \
                                ... error: broken link: message returned to global \
                                root client";
        let expected_with = "failure on mesh \"training\" with event: \
                             Supervision event: actor \
                             worker_proc@inproc://0,dead_actor failed:\n  \
                             message not delivered: undeliverable message error: \
                             ... error: broken link: message returned to global \
                             root client";
        assert_eq!(format!("{}", without_mesh_name), expected_without);
        assert_eq!(format!("{}", with_mesh_name), expected_with);

        // Note: the inner event here has `display_name = None` (the
        // synthesis site at `global_context.rs` has no PythonActor
        // context to populate it), so the inner actor mention
        // renders via raw `ActorAddr` text. That is a separate concern
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
            let proc_id = ResourceId::proc_addr_from_name(ChannelAddr::Local(0), "worker_proc");
            ActorSupervisionEvent::new(
                proc_id.actor_addr("philosopher_1"),
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
            mesh_id: None,
            event: panicked_event.clone(),
            crashed_ranks: vec![],
            reporting_controller: None,
            coordinate: None,
        };
        let with_mesh_name = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            mesh_id: Some("training_12345".to_string()),
            event: panicked_event,
            crashed_ranks: vec![],
            reporting_controller: None,
            coordinate: None,
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
    // `actor_mesh.rs` synthesizes a `MeshFailure` with the stable
    // mesh ID populated but no display name. The inner event's
    // `display_name` is also `None` because the construction site
    // has no `PythonActor` context. This test pins the rendered
    // string for that exact shape.
    #[test]
    fn proof_controller_unreachable() {
        let controller_timeout_event = {
            let proc_id = ResourceId::proc_addr_from_name(ChannelAddr::Local(0), "controller_proc");
            ActorSupervisionEvent::new(
                proc_id.actor_addr("training_controller"),
                None,
                ActorStatus::generic_failure(
                    "timed out reaching controller ... Assuming controller's proc is dead"
                        .to_string(),
                ),
                None,
            )
        };
        let failure = MeshFailure {
            actor_mesh_name: None,
            mesh_id: Some("training_12345".to_string()),
            event: controller_timeout_event,
            crashed_ranks: vec![],
            reporting_controller: None,
            coordinate: None,
        };
        let expected = "failure with event: \
                        Supervision event: actor \
                        controller_proc@inproc://0,training_controller \
                        failed:\n  \
                        timed out reaching controller ... Assuming \
                        controller's proc is dead";
        assert_eq!(format!("{}", failure), expected);
    }

    // Distinct actor id for use as a controller identity or an event subject.
    fn mk_actor_id(proc: &str, name: &str) -> ActorId {
        ResourceId::proc_addr_from_name(ChannelAddr::Local(0), proc)
            .actor_addr(name)
            .id()
            .clone()
    }

    // MFCA-4 + wire format: `reporting_controller` round-trips through bincode
    // for both `Some` and `None`.
    #[test]
    fn reporting_controller_serde_round_trips() {
        for reporting_controller in [Some(mk_actor_id("ctrl_proc", "controller")), None] {
            let failure = MeshFailure {
                actor_mesh_name: Some("training".to_string()),
                event: test_event("actor_a", None),
                crashed_ranks: vec![0],
                reporting_controller: reporting_controller.clone(),
            };
            let bytes = bincode::serde::encode_to_vec(&failure, bincode::config::legacy()).unwrap();
            let (decoded, _): (MeshFailure, usize) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::legacy()).unwrap();
            assert_eq!(decoded, failure);
            assert_eq!(decoded.reporting_controller, reporting_controller);
        }
    }

    // MFCA-3: the reporter and the event subject are separate roles carried
    // independently. Here they hold different ids (a ProcAgent subject, a
    // distinct controller reporter); on a controller-originated event they may
    // instead hold the same id.
    #[test]
    fn reporting_controller_and_event_subject_are_independent() {
        let controller = mk_actor_id("ctrl_proc", "controller");
        let subject = ResourceId::proc_addr_from_name(ChannelAddr::Local(0), "agent_proc")
            .actor_addr("proc_agent");
        let failure = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            event: ActorSupervisionEvent::new(
                subject.clone(),
                None,
                ActorStatus::Failed(ActorErrorKind::Generic("boom".to_string())),
                None,
            ),
            crashed_ranks: Vec::new(),
            reporting_controller: Some(controller.clone()),
        };
        assert_eq!(failure.reporting_controller, Some(controller));
        assert_eq!(failure.event.actor_id, subject);
    }
}
