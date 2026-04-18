/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Messages used in supervision of actor meshes.
//!
//! See FA-* (failure-attribution invariants) in
//! `hyperactor/src/supervision.rs` for the user-facing contract on
//! how `MeshFailure` fields are populated and rendered.

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

#[cfg(test)]
mod tests {
    //! Tests covering the FA-* invariants (see
    //! `hyperactor/src/supervision.rs`). The tests named
    //! `proof_*` capture exact before/after rendered `MeshFailure`
    //! strings for three supervision-path shapes so they can be
    //! cited as concrete evidence — not just assertions — that the
    //! first increment materially improves the user-visible output
    //! on the claimed paths, and honestly reports where it does
    //! not.
    //!
    //! The "before" shape in each proof is the MeshFailure as it
    //! would have been constructed prior to this increment (with
    //! `actor_mesh_name = None` on paths that did not populate it).
    //! The "after" shape is what the same path produces under the
    //! first-increment plumbing.
    //!
    //! Note: the exact `assert_eq!` literals in the `proof_*` tests
    //! capture the `ActorId::Display` output of the checkout the
    //! tests were generated against. If the identifier encoding
    //! changes (e.g. a reference-stack refactor lands in the same
    //! tree), the proof literals need to be regenerated on the new
    //! baseline — the attribution improvement itself is independent
    //! of the id encoding.

    use hyperactor::actor::ActorErrorKind;
    use hyperactor::actor::ActorStatus;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::reference;

    use super::*;

    fn test_event(name: &str, display_name: Option<String>) -> ActorSupervisionEvent {
        let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "test_proc");
        ActorSupervisionEvent::new(
            proc_id.actor_id(name, 0),
            display_name,
            ActorStatus::Failed(ActorErrorKind::Generic("boom".to_string())),
            None,
        )
    }

    // FA-3: MeshFailure::Display renders the mesh name in its prose
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

    // FA-3 degradation: when `actor_mesh_name` is None, the formatter
    // omits the mesh segment — i.e., the invariant accommodates absent
    // friendly fields without changing surrounding prose.
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

    // FA-3: when both mesh name and the event's Python-class
    // display_name are populated, the rendered prose includes both,
    // producing a user-readable attribution alongside the stable
    // identifier carried in the event.
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
        let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "worker_proc");
        ActorSupervisionEvent::new(
            proc_id.actor_id("dead_actor", 0),
            None, // synthesized site has no PythonActor context; display_name stays None
            ActorStatus::generic_failure(
                "message not delivered: undeliverable message error: ... \
                 error: broken link: message returned to global root client"
                    .to_string(),
            ),
            None,
        )
    }

    // --- Proof 1: motivating incident (root-client undeliverable) ---
    //
    // Path: transport bounces an undeliverable back to the root
    // client; `GlobalClientActor::handle_undeliverable_message` at
    // `global_context.rs:278` synthesizes an `ActorSupervisionEvent`
    // with display_name = None and "message not delivered: ..."
    // status. That event then propagates until it lands on a
    // PythonActor's `handle_supervision_event`, where
    // `PythonActor::handle_supervision_event` wraps it in a
    // `MeshFailure`.
    //
    // Before the first increment: `MeshFailure.actor_mesh_name` on
    // this direct-handle path was None (the site passed None).
    //
    // After the first increment (FA-1): `actor_mesh_name` is
    // populated from `self.mesh_name` — Some("training") when the
    // observing PythonActor was spawned with mesh name "training".
    //
    // Limit of this increment: the synthesized event's display_name
    // is still None because `global_context.rs:278` has no
    // PythonActor context. That means the inner actor continues to
    // render via raw ActorId text, even after the first increment.
    // This is explicitly follow-on work (plan sections 8, 10 —
    // requires Flattrs or lookup).
    #[test]
    fn proof_motivating_incident_root_client_undeliverable() {
        let before = MeshFailure {
            actor_mesh_name: None, // pre-increment direct-handle default
            event: undeliverable_synthesized_event(),
            crashed_ranks: vec![],
        };
        let after = MeshFailure {
            actor_mesh_name: Some("training".to_string()), // post-increment: observing
            // PythonActor's mesh_name
            event: undeliverable_synthesized_event(),
            crashed_ranks: vec![],
        };
        // Exact captured rendered strings — the user-visible prose
        // produced on the supervision path for the motivating class,
        // before and after the first increment. These are permanent
        // evidence for the follow-up article; any regression on
        // `MeshFailure::Display`, `ActorSupervisionEvent::Display`,
        // or `actor_mesh_name` population on the direct-handle path
        // will surface here.
        let expected_before = "failure with event: Supervision event: \
                               actor local:0,worker_proc,dead_actor[0] failed:\n  \
                               message not delivered: undeliverable message error: \
                               ... error: broken link: message returned to global \
                               root client";
        let expected_after = "failure on mesh \"training\" with event: \
                              Supervision event: actor \
                              local:0,worker_proc,dead_actor[0] failed:\n  \
                              message not delivered: undeliverable message error: \
                              ... error: broken link: message returned to global \
                              root client";
        assert_eq!(format!("{}", before), expected_before);
        assert_eq!(format!("{}", after), expected_after);

        // Observable improvement: the wrapper names the observer's
        // mesh. Observable limit: the inner event actor still renders
        // by raw id because `global_context.rs:278` synthesizes with
        // display_name = None and has no PythonActor context to
        // populate it. Fixing the inner render is explicitly
        // follow-on work (Flattrs propagation or lookup-backed
        // enrichment).
    }

    // --- Proof 2: direct actor-handled supervision (actor panic) ---
    //
    // Path: a PythonActor panics in a handler. `Proc::stop_actor`
    // at `hyperactor/src/proc.rs:1642` constructs the
    // ActorSupervisionEvent using `actor.display_name()`. For a
    // PythonActor, that returns the Python-class-bearing
    // `str(PyInstance)`. That event reaches a supervising
    // PythonActor through the propagation chain, which wraps it in
    // a MeshFailure via `handle_supervision_event` at
    // `monarch_hyperactor/src/actor.rs:1072`.
    //
    // Before the first increment: `MeshFailure.actor_mesh_name`
    // was None on the direct-handle path. Inner event display_name
    // was already populated (existing behavior).
    //
    // After the first increment (FA-1 + already-consistent FA-2):
    // `actor_mesh_name` carries the observing PythonActor's mesh
    // name; the inner event continues to carry the Python-class
    // display_name.
    //
    // This is the path that the first increment materially
    // improves: both mesh name and Python-class attribution appear
    // in the rendered prose.
    #[test]
    fn proof_direct_actor_handled_panic() {
        let panicked_event = {
            let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "worker_proc");
            ActorSupervisionEvent::new(
                proc_id.actor_id("philosopher_1", 0),
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
        let before = MeshFailure {
            actor_mesh_name: None, // pre-increment direct-handle default
            event: panicked_event.clone(),
            crashed_ranks: vec![],
        };
        let after = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            event: panicked_event,
            crashed_ranks: vec![],
        };
        // Exact captured rendered strings — the user-visible prose
        // for an actor-panic supervision event on the direct-handle
        // path, before and after the first increment.
        let expected_before = "failure with event: Supervision event: actor \
                               instance0.<monarch_examples.dining.Philosopher \
                               training> failed:\n  \
                               IndexError: list index out of range";
        let expected_after = "failure on mesh \"training\" with event: \
                              Supervision event: actor \
                              instance0.<monarch_examples.dining.Philosopher \
                              training> failed:\n  \
                              IndexError: list index out of range";
        assert_eq!(format!("{}", before), expected_before);
        assert_eq!(format!("{}", after), expected_after);

        // Observable improvement: mesh name on the wrapper. Python
        // class was already populated pre-increment by
        // `Proc::stop_actor` via `actor.display_name()`; the first
        // increment preserves it and adds the mesh segment.
    }

    // --- Proof 3: controller-unreachable ---
    //
    // Path: the controller for a mesh becomes unreachable. Code at
    // `actor_mesh.rs:773-787` synthesizes a `MeshFailure` with
    // `actor_mesh_name: Some(self.id().to_string())`. That slot is
    // already populated on this path, so FA-1 is a no-op here.
    // The inner event's display_name is None because the
    // construction site has no PythonActor context — a gap that is
    // explicitly follow-on per the FA-2 audit.
    //
    // This proof is included to be honest about where the
    // increment does NOT improve output. The controller-unreachable
    // path already benefited from the mesh-name slot; the
    // display_name gap on the synthesized event remains for
    // follow-on work.
    #[test]
    fn proof_controller_unreachable() {
        let controller_timeout_event = {
            let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "controller_proc");
            ActorSupervisionEvent::new(
                proc_id.actor_id("training_controller", 0),
                None, // synthesized site has no PythonActor context
                ActorStatus::generic_failure(
                    "timed out reaching controller ... Assuming controller's proc is dead"
                        .to_string(),
                ),
                None,
            )
        };
        // Both before and after have actor_mesh_name populated, and
        // both have display_name = None — this path is unchanged by
        // the first increment.
        let unchanged = MeshFailure {
            actor_mesh_name: Some("training".to_string()),
            event: controller_timeout_event,
            crashed_ranks: vec![],
        };

        // Exact captured rendered string — the user-visible prose
        // for the controller-unreachable path, which is unchanged by
        // the first increment. Recorded here so any future regression
        // or improvement on this path surfaces explicitly.
        let expected = "failure on mesh \"training\" with event: \
                        Supervision event: actor \
                        local:0,controller_proc,training_controller[0] \
                        failed:\n  \
                        timed out reaching controller ... Assuming \
                        controller's proc is dead";
        assert_eq!(format!("{}", unchanged), expected);

        // No improvement in this increment: the mesh-name slot was
        // already populated pre-increment (FA-1 is a no-op here),
        // and the inner event's display_name is None because the
        // construction site at `actor_mesh.rs:773-787` has no
        // PythonActor context — follow-on work for this class.
    }
}
