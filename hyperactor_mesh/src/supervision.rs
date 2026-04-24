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
//!
//! ## Attribution-transport invariants (AT-*)
//!
//! Refs carry structured attribution as `Attrs`; envelopes carry
//! the same declared keys as `Flattrs` headers; synthesis
//! boundaries may project transport values into the event-level
//! `Attribution` struct in `hyperactor/src/supervision.rs`. The
//! `Attribution` carrier is a neutral shape; the transport
//! vocabulary feeding it today is destination-side (see AT-3).
//!
//! - **AT-1 (declared-key-only transport).**
//!   Supervision-path attribution transport uses only declared
//!   keys that participate in the declaration-driven
//!   attribution-header vocabulary: a declared attr key joins the
//!   vocabulary by carrying the shared
//!   `hyperactor_config::attrs::ATTRIBUTION_HEADER` meta tag (see
//!   the `DEST_*` declarations below for the idiom). The same
//!   declared key addresses the same attribution dimension on both
//!   the ref-level `Attrs` and the envelope-level `Flattrs`:
//!   stamping and hoisting copy values by key and do not
//!   reinterpret, rename, or synthesize. No stringly-typed ad-hoc
//!   keys, no parsing of rendered output, no identifier-text
//!   recovery.
//!
//!   Enforcement is mechanical. One shared marker-driven stamping /
//!   copying mechanism in `hyperactor_config::attrs`
//!   (`stamp_marked_attrs_into_flattrs`, `copy_marked_flattrs`)
//!   filters on the marker at every use site; entries whose
//!   declared key lacks the marker are silently skipped. Cast,
//!   comm-forwarding, comm-hoist, and substrate direct-send paths
//!   all go through that one mechanism, either via the mesh helpers
//!   in this module (`stamp_attribution_into`,
//!   `stamp_attribution_from_headers`,
//!   `hoist_attribution_onto_envelope`) or via the substrate
//!   direct-send stamper
//!   (`hyperactor::reference::stamp_ref_attribution_onto`). None
//!   has a private key set. The substrate imports no `DEST_*` name;
//!   `hyperactor_mesh` declares them and joins the vocabulary by
//!   marking them.
//!
//! - **AT-3 (current transport vocabulary is destination-side).**
//!   The `DEST_*` keys describe the destination actor/ref the
//!   message is being sent to, not the sender.
//!
//! - **AT-4 (identity independence).**
//!   Ref attribution (`Attrs` on `ActorRef`/`PortRef`/
//!   `OncePortRef`/`ActorMeshRef`) does not participate in ref
//!   identity, equality, ordering, or hashing.
//!
//! - **AT-5 (no lookup during propagation or synthesis).**
//!   Cast, direct-send, comm-forwarding, root-client
//!   undeliverable, and controller-unreachable paths propagate or
//!   project only the structured keys already present on
//!   refs/headers. They do not perform a lookup to recover
//!   attribution. If a value is not locally present, `None` at the
//!   event layer is correct.

use hyperactor::Bind;
use hyperactor::Unbind;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::context;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Flattrs;
use hyperactor_config::attrs::ATTRIBUTION_HEADER;
use hyperactor_config::attrs::Attrs;
use hyperactor_config::attrs::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

// Attribution-transport declared keys (AT-1). Per the mantra,
// these keys address the same attribution dimensions on both the
// ref-level `Attrs` (in-memory) and the envelope-level `Flattrs`
// (on the wire). `DEST_*` describes the destination (AT-3).
//
// Each key joins the declaration-driven attribution-header
// vocabulary by carrying `@meta(ATTRIBUTION_HEADER = true)`. The
// shared marker-driven stamping/copying mechanism in
// `hyperactor_config::attrs` filters on that tag at every transport
// call site (substrate direct-send stamper, mesh-side stamp /
// forward / hoist wrappers below). No call site imports the
// specific key names to make this work. See AT-1.
declare_attrs! {
    /// Mesh name of the destination actor, if known at send time.
    @meta(ATTRIBUTION_HEADER = true)
    pub attr DEST_MESH_NAME: String;
    /// Qualified actor class token of the destination actor, if
    /// known at send time (e.g. Python class qualified name).
    @meta(ATTRIBUTION_HEADER = true)
    pub attr DEST_ACTOR_CLASS: String;
    /// Spawn-time rendered display string for the destination
    /// actor. Populated at spawn time from
    /// `supervision_display_name`; not recomputed at runtime from
    /// a live destination actor instance.
    @meta(ATTRIBUTION_HEADER = true)
    pub attr DEST_ACTOR_DISPLAY_NAME: String;
    /// Destination rank on paths where the sender held a
    /// per-rank ref (e.g. `ActorMeshRef::get(rank)`).
    @meta(ATTRIBUTION_HEADER = true)
    pub attr DEST_RANK: u64;
}

/// Copy attribution-header entries from an `Attrs` carrier (e.g.
/// a ref's in-memory `Attrs`) onto a `Flattrs` headers buffer.
///
/// Thin wrapper over the generic category-filter stamp in
/// `hyperactor_config::attrs::stamp_marked_attrs_into_flattrs`,
/// parameterized with the `ATTRIBUTION_HEADER` marker. The
/// vocabulary of attribution-header keys is defined declaratively
/// by `@meta(ATTRIBUTION_HEADER = true)` on each declared key
/// (see the `DEST_*` declarations above); this function follows
/// that vocabulary rather than hard-coding any particular key
/// set. AT-1.
pub fn stamp_attribution_into(headers: &mut Flattrs, attrs: &Attrs) {
    hyperactor_config::attrs::stamp_marked_attrs_into_flattrs(headers, attrs, ATTRIBUTION_HEADER);
}

/// Copy attribution-header entries from one `Flattrs` buffer to
/// another. Used by `CommActor::forward` to carry attribution from
/// the inbound envelope's flattrs onto the outbound envelope's
/// flattrs, and by Branch 1 of `CommActor::handle_undeliverable_message`
/// to hoist attribution from an inner `CastMessageEnvelope.headers`.
///
/// Thin wrapper over the generic category-filter
/// `hyperactor_config::attrs::copy_marked_flattrs` parameterized
/// with the `ATTRIBUTION_HEADER` marker. AT-1.
pub fn stamp_attribution_from_headers(src: &Flattrs, dst: &mut Flattrs) {
    hyperactor_config::attrs::copy_marked_flattrs(dst, src, ATTRIBUTION_HEADER);
}

/// Hoist attribution-header entries from a source `Flattrs` onto a
/// `MessageEnvelope`'s headers. Used by Branch 1 of
/// `CommActor::handle_undeliverable_message` to carry attribution
/// from the inner `CastMessageEnvelope.headers` onto the outer
/// envelope being returned to the origin.
///
/// Thin wrapper over the generic category-filter
/// `hyperactor_config::attrs::copy_marked_flattrs` parameterized
/// with the `ATTRIBUTION_HEADER` marker; the destination is the
/// envelope's mutable headers buffer. AT-1.
pub fn hoist_attribution_onto_envelope(
    envelope: &mut hyperactor::mailbox::MessageEnvelope,
    src: &Flattrs,
) {
    hyperactor_config::attrs::copy_marked_flattrs(envelope.headers_mut(), src, ATTRIBUTION_HEADER);
}

/// Read the four declared `DEST_*` keys from a `Flattrs` headers
/// buffer and construct the event-level `Attribution` struct.
///
/// Transport-to-event projection boundary per the mantra: transport
/// (attrs / flattrs) projects into the closed event-level
/// `Attribution` struct exactly here. Projection is a separate
/// concern from the shared marker-driven transport mechanism
/// above — it is specific to the `DEST_*` vocabulary and the
/// named-field shape of `Attribution`, and is not generic over
/// attribution-header markers.
///
/// Returns `None` if none of the keys were present — a completely
/// empty attribution would be observationally indistinguishable
/// from `None` at the event layer.
pub fn attribution_from_headers(headers: &Flattrs) -> Option<hyperactor::supervision::Attribution> {
    let mesh_name = headers.get(DEST_MESH_NAME);
    let actor_class = headers.get(DEST_ACTOR_CLASS);
    let actor_display_name = headers.get(DEST_ACTOR_DISPLAY_NAME);
    let rank = headers.get(DEST_RANK).map(|r| r as usize);
    if mesh_name.is_none()
        && actor_class.is_none()
        && actor_display_name.is_none()
        && rank.is_none()
    {
        return None;
    }
    Some(hyperactor::supervision::Attribution {
        mesh_name,
        actor_class,
        actor_display_name,
        rank,
    })
}

/// As `attribution_from_headers`, but reading from an in-memory
/// `Attrs` carrier (e.g., `ActorMeshRef::attribution`). Used by
/// the controller-unreachable synthesis site, which projects
/// directly from the mesh's `Attrs` rather than from a `Flattrs`
/// that was already stamped onto an envelope.
pub fn attribution_from_attrs(attrs: &Attrs) -> Option<hyperactor::supervision::Attribution> {
    let mesh_name = attrs.get(DEST_MESH_NAME).cloned();
    let actor_class = attrs.get(DEST_ACTOR_CLASS).cloned();
    let actor_display_name = attrs.get(DEST_ACTOR_DISPLAY_NAME).cloned();
    let rank = attrs.get(DEST_RANK).map(|r| *r as usize);
    if mesh_name.is_none()
        && actor_class.is_none()
        && actor_display_name.is_none()
        && rank.is_none()
    {
        return None;
    }
    Some(hyperactor::supervision::Attribution {
        mesh_name,
        actor_class,
        actor_display_name,
        rank,
    })
}

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
        let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "test_proc");
        ActorSupervisionEvent::new(
            proc_id.actor_id(name, 0),
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
    // that `GlobalClientActor::handle_undeliverable_message`
    // produces in `hyperactor_mesh/src/global_context.rs`:
    // display_name = None, actor_status =
    // generic_failure("message not delivered: ...").
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
                                actor local:0,worker_proc,dead_actor[0] failed:\n  \
                                message not delivered: undeliverable message error: \
                                ... error: broken link: message returned to global \
                                root client";
        let expected_with = "failure on mesh \"training\" with event: \
                             Supervision event: actor \
                             local:0,worker_proc,dead_actor[0] failed:\n  \
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
            let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "controller_proc");
            ActorSupervisionEvent::new(
                proc_id.actor_id("training_controller", 0),
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
                        local:0,controller_proc,training_controller[0] \
                        failed:\n  \
                        timed out reaching controller ... Assuming \
                        controller's proc is dead";
        assert_eq!(format!("{}", failure), expected);
    }
    // --- Helper-level tests for the transport/event boundary ---
    //
    // Pure-function coverage of the helpers this module exposes:
    //   - `stamp_attribution_into` (Attrs → Flattrs, 4 keys)
    //   - `stamp_attribution_from_headers` (Flattrs → Flattrs)
    //   - `attribution_from_headers` (Flattrs → `Attribution`)
    //   - `attribution_from_attrs` (Attrs → `Attribution`)
    //   - `hoist_attribution_onto_envelope` (Flattrs → MessageEnvelope)
    //
    // Pins key-aware copying (AT-1) and the synthesis-boundary
    // projection from the transport carriers into the event-level
    // `Attribution` struct.

    use hyperactor::mailbox::MessageEnvelope;
    use hyperactor::reference::PortId;

    // Neutral tokens for preservation / round-trip / projection
    // tests that don't assert rendered format.
    const MESH_NAME_TOKEN: &str = "MESH_NAME";
    const ACTOR_CLASS_TOKEN: &str = "ACTOR_CLASS";
    const ACTOR_DISPLAY_NAME_TOKEN: &str = "DISPLAY_NAME";
    const RANK_TOKEN: u64 = 7;

    fn populated_mesh_attrs() -> Attrs {
        let mut attrs = Attrs::new();
        attrs.set(DEST_MESH_NAME, MESH_NAME_TOKEN.to_string());
        attrs.set(DEST_ACTOR_CLASS, ACTOR_CLASS_TOKEN.to_string());
        attrs.set(
            DEST_ACTOR_DISPLAY_NAME,
            ACTOR_DISPLAY_NAME_TOKEN.to_string(),
        );
        attrs.set(DEST_RANK, RANK_TOKEN);
        attrs
    }

    // AT-1: `stamp_attribution_into` copies exactly the four
    // declared `DEST_*` keys by value from an `Attrs` carrier onto
    // a `Flattrs` headers buffer. No reinterpretation, no synthesis.
    #[test]
    fn stamp_attribution_into_copies_declared_keys_by_value() {
        let attrs = populated_mesh_attrs();
        let mut headers = Flattrs::new();
        stamp_attribution_into(&mut headers, &attrs);
        assert_eq!(
            headers.get(DEST_MESH_NAME),
            Some(MESH_NAME_TOKEN.to_string())
        );
        assert_eq!(
            headers.get(DEST_ACTOR_CLASS),
            Some(ACTOR_CLASS_TOKEN.to_string()),
        );
        assert_eq!(
            headers.get(DEST_ACTOR_DISPLAY_NAME),
            Some(ACTOR_DISPLAY_NAME_TOKEN.to_string()),
        );
        assert_eq!(headers.get(DEST_RANK), Some(RANK_TOKEN));
    }

    // If a key is missing from the source attrs the helper does not
    // emit that key on the destination. No defaults, no empty
    // strings. Callers must treat "absent" as `None` at the event
    // layer.
    #[test]
    fn stamp_attribution_into_skips_missing_keys() {
        let mut attrs = Attrs::new();
        attrs.set(DEST_MESH_NAME, MESH_NAME_TOKEN.to_string());
        let mut headers = Flattrs::new();
        stamp_attribution_into(&mut headers, &attrs);
        assert_eq!(
            headers.get(DEST_MESH_NAME),
            Some(MESH_NAME_TOKEN.to_string())
        );
        assert_eq!(headers.get::<String>(DEST_ACTOR_CLASS), None);
        assert_eq!(headers.get::<String>(DEST_ACTOR_DISPLAY_NAME), None);
        assert_eq!(headers.get::<u64>(DEST_RANK), None);
    }

    // AT-1: stamping onto a headers buffer that already carries
    // colliding values must replace them, not append. The helper
    // uses typed `Flattrs::set` which goes through
    // `set_serialized`'s find-and-replace logic — matching the
    // `get` (first-match) read semantics.
    #[test]
    fn stamp_attribution_into_overwrites_colliding_headers() {
        let mut headers = Flattrs::new();
        headers.set(DEST_MESH_NAME, "STALE".to_string());
        headers.set(DEST_RANK, 99u64);
        let attrs = populated_mesh_attrs();
        stamp_attribution_into(&mut headers, &attrs);
        assert_eq!(
            headers.get(DEST_MESH_NAME),
            Some(MESH_NAME_TOKEN.to_string())
        );
        assert_eq!(headers.get(DEST_RANK), Some(RANK_TOKEN));
    }

    // AT-1: forwarding-site helper copies the four declared keys
    // from one `Flattrs` to another.
    #[test]
    fn stamp_attribution_from_headers_copies_declared_keys() {
        let mut src = Flattrs::new();
        let attrs = populated_mesh_attrs();
        stamp_attribution_into(&mut src, &attrs);

        let mut dst = Flattrs::new();
        stamp_attribution_from_headers(&src, &mut dst);

        assert_eq!(dst.get(DEST_MESH_NAME), Some(MESH_NAME_TOKEN.to_string()));
        assert_eq!(
            dst.get(DEST_ACTOR_CLASS),
            Some(ACTOR_CLASS_TOKEN.to_string())
        );
        assert_eq!(
            dst.get(DEST_ACTOR_DISPLAY_NAME),
            Some(ACTOR_DISPLAY_NAME_TOKEN.to_string()),
        );
        assert_eq!(dst.get(DEST_RANK), Some(RANK_TOKEN));
    }

    // AT-1: forwarding replaces colliding values on dst (same
    // semantics as the direct-stamp path above).
    #[test]
    fn stamp_attribution_from_headers_overwrites_dst() {
        let mut src = Flattrs::new();
        src.set(DEST_MESH_NAME, "FRESH".to_string());
        let mut dst = Flattrs::new();
        dst.set(DEST_MESH_NAME, "STALE".to_string());
        stamp_attribution_from_headers(&src, &mut dst);
        assert_eq!(dst.get(DEST_MESH_NAME), Some("FRESH".to_string()));
    }

    // Synthesis-boundary projection: a `Flattrs` with all four
    // declared keys populated projects into a fully-populated
    // `Attribution`. This is the read side of AT-1 at the
    // root-client observation site.
    #[test]
    fn attribution_from_headers_all_keys_present() {
        let attrs = populated_mesh_attrs();
        let mut headers = Flattrs::new();
        stamp_attribution_into(&mut headers, &attrs);
        let attribution = attribution_from_headers(&headers).expect("all keys populated ⇒ Some");
        assert_eq!(attribution.mesh_name.as_deref(), Some(MESH_NAME_TOKEN));
        assert_eq!(attribution.actor_class.as_deref(), Some(ACTOR_CLASS_TOKEN));
        assert_eq!(
            attribution.actor_display_name.as_deref(),
            Some(ACTOR_DISPLAY_NAME_TOKEN),
        );
        assert_eq!(attribution.rank, Some(RANK_TOKEN as usize));
    }

    // A Flattrs with none of the four declared keys projects to
    // `None`, which the event layer treats as "no attribution"
    // (indistinguishable from a fully-empty `Attribution`).
    #[test]
    fn attribution_from_headers_none_when_empty() {
        let headers = Flattrs::new();
        assert!(attribution_from_headers(&headers).is_none());
    }

    // Partial population: any one of the four keys present ⇒
    // `Some(Attribution { ... })` with the absent fields `None`.
    // Each dimension is optional independently.
    #[test]
    fn attribution_from_headers_partial_keys() {
        let mut headers = Flattrs::new();
        headers.set(DEST_MESH_NAME, MESH_NAME_TOKEN.to_string());
        let attribution = attribution_from_headers(&headers).expect("at least one key ⇒ Some");
        assert_eq!(attribution.mesh_name.as_deref(), Some(MESH_NAME_TOKEN));
        assert!(attribution.actor_class.is_none());
        assert!(attribution.actor_display_name.is_none());
        assert!(attribution.rank.is_none());
    }

    // AT-1 consistency: the two synthesis-boundary projections
    // (`attribution_from_attrs` for an in-memory `Attrs` carrier,
    // `attribution_from_headers` for a wire-shape `Flattrs`
    // carrier) must produce equivalent `Attribution` shapes for
    // the same key values, so a cast path and a
    // controller-unreachable path build the same event-level
    // shape.
    #[test]
    fn attribution_from_attrs_matches_headers_projection() {
        let attrs = populated_mesh_attrs();
        let mut headers = Flattrs::new();
        stamp_attribution_into(&mut headers, &attrs);

        let from_headers = attribution_from_headers(&headers).expect("some");
        let from_attrs = attribution_from_attrs(&attrs).expect("some");
        assert_eq!(from_attrs.mesh_name, from_headers.mesh_name);
        assert_eq!(from_attrs.actor_class, from_headers.actor_class);
        assert_eq!(
            from_attrs.actor_display_name,
            from_headers.actor_display_name
        );
        assert_eq!(from_attrs.rank, from_headers.rank);
    }

    #[test]
    fn attribution_from_attrs_none_when_empty() {
        let attrs = Attrs::new();
        assert!(attribution_from_attrs(&attrs).is_none());
    }

    // AT-1: `hoist_attribution_onto_envelope` copies the four
    // declared keys from a source `Flattrs` onto a
    // `MessageEnvelope`'s headers via the typed `set_header` path
    // (which goes through `set_serialized` and therefore replaces
    // colliding entries). This is the write path the Branch 1
    // (forwarding-failure) undeliverable uses to carry attribution
    // from the inner cast envelope onto the outer envelope
    // returning to origin.
    #[test]
    fn hoist_attribution_onto_envelope_copies_declared_keys() {
        // Build a MessageEnvelope with an initial empty headers
        // and hoist attribution from a separate src Flattrs.
        let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "p");
        let sender = proc_id.actor_id("sender", 0);
        let dest = PortId::new(proc_id.actor_id("dest", 0), 0);
        let mut envelope = MessageEnvelope::new(
            sender,
            dest,
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );

        let mut src = Flattrs::new();
        let attrs = populated_mesh_attrs();
        stamp_attribution_into(&mut src, &attrs);

        hoist_attribution_onto_envelope(&mut envelope, &src);

        let h = envelope.headers();
        assert_eq!(h.get(DEST_MESH_NAME), Some(MESH_NAME_TOKEN.to_string()));
        assert_eq!(h.get(DEST_ACTOR_CLASS), Some(ACTOR_CLASS_TOKEN.to_string()));
        assert_eq!(
            h.get(DEST_ACTOR_DISPLAY_NAME),
            Some(ACTOR_DISPLAY_NAME_TOKEN.to_string()),
        );
        assert_eq!(h.get(DEST_RANK), Some(RANK_TOKEN));
    }

    // Branch 1 and Branch 2 of the comm undeliverable handler must
    // deliver the same populated `DEST_*` key set to the root-client
    // observation site, even though they reach the site via
    // different mechanics — Branch 1 via an explicit hoist from the
    // inner `CastMessageEnvelope.headers`, Branch 2 via envelope
    // passthrough after `deliver_to_dest`. This test pins the
    // property at the helper layer (the shared helpers both branches
    // use must, given the same inputs, produce the same Attribution
    // projection). Full end-to-end coverage across real branches
    // lives in the proof-matrix rows.
    #[test]
    fn branch1_and_branch2_helpers_agree_on_keys() {
        // Branch 1: inner cast envelope headers → hoist onto outer envelope.
        let mut inner_headers = Flattrs::new();
        stamp_attribution_into(&mut inner_headers, &populated_mesh_attrs());

        let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "p");
        let sender = proc_id.actor_id("sender", 0);
        let dest = PortId::new(proc_id.actor_id("dest", 0), 0);
        let mut branch1 = MessageEnvelope::new(
            sender.clone(),
            dest.clone(),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        hoist_attribution_onto_envelope(&mut branch1, &inner_headers);

        // Branch 2: envelope headers already carry the attribution
        // (envelope passthrough after `deliver_to_dest`).
        let mut branch2_headers = Flattrs::new();
        stamp_attribution_into(&mut branch2_headers, &populated_mesh_attrs());
        let branch2 = MessageEnvelope::new(
            sender,
            dest,
            wirevalue::Any::serialize(&()).unwrap(),
            branch2_headers,
        );

        // Both branches produce identical Attribution projections.
        let a1 = attribution_from_headers(branch1.headers()).expect("some");
        let a2 = attribution_from_headers(branch2.headers()).expect("some");
        assert_eq!(a1.mesh_name, a2.mesh_name);
        assert_eq!(a1.actor_class, a2.actor_class);
        assert_eq!(a1.actor_display_name, a2.actor_display_name);
        assert_eq!(a1.rank, a2.rank);
    }
}
