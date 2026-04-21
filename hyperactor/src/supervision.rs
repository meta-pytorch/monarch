/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Messages used in supervision.
//!
//! ## Supervision invariants (SV-*)
//!
//! - **SV-1 (root-cause attribution):** For an
//!   `UnhandledSupervisionEvent` chain, `actually_failing_actor()`
//!   returns the event that should be treated as the root cause
//!   for structured failure attribution. In particular, if a
//!   failed parent wraps a stopped child event, the stopped child
//!   remains the root cause.
//!
//! ## Failure-attribution invariants (FA-*)
//!
//! These invariants govern the supervision-path rendering contract at
//! the hyperactor substrate level. They describe how
//! `ActorSupervisionEvent` and its renderer must behave, and what
//! higher-level crates (for example, crates that add friendly
//! attribution fields such as a mesh name or a language-binding class
//! name) must do when they plug data into this substrate. Hyperactor
//! itself does not define mesh-level or binding-level concepts; those
//! interpretations live alongside the types that introduce them (for
//! mesh-specific interpretation, see
//! `hyperactor_mesh/src/supervision.rs`; for Python-binding
//! interpretation, see the spawn path in
//! `monarch_hyperactor/src/actor.rs`).
//!
//! - **FA-1 (no lookup at construction).** When a higher-level crate
//!   attaches friendly-attribution data to an `ActorSupervisionEvent`
//!   — or to a wrapper that carries one — the value must come from
//!   structured context already in scope at the construction site.
//!   Construction sites do not perform a lookup to obtain
//!   attribution. If the value is not locally available, `None` is
//!   correct; lookups are not a permitted workaround.
//!
//! - **FA-2 (`display_name` is presentation-only).**
//!   `ActorSupervisionEvent.display_name` is a rendered-presentation
//!   string carried for display. Downstream code MUST NOT parse it to
//!   recover structured attribution. If a consumer needs a
//!   programmatic attribution field, that field travels on its own
//!   structured carrier — never on `display_name`.
//!
//! - **FA-3 (rendering preference order, with stable-id fallback).**
//!   `ActorSupervisionEvent::Display` and the helpers it uses
//!   (notably `actor_name()`) render, at a given actor mention,
//!   the first of the following that is `Some`:
//!     1. `attribution.actor_display_name`
//!     2. `attribution.actor_class`
//!     3. `display_name`
//!     4. `actor_id.to_string()` (stable-id fallback)
//!
//!   A given actor mention renders exactly one of those tiers — the
//!   friendly name and the stable id are not shown together at that
//!   mention. Ensuring both appear at every mention is follow-on
//!   work outside the scope of this invariant. FA-3 is independent
//!   of the specific text format used by `ActorId::Display` — it
//!   describes the render-chain preference contract, not the id
//!   format.
//!
//! - **FA-4 (no rendered-output parsing back into attribution).**
//!   Structured attribution must not be recovered at runtime by
//!   parsing formatted `display_name` strings, identifier text, or
//!   other rendered output from this path. Higher-level crates that
//!   add supervision-path attribution do so via structured carriers,
//!   not by inversion of rendered text. Consumers that sit outside
//!   this path (for example, generic telemetry that happens to read a
//!   rendered string) are outside this invariant's scope; their own
//!   contracts govern what they do.

use std::fmt;
use std::fmt::Debug;
use std::fmt::Write;
use std::time::SystemTime;

use derivative::Derivative;
use hyperactor_config::Flattrs;
use indenter::indented;
use serde::Deserialize;
use serde::Serialize;

use crate::actor::ActorErrorKind;
use crate::actor::ActorStatus;
use crate::reference;

/// Structured attribution carrier for supervision events. Distinct
/// from `display_name`, which remains presentation-only. Consumers
/// that need programmatic attribution read this directly rather than
/// parsing rendered output.
///
/// Producers MUST maintain the invariant that when
/// `ActorSupervisionEvent.attribution` is `Some` and
/// `attribution.actor_display_name` is `Some`, the legacy
/// `ActorSupervisionEvent.display_name` field is either `None` or
/// equal to `attribution.actor_display_name`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Attribution {
    /// Friendly container/context name, when available.
    pub mesh_name: Option<String>,
    /// Structured actor-class/type token, when available.
    pub actor_class: Option<String>,
    /// Free-form rendered actor display name, when available.
    pub actor_display_name: Option<String>,
    /// Per-rank rank, when available.
    pub rank: Option<usize>,
}

/// This is the local actor supervision event. Child actor will propagate this event to its parent.
#[derive(Clone, Debug, Derivative, Serialize, Deserialize, typeuri::Named)]
#[derivative(PartialEq, Eq)]
pub struct ActorSupervisionEvent {
    /// The actor id of the child actor where the event is triggered.
    pub actor_id: reference::ActorId,
    /// Friendly rendered display name, if customized by the actor.
    /// Presentation-only per FA-2. When
    /// `attribution.actor_display_name` is `Some`, this field must be
    /// `None` or equal to it.
    pub display_name: Option<String>,
    /// The time when the event is triggered.
    #[derivative(PartialEq = "ignore")]
    pub occurred_at: SystemTime,
    /// Status of the child actor.
    pub actor_status: ActorStatus,
    /// If this event is associated with a message, the message headers.
    #[derivative(PartialEq = "ignore")]
    pub message_headers: Option<Flattrs>,
    /// Structured programmatic attribution for this supervision
    /// event. Downstream code reads this carrier directly instead of
    /// parsing `display_name` or rendered output. May be `None` at
    /// construction sites that do not have structured attribution
    /// locally in scope — lookups are not used as a workaround
    /// (FA-1).
    pub attribution: Option<Attribution>,
}
wirevalue::register_type!(ActorSupervisionEvent);

impl ActorSupervisionEvent {
    /// Create a new supervision event. Timestamp is set to the
    /// current time.
    ///
    /// Preserves the FA-2 single-source-of-truth invariant: when
    /// `attribution` carries a `Some(actor_display_name)` value,
    /// the `display_name` parameter must either be `None` or equal
    /// to that value. Enforced by a debug assertion.
    pub fn new(
        actor_id: reference::ActorId,
        display_name: Option<String>,
        actor_status: ActorStatus,
        message_headers: Option<Flattrs>,
        attribution: Option<Attribution>,
    ) -> Self {
        debug_assert!(
            match (
                &display_name,
                attribution
                    .as_ref()
                    .and_then(|a| a.actor_display_name.as_ref()),
            ) {
                (Some(d), Some(a)) => d == a,
                _ => true,
            },
            "ActorSupervisionEvent.display_name and attribution.actor_display_name must not diverge"
        );
        Self {
            actor_id,
            display_name,
            occurred_at: std::time::SystemTime::now(),
            actor_status,
            message_headers,
            attribution,
        }
    }

    /// Render the human-facing name for this actor mention.
    /// Preference order (FA-3):
    /// `attribution.actor_display_name` → `attribution.actor_class`
    /// → `display_name` → stable `actor_id.to_string()` fallback.
    fn actor_name(&self) -> String {
        if let Some(a) = &self.attribution {
            if let Some(n) = &a.actor_display_name {
                return n.clone();
            }
            if let Some(c) = &a.actor_class {
                return c.clone();
            }
        }
        self.display_name
            .clone()
            .unwrap_or_else(|| self.actor_id.to_string())
    }

    /// Walk the `UnhandledSupervisionEvent` chain to the root-cause
    /// event — the first event whose status is not
    /// `UnhandledSupervisionEvent`.
    pub fn caused_by(&self) -> &ActorSupervisionEvent {
        let mut event = self;
        while let ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(inner)) =
            &event.actor_status
        {
            event = inner;
        }
        event
    }

    /// Walk the `UnhandledSupervisionEvent` chain to find the root-cause
    /// actor that originally failed.
    ///
    /// Returns `None` if the event is not a failure. Always returns the
    /// leaf of the chain — the actor whose status is the root cause,
    /// even if that leaf is a non-failure (e.g. a stopped process).
    pub fn actually_failing_actor(&self) -> Option<&ActorSupervisionEvent> {
        if !self.is_error() {
            return None;
        }
        Some(self.caused_by())
    }

    /// This event is for a supervision error.
    pub fn is_error(&self) -> bool {
        self.actor_status.is_failed()
    }

    /// Produce a concise failure report. Returns `None` for non-failure
    /// events.
    pub fn failure_report(&self) -> Option<String> {
        if !self.is_error() {
            return None;
        }
        let mut output = String::new();
        self.write_failure_report(&mut output)
            .expect("writing to String cannot fail");
        Some(output)
    }

    fn write_failure_report(&self, f: &mut String) -> fmt::Result {
        let mut current = self;
        let mut last_unhandled: Option<&ActorSupervisionEvent> = None;
        while let ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(inner)) =
            &current.actor_status
        {
            last_unhandled = Some(current);
            current = inner;
        }

        if !current.actor_status.is_failed() {
            let parent = last_unhandled.expect(
                "top-level event is a failure but leaf is not; \
                 chain must contain an UnhandledSupervisionEvent",
            );
            writeln!(
                f,
                "The actor {} failed because it did not handle a supervision event \
                 from its child. The event was:",
                parent.actor_name()
            )?;
            return write!(indented(f).with_str("  "), "{}", current);
        }

        writeln!(
            f,
            "The actor {} and all its descendants have failed:",
            current.actor_name()
        )?;
        match &current.actor_status {
            ActorStatus::Failed(ActorErrorKind::ErrorDuringHandlingSupervision(msg, child)) => {
                writeln!(indented(f).with_str("  "), "{}", msg.trim_end())?;
                writeln!(f, "This error occurred while handling another failure:")?;
                let child_report = child
                    .failure_report()
                    .expect("child of ErrorDuringHandlingSupervision is always a failure");
                write!(indented(f).with_str("  "), "{}", child_report)
            }
            ActorStatus::Failed(err) => write!(indented(f).with_str("  "), "{}", err),
            _ => unreachable!("current.is_failed() was true"),
        }
    }
}

impl std::error::Error for ActorSupervisionEvent {}

impl fmt::Display for ActorSupervisionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self.actor_name();
        match &self.actor_status {
            ActorStatus::Failed(
                err @ (ActorErrorKind::Generic(_) | ActorErrorKind::Aborted(_)),
            ) => {
                writeln!(f, "Supervision event: actor {} failed:", name)?;
                write!(indented(f).with_str("  "), "{}", err)
            }
            ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(child)) => {
                writeln!(
                    f,
                    "Supervision event: actor {} failed because it did not handle \
                     a supervision event from its child. The child's event was:",
                    name
                )?;
                write!(indented(f).with_str("  "), "{}", child)
            }
            ActorStatus::Failed(ActorErrorKind::ErrorDuringHandlingSupervision(msg, child)) => {
                writeln!(f, "Supervision event: actor {} failed:", name)?;
                writeln!(indented(f).with_str("  "), "{}", msg.trim_end())?;
                writeln!(
                    f,
                    "This error occurred while handling a supervision event from \
                     its child. The event was:"
                )?;
                write!(indented(f).with_str("  "), "{}", child)
            }
            ActorStatus::Stopped(_)
                if self.actor_id.name() == "host_agent" || self.actor_id.name() == "proc_agent" =>
            {
                let addr = self.actor_id.proc_id().addr().to_string();
                write!(
                    f,
                    "Supervision event: the process {} owned by actor {} became unresponsive \
                     and is assumed dead, check the log on the host for details",
                    addr,
                    self.actor_name()
                )
            }
            status => {
                writeln!(f, "Supervision event: actor {} has status:", name)?;
                write!(indented(f).with_str("  "), "{}", status)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorErrorKind;
    use crate::actor::ActorStatus;
    use crate::channel::ChannelAddr;

    fn test_event(name: &str, status: ActorStatus) -> ActorSupervisionEvent {
        let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "test_proc");
        ActorSupervisionEvent::new(
            proc_id.actor_id(name, 0),
            Some(name.to_string()),
            status,
            None,
            None,
        )
    }

    fn test_event_with_addr(
        name: &str,
        addr: ChannelAddr,
        status: ActorStatus,
    ) -> ActorSupervisionEvent {
        let proc_id = reference::ProcId::with_name(addr, "test_proc");
        ActorSupervisionEvent::new(proc_id.actor_id(name, 0), None, status, None, None)
    }

    fn generic(name: &str, msg: &str) -> ActorSupervisionEvent {
        test_event(
            name,
            ActorStatus::Failed(ActorErrorKind::Generic(msg.to_string())),
        )
    }

    fn aborted(name: &str, msg: &str) -> ActorSupervisionEvent {
        test_event(
            name,
            ActorStatus::Failed(ActorErrorKind::Aborted(msg.to_string())),
        )
    }

    fn unhandled(name: &str, child: ActorSupervisionEvent) -> ActorSupervisionEvent {
        test_event(
            name,
            ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(Box::new(child))),
        )
    }

    fn error_during(name: &str, msg: &str, child: ActorSupervisionEvent) -> ActorSupervisionEvent {
        test_event(
            name,
            ActorStatus::Failed(ActorErrorKind::ErrorDuringHandlingSupervision(
                msg.to_string(),
                Box::new(child),
            )),
        )
    }

    fn stopped(name: &str, reason: &str) -> ActorSupervisionEvent {
        test_event(name, ActorStatus::Stopped(reason.to_string()))
    }

    // Display tests

    #[test]
    fn test_display_generic() {
        let e = generic("actor_a", "something went wrong");
        assert_eq!(
            format!("{}", e),
            "Supervision event: actor actor_a failed:\n\
             \x20 something went wrong"
        );
    }

    #[test]
    fn test_display_aborted() {
        let e = aborted("actor_a", "user requested");
        assert_eq!(
            format!("{}", e),
            "Supervision event: actor actor_a failed:\n\
             \x20 actor explicitly aborted due to: user requested"
        );
    }

    #[test]
    fn test_display_unhandled_with_generic_child() {
        let child = generic("child", "child error");
        let parent = unhandled("parent", child);
        assert_eq!(
            format!("{}", parent),
            "Supervision event: actor parent failed because it did not handle \
             a supervision event from its child. The child's event was:\n\
             \x20 Supervision event: actor child failed:\n\
             \x20   child error"
        );
    }

    #[test]
    fn test_display_error_during_handling() {
        let child = generic("child", "child error");
        let parent = error_during("parent", "handler crashed", child);
        assert_eq!(
            format!("{}", parent),
            "Supervision event: actor parent failed:\n\
             \x20 handler crashed\n\
             This error occurred while handling a supervision event from \
             its child. The event was:\n\
             \x20 Supervision event: actor child failed:\n\
             \x20   child error"
        );
    }

    #[test]
    fn test_display_stopped() {
        let e = stopped("actor_a", "done");
        assert_eq!(
            format!("{}", e),
            "Supervision event: actor actor_a has status:\n\
             \x20 stopped: done"
        );
    }

    #[test]
    fn test_display_deep_nesting() {
        let leaf = generic("leaf", "root cause");
        let mid = unhandled("mid", leaf);
        let top = unhandled("top", mid);
        let output = format!("{}", top);
        assert!(output.contains("actor top failed because"));
        assert!(output.contains("  Supervision event: actor mid failed because"));
        assert!(output.contains("    Supervision event: actor leaf failed:"));
        assert!(output.contains("      root cause"));
    }

    #[test]
    fn test_display_unhandled_stopped_child() {
        let child = stopped("child", "process exited");
        let parent = unhandled("parent", child);
        assert_eq!(
            format!("{}", parent),
            "Supervision event: actor parent failed because it did not handle \
             a supervision event from its child. The child's event was:\n\
             \x20 Supervision event: actor child has status:\n\
             \x20   stopped: process exited"
        );
    }

    // failure_report tests

    #[test]
    fn test_failure_report_generic() {
        let e = generic("actor_a", "boom");
        assert_eq!(
            e.failure_report().unwrap(),
            "The actor actor_a and all its descendants have failed:\n\
             \x20 boom"
        );
    }

    #[test]
    fn test_failure_report_aborted() {
        let e = aborted("actor_a", "user requested");
        assert_eq!(
            e.failure_report().unwrap(),
            "The actor actor_a and all its descendants have failed:\n\
             \x20 actor explicitly aborted due to: user requested"
        );
    }

    #[test]
    fn test_failure_report_unhandled_chain_to_generic() {
        let leaf = generic("leaf", "root cause");
        let mid = unhandled("mid", leaf);
        let top = unhandled("top", mid);
        assert_eq!(
            top.failure_report().unwrap(),
            "The actor leaf and all its descendants have failed:\n\
             \x20 root cause"
        );
    }

    #[test]
    fn test_failure_report_unhandled_chain_to_stopped() {
        let leaf = stopped("some_actor", "process exited");
        let mid = unhandled("mid", leaf);
        let top = unhandled("top", mid);
        let report = top.failure_report().unwrap();
        assert_eq!(
            report,
            "The actor mid failed because it did not handle a supervision event \
             from its child. The event was:\n\
             \x20 Supervision event: actor some_actor has status:\n\
             \x20   stopped: process exited"
        );
    }

    #[test]
    fn test_failure_report_unhandled_chain_to_stopped_proc_agent() {
        let leaf = test_event_with_addr(
            "proc_agent",
            ChannelAddr::Local(99),
            ActorStatus::Stopped("process exited".to_string()),
        );
        let mid = unhandled("mid", leaf);
        let top = unhandled("top", mid);
        let report = top.failure_report().unwrap();
        assert!(
            report.contains("did not handle a supervision event"),
            "got: {}",
            report
        );
        assert!(
            report.contains("process local:99 owned by actor") && report.contains("unresponsive"),
            "got: {}",
            report
        );
    }

    #[test]
    fn test_failure_report_error_during_handling() {
        let child = generic("child", "original error");
        let parent = error_during("parent", "handler failed", child);
        assert_eq!(
            parent.failure_report().unwrap(),
            "The actor parent and all its descendants have failed:\n\
             \x20 handler failed\n\
             This error occurred while handling another failure:\n\
             \x20 The actor child and all its descendants have failed:\n\
             \x20   original error"
        );
    }

    #[test]
    fn test_failure_report_error_during_handling_nested() {
        let leaf = generic("leaf", "root cause");
        let mid = error_during("mid", "mid failed", leaf);
        let top = error_during("top", "top failed", mid);
        let report = top.failure_report().unwrap();
        assert!(report.starts_with(
            "The actor top and all its descendants have failed:\n\
             \x20 top failed\n\
             This error occurred while handling another failure:\n\
             \x20 The actor mid and all its descendants have failed:\n\
             \x20   mid failed\n\
             \x20 This error occurred while handling another failure:\n\
             \x20   The actor leaf and all its descendants have failed:\n\
             \x20     root cause"
        ));
    }

    #[test]
    fn test_failure_report_unhandled_to_error_during_handling() {
        let leaf = generic("leaf", "root cause");
        let handler_err = error_during("handler", "while handling", leaf);
        let top = unhandled("top", handler_err);
        let report = top.failure_report().unwrap();
        assert!(report.contains("The actor handler and all its descendants have failed:"));
        assert!(report.contains("while handling"));
        assert!(report.contains("root cause"));
    }

    #[test]
    fn test_failure_report_none_on_non_failure() {
        let e = stopped("actor_a", "done");
        assert!(e.failure_report().is_none());
    }

    #[test]
    fn test_failure_report_direct_generic_no_chain() {
        let e = generic("solo", "direct error");
        assert_eq!(
            e.failure_report().unwrap(),
            "The actor solo and all its descendants have failed:\n\
             \x20 direct error"
        );
    }

    #[test]
    fn test_display_host_agent_stopped() {
        let e = test_event_with_addr(
            "host_agent",
            ChannelAddr::Local(42),
            ActorStatus::Stopped("gone".to_string()),
        );
        let output = format!("{}", e);
        assert!(
            output.contains("process local:42 owned by actor") && output.contains("unresponsive"),
            "got: {}",
            output
        );
    }

    #[test]
    fn test_display_proc_agent_stopped() {
        let e = test_event_with_addr(
            "proc_agent",
            ChannelAddr::Local(7),
            ActorStatus::Stopped("dead".to_string()),
        );
        let output = format!("{}", e);
        assert!(
            output.contains("process local:7 owned by actor") && output.contains("unresponsive"),
            "got: {}",
            output
        );
    }

    #[test]
    fn test_display_error_during_handling_trim_end() {
        let child = generic("child", "child error");
        let parent = error_during("parent", "msg with trailing newline\n", child);
        let output = format!("{}", parent);
        assert!(
            output.contains("  msg with trailing newline\nThis error occurred"),
            "writeln! should trim trailing newline from msg: {}",
            output
        );
    }

    #[test]
    fn test_failure_report_error_during_handling_trim_end() {
        let child = generic("child", "child error");
        let parent = error_during("parent", "msg with trailing newline\n", child);
        let report = parent.failure_report().unwrap();
        assert!(
            report.contains("  msg with trailing newline\nThis error occurred"),
            "writeln! should trim trailing newline from msg: {}",
            report
        );
    }

    /// Exercises SV-1 (see module doc): for a parent wrapping a
    /// stopped child in `UnhandledSupervisionEvent`,
    /// `actually_failing_actor()` returns the stopped child as
    /// root cause for structured failure attribution.
    #[test]
    fn test_sv1_actually_failing_actor_returns_stopped_child() {
        let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "test_proc");
        let child_id = proc_id.actor_id("proc_agent", 0);
        let parent_id = proc_id.actor_id("controller", 0);

        let child_event = ActorSupervisionEvent::new(
            child_id.clone(),
            Some("proc_agent".into()),
            ActorStatus::Stopped("host died".into()),
            None,
            None,
        );
        let parent_event = ActorSupervisionEvent::new(
            parent_id,
            Some("controller".into()),
            ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(Box::new(
                child_event,
            ))),
            None,
            None,
        );

        // SV-1: root cause is the stopped child, not the parent.
        let root = parent_event
            .actually_failing_actor()
            .expect("parent_event is a failure");
        assert_eq!(root.actor_id, child_id);
        assert!(
            matches!(root.actor_status, ActorStatus::Stopped(_)),
            "root cause should be the stopped child, got: {:?}",
            root.actor_status,
        );
    }

    /// FA-3: `ActorSupervisionEvent::actor_name()` renders with
    /// preference order
    /// `attribution.actor_display_name` →
    /// `attribution.actor_class` →
    /// `display_name` →
    /// `actor_id.to_string()`.
    ///
    /// This locks the rendering contract so any future regression
    /// (e.g. swapping the order or skipping a tier) surfaces as a
    /// unit-test failure.
    #[test]
    fn test_fa3_actor_name_preference_order() {
        let proc_id = reference::ProcId::with_name(ChannelAddr::Local(0), "p");
        let actor_id = proc_id.actor_id("a", 0);

        // Build events that differ only in which tiers are populated.
        let mk = |display_name: Option<String>, attribution: Option<Attribution>| {
            ActorSupervisionEvent::new(
                actor_id.clone(),
                display_name,
                ActorStatus::Failed(ActorErrorKind::Generic("boom".into())),
                None,
                attribution,
            )
        };
        let raw_id = actor_id.to_string();

        // Tier 4 — all carriers absent: fall back to stable id.
        assert_eq!(mk(None, None).actor_name(), raw_id);

        // Tier 3 — display_name present, attribution absent.
        assert_eq!(mk(Some("dn".into()), None).actor_name(), "dn",);

        // Tier 2 — attribution.actor_class present, no display_name.
        let attr_class_only = Attribution {
            mesh_name: None,
            actor_class: Some("CLS".into()),
            actor_display_name: None,
            rank: None,
        };
        assert_eq!(mk(None, Some(attr_class_only)).actor_name(), "CLS",);

        // Tier 1 — attribution.actor_display_name wins over
        // everything below it. FA-2 invariant requires
        // display_name to be None or equal; use equal here.
        let attr_display = Attribution {
            mesh_name: Some("m".into()),
            actor_class: Some("CLS".into()),
            actor_display_name: Some("DN".into()),
            rank: Some(7),
        };
        assert_eq!(
            mk(Some("DN".into()), Some(attr_display.clone())).actor_name(),
            "DN",
        );

        // Tier 1 also wins when display_name is None.
        assert_eq!(mk(None, Some(attr_display)).actor_name(), "DN",);

        // Tier 2 beats Tier 3: attribution.actor_class wins even
        // when display_name is also set, because attribution is
        // authoritative when present.
        let attr_class_with_dn = Attribution {
            mesh_name: None,
            actor_class: Some("CLS".into()),
            actor_display_name: None,
            rank: None,
        };
        assert_eq!(
            mk(Some("dn".into()), Some(attr_class_with_dn)).actor_name(),
            "CLS",
        );
    }
}
