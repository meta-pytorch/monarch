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
//! ## Supervision rendering invariants (SR-*)
//!
//! - **SR-1 (`display_name` is presentation-only).**
//!   `ActorSupervisionEvent.display_name` is rendered display text.
//!   Downstream code must not parse it to recover structured data.
//!
//! - **SR-2 (rendering falls back to stable ids).**
//!   `ActorSupervisionEvent::Display` and its helpers render
//!   `display_name` when present and otherwise fall back to
//!   `actor_id.to_string()`. A given actor mention renders one or
//!   the other, not both.
//!
//! - **SR-3 (no rendered-output parsing back into structure).**
//!   Structured data must not be reconstructed by parsing formatted
//!   `display_name`, identifier text, or other rendered output from
//!   this path.

use std::fmt;
use std::fmt::Debug;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use derivative::Derivative;
use hyperactor_config::Flattrs;
use indenter::indented;
use serde::Deserialize;
use serde::Serialize;

use crate::ActorAddr;
use crate::actor::ActorErrorKind;
use crate::actor::ActorStatus;

/// This is the local actor supervision event. Child actor will propagate this event to its parent.
#[derive(Clone, Debug, Derivative, Serialize, Deserialize, typeuri::Named)]
#[derivative(PartialEq, Eq)]
pub struct ActorSupervisionEvent {
    /// The actor id of the child actor where the event is triggered.
    pub actor_id: ActorAddr,
    /// Friendly display name, if the actor class customized it.
    pub display_name: Option<String>,
    /// The time when the event is triggered.
    #[derivative(PartialEq = "ignore")]
    pub occurred_at: SystemTime,
    /// Status of the child actor.
    pub actor_status: ActorStatus,
    /// If this event is associated with a message, the message headers.
    #[derivative(PartialEq = "ignore")]
    pub message_headers: Option<Flattrs>,
    /// Semantic labels that describe the actor, such as its point in a mesh.
    /// Each frame carries its actor's labels.
    #[serde(default)]
    #[derivative(PartialEq = "ignore")]
    pub labels: Flattrs,
    /// Whether this frame is a causal wrapper in the Python view.
    #[serde(default)]
    #[derivative(PartialEq = "ignore")]
    pub transparent: bool,
    /// The child event that caused a normal stop. Failure causes are retained
    /// in `actor_status`; [`Self::cause`] traverses both forms.
    #[serde(default)]
    pub(crate) stop_cause: Option<Box<ActorSupervisionEvent>>,
    /// Information about another actor affected by this event. That actor need
    /// not have failed or even have been created.
    #[serde(default)]
    pub observation: Option<SupervisionObservation>,
    #[serde(skip, default = "local_fence")]
    #[derivative(PartialEq = "ignore")]
    pub(crate) local_fence: Arc<AtomicBool>,
}
wirevalue::register_type!(ActorSupervisionEvent);

/// An actor affected by a supervision failure or a failed request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisionTarget {
    /// The affected actor's address.
    pub actor_id: ActorAddr,
    /// The affected actor's display name, when known.
    pub display_name: Option<String>,
}

/// An observation that accompanies the actual actor's terminal event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupervisionObservation {
    /// The supervision link to this actor was lost. The actor may still run.
    LinkLost(SupervisionTarget),
    /// The supervision link to this actor's process was lost.
    ProcLinkLost(SupervisionTarget),
    /// A request concerning this actor failed. The actor may not exist yet.
    RequestFailed(SupervisionTarget),
}

pub(crate) fn local_fence() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

impl ActorSupervisionEvent {
    /// Create a new supervision event. Timestamp is set to the current time.
    pub fn new(
        actor_id: impl Into<ActorAddr>,
        display_name: Option<String>,
        actor_status: ActorStatus,
        message_headers: Option<Flattrs>,
    ) -> Self {
        Self {
            actor_id: actor_id.into(),
            display_name,
            occurred_at: std::time::SystemTime::now(),
            actor_status,
            message_headers,
            labels: Flattrs::new(),
            transparent: false,
            stop_cause: None,
            observation: None,
            local_fence: local_fence(),
        }
    }

    /// The event for an actor that fails because it did not handle `cause`.
    pub fn unhandled(
        actor_id: impl Into<ActorAddr>,
        display_name: Option<String>,
        cause: ActorSupervisionEvent,
    ) -> Self {
        Self::new(
            actor_id,
            display_name,
            ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(Box::new(cause))),
            None,
        )
    }

    /// The event for an actor that fails with `message` while handling `cause`.
    pub fn error_during_handling(
        actor_id: impl Into<ActorAddr>,
        display_name: Option<String>,
        message: String,
        cause: ActorSupervisionEvent,
    ) -> Self {
        Self::new(
            actor_id,
            display_name,
            ActorStatus::Failed(ActorErrorKind::ErrorDuringHandlingSupervision(
                message,
                Box::new(cause),
            )),
            None,
        )
    }

    /// Replace the event's labels.
    pub fn with_labels(mut self, labels: Flattrs) -> Self {
        self.labels = labels;
        self
    }

    /// Set whether this frame is a causal wrapper in the Python view.
    pub fn with_transparent(mut self, transparent: bool) -> Self {
        self.transparent = transparent;
        self
    }

    /// Set the child event that caused this actor to stop normally.
    pub fn with_stop_cause(mut self, cause: Option<Box<Self>>) -> Self {
        assert!(self.actor_status.is_stopped());
        self.stop_cause = cause;
        self
    }

    /// Set information about another actor affected by this event.
    pub fn with_observation(mut self, observation: Option<SupervisionObservation>) -> Self {
        self.observation = observation;
        self
    }

    /// Take the event that caused this actor to stop normally.
    pub fn take_stop_cause(&mut self) -> Option<Box<Self>> {
        self.stop_cause.take()
    }

    /// The direct cause of this event, including causes of normal stops.
    pub fn cause(&self) -> Option<&Self> {
        match &self.actor_status {
            ActorStatus::Failed(
                ActorErrorKind::UnhandledSupervisionEvent(cause)
                | ActorErrorKind::ErrorDuringHandlingSupervision(_, cause),
            ) => Some(cause),
            ActorStatus::Stopped(_) => self.stop_cause.as_deref(),
            _ => None,
        }
    }

    /// Iterate over this event and all its physical causes.
    pub fn causal_chain(&self) -> impl Iterator<Item = &Self> {
        std::iter::successors(Some(self), |event| event.cause())
    }

    pub(crate) fn with_local_fence(mut self, local_fence: Arc<AtomicBool>) -> Self {
        self.local_fence = local_fence;
        self
    }

    pub(crate) fn is_locally_cancelled(&self) -> bool {
        self.local_fence.load(Ordering::Acquire)
    }

    fn actor_name(&self) -> String {
        self.display_name.clone().unwrap_or_else(|| {
            if self.actor_id.is_root() {
                format!("{},{}", self.actor_id.proc_addr(), self.actor_id.log_name())
            } else {
                self.actor_id.to_string()
            }
        })
    }

    /// Find the root cause through unhandled failures and caused normal stops.
    /// An error raised while handling supervision remains its own root cause.
    pub fn caused_by(&self) -> &Self {
        let mut event = self;
        loop {
            match &event.actor_status {
                ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(cause)) => {
                    event = cause;
                }
                ActorStatus::Stopped(_) if event.stop_cause.is_some() => {
                    event = event.stop_cause.as_deref().unwrap();
                }
                _ => return event,
            }
        }
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
                let (description, child_report) = match child.failure_report() {
                    Some(report) => ("another failure", report),
                    None => ("a supervision event", child.to_string()),
                };
                writeln!(f, "This error occurred while handling {}:", description)?;
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
            ActorStatus::Stopped(reason) if self.stop_cause.is_some() => {
                writeln!(f, "Supervision event: actor {} stopped: {}", name, reason)?;
                writeln!(f, "This actor stopped because of its child's event:")?;
                write!(
                    indented(f).with_str("  "),
                    "{}",
                    self.stop_cause.as_deref().unwrap()
                )
            }
            ActorStatus::Stopped(_)
                if self
                    .actor_id
                    .label()
                    .is_some_and(|l| l.as_str() == "host_agent" || l.as_str() == "proc_agent") =>
            {
                let addr = self.actor_id.proc_addr().addr().to_string();
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
    use crate::ProcAddr;
    use crate::actor::ActorErrorKind;
    use crate::actor::ActorStatus;
    use crate::channel::ChannelAddr;

    fn test_event(name: &str, status: ActorStatus) -> ActorSupervisionEvent {
        let proc_id = ProcAddr::singleton(ChannelAddr::Local(0), "test_proc");
        ActorSupervisionEvent::new(
            proc_id.actor_addr(name),
            Some(name.to_string()),
            status,
            None,
        )
    }

    fn test_event_with_addr(
        name: &str,
        addr: ChannelAddr,
        status: ActorStatus,
    ) -> ActorSupervisionEvent {
        let proc_id = ProcAddr::singleton(addr, "test_proc");
        ActorSupervisionEvent::new(proc_id.actor_addr(name), None, status, None)
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

    #[test]
    fn test_supervision_structure_survives_serialization() {
        let child = stopped("remote_child", "done");
        let stopped_chain = stopped("supervisor", "worker stopped")
            .with_transparent(true)
            .with_stop_cause(Some(Box::new(
                stopped("worker", "child stopped")
                    .with_transparent(true)
                    .with_stop_cause(Some(Box::new(child.clone()))),
            )));
        let lost_link = unhandled("supervisor", generic("keepalive", "link failed"))
            .with_transparent(true)
            .with_observation(Some(SupervisionObservation::LinkLost(SupervisionTarget {
                actor_id: child.actor_id.clone(),
                display_name: child.display_name.clone(),
            })));
        for event in [stopped_chain, lost_link] {
            let bytes = bincode::serde::encode_to_vec(&event, bincode::config::legacy()).unwrap();
            let (restored, _): (ActorSupervisionEvent, _) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::legacy()).unwrap();
            assert_eq!(restored, event);
            assert!(restored.transparent);
            assert_eq!(
                restored.causal_chain().count(),
                event.causal_chain().count()
            );
            assert_eq!(restored.to_string(), event.to_string());
        }
    }

    #[test]
    fn test_causal_chain_includes_handler_errors_and_stops() {
        let child = stopped("child", "done");
        let worker = stopped("worker", "child stopped").with_stop_cause(Some(Box::new(child)));
        let handler = error_during("handler", "handler failed", worker);
        let event = unhandled("parent", handler);
        let names: Vec<_> = event
            .causal_chain()
            .map(|event| event.display_name.as_deref().unwrap())
            .collect();
        assert_eq!(names, ["parent", "handler", "worker", "child"]);
        assert_eq!(event.caused_by().display_name.as_deref(), Some("handler"));
        let report = event.failure_report().unwrap();
        assert!(report.contains("handler failed"));
        assert!(report.contains("actor worker stopped: child stopped"));
        assert!(report.contains("actor child has status:"));
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
        let proc_id = ProcAddr::singleton(ChannelAddr::Local(0), "test_proc");
        let child_id = proc_id.actor_addr("proc_agent");
        let parent_id = proc_id.actor_addr("controller");

        let child_event = ActorSupervisionEvent::new(
            child_id.clone(),
            Some("proc_agent".into()),
            ActorStatus::Stopped("host died".into()),
            None,
        );
        let parent_event = ActorSupervisionEvent::new(
            parent_id,
            Some("controller".into()),
            ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(Box::new(
                child_event,
            ))),
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
}
