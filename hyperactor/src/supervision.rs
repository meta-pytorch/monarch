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

/// This is the local actor supervision event. Child actor will propagate this event to its parent.
#[derive(Clone, Debug, Derivative, Serialize, Deserialize, typeuri::Named)]
#[derivative(PartialEq, Eq)]
pub struct ActorSupervisionEvent {
    /// The actor id of the child actor where the event is triggered.
    pub actor_id: reference::ActorId,
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
}
wirevalue::register_type!(ActorSupervisionEvent);

impl ActorSupervisionEvent {
    /// Create a new supervision event. Timestamp is set to the current time.
    pub fn new(
        actor_id: reference::ActorId,
        display_name: Option<String>,
        actor_status: ActorStatus,
        message_headers: Option<Flattrs>,
    ) -> Self {
        Self {
            actor_id,
            display_name,
            occurred_at: std::time::SystemTime::now(),
            actor_status,
            message_headers,
        }
    }

    fn actor_name(&self) -> String {
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
        )
    }

    fn test_event_with_addr(
        name: &str,
        addr: ChannelAddr,
        status: ActorStatus,
    ) -> ActorSupervisionEvent {
        let proc_id = reference::ProcId::with_name(addr, "test_proc");
        ActorSupervisionEvent::new(proc_id.actor_id(name, 0), None, status, None)
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
