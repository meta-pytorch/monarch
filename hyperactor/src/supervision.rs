/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Messages used in supervision.

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

    /// Walk the `UnhandledSupervisionEvent` chain to find the root-cause
    /// actor that originally failed.
    ///
    /// Returns `None` if the event is not a failure.
    pub fn actually_failing_actor(&self) -> Option<&ActorSupervisionEvent> {
        if !self.is_error() {
            return None;
        }

        let mut event = self;
        while let ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(e)) =
            &event.actor_status
        {
            event = e;
        }

        Some(event)
    }

    /// This event is for a a supervision error.
    pub fn is_error(&self) -> bool {
        self.actor_status.is_failed()
    }
}

impl std::error::Error for ActorSupervisionEvent {}

fn fmt_status<'a>(
    actor_id: &reference::ActorId,
    status: &'a ActorStatus,
    f: &mut fmt::Formatter<'_>,
) -> Result<Option<&'a ActorSupervisionEvent>, fmt::Error> {
    let mut f = indented(f).with_str(" ");

    match status {
        ActorStatus::Stopped(_)
            if actor_id.name() == "host_agent" || actor_id.name() == "proc_agent" =>
        {
            // Host agent stopped - use simplified message from D86984496
            let name = actor_id.proc_id().addr().to_string();
            writeln!(
                f,
                "The process {} owned by this actor became unresponsive and is assumed dead, check the log on the host for details",
                name
            )?;
            Ok(None)
        }
        ActorStatus::Failed(ActorErrorKind::ErrorDuringHandlingSupervision(
            msg,
            during_handling_of,
        )) => {
            writeln!(f, "{}", msg.trim_end())?;
            Ok(Some(during_handling_of))
        }
        ActorStatus::Failed(ActorErrorKind::Generic(msg)) => {
            writeln!(f, "{}", msg.trim_end())?;
            Ok(None)
        }
        status => {
            writeln!(f, "{}", status)?;
            Ok(None)
        }
    }
}

impl fmt::Display for ActorSupervisionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let actor_name = self.actor_name();
        if let Some(next_event) = self.actually_failing_actor() {
            let next_name = next_event.actor_name();
            if next_name == actor_name {
                // The reporting actor is also the one that failed.
                writeln!(
                    f,
                    "The actor {} and all its descendants have failed. It errored with:",
                    actor_name
                )?;
            } else {
                // A descendant caused the failure.
                writeln!(
                    f,
                    "The actor {} and all its descendants failed because one of its descendants, {}, failed with the error:",
                    actor_name, next_name
                )?;
            }
            let during_handling_of = fmt_status(&next_event.actor_id, &next_event.actor_status, f)?;
            if let Some(event) = during_handling_of {
                writeln!(
                    f,
                    "This error occurred during the handling of another failure:"
                )?;
                write!(indented(f).with_str("  "), "{}", event)?;
            }
        } else {
            writeln!(
                f,
                "Non-failure supervision event from actor {}.",
                actor_name
            )?;
            fmt_status(&self.actor_id, &self.actor_status, f)?;
        }
        Ok(())
    }
}
