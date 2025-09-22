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
use std::time::SystemTime;

use chrono::DateTime;
use chrono::offset::Local;
use derivative::Derivative;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use serde::Deserialize;
use serde::Serialize;

use crate as hyperactor; // for macros
use crate::Named;
use crate::actor::ActorStatus;
use crate::attrs::Attrs;
use crate::reference::ActorId;

/// This is the local actor supervision event. Child actor will propagate this event to its parent.
#[derive(Clone, Debug, Derivative, Serialize, Deserialize, Named)]
#[derivative(PartialEq, Eq)]
pub struct ActorSupervisionEvent {
    /// The actor id of the child actor where the event is triggered.
    pub actor_id: ActorId,
    /// The time when the event is triggered.
    #[derivative(PartialEq = "ignore")]
    pub occurred_at: SystemTime,
    /// Status of the child actor.
    pub actor_status: ActorStatus,
    /// If this event is associated with a message, the message headers.
    #[derivative(PartialEq = "ignore")]
    pub message_headers: Option<Attrs>,
    /// Optional supervision event that caused this event, for recursive propagation.
    pub caused_by: Option<Box<ActorSupervisionEvent>>,
}

impl ActorSupervisionEvent {
    /// Create a new supervision event. Timestamp is set to the current time.
    pub fn new(
        actor_id: ActorId,
        actor_status: ActorStatus,
        message_headers: Option<Attrs>,
        caused_by: Option<Box<ActorSupervisionEvent>>,
    ) -> Self {
        Self {
            actor_id,
            occurred_at: RealClock.system_time_now(),
            actor_status,
            message_headers,
            caused_by,
        }
    }
    /// Compute an actor status from this event, ensuring that "caused-by"
    /// events are included in failure states. This should be used as the
    /// actor status when reporting events to users.
    pub fn status(&self) -> ActorStatus {
        match &self.actor_status {
            ActorStatus::Failed(msg) => ActorStatus::Failed(format!("{}: {}", self, msg)),
            status => status.clone(),
        }
    }
}

impl std::error::Error for ActorSupervisionEvent {}

impl fmt::Display for ActorSupervisionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} at {}",
            self.actor_id,
            self.actor_status,
            DateTime::<Local>::from(self.occurred_at),
        )?;
        if let Some(message_headers) = &self.message_headers {
            let headers = serde_json::to_string(&message_headers)
                .expect("could not serialize message headers");
            write!(f, " (headers: {})", headers)?;
        }
        if let Some(caused_by) = &self.caused_by {
            write!(f, ": (caused by: {})", caused_by)?;
        }
        Ok(())
    }
}
