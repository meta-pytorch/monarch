/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Log subject for structured logging.
//!
//! Each log message in hyperactor can have a **subject**: the entity the
//! message pertains to (an actor, a proc, a channel, etc.). The subject
//! is set as a tracing span field and rendered prominently in log output.
//!
//! ```text
//! <actor metatls:host:1234,local,my_actor[0]> undeliverable message, ...
//! ```

use std::fmt;

/// Identifies the entity a log message pertains to.
///
/// Used as a tracing span field via `%actor_id.subject()`.
/// The GlogSink recognizes the well-known `subject` field and renders
/// it as a prefix; other subscribers see it as a regular string field.
pub struct Subject<'a> {
    kind: &'static str,
    id: &'a dyn fmt::Display,
}

impl<'a> Subject<'a> {
    /// An actor subject.
    pub fn actor(id: &'a (impl fmt::Display + 'a)) -> Self {
        Self { kind: "actor", id }
    }

    /// A proc subject.
    pub fn proc(id: &'a (impl fmt::Display + 'a)) -> Self {
        Self { kind: "proc", id }
    }
}

impl fmt::Display for Subject<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{} {}>", self.kind, self.id)
    }
}

/// Extension trait for types that can be used as a log subject.
pub trait AsSubject: fmt::Display {
    /// Return a [`Subject`] for this ID.
    fn subject(&self) -> Subject<'_>;
}

impl AsSubject for crate::reference::ActorId {
    fn subject(&self) -> Subject<'_> {
        Subject::actor(self)
    }
}

impl AsSubject for crate::reference::ProcId {
    fn subject(&self) -> Subject<'_> {
        Subject::proc(self)
    }
}
