/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use enum_as_inner::EnumAsInner;
use hyperactor::ActorId;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::RefClient;
use hyperactor::data::Serialized;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::controller::DeviceFailure;
use crate::controller::Seq;
use crate::controller::WorkerError;
use crate::debugger::DebuggerAction;

/// An exception to commanded execution, exposed to the client.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    thiserror::Error,
    EnumAsInner
)]
pub enum Exception {
    /// A failure is a non-deterministic problem with the underlying device
    /// or its infrastructure. For example, a controller may enter a crash loop,
    /// or its GPU may be lost
    #[error("failure: {0}")]
    Failure(#[from] DeviceFailure),

    /// A deterministic problem with the user's code. For example, an OOM
    /// resulting in trying to allocate too much GPU memory, or violating
    /// some invariant enforced by the various APIs.
    #[error("WorkerError: seq: {0}, error: {1}")]
    Error(Seq, Seq, WorkerError),
}

/// Log levels for ClientMessage::Log.
#[derive(Debug, Deserialize, Clone, Serialize)]
pub enum LogLevel {
    /// Log with severity INFO
    Info,

    /// Log with severity WARNING
    Warn,

    /// Log with severity ERROR
    Error,
}

/// Client messages. These define the messages that the controller can
/// send to the client. The actual handling of these messages will be
/// defined on the python side in the client implementation.
// TODO: Potentially just create a Client derive macro that can be used
// to just generate the client code without generating the handler for
// cases where the actor will be implemented in a different language over
// ffi.
#[derive(
    Handler,
    HandleClient,
    RefClient,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    EnumAsInner,
    Named
)]
pub enum ClientMessage {
    /// A fetched result of an invoked operation.
    Result {
        seq: Seq,
        result: Option<Result<Serialized, Exception>>,
    },

    /// Notify the client of an event.
    Log { level: LogLevel, message: String },

    /// Notify the client of a debugger event.
    DebuggerMessage {
        /// The actor id of the debugger.
        debugger_actor_id: ActorId,
        /// The action to take.
        action: DebuggerAction,
    },
}

hyperactor::behavior!(ClientActor, ClientMessage);
